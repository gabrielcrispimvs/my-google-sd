import rpyc
from threading import Thread
from rpyc.utils.server import ThreadedServer
import sys

try:
    registry_ip = sys.argv[1]
    registry_port = int(sys.argv[2])
except:
    print('Passe IP e porta do registry como argumentos.')
    exit()

# registry_ip = '192.168.40.240'
# registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)

class LoadBalancerService (rpyc.Service):
    
    # Solicitando nós 
    next_node = 0
    node_list = []


    def exposed_get_nodes_insert (self, qnt):
        if qnt >= len(self.node_list):
            return self.node_list

        to_return = []
        for i in range(qnt):
            if self.next_node >= len(self.node_list):
                self.next_node = 0
            to_return += [self.node_list[self.next_node]]
            self.next_node += 1
        return to_return
        
    def exposed_get_nodes_search (self):
        search_dict = {}
        for node in self.node_list:
            search_dict[node] = []
        
        for f in self.index:
            qnt_nodes = len(self.index[f])
            for idx, node in enumerate(self.index[f]):
                search_dict[node] += [ (f, idx+1, qnt_nodes) ]

        return search_dict
            

    index = {}











lb = LoadBalancerService()

import pika
import pickle



def remove_node (ch, method, props, body):
    node_name = body.decode()

    if node_name in lb.node_list:
        lb.node_list.remove(node_name)

    for file_name in lb.index:
        if node_name in lb.index[file_name]:
            lb.index[file_name].remove(node_name)

    ch.basic_ack(method.delivery_tag)

    print(
        f'Índice e lista de nós atualizados:\n'
        f'{lb.node_list}\n'
        f'{lb.index}\n'
    )


conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = conn.channel()

channel.exchange_declare(exchange='chunk_conn', exchange_type='direct')

############################

channel.exchange_declare(exchange='lb_update', exchange_type='direct')
channel.exchange_declare(exchange='lb_request', exchange_type='direct')


result = channel.queue_declare('', exclusive=True)
insert_req_queue = result.method.queue
# print(insert_req_queue)
channel.queue_bind(
    queue=insert_req_queue,
    exchange='lb_request',
    routing_key='insert'
)

result = channel.queue_declare('', exclusive=True)
node_remove_queue = result.method.queue
channel.queue_bind(
    queue=node_remove_queue,
    exchange='lb_update',
    routing_key='node_remove'
)

result = channel.queue_declare('', exclusive=True)
file_remove_queue = result.method.queue
channel.queue_bind(
    queue=file_remove_queue,
    exchange='lb_update',
    routing_key='file_remove'
)

result = channel.queue_declare('', exclusive=True)
add_queue = result.method.queue
channel.queue_bind(
    queue=add_queue,
    exchange='lb_update',
    routing_key='add'
)


### Funções


def add_file (ch, method, props, body):
    node_name, file_list = pickle.loads(body)
    if node_name not in lb.node_list:
        lb.node_list += [node_name]

    for file_name in file_list:
        if not file_name in lb.index:
            lb.index[file_name] = [node_name]
        elif not node_name in lb.index[file_name]:
            lb.index[file_name] += [node_name]
    print(
        f'Índice e lista de nós atualizados:\n'
        f'{lb.node_list}\n'
        f'{lb.index}\n'
    )
    

def connect_datanodes_to_chunk (chosen_nodes, file_name, chunk_num):
    result = channel.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue

    # print(f'Callback: {callback_queue}')
    # print('Conectando os nós aos tópicos do chunk.')

    response = {n : False for n in chosen_nodes}

    def on_response (ch, method, props, body):
        # print('resposta')
        nonlocal response
        if props.correlation_id in chosen_nodes:
            response[props.correlation_id] = True

        ch.basic_ack(method.delivery_tag)


    for node_name in chosen_nodes:
        channel.basic_publish(
            exchange='chunk_conn',
            routing_key=node_name,
            body=pickle.dumps( (file_name, chunk_num) ),
            properties=pika.BasicProperties(reply_to=callback_queue)
        )

    # print('msg enviada.')

    while False in response.values():
        queue_state = channel.queue_declare(callback_queue, passive=True)
        if queue_state.method.message_count != 0:
            method, props, body = channel.basic_get(callback_queue)
            on_response(channel, method, props, body)

    # print('ok')



round_robin_next = 0

def choose_nodes (ch, method, props, body):
    global round_robin_next

    file_name, chunk_num, qnt = pickle.loads(body)

    # print(f'Selecionando nós para inserção do chunk {chunk_num} do arquivo {file_name}')

    chosen_nodes = []

    if (file_name in lb.index) and (chunk_num in lb.index[file_name]):
        qnt -= len(lb.index[file_name][chunk_num])



    if qnt >= len(lb.node_list):
        chosen_nodes = lb.node_list
    else:
        while len(chosen_nodes) < qnt:
            if round_robin_next >= len(lb.node_list):
                round_robin_next = 0
            chosen_nodes += [lb.node_list[round_robin_next]]
            round_robin_next += 1

    connect_datanodes_to_chunk(chosen_nodes, file_name, chunk_num)

    channel.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        body=''
    )



###########


channel.basic_consume(
    queue=node_remove_queue,
    on_message_callback=remove_node
)

channel.basic_consume(
    queue=insert_req_queue,
    on_message_callback=choose_nodes,
    auto_ack=True
)

channel.basic_consume(
    queue=add_queue,
    on_message_callback=add_file
)

###########

t = Thread(target=channel.start_consuming)
t.start()
# Levantando Load Balancer
s = ThreadedServer(lb, registrar=r, auto_register=True)
s.start()


# t = Thread(target=s.start)
# t.start()
