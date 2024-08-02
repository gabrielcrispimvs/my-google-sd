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
        f'Índice atualizado:\n'
        f'{lb.index}'
    )


conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = conn.channel()

channel.exchange_declare(
    exchange='lb_update',
    exchange_type='direct'
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


channel.basic_consume(
    queue=node_remove_queue,
    on_message_callback=remove_node
)


def add_fn (ch, method, props, body):
    node_name, file_list = pickle.loads(body)
    if node_name not in lb.node_list:
        lb.node_list += [node_name]

    for file_name in file_list:
        if not file_name in lb.index:
            lb.index[file_name] = [node_name]
        elif not node_name in lb.index[file_name]:
            lb.index[file_name] += [node_name]
    print(
        f'Índice atualizado:\n'
        f'{lb.index}'
    )
    

channel.basic_consume(
    queue=add_queue,
    on_message_callback=add_fn
)

t = Thread(target=channel.start_consuming)
t.start()
# Levantando Load Balancer
s = ThreadedServer(lb, registrar=r, auto_register=True)
s.start()


# t = Thread(target=s.start)
# t.start()
