import rpyc
from threading import Thread
from rpyc.utils.server import ThreadedServer
import sys

from string import ascii_lowercase
from string import digits
from random import choices

import pika
import pickle

# Solicitando nós 
next_node = 0
node_list = []

index = {}

file_dict = {}

def generate_corr_id (qnt=12):
    return ''.join(choices(ascii_lowercase + digits, k=qnt))

try:
    broker_ip = sys.argv[1]
except:
    broker_ip = '192.168.40.39'
    print(f'Usando endereço padrão: {broker_ip}')

try:
    broker_port = int(sys.argv[2])
except:
    broker_port = 5672
    print(f'Usando porta padrão: {broker_port}')

def remove_node (ch, method, props, body):
    node_name = body.decode()

    if node_name in node_list:
        node_list.remove(node_name)

    for file_name in index:
        if node_name in index[file_name]:
            index[file_name].remove(node_name)

    ch.basic_ack(method.delivery_tag)

    print(
        f'Índice e lista de nós atualizados:\n'
        f'{node_list}\n'
        f'{index}\n'
    )


# Iniciando conexão com RabbitMQ
conn = pika.BlockingConnection(pika.ConnectionParameters(host=broker_ip, port=broker_port))
channel = conn.channel()

channel.exchange_declare(exchange='chunk_conn', exchange_type='direct')
channel.exchange_declare(exchange='lb_update', exchange_type='direct')
channel.exchange_declare(exchange='lb_request', exchange_type='direct')


channel.queue_declare('search_req', exclusive=True)
channel.queue_bind(
    queue='search_req',
    exchange='lb_request',
    routing_key='search'
)

channel.queue_declare('update_file', exclusive=True)
channel.queue_bind(
    queue='update_file',
    exchange='lb_request',
    routing_key='update'
)

channel.queue_declare('insert_req', exclusive=True)
channel.queue_bind(
    queue='insert_req',
    exchange='lb_request',
    routing_key='insert'
)

channel.queue_declare('node_remove', exclusive=True)
channel.queue_bind(
    queue='node_remove',
    exchange='lb_update',
    routing_key='node_remove'
)

channel.queue_declare('file_remove', exclusive=True)
channel.queue_bind(
    queue='file_remove',
    exchange='lb_update',
    routing_key='file_remove'
)

channel.queue_declare('add', exclusive=True)
channel.queue_bind(
    queue='add',
    exchange='lb_update',
    routing_key='add'
)


### Funções


def add_file (ch, method, props, body):
    global node_list
    node_name, file_list = pickle.loads(body)
    if node_name not in node_list:
        node_list += [node_name]

    for file_name in file_list:
        if not file_name in index:
            index[file_name] = [node_name]
        elif not node_name in index[file_name]:
            index[file_name] += [node_name]
    print(
        f'Índice e lista de nós atualizados:\n'
        f'{node_list}\n'
        f'{index}\n'
    )
    

def connect_datanodes_to_chunk (chosen_nodes, file_name, chunk_num):
    result = channel.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue

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

    if (file_name in index) and (chunk_num in index[file_name]):
        qnt -= len(index[file_name][chunk_num])

    if qnt >= len(node_list):
        chosen_nodes = node_list
    else:
        while len(chosen_nodes) < qnt:
            if round_robin_next >= len(node_list):
                round_robin_next = 0
            chosen_nodes += [node_list[round_robin_next]]
            round_robin_next += 1

    connect_datanodes_to_chunk(chosen_nodes, file_name, chunk_num)

    channel.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        body=''
    )

def update_files (ch, method, props, body):
    global file_dict
    file_name, qnt_chunk = pickle.loads(body)
    file_dict[file_name] = qnt_chunk

    ch.basic_ack(method.delivery_tag)


def get_files (ch, method, props, body):
    
    response = file_dict
    channel.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        body=pickle.dumps(response),
        properties=pika.BasicProperties(correlation_id=props.correlation_id)
    )

    ch.basic_ack(method.delivery_tag)

###########

channel.basic_consume(
    queue='update_file',
    on_message_callback=update_files
)

channel.basic_consume(
    queue='search_req',
    on_message_callback=get_files
)

channel.basic_consume(
    queue='node_remove',
    on_message_callback=remove_node
)

channel.basic_consume(
    queue='insert_req',
    on_message_callback=choose_nodes,
    auto_ack=True
)

channel.basic_consume(
    queue='add',
    on_message_callback=add_file
)

###########

channel.start_consuming()
