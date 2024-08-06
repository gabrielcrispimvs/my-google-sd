import pika
import pickle
from re import sub
import sys

from time import perf_counter

from string import ascii_lowercase
from string import digits
from random import choices

def generate_corr_id (qnt=12):
    return ''.join(choices(ascii_lowercase + digits, k=qnt))

try:
    broker_ip = sys.argv[1]
except:
    broker_ip = '192.168.40.141'
    print(f'Usando endereço padrão: {broker_ip}')

try:
    broker_port = int(sys.argv[2])
except:
    broker_port = 5672
    print(f'Usando porta padrão: {broker_port}')


fator_replica = 3


conn = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
channel = conn.channel()


channel.exchange_declare('lb_request', exchange_type='direct')
channel.exchange_declare('send_chunk', exchange_type='topic')

####################################

channel.exchange_declare('client_cmd', exchange_type='direct')

channel.queue_declare('insert')
channel.queue_bind(
    queue='insert',
    exchange='client_cmd',
    routing_key='insert'
)

channel.queue_declare('search')
channel.queue_bind(
    queue='search',
    exchange='client_cmd',
    routing_key='search'
)

# Funções

def request_nodes (file_name, chunk_num, qnt):
    result = channel.queue_declare('', exclusive=True)
    callback_queue = result.method.queue

    response = False

    def on_response (ch, method, properties, body):
        nonlocal response
        response = True
        ch.basic_ack(method.delivery_tag)

    msg = pickle.dumps((file_name, chunk_num, qnt))

    channel.basic_publish(
        exchange='lb_request',
        routing_key='insert',
        body=msg,
        properties=pika.BasicProperties(reply_to=callback_queue)
    )

    while not response:
        queue_state = channel.queue_declare(callback_queue, passive=True)
        if queue_state.method.message_count != 0:
            method, props, body = channel.basic_get(callback_queue)
            on_response(channel, method, props, body)

def insert_file (ch, method, props, body):
    file_name, chunk_num, buffer = pickle.loads(body)

    if buffer == '':
        channel.basic_publish(
            exchange='lb_request',
            routing_key='update',
            body=pickle.dumps((file_name, chunk_num))
        )
        ch.basic_ack(method.delivery_tag)
        return

    file_name_topic = sub(r'\.', '/', file_name)

    # print(f'Inserindo chunk {chunk_num} do arquivo {file_name}')

    request_nodes(file_name, chunk_num, fator_replica)

    channel.basic_publish(
        exchange='send_chunk',
        routing_key=file_name_topic+'.'+str(chunk_num),
        body=body
    )

    ch.basic_ack(method.delivery_tag)




########################


channel.basic_consume(
    queue='insert',
    on_message_callback=insert_file
)

channel.start_consuming()