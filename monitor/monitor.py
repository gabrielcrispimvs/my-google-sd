import pika
import pickle
from threading import Thread
from time import sleep
import sys

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


countdown_time = 5.0 # Em segundos
tolerancia = 3

node_timer = {}


def monitor(channel):
    try:
        while True:
            sleep(countdown_time)
            print('Countdown')
            # print(m.node_timer)

            to_remove = []

            for node_name in node_timer:
                if node_timer[node_name] == 0:
                    channel.basic_publish(
                        exchange='lb_update',
                        routing_key='node_remove',
                        body=node_name
                    )

                    to_remove += [node_name]
                else:
                    node_timer[node_name] -= 1
            
            for node_name in to_remove:
                del node_timer[node_name]
    
    except KeyboardInterrupt:
        exit()

def keep_alive (ch, method, properties, body):
    print(f'Nó {str(body)} vivo')
    node_name = body.decode()
    node_timer[node_name] = tolerancia



def new_node (ch, method, properties, body):
    node_name, file_list = pickle.loads(body)
    node_timer[node_name] = tolerancia
    print(
        f'Nó {node_name} registrado com os arquivos:\n'
        f'{file_list}'
    )
    ch.basic_ack(method.delivery_tag)
    channel.basic_publish(
        exchange='lb_update',
        routing_key='add',
        body=body
    )



# Iniciando conexão com RabbitMQ
conn = pika.BlockingConnection(pika.ConnectionParameters(host=broker_ip, port=broker_port))
channel = conn.channel()
channel.exchange_declare(exchange='monitoring', exchange_type='direct')
channel.exchange_declare(exchange='lb_update', exchange_type='direct')


# Escutando pings dos datanodes
result = channel.queue_declare('', exclusive=True)
keep_alive_queue = result.method.queue
# print(f'keep_alive: {keep_alive_queue}')
channel.queue_bind(
    exchange='monitoring',
    queue=keep_alive_queue,
    routing_key='keep_alive'
)

channel.basic_consume(
    queue=result.method.queue,
    auto_ack=True,
    on_message_callback=keep_alive
)

# Escutando registro de novos datanodes
result = channel.queue_declare('', exclusive=True)
register_queue = result.method.queue
# print(f'register: {register_queue}')
channel.queue_bind(
    exchange='monitoring',
    queue=register_queue,
    routing_key='register'
)

channel.basic_consume(
    queue=register_queue,
    on_message_callback=new_node
)

# Iniciando nó
t = Thread(target=monitor, args=[channel])
t.daemon = True
t.start()

channel.start_consuming()