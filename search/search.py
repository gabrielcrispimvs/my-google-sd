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
    broker_ip = 'localhost'
    print(f'Usando endereço padrão: {broker_ip}')

try:
    broker_port = int(sys.argv[2])
except:
    broker_port = 5672
    print(f'Usando porta padrão: {broker_port}')



conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = conn.channel()


channel.exchange_declare('lb_request', exchange_type='direct')
channel.exchange_declare('send_chunk', exchange_type='topic')

####################################

channel.exchange_declare('client_cmd', exchange_type='direct')

channel.queue_declare('search')
channel.queue_bind(
    queue='search',
    exchange='client_cmd',
    routing_key='search'
)

# Funções


def get_files ():
    print(f'Adquirindo lista de arquivos.')

    result = channel.queue_declare('', exclusive=True)
    callback_queue = result.method.queue
    print(f'get_files callback: {callback_queue}')

    response = None
    corr_id = generate_corr_id()

    def on_response (ch, method, props, body):
        nonlocal response
        print(corr_id, props.correlation_id)
        if props.correlation_id == corr_id:
            response = pickle.loads(body)
            print(response)
            ch.basic_ack(method.delivery_tag)

    channel.basic_publish(
        exchange='lb_request',
        routing_key='search',
        body='',
        properties=pika.BasicProperties(reply_to=callback_queue, correlation_id=corr_id)
    )
    print(f'mensagem enviada.')

    while response == None:
        queue_state = channel.queue_declare(callback_queue, passive=True)
        if queue_state.method.message_count != 0:
            method, props, body = channel.basic_get(callback_queue)
            on_response(channel, method, props, body)

    print('sucesso')

    return response

def search_keyword (ch, method, props, body):
    keyword = str(body)

    print(f'Buscando {keyword}\n')

    result = channel.queue_declare('', exclusive=True)
    callback_queue = result.method.queue

    files = get_files()

    # print(f'Lista de arquivos adquirida')

    response = {}
    answer = []

    def on_response (ch, method, props, body):
        nonlocal response
        nonlocal answer
        file_name, chunk, news = pickle.loads(body)
        print(news)
        answer += news
        response[ (file_name, chunk) ] = True
        ch.basic_ack(method.delivery_tag)

    print(files)

    for file_name in files:
        qnt_chunk = files[file_name]
        for chunk in range(1, qnt_chunk+1):
            response[ (file_name, chunk) ] = False
            print(f'Enviando busca do chunk {chunk} do arquivo {file_name}')
            channel.basic_publish(
                exchange='',
                routing_key=file_name+'/'+str(chunk),
                body=pickle.dumps((file_name, chunk, keyword)),
                properties=pika.BasicProperties(reply_to=callback_queue)
            )

    while False in response.values():
        queue_state = channel.queue_declare(callback_queue, passive=True)
        if queue_state.method.message_count != 0:
            new_method, new_props, new_body = channel.basic_get(callback_queue)
            on_response(channel, new_method, new_props, new_body)

    print('Busca finalizada.')
    print(answer)

    channel.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        body=pickle.dumps(answer),
        properties=pika.BasicProperties(correlation_id=props.correlation_id)
    )

    ch.basic_ack(method.delivery_tag)

########################

channel.basic_consume(
    queue='search',
    on_message_callback=search_keyword
)

channel.start_consuming()