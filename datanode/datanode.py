import rpyc
from time import sleep
from threading import Thread
from rpyc.utils.server import ThreadedServer

from string import ascii_lowercase
from string import digits
from random import choices

import json
import re
from os import listdir
from os import mkdir
from os.path import join
import sys
# from tqdm import tqdm

try:
    registry_ip = sys.argv[1]
    registry_port = int(sys.argv[2])
except:
    print('Passe IP e porta do registry como argumentos.')
    exit()

# registry_ip = '192.168.40.240'
# registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)

keep_alive_interval = 5.0

# Cria a pasta nodes caso não exista
try:
    listdir('nodes')
except:
    mkdir('nodes')




try:
    # Nome do nó fornecido
    node_name = sys.argv[3]

except IndexError:
    # Nome do nó não fornecido

    try: # Algum nome de nó já existente
        names_list = listdir('nodes')
        node_name = names_list[0]
    except: # Nenhum nome de nó existente. Criando um novo
        node_name = ''.join(choices(ascii_lowercase + digits, k=6))



files_dir = 'nodes/' + node_name

try:
    listdir(files_dir)
except:
    mkdir(files_dir)


class DataNodeService(rpyc.Service):
    ALIASES = ['DATANODE_' + node_name]

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_save_chunk(self, file_name, chunk_num, data):
        chunks_dir = join(files_dir, file_name)

        try:
            listdir(chunks_dir)
        except:
            mkdir(chunks_dir)

        with open(join(chunks_dir, str(chunk_num)), mode='w') as chunk:
            chunk.write(data)

    def exposed_open_file(self, file_name):
        return open(join(files_dir, file_name), mode='w')

    def exposed_close_file(self, server_file):
        server_file.close()


    def update_monitor(self, added_files):
        ip, port = r.discover('MONITOR')[0]
        conn = rpyc.connect(ip, port)
        m = conn.root
        m.register_node(node_name, added_files)
        conn.close()


    ### BUSCA
    def exposed_search(self, keyword, file_name, part, qnt_parts, result_list):
        print(f'Buscando: {keyword}')
        re_pattern = re.compile(r'\s' + keyword + r'\s', flags=re.IGNORECASE)

        # result_list = []
        chunks_dir = join(files_dir, file_name)
        chunk_list = listdir(chunks_dir)

        part_size = len(chunk_list) // qnt_parts

        chunk_list = [chunk for idx, chunk in enumerate(chunk_list) if idx % qnt_parts == part-1]

        for chunk in chunk_list:
            # print(f'Buscando no chunk {chunk} do arquivo {file_name}...')
            with open(join(chunks_dir, chunk), encoding='utf-8') as f:
                while True:
                    line = f.readline()
                    # print(line)
                    if line == '':
                        break

                    try:
                        news_item = json.loads(line)
                        # print(news_item)

                        if news_item['title'] == None:
                            news_item['title'] = ''
                        if news_item['maintext'] == None:
                            news_item['maintext'] = ''

                        if (re_pattern.search(news_item['title'])) or (re_pattern.search(news_item['maintext'])):
                            result_list += [(file_name, news_item)]
                    except:
                        ### Tentando ler arquivo que não é de notícias (como o arquivo teste.txt)
                        break

        print(f'Busca finalizada.')
        # for file_name, news_item in result_list:
        #     print(news_item)
        # return result_list


import pika
import pickle
from re import sub

conn = pika.BlockingConnection(pika.ConnectionParameters('192.168.40.141'))
channel = conn.channel()

channel.exchange_declare(exchange='monitoring', exchange_type='direct')

channel.exchange_declare(exchange='send_chunk', exchange_type='topic')
channel.exchange_declare(exchange='chunk_conn', exchange_type='direct')


def save_chunk (ch, method, props, body):
    file_name, chunk_num, buffer = pickle.loads(body)

    # print(f'Escrevendo chunk {chunk_num} do arquivo {file_name}.')

    chunks_dir = join(files_dir, file_name)

    try:
        listdir(chunks_dir)
    except:
        mkdir(chunks_dir)

    with open(join(chunks_dir, str(chunk_num)), mode='w') as chunk:
        chunk.write(buffer)

    ch.basic_ack(method.delivery_tag)



def connect_to_chunk (ch, method, props, body):
    file_name, chunk = pickle.loads(body)
    file_name_topic = sub(r'\.', '/', file_name)

    # print(f'Conectando ao chunk {chunk} do arquivo {file_name}')

    result = channel.queue_declare('', exclusive=True)
    chunk_queue = result.method.queue

    channel.queue_bind(
        queue=chunk_queue,
        exchange='send_chunk',
        routing_key=file_name_topic+'.'+str(chunk)
    )

    channel.basic_consume(
        queue=chunk_queue,
        on_message_callback=save_chunk
    )

    channel.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        body='',
        properties=pika.BasicProperties(correlation_id=node_name)
    )


def ping_monitor(keep_alive_interval):
    ping_connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.40.141'))
    ping_channel = ping_connection.channel()

    ping_channel.exchange_declare('monitoring', exchange_type='direct')

    try:
        while True:
            sleep(keep_alive_interval)
            ping_channel.basic_publish(
                exchange='monitoring',
                routing_key='keep_alive',
                body=node_name
            )
    
    except KeyboardInterrupt:
        ping_connection.close()





result = channel.queue_declare('', exclusive=True)
chunk_conn_queue = result.method.queue
# print(chunk_conn_queue)
# print(node_name)

channel.queue_bind(
    queue=chunk_conn_queue,
    exchange='chunk_conn',
    routing_key=node_name
)

channel.basic_consume(
    queue=chunk_conn_queue,
    on_message_callback=connect_to_chunk,
    auto_ack=True
)




file_list = listdir(files_dir)

channel.basic_publish(
    exchange='monitoring',
    routing_key='register',
    body=pickle.dumps((node_name, file_list))
)

t = Thread(target=channel.start_consuming)
t.start()

t1 = Thread(target=ping_monitor, args=[keep_alive_interval])
t1.start()


s = ThreadedServer(DataNodeService, registrar=r, auto_register=True, protocol_config={'allow_public_attrs': True})
s.start()