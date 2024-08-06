import os
import rpyc
from time import sleep
from threading import Thread
from rpyc.utils.server import ThreadedServer

import json
import re
from os import listdir
from os import mkdir
from os.path import join
import sys

# from tqdm import tqdm

from string import ascii_lowercase
from string import digits
from random import choices


def generate_corr_id(qnt=12):
    return "".join(choices(ascii_lowercase + digits, k=qnt))


keep_alive_interval = 3.0

# Cria a pasta nodes caso não exista
try:
    listdir("nodes")
except:
    mkdir("nodes")

try:
    # Nome do nó fornecido
    node_name = sys.argv[3]

except IndexError:
    # Nome do nó não fornecido
    try:  # Algum nome de nó já existente
        names_list = listdir("nodes")
        node_name = names_list[0]
    except:  # Nenhum nome de nó existente. Criando um novo
        node_name = "".join(choices(ascii_lowercase + digits, k=6))


files_dir = "nodes/" + node_name

try:
    listdir(files_dir)
    print(listdir(files_dir))
except:
    mkdir(files_dir)

import pika
import pickle
from re import sub

try:
    broker_ip = sys.argv[1]
except:
    broker_ip = "192.168.40.141"
    print(f"Usando endereço padrão: {broker_ip}")

try:
    broker_port = int(sys.argv[2])
except:
    broker_port = 5672
    print(f"Usando porta padrão: {broker_port}")

conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=broker_ip, port=broker_port)
)
channel = conn.channel()


channel.exchange_declare(exchange="monitoring", exchange_type="direct")
channel.exchange_declare(exchange="send_chunk", exchange_type="topic")
channel.exchange_declare(exchange="chunk_conn", exchange_type="direct")


def search_chunk(ch, method, props, body):
    file_name, chunk, keyword = pickle.loads(body)

    keyword_bytes = keyword.encode("utf-8").lower()

    result_list = []

    file_path = os.path.join(files_dir, file_name, str(chunk))
    print(f"Verificando o arquivo: {file_path}")

    try:
        with open(file_path, mode="rb") as f:  # Modo de leitura binária
            for line in f:
                line_lower = line.lower()
                print(f"Verificando a linha: {line_lower}")
                print(
                    f"Contém a palavra-chave '{keyword_bytes}': {keyword_bytes in line_lower}"
                )
                if keyword_bytes in line_lower:
                    print(f"Encontrado em {file_name}/{chunk}")
                    result_list.append(
                        line.decode("utf-8", errors="ignore")
                    )  # Decodificar para string para armazenamento
    except FileNotFoundError:
        print(f"Arquivo {file_path} não encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")

    print(f"Resultados encontrados: {result_list}")

    channel.basic_publish(
        exchange="",
        routing_key=props.reply_to,
        body=pickle.dumps((file_name, chunk, result_list)),
    )


def save_chunk(ch, method, props, body):
    file_name, chunk_num, buffer = pickle.loads(body)

    # print(f'Escrevendo chunk {chunk_num} do arquivo {file_name}.')

    chunks_dir = join(files_dir, file_name)

    try:
        listdir(chunks_dir)
    except:
        mkdir(chunks_dir)

    with open(join(chunks_dir, str(chunk_num)), mode="w") as chunk:
        chunk.write(buffer)

    channel.queue_declare(file_name + "/" + str(chunk_num))
    channel.basic_consume(
        queue=file_name + "/" + str(chunk_num), on_message_callback=search_chunk
    )

    ch.basic_ack(method.delivery_tag)


def connect_to_chunk(ch, method, props, body):
    file_name, chunk = pickle.loads(body)
    file_name_topic = sub(r"\.", "/", file_name)

    # print(f'Conectando ao chunk {chunk} do arquivo {file_name}')

    result = channel.queue_declare("", exclusive=True)
    chunk_queue = result.method.queue

    channel.basic_consume(queue=chunk_queue, on_message_callback=save_chunk)

    channel.queue_bind(
        queue=chunk_queue,
        exchange="send_chunk",
        routing_key=file_name_topic + "." + str(chunk),
    )

    channel.basic_consume(queue=chunk_queue, on_message_callback=save_chunk)

    channel.basic_publish(
        exchange="",
        routing_key=props.reply_to,
        body="",
        properties=pika.BasicProperties(correlation_id=node_name),
    )


def ping_monitor(keep_alive_interval):
    ping_connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
    ping_channel = ping_connection.channel()

    ping_channel.exchange_declare("monitoring", exchange_type="direct")

    try:
        while True:
            sleep(keep_alive_interval)
            ping_channel.basic_publish(
                exchange="monitoring", routing_key="keep_alive", body=node_name
            )

    except KeyboardInterrupt:
        ping_connection.close()


result = channel.queue_declare("", exclusive=True)
chunk_conn_queue = result.method.queue

channel.queue_bind(queue=chunk_conn_queue, exchange="chunk_conn", routing_key=node_name)

channel.basic_consume(
    queue=chunk_conn_queue, on_message_callback=connect_to_chunk, auto_ack=True
)

file_list = listdir(files_dir)

channel.basic_publish(
    exchange="monitoring",
    routing_key="register",
    body=pickle.dumps((node_name, file_list)),
)

for file in file_list:
    # chunk_num = lista arquivos, pega o int(nome) max
    chunk_num = max(
        [
            int(sub(r"\D", "", f))
            for f in listdir(join(files_dir, file))
            if sub(r"\D", "", f) != ""
        ]
    )
    channel.basic_publish(
        exchange="lb_request",
        routing_key="update",
        body=pickle.dumps((file, chunk_num)),
    )

t1 = Thread(target=ping_monitor, args=[keep_alive_interval])
t1.daemon = True
t1.start()

channel.start_consuming()
