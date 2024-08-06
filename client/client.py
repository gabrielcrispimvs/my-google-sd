import rpyc
from rpyc.utils.registry import TCPRegistryClient
import sys

from string import ascii_lowercase
from string import digits
from random import choices

def generate_corr_id (qnt=12):
    return ''.join(choices(ascii_lowercase + digits, k=qnt))

try:
    registry_ip = sys.argv[1]
    registry_port = int(sys.argv[2])
except:
    print('Passe IP e porta do registry como argumentos.')
    exit()

# registry_ip = '192.168.40.240'
# registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)


import pika
import pickle

from re import findall

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = conn.channel()

channel.exchange_declare('client_cmd', exchange_type='direct')

result = channel.queue_declare('', exclusive=True)
callback_queue = result.method.queue
print(callback_queue)

while True:

    cmd = input(
        f'Selecione uma opção:\n'
        f'1. Pesquisar\n'
        f'2. Inserir arquivo\n'
    )

    match cmd:
        case '1': # Pesquisa
            keyword = input(f'Entre com o termo a ser buscado:\n')
            
            response = None
            corr_id = generate_corr_id()

            def on_response (ch, method, props, body):
                # nonlocal response
                if props.correlation_id == corr_id:
                    response = pickle.loads(body)
                    ch.basic_ack(method.delivery_tag)

            print(callback_queue)
                        
            channel.basic_publish(
                exchange='client_cmd',
                routing_key='search',
                body=keyword,
                properties=pika.BasicProperties(reply_to=callback_queue, correlation_id=corr_id)
            )

            while response == None:
                queue_state = channel.queue_declare(callback_queue, passive=True)
                if queue_state.method.message_count != 0:
                    method, props, body = channel.basic_get(callback_queue)
                    on_response(channel, method, props, body)

            if response == []:
                print('Nenhuma notícia encontrada.')

            else:
                for file_name, news_item in response:
                    print(
                        f"Notícia encontrada no arquivo {file_name}:\n"
                        f"{news_item['title']}\n"
                        f"{news_item['maintext']}\n"
                        f"Link: {news_item['url']}\n"
                    )

        case '2':
            try:
                file_path = input(f'Digite o caminho do arquivo a ser inserido.\n')
                    
                with open(file_path, mode='r') as f:
                    file_name = findall(r'(\w+\.\w+)\Z', file_path)[0]
                    print(f'Inserindo arquivo com nome {file_name}')

                    buffer = ''
                    chunk_num = 0
                    while True:
                        buffer += f.read(5000000)

                        if buffer == '':
                            channel.basic_publish(
                                exchange='client_cmd',
                                routing_key='insert',
                                body=pickle.dumps((file_name,chunk_num,''))
                            )

                            break;

                        news_sep_idx = len(buffer)
                        for idx, char in enumerate(buffer[::-1]):
                            if char == '\n':
                                news_sep_idx -= idx
                                break;

                        send_buffer = buffer[:news_sep_idx]
                        buffer = buffer[news_sep_idx:]

                        chunk_num += 1

                        msg = pickle.dumps((file_name, chunk_num, send_buffer))

                        channel.basic_publish(
                            exchange='client_cmd',
                            routing_key='insert',
                            body=msg
                        )

            except FileNotFoundError:
                print(f'Arquivo não encontrado.')

        case '3':
            file_path = input(
                f'Digite o caminho do arquivo a ser inserido.\n'
            )
            with open(file_path, mode='r') as f:
                ip, port = r.discover('INSERT')[0]
                conn = rpyc.connect(ip, port, config={'allow_public_attrs': True, 'sync_request_timeout': 240})
                m = conn.root
                m.insert(f)
                conn.close()
        case '4':
            pass
        case other:
            print(f'{other} não é uma opção válida.\n')

# c.root






