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
from tqdm import tqdm

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

# Verifica
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

        for chunk in tqdm(chunk_list):
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




ip, port = r.discover('MONITOR')[0]
conn = rpyc.connect(ip, port)
conn.root.register_node(node_name, listdir(files_dir))
conn.close()

def ping_monitor(keep_alive_interval):
    ip, port = r.discover('MONITOR')[0]
    conn = rpyc.connect(ip, port)
    m = conn.root
    while True:
        sleep(keep_alive_interval)
        m.keep_alive(node_name)
    conn.close()

t = Thread(target=ping_monitor, args=[keep_alive_interval])
t.start()

s = ThreadedServer(DataNodeService, registrar=r, auto_register=True, protocol_config={'allow_public_attrs': True})
s.start()