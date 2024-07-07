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

registry_ip = 'localhost'
registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)

keep_alive_interval = 5.0
node_name = sys.argv[1]
files_dir = 'files/' + node_name

try:
    listdir(files_dir)
except:
    mkdir(files_dir)

# print(listdir(files_dir))    

class SaveFileService(rpyc.Service):
    ALIASES = ['SAVEFILE_' + node_name]

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

    # def exposed_save_file(self, f):
        
    #     file_name = re.findall(r'/(.+)\Z', f.name)[0]
    #     print(f'Salvando arquivo {file_name} ...')

    #     with open(join(files_dir, file_name), 'w') as lf:
    #         while True:
    #             line = f.readline()
    #             if line == '':
    #                 break
    #             lf.write(line)
        
    #     print(f'Arquivo inserido.')

    #     self.update_monitor([file_name])

    def update_monitor(self, added_files):
        ip, port = r.discover('MONITOR')[0]
        conn = rpyc.connect(ip, port)
        m = conn.root
        m.register_node(node_name, added_files)
        conn.close()


class SearchService(rpyc.Service):
    ALIASES = ['SEARCH_' + node_name]

    def on_connect(self, conn):
        # print(f'Conectado: {conn}')
        pass

    def on_disconnect(self, conn):
        # print(f'Conexão fechada: {conn}')
        pass

    def exposed_list_files(self):
        return listdir(files_dir)

    def exposed_search(self, keyword):
        print(f'Buscando: {keyword}')
        re_pattern = re.compile(r'\s' + keyword + r'\s', flags=re.IGNORECASE)

        result_list = []
        file_list = listdir(files_dir)

        for file_name in file_list:
            print(f'Buscando no arquivo {file_name}...')
            with open(join(files_dir, file_name), encoding='utf-8') as f:
                while True:
                    line = f.readline()
                    if line == '':
                        break
                    news_item = json.loads(line)
                    print(news_item)

                    if news_item['title'] == None:
                        news_item['title'] = ''
                    if news_item['maintext'] == None:
                        news_item['maintext'] = ''

                    if (re_pattern.search(news_item['title'])) or (re_pattern.search(news_item['maintext'])):
                        result_list.append((file_name, news_item))

        print(f'Busca finalizada.')
        print(result_list)
        return result_list


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

s = ThreadedServer(SaveFileService, registrar=r, auto_register=True, protocol_config={'allow_public_attrs': True})
s.start()