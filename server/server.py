import rpyc
import json
import re
from os import listdir
from os.path import join

files_dir = 'files'


class MyService(rpyc.Service):
    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_get_answer(self): # this is an exposed method
        return 42

    exposed_the_real_answer_though = 43     # an exposed attribute

    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"


class InsertService(rpyc.Service):
    def on_connect(self, conn):
        print(f'Conectado: {conn}')
        pass

    def on_disconnect(self, conn):
        print(f'Conexão fechada: {conn}')
        pass

    def exposed_insert(self, file):
        print(f'Inserindo arquivo...')
        print(f'Arquivo inserido.')


class SearchService(rpyc.Service):
    def on_connect(self, conn):
        print(f'Conectado: {conn}')
        pass

    def on_disconnect(self, conn):
        print(f'Conexão fechada: {conn}')
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

from rpyc.utils.server import ThreadedServer
r = rpyc.utils.registry.TCPRegistryClient('localhost', port=18811)
t = ThreadedServer(SearchService, port=18812, registrar=r, auto_register=True)
t.start()