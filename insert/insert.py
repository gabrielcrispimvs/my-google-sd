import rpyc
from re import findall
from threading import Thread
from rpyc.utils.server import ThreadedServer
from tqdm import tqdm
import sys

from time import perf_counter

try:
    registry_ip = sys.argv[1]
    registry_port = int(sys.argv[2])
except:
    print('Passe IP e porta do registry como argumentos.')
    exit()

# registry_ip = '192.168.40.240'
# registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)


class InsertService(rpyc.Service):
    
    def exposed_insert(self, f):
        node_list = self.request_nodes(3)
        self.insert_file(f, node_list)


    def request_nodes (self, qnt):
        print('Solicitando nós ao load balancer...')
        ip, port = r.discover('LOADBALANCER')[0]
        conn = rpyc.connect(ip, port)
        lb = conn.root
        node_list = lb.get_nodes_insert(qnt)
        # conn.close()
        print('Nós adquiridos.')
        return node_list

    def insert_file (self, f, node_list):
        conn_list = []
        for node_name in node_list:
            ip, port = r.discover('DATANODE_' + node_name)[0]
            conn_list += [rpyc.connect(ip, port, config={'allow_public_attrs': True})]

    
        file_name = findall(r'/(.+)\Z', f.name)[0]

        serv_list = [conn.root for conn in conn_list]

        print('Inserindo arquivos...')

        t_s = perf_counter()

        buffer = ''
        chunk_num = 0
        while True:
            buffer += f.read(500000)

            if buffer == '':
                break;

            # print(f'readline: {buffer}')

            news_sep_idx = len(buffer)
            for idx, char in enumerate(buffer[::-1]):
                # print(f'idx:{idx} char:{char}')
                if char == '\n':
                    news_sep_idx -= idx
                    break;

            # print(f'idx: {news_sep_idx}')

            send_buffer = buffer[:news_sep_idx]
            buffer = buffer[news_sep_idx:]

            # print(f'Send: {send_buffer}')
            # print(f'Buff: {buffer}\n')

            chunk_num += 1 
            for serv in serv_list:
                serv.save_chunk(file_name, chunk_num, send_buffer)
                # Thread(target=serv_file.write, args=[buffer])

        t_e = perf_counter()

        for conn in conn_list:
            conn.close()

        del buffer

        
        print('Inserção finalizada.')
        print(t_e - t_s)

        for node_name in node_list:
            self.update_lb(file_name, node_name)

    def update_lb (self, file_name, node):
        ip, port = r.discover('LOADBALANCER')[0]
        conn = rpyc.connect(ip, port)
        lb = conn.root
        lb.idx_add(node, [file_name])
        conn.close()




s = ThreadedServer(InsertService(), registrar=r, auto_register=True)
s.start()