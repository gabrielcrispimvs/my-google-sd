import rpyc
from re import findall
from threading import Thread
from rpyc.utils.server import ThreadedServer
from tqdm import tqdm


from time import perf_counter


registry_ip = 'localhost'
registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)


class InsertService(rpyc.Service):
    
    def exposed_insert(self, f):
        print(f'Inserindo arquivo')
        node_list = self.request_nodes(3)
        self.insert_file(f, node_list)


    def request_nodes (self, qnt):
        ip, port = r.discover('LOADBALANCER')[0]
        conn = rpyc.connect(ip, port)
        lb = conn.root
        node_list = lb.get_nodes(qnt)
        return node_list

    def insert_file (self, f, node_list):
        conn_list = []
        for node_name in node_list:
            ip, port = r.discover('SAVEFILE_' + node_name)[0]
            conn_list += [rpyc.connect(ip, port, config={'allow_public_attrs': True})]

    
        file_name = findall(r'/(.+)\Z', f.name)[0]

        serv_file_list = [conn.root.open_file(file_name) for conn in conn_list]

        print('Inserindo arquivos...')

        t_s = perf_counter()

        while True:
            buffer = f.read(500000)
            if buffer == '':
                break; 
            for serv_file in serv_file_list:
                serv_file.write(buffer)
                # Thread(target=serv_file.write, args=[buffer])

        for serv_file in serv_file_list:
            serv_file.close()

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