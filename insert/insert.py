import rpyc
from threading import Thread
from rpyc.utils.server import ThreadedServer

r = rpyc.utils.registry.TCPRegistryClient('localhost', port=18811)


class InsertService(rpyc.Service):
    
    def exposed_insert(self, f):
        print(f'Inserindo arquivo')
        node_list = self.request_nodes(3)
        self.insert_files(f, node_list)


    def request_nodes (self, qnt):
        ip, port = r.discover('LOADBALANCER')[0]
        lb = rpyc.connect(ip, port).root
        return lb.get_nodes(qnt)

    def insert_files (self, f, node_list):
        for node_name in node_list:
            ip, port = r.discover('SAVEFILE_' + node_name)[0]
            node = rpyc.connect(ip, port, config={'allow_public_attrs': True}).root
            node.save_file(f)
            print(f'Inserindo no nó: {node_name}')



s = ThreadedServer(InsertService(), registrar=r, auto_register=True)
s.start()