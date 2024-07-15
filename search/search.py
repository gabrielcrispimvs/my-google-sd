import rpyc
from rpyc.utils.server import ThreadedServer
from threading import Thread
import sys

try:
    registry_ip = sys.argv[1]
    registry_port = sys.argv[2]
except:
    print('Passe IP e porta do registry como argumentos.')
    exit()

# registry_ip = '192.168.40.240'
# registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)


class SearchService(rpyc.Service):

    def exposed_search(self, keyword):
        search_dict = self.request_nodes()
        print('OK')
        print(f'dict: {search_dict}')
        results = self.request_search(keyword, search_dict)
        print('Busca finalizada.')
        return results


    def request_nodes(self):
        print('Solicitando nós ao load balancer...')
        ip, port = r.discover('LOADBALANCER')[0]
        conn = rpyc.connect(ip, port)
        lb = conn.root
        search_dict = lb.get_nodes_search()
        return search_dict


    def request_search(self, keyword, search_dict):
        thread_list = []
        results = []
        for node in search_dict:
            ip, port = r.discover('DATANODE_' + node)[0]
            
            print(f'conectando com nó {node}')
            conn = rpyc.connect(ip, port, config={'allow_public_attrs': True, 'sync_request_timeout': 240})
            datanode = conn.root
            for file_name in search_dict[node]:
                f, part, qnt_parts = file_name
                t = Thread(target=datanode.search, args=[keyword, f, part, qnt_parts, results])
                t.start()
                thread_list += [t]
                # results += t.join()
            
            # results += datanode.search(f, part, qnt_parts)
        
        for t in thread_list:
            t.join()
        # print(results)
        return results


s = ThreadedServer(SearchService, registrar=r, auto_register=True)
s.start()