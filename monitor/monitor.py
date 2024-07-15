import rpyc
from threading import Thread
from rpyc.utils.server import ThreadedServer
from time import sleep
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

countdown_time = 5.0 # Em segundos
tolerancia = 3



class MonitorService (rpyc.Service):

    def exposed_register_node (self, node_name, file_list):
        self.node_timer[node_name] = tolerancia
        print(
            f'Nó {node_name} registrado com os arquivos:\n'
            f'{file_list}'
        )
        self.update_lb_add(node_name, file_list)
        
    def exposed_keep_alive (self, node_name):
        print(f'{node_name} vivo.')
        self.node_timer[node_name] = tolerancia


    def update_lb_add (self, node_name, file_list):
        ip, port = r.discover('LOADBALANCER')[0]
        conn = rpyc.connect(ip, port)
        lb = conn.root
        lb.idx_add(node_name, file_list)
        conn.close()

    def update_lb_remove (self, node_name, file_list=[]):
        ip, port = r.discover('LOADBALANCER')[0]
        conn = rpyc.connect(ip, port)
        lb = conn.root
        lb.idx_remove(node_name, file_list)
        conn.close()


    node_timer = {}

    def monitor(self):
        while True:
            sleep(countdown_time)
            print('Countdown')

            to_remove = []

            for node_name in self.node_timer:
                if self.node_timer[node_name] == 0:
                    self.update_lb_remove(node_name)
                    to_remove += [node_name]
                else:
                    self.node_timer[node_name] -= 1
            
            for node_name in to_remove:
                del self.node_timer[node_name]
            



## Levantando o nó
m = MonitorService()
t1 = Thread(target=m.monitor)
t1.start()

s = ThreadedServer(m, registrar=r, auto_register=True)
s.start()

# m.monitor()
