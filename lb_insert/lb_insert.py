import rpyc
from threading import Thread
from rpyc.utils.server import ThreadedServer

registry_ip = 'localhost'
registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)

class LoadBalancerService (rpyc.Service):
    
    # Solicitando nós 
    next_node = 0
    node_list = []


    def exposed_get_nodes (self, qnt):
        if qnt >= len(self.node_list):
            return self.node_list

        to_return = []
        for i in range(qnt):
            if self.next_node >= len(self.node_list):
                self.next_node = 0
            to_return += [self.node_list[self.next_node]]
            self.next_node += 1
        return to_return
        

    # 
    def exposed_idx_add (self, node_name, file_list):
        if node_name not in self.node_list:
            self.node_list += [node_name]

        for file_name in file_list:
            if not file_name in self.index:
                self.index[file_name] = [node_name]
            elif not node_name in self.index[file_name]:
                self.index[file_name] += [node_name]
        print(
            f'Índice atualizado:\n'
            f'{self.index}'
        )

    def exposed_idx_remove (self, node_name, file_list=None):
        if node_name in self.node_list:
            self.node_list.remove(node_name)

        for file_name in self.index:
            if node_name in self.index[file_name]:
                self.index[file_name].remove(node_name)
        print(
            f'Índice atualizado:\n'
            f'{self.index}'
        )

    index = {}


# Levantando Load Balancer
s = ThreadedServer(LoadBalancerService(), registrar=r, auto_register=True)
s.start()


# t = Thread(target=s.start)
# t.start()
