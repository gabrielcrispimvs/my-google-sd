import rpyc
import socket

r = rpyc.utils.registry.TCPRegistryServer(port=18811, allow_listing=True)
r.start()