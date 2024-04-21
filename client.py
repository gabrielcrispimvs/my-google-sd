import socket
import json

serv_addr = ('localhost', 12345)

conn = socket.create_connection(serv_addr)

print(f'Conexão estabelecida:\n'
      f'Cliente: {conn.getsockname()}\tServidor: {conn.getpeername()}')

while True:
    keyword = input()
    conn.send(keyword.encode())

    result_count = 0
    while True:
        msg = conn.recv(16384).decode()
        if msg == '1':
            if result_count == 0:
                print('Nenhum resultado encontrado.')
            break
        
        result_count += 1
        # print(msg)
        news_item = json.loads(msg)

        print(
            f'{result_count}. '
            f'{news_item["title"]}\n'
            f'{news_item["maintext"]}\n'
            f'Link: {news_item["url"]}\n'
        )
