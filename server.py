import socket
import re
import json
import locale

addr = ('localhost', 12345)
server = socket.create_server(addr)

server.listen()
conn, peer_addr = server.accept()

print(f'Conexão estabelecida:\n'
      f'Cliente: {conn.getpeername()}\tServidor: {conn.getsockname()}')

while True:
    keyword = conn.recv(4096).decode()
    if keyword == '':
        continue
    else:
        result_count = 0
        
        with open('dataset/2021_pt.jsonl', encoding='utf-8') as file:
            for i in range(10000):
                line = file.readline()
                news_item = json.loads(line)

                if re.search(keyword, news_item['title']) or re.search(keyword, news_item['maintext']):
                    result_count += 1
                    msg = json.dumps(news_item)
                    # print(news_item)
                    # print(msg)
                    msg = msg.encode()
                    print(f'{result_count}. {len(msg)}')
                    conn.send(msg)
                    if result_count == 10:
                        break
            conn.send('1'.encode())
        
