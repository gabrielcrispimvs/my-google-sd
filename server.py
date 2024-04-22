import socket
import re
import json
import locale
import threading

addr = ('localhost', 12345)
server = socket.create_server(addr)
server.listen()

print(f'Server started: {server.getsockname()}')

# print(f'Conexão estabelecida:\n'
#       f'Cliente: {conn.getpeername()}\tServidor: {conn.getsockname()}')

def handle_request (conn):
    # print(f'OK: {conn}')
    keyword = conn.recv(4096).decode()
    # print(keyword)
    if keyword == '':
        return
    else:
        result_count = 0
        
        with open('dataset/2021_pt.jsonl', encoding='utf-8') as file:
            for i in range(10000):
                line = file.readline()
                news_item = json.loads(line)
                # print(f'Searching for {keyword} in {news_item}\n')
                title = news_item['title'] if news_item['title'] != None else ''
                maintext = news_item['maintext'] if news_item['maintext'] != None else ''
                if re.search(keyword, title) or re.search(keyword, maintext):
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

while True:
    conn, peer_addr = server.accept()
    threading.Thread(target=handle_request, args=[conn]).start()
    #  print(f'OK: {conn}')
