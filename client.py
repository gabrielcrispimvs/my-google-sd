import socket
import json
import datetime as dt
import threading

def request_search (request_msg, index):
    conn.send(request_msg)
    send_time = dt.datetime.now()

    result_count = 0
    while True:
        msg = conn.recv(16384).decode()
        if msg == '1':
            if result_count == 0:
                rcv_time = dt.datetime.now()
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
    
    result[index] = (rcv_time - send_time).total_seconds()



serv_addr = ('localhost', 12345)

qnt_req_seg = 50
total_time = dt.timedelta(seconds=60)


result = [None] * qnt_req_seg * total_time.seconds
time_between_requests = dt.timedelta(microseconds= (1_000_000 // qnt_req_seg) )
last_request_time = dt.datetime(year=1, month=1, day=1)
index = 0
start_time = dt.datetime.now()

while last_request_time - start_time < total_time:
    keyword = 'acbdefgh'
    request_msg = keyword.encode()
    if dt.datetime.now() - last_request_time > time_between_requests:
        conn = socket.create_connection(serv_addr)
        print(f'OK: {conn}')
        threading.Thread(target=request_search, args=(request_msg, index)).start()
        last_request_time = dt.datetime.now()
        index += 1
        if index >= len(result):
            break

while threading.active_count() > 1:
    pass

print(result)
with open(f'log_{qnt_req_seg}_req_seg', mode='w') as file:
    file.write(f'ReqPorSegundo: {qnt_req_seg}; DurTeste: {total_time.total_seconds()}s\n')
    for time in result:
        if time != None:
            file.write(f'{time};')
