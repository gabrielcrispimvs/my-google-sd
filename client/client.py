import rpyc
from rpyc.utils.registry import TCPRegistryClient

registry_ip = 'localhost'
registry_port = 18811

r = rpyc.utils.registry.TCPRegistryClient(registry_ip, registry_port)


while True:

    cmd = input(
        f'Selecione uma opção:\n'
        f'1. Pesquisar\n'
        f'2. Listar arquivos\n'
        f'3. Inserir arquivo\n'
        f'4. Remover arquivo\n'
    )

    match cmd:
        case '1': # Pesquisa
            keyword = input(f'Entre com o termo a ser buscado:\n')
            
            ip, port = r.discover('SEARCH')[0]
            conn = rpyc.connect(ip, port, config={'sync_request_timeout': 240})
            srch = conn.root
            results = srch.search(keyword)

            for file_name, news_item in results:
                print(
                    f'Notícia encontrada no arquivo {file_name}:\n'
                    f'{news_item['title']}\n'
                    f'{news_item['maintext']}\n'
                    f'Link: {news_item['url']}\n'
                )

        case '2':
            pass

        case '3':
            file_path = input(f'Digite o caminho do arquivo a ser inserido:\n')
            with open(file_path, mode='r') as f:
                ip, port = r.discover('INSERT')[0]
                conn = rpyc.connect(ip, port, config={'allow_public_attrs': True, 'sync_request_timeout': 240})
                m = conn.root
                m.insert(f)
                conn.close()
        case '4':
            pass
        case other:
            print(f'{other} não é uma opção válida.\n')

# c.root