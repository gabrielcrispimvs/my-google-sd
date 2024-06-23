import rpyc
from rpyc.utils.registry import TCPRegistryClient

r = rpyc.utils.registry.TCPRegistryClient('localhost', port=18811)

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
            servers = registry.discover('SEARCH')

            results = []

            for ip, port in servers:
                conn = rpyc.connect(ip, port)
                results += conn.root.search(keyword)
    
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
                m = rpyc.connect(ip, port, config={'allow_public_attrs': True}).root
                m.insert(f)
        case '4':
            pass
        case other:
            print(f'{other} não é uma opção válida.\n')

# c.root