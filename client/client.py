import rpyc
from rpyc.utils.registry import TCPRegistryClient

registry = TCPRegistryClient('localhost', port=18811)
print(registry.discover('SEARCH'))

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
            pass
        case '4':
            pass
        case other:
            print(f'{other} não é uma opção válida.\n')

# c.root