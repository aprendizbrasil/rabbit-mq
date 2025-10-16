# terminal/send.py

import pika
import json
import sys

# --- CARREGAR CONFIGURAÇÕES ---
def load_config():
    """
    Carrega configurações e credenciais.
    """
    try:
        with open('../settings.json', 'r') as f:
            settings = json.load(f)
        with open('../.credentials.json', 'r') as f:
            credentials = json.load(f)

        # Mapeia opções numéricas para nomes de ambiente
        env_map = {
            "1": "maxxtrack",
            "2": "homologacao",
            "3": "local"
        }

        print("Escolha o ambiente de conexão:")
        print(env_map)
        env_choice = input("Digite 1, 2 ou 3: ").strip()
        if env_choice not in env_map:
            print("[ERRO] Escolha inválida. Encerrando o programa.")
            sys.exit(1) 
        
        env_name = env_map[env_choice]
        settings = settings[env_name]
        credentials = credentials[env_name] 
        print(f"Ambiente: {env_name}")

        # Remove "https://" se existir 
        settings['SERVER_ADDRESS'] = settings['SERVER_ADDRESS'].replace('https://', '').replace('http://', '')
            
        # Combina as info em um só 'config'
        config = {**settings, **credentials}
        return config
    
        # Trata erros comuns
    except FileNotFoundError as e:
        print(f"[ERRO] Arquivo de configuração não encontrado: {e.filename}")
        print("Certifique-se que os arquivos 'settings.json' e 'credentials.json' estão na mesma pasta do script.")
        sys.exit(1) # Encerra o programa
    except json.JSONDecodeError:
        print("[ERRO] Um dos arquivos de configuração contém um JSON inválido.")
        sys.exit(1) # Encerra o programa
    except KeyError as e:
        print(f"[ERRO] Chave ausente em um dos arquivos de configuração: {e}")
        sys.exit(1)


def main():
    """
    main() Carrega as configs e inicia o sender.
    """
    # Carrega as configurações dos arquivos JSON
    config = load_config()

    # Carrega os parâmetros de conexão
    host = config['SERVER_ADDRESS']
    port = config['SERVER_PORT']
    user = config['USER']
    password = config['PASSWORD']
    queue = config['QUEUE_NAME']
    print(f"Conectando a {host}:{port}, fila: {queue}, user: {user}, password: {password}")

    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host='/',
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    try:
        print(f"[*] Verificando se a fila '{queue}' existe...")
        channel.queue_declare(queue=queue, passive=True)
        print(f"[*] Verificação bem-sucedida. A fila existe.")

    except pika.exceptions.ChannelClosedByBroker as e:
        print(f"[ERRO] A fila '{queue}' não existe. Reconectando ao canal e criando a fila...")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)

    msg = 'Hello World!'
    while msg != '':
        channel.basic_publish(exchange='', 
                            routing_key=queue, 
                            body=msg,
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # Persiste a mensagem na fila
                            ))
            
        print(f" [x] Sent '{msg}'")
        msg = input("Digite uma nova mensagem ou 'Enter' para sair... ")

    connection.close()

if __name__ == '__main__':
    main()