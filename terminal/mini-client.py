import pika
import json
from datetime import datetime
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


class RabbitMQConsumer:
    """
    Classe para consumir mensagens de uma fila RabbitMQ da Maxtrack.
    """
    def __init__(self, host, port, user, password, queue):
        self.credentials = pika.PlainCredentials(user, password)
        self.parameters = pika.ConnectionParameters(host=host, port=port, credentials=self.credentials)
        self.queue_name = queue
        self.connection = None
        self.channel = None
        

    def _callback(self, ch, method, properties, body):
        """
        Webhook - Executa sempre que uma mensagem é recebida da fila.
        """

        # Garante que 'headers' seja um dicionário, mesmo se a mensagem vier sem cabeçalhos
        headers = properties.headers if properties.headers is not None else {}
        message_type = headers.get("Message-type")

        # Decodifica o body de bytes para string
        message_body = body.decode('utf-8')

        print(f"\n--- Nova Mensagem Recebida ---")
        print(f"Fila: {self.queue_name}")
        print(f"Tipo da Mensagem (MT): {message_type}")



        if message_type == '1': # '1' corresponde a 'MultipleReportData' [253]
            try:
                position = json.loads(message_body)
                
                # Data
                timestamp = position.get("newReportData", {}).get("dateTime", 0)
                dev_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                
                # Posição
                pos_info = position.get("newReportData", {}).get("positionInfo", [{}])[0]
                latitude = pos_info.get("latitude")
                longitude = pos_info.get("longitude")
                
                # Imprime os dados
                print(f"  ID do Dispositivo: {position.get('deviceID')}")
                print(f"  ID do Pacote: {position.get('packetID')}")
                print(f"  Data e Hora: {dev_date}")
                print(f"  Latitude: {latitude}")
                print(f"  Longitude: {longitude}")

            except json.JSONDecodeError:
                print("  [ERRO] Não foi possível decodificar o corpo da mensagem JSON.")
            except Exception as e:
                print(f"  [ERRO] Ocorreu um erro ao processar a mensagem: {e}")
        else:
            print(f"  Mensagem do tipo {message_type} recebida, mas não processada por este script.")
            print(f"  Corpo da Mensagem: {message_body}")
        
        # ACK - Confirma o recebimento depois de processar a msg 
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def start_consuming(self):
        """
        Abre a conexão e começa a ouvir a fila.
        """
        try:
            # Cria a conexão e um canal
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()

            # Testa se a fila existe - Caso contrário, cria a fila          
            try: 
                print(f"[*] Verificando se a fila '{self.queue_name}' existe...")
                self.channel.queue_declare(queue=self.queue_name, passive=True)
                print(f"[*] Verificação bem-sucedida. A fila existe.")
            except pika.exceptions.ChannelClosedByBroker as e:
                    print(f"[ERRO] A fila '{self.queue_name}' não existe. Reconectando ao canal e criando a fila...")
                    self.connection.close()
                    # Recria a conexão e o canal
                    self.connection = pika.BlockingConnection(self.parameters)
                    self.channel = self.connection.channel()
                    self.channel.queue_declare(queue=self.queue_name, durable=True)

            # Configura o webhook da fila, sem 'auto_ack'
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._callback,
                auto_ack=False # Enviar o ACK só após o processamento (manualmente)
            )

            print(f"[*] Aguardando por mensagens na fila '{self.queue_name}'. Pressione CTRL+C para sair.")
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[ERRO DE CONEXÃO] Verifique suas credenciais e o endereço do servidor: {e}")
        except KeyboardInterrupt:
            print("\n[*] Consumo interrompido pelo usuário.")
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            print(f"[ERRO INESPERADO] Ocorreu um erro: {e}")


def main():
    """
    main() Carrega as configs e inicia o cliente.
    """
    # Carrega as configurações dos arquivos JSON
    config = load_config()

    # Instancia o consumidor passando as configurações carregadas
    consumer = RabbitMQConsumer(
        host=config['SERVER_ADDRESS'],
        port=config['SERVER_PORT'],
        user=config['USER'],
        password=config['PASSWORD'],
        queue=config['QUEUE_NAME']
    )
    consumer.start_consuming()

if __name__ == '__main__':
    # print("Verifique se a biblioteca 'pika' está instalada. Execute: pip install pika")
    main()