import os
import pika
import json
import sys
from datetime import datetime
import time

# Obtém a pasta raiz do projeto de forma dinâmica
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
settings_path = os.path.join(BASE_DIR, 'settings.json')
credentials_path = os.path.join(BASE_DIR, '.credentials.json')

# --- CARREGAR CONFIGURAÇÕES ---
def load_config():
    """
    Carrega configurações e credenciais para o ambiente Satcom.
    """
    try:
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)

        if "satcom" not in settings or "satcom" not in credentials:
            print("[ERRO] Configuração 'satcom' não encontrada em settings.json ou .credentials.json.")
            sys.exit(1)
        
        config = settings["satcom"]
        # A URL já tem as credenciais, mas adicionamos para consistência se necessário
        config["USER"] = credentials["satcom"]["USER"]
        config["PASSWORD"] = credentials["satcom"]["PASSWORD"]
        
        print("Ambiente: satcom (Kezpo)")
        return config
    
    except FileNotFoundError as e:
        print(f"[ERRO] Arquivo de configuração não encontrado: {e.filename}")
        sys.exit(1)
    except json.JSONDecodeError:
        print("[ERRO] Um dos arquivos de configuração contém um JSON inválido.")
        sys.exit(1)
    except KeyError as e:
        print(f"[ERRO] Chave ausente em um dos arquivos de configuração: {e}")
        sys.exit(1)

def main():
    config = load_config()
    queue_name = config['QUEUE_NAME']
    
    # Usar URLParameters para AMQPS e vhost
    parameters = pika.URLParameters(config['URL'])

    connection = None
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        print(f"[*] Verificando se a fila '{queue_name}' existe...")
        channel.queue_declare(queue=queue_name, passive=True)
        print(f"[*] Verificação bem-sucedida. A fila existe.")

        print("\n⚠️ AVISO: Esta fila é round-robin. Consumir mensagens aqui significa que outros sistemas NÃO as receberão.")
        print("         Use este monitor com CAUTELA e APENAS para observação pontual.")
        print("         Pressione CTRL+C para parar a qualquer momento.")
        print(f"[*] Aguardando por mensagens na fila '{queue_name}'.")

        def callback(ch, method, properties, body):
            message_body = body.decode('utf-8')
            print(f"\n--- Nova Mensagem Recebida ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")
            print(f"Fila: {queue_name}")
            
            try:
                satcom_data = json.loads(message_body)
                print("--- Dados Satcom (Kezpo) ---")
                
                position = satcom_data.get("position", {})
                ts = position.get("ts")
                lat = position.get("latitude")
                lon = position.get("longitude")
                speed = position.get("speed")
                vehicle_id = satcom_data.get("vehicleId")
                plate = satcom_data.get("plate")

                print(f"  Vehicle ID: {vehicle_id}")
                print(f"  Plate: {plate}")
                if ts:
                    print(f"  Timestamp: {datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Latitude: {lat}")
                print(f"  Longitude: {lon}")
                print(f"  Velocidade: {speed} km/h")
                print(f"  JSON Completo: {json.dumps(satcom_data, indent=2)}")
            except json.JSONDecodeError:
                print("  [ERRO] Mensagem Satcom não é um JSON válido.")
                print(f"  Corpo da Mensagem Bruta: {message_body}")
            except Exception as e:
                print(f"  [ERRO] Ocorreu um erro ao processar a mensagem Satcom: {e}")
            
            # Confirma o recebimento para remover da fila (modo contínuo, round-robin)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"[ERRO DE CONEXÃO RabbitMQ para Satcom] Verifique suas credenciais, URL e conexão de rede: {e}")
    except KeyboardInterrupt:
        print("\n[*] Consumo interrompido pelo usuário.")
    except Exception as e:
        print(f"[ERRO INESPERADO] Ocorreu um erro: {e}")
    finally:
        if connection and connection.is_open:
            print("[*] Fechando conexão RabbitMQ.")
            connection.close()

if __name__ == '__main__':
    main()