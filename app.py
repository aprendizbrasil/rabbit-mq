# app.py
import eventlet
eventlet.monkey_patch()  # Deixa o Flask-SocketIO funcionar bem com threads

from flask import Flask, render_template
from flask_socketio import SocketIO
import pika
import threading
import json
from datetime import datetime




app = Flask(__name__)
socketio = SocketIO(app, async_mode="eventlet")

# Carrega configurações
with open('.credentials.json') as f:
    cred = json.load(f)
with open('settings.json') as f:
    config = json.load(f)

def start_rabbitmq_listener(env_name):
    connection = None
    try:
        if env_name == "satcom":
            url = config[env_name]['URL']
            parameters = pika.URLParameters(url)
        else:
            user = cred[env_name]['USER']
            password = cred[env_name]['PASSWORD']
            server = config[env_name]['SERVER_ADDRESS']
            port = config[env_name]['SERVER_PORT']
            queue = config[env_name]['QUEUE_NAME']
            vhost = config[env_name].get('VHOST', '/')

            credentials = pika.PlainCredentials(user, password)
            parameters = pika.ConnectionParameters(server, port, vhost, credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        queue_name = config[env_name]['QUEUE_NAME']

        try:
            print(f"[*] Verificando se a fila '{queue_name}' existe...")
            channel.queue_declare(queue=queue_name, passive=True)
            print(f"[*] Verificação bem-sucedida. A fila existe.")
        except pika.exceptions.ChannelClosedByBroker:
            print(f"[ERRO] A fila '{queue_name}' não existe. Criando a fila...")
            channel.queue_declare(queue=queue_name, durable=True)

        def callback(ch, method, properties, body):
            # Decodifica o corpo da mensagem
            message_body = body.decode('utf-8')
            msg_to_display = f"Ambiente: {env_name} - {message_body}"
            print(f" [x] Received {msg_to_display}")

            # Tenta parsear como JSON para exibir formatado
            try:
                json_msg = json.loads(message_body)
                # Se for Satcom, formata a posição
                if env_name == "satcom" and "position" in json_msg:
                    pos_data = json_msg["position"]
                    timestamp = pos_data.get("ts") # Exemplo, ajuste conforme a chave real
                    latitude = pos_data.get("latitude")
                    longitude = pos_data.get("longitude")
                    # Adicione outros campos relevantes

                    formatted_msg = {
                        "datetime": datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'N/A',
                        "latitude": latitude,
                        "longitude": longitude,
                        "raw": json_msg
                    }
                    msg_to_display = f"Satcom Pos: {formatted_msg['latitude']}, {formatted_msg['longitude']} @ {formatted_msg['datetime']}"
                else:
                    # Outros JSONs ou JSON da Satcom sem "position"
                    msg_to_display = json.dumps(json_msg, indent=2)
            except json.JSONDecodeError:
                # Se não for JSON, exibe como texto puro
                pass # Já está em message_body
            
            socketio.emit('nova_mensagem', {"ambiente": env_name, "msg": msg_to_display})
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[ERRO DE CONEXÃO RabbitMQ para {env_name}] {e}")
        socketio.emit('status_conexao', {'ambiente': env_name, 'status': 'Erro de Conexão', 'error': str(e)})
    except Exception as e:
        print(f"[ERRO INESPERADO no listener para {env_name}] {e}")
        socketio.emit('status_conexao', {'ambiente': env_name, 'status': 'Erro', 'error': str(e)})
    finally:
        if connection and connection.is_open:
            connection.close()

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('selecionar_ambiente')
@socketio.on('selecionar_ambiente')
def handle_ambiente(data):
    env_name = data['ambiente']
    print(f" [x] Conectado ao ambiente {env_name}")
    socketio.emit('status_conexao', {'ambiente': env_name, 'status': 'Conectando...'})
    threading.Thread(target=start_rabbitmq_listener, args=(env_name,), daemon=True).start()

if __name__ == '__main__':
    socketio.run(app, host="127.0.0.1", port=5000, debug=True)
    #socketio.run(app, debug=True)
