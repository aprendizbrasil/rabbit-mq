# app.py
import eventlet
eventlet.monkey_patch()  # Deixa o Flask-SocketIO funcionar bem com threads

from flask import Flask, render_template, request
from flask_socketio import SocketIO
import pika
import threading
import json




app = Flask(__name__)
socketio = SocketIO(app, async_mode="eventlet")

# Carrega configurações
with open('.credentials.json') as f:
    cred = json.load(f)
with open('settings.json') as f:
    config = json.load(f)

def start_rabbitmq_listener(env_name):
    user = cred[env_name]['USER']
    password = cred[env_name]['PASSWORD']
    server = config[env_name]['SERVER_ADDRESS']
    port = config[env_name]['SERVER_PORT']
    queue = config[env_name]['QUEUE_NAME']

    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(server, port, '/', credentials)
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
    

    def callback(ch, method, properties, body):
        msg = body.decode()
        print(f" [x] Received {msg}")
        socketio.emit('nova_mensagem', msg, )

    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('selecionar_ambiente')
def handle_ambiente(data):
    env_name = data['ambiente']
    print(f" [x] Conectado ao ambiente {env_name}")
    threading.Thread(target=start_rabbitmq_listener, args=(env_name,), daemon=True).start()

if __name__ == '__main__':
    socketio.run(app, debug=True)
