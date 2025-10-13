# terminal/send.py

import pika
import json

credentials = pika.PlainCredentials('dwith', '123')
parameters = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    virtual_host='/',
    credentials=credentials
)
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()

channel.queue_declare(queue='local')

channel.basic_publish(exchange='', routing_key='local', body='Hello World!')
print(" [x] Sent 'Hello World!'")
connection.close()