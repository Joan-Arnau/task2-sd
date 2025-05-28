import pika
import sys
import os

# Add the parent directory of 'conf' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conf import conf

# Configuration from conf.py
RABBITMQ_HOST = conf.RABBITMQ_HOST
RABBITMQ_QUEUE = conf.RABBITMQ_QUEUE
RABBITMQ_USER = conf.RABBITMQ_USER
RABBITMQ_PASS = conf.RABBITMQ_PASS

def send_message(text_to_send):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=text_to_send,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(f" [x] Sent '{text_to_send}' to queue '{RABBITMQ_QUEUE}'")
        connection.close()
    except Exception as e:
        print(f"Error sending message: {e}")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        message = " ".join(sys.argv[1:])
        sentences = [sentence.strip() for sentence in message.split('.') if sentence.strip()]
        for sentence in sentences:
            send_message(sentence)
    else:
        # Send some example messages if none are specified
        print("Sending example messages...")
        send_message("Aquest es un text normal")
        send_message("Quin dia mes lleig i tonto")
        send_message("Ets un capsigrany!")