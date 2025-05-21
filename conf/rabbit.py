import pika

RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'
RABBITMQ_QUEUE = 'texts_to_filter_queue'


def connect_and_create_queue():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    print(f"Cua '{RABBITMQ_QUEUE}' creada o existent.")
    
    connection.close()

# Executar
connect_and_create_queue()
