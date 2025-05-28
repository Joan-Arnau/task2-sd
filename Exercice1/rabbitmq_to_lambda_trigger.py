import pika
import json
import boto3
import os
import sys

# Add the parent directory of 'conf' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conf import conf

# Configuration from conf.py
RABBITMQ_HOST = conf.RABBITMQ_HOST
RABBITMQ_QUEUE = conf.RABBITMQ_QUEUE
RABBITMQ_USER = conf.RABBITMQ_USER
RABBITMQ_PASS = conf.RABBITMQ_PASS
LAMBDA_FUNCTION_NAME = conf.LAMBDA_FUNCTION_NAME
AWS_REGION = conf.AWS_REGION

# Lambda client
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def invoke_filter_lambda(text_message_body):
    """
    Invokes the Lambda function InsultFilterWorkerLambda asynchronously.
    """
    # The payload expected by the Lambda
    payload_for_lambda = json.dumps({'text_to_filter': text_message_body})
    
    try:
        print(f"Invoking Lambda '{LAMBDA_FUNCTION_NAME}' with payload: {payload_for_lambda}")
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType='Event',  # Asynchronous. We do not wait for the Lambda response here. Synchronous would be 'RequestResponse'.
            Payload=payload_for_lambda
        )
        # For 'Event', StatusCode 202 means the request has been accepted for processing.
        print(f"Lambda invocation requested. AWS Response StatusCode: {response.get('StatusCode')}")
    except Exception as e:
        print(f"ERROR invoking Lambda '{LAMBDA_FUNCTION_NAME}': {e}")

def callback_process_message(channel, method, properties, body):
    try:
        message_body_str = body.decode('utf-8')
        print(f" [OK] Received from RabbitMQ: '{message_body_str}'")
        
        invoke_filter_lambda(message_body_str) # Send to Lambda for filtering
        
        channel.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge the message to RabbitMQ
        print(f" [OK] Message acknowledged to RabbitMQ.")
        
    except Exception as e:
        print(f"ERROR processing message: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_rabbitmq_consumer():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True) 
        print(f"[*] Waiting for messages on queue '{RABBITMQ_QUEUE}'. To exit press CTRL+C")

        # Process one message at a time
        channel.basic_qos(prefetch_count=1) 

        channel.basic_consume(
            queue=RABBITMQ_QUEUE, 
            on_message_callback=callback_process_message
        )

        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"CRITICAL: Could not connect to RabbitMQ at {RABBITMQ_HOST}. Error: {e}")
        print("Please check RabbitMQ server, network, and credentials.")
    except KeyboardInterrupt:
        print("Consumer stopped manually.")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
            print("RabbitMQ connection closed.")

if __name__ == '__main__':
    start_rabbitmq_consumer()