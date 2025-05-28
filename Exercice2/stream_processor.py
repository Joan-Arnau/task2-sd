import pika
import json
import boto3
import concurrent.futures
import threading
import argparse
import sys
import os

# Add the parent directory of 'conf' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conf import conf

# --- Configuration from conf.py ---
RABBITMQ_HOST = conf.RABBITMQ_HOST
RABBITMQ_USER = conf.RABBITMQ_USER
RABBITMQ_PASS = conf.RABBITMQ_PASS
LAMBDA_AWS_REGION = conf.AWS_REGION

# Event to signal shutdown to all threads
shutdown_event = threading.Event()

# Global channel variable for thread-safe Pika operations
pika_channel = None
pika_connection = None # To access add_callback_threadsafe

lambda_client = boto3.client('lambda', region_name=LAMBDA_AWS_REGION)

def invoke_processing_lambda(lambda_name, message_body_str):
    """Invokes the specified Lambda function asynchronously."""
    payload = json.dumps({'text_to_filter': message_body_str}) # Assumes this payload structure

    try:
        print(f"Thread {threading.get_ident()}: Invoking Lambda '{lambda_name}' with payload: {message_body_str[:60]}...")
        lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType='Event',  # Asynchronous invocation
            Payload=payload
        )
        print(f"Thread {threading.get_ident()}: Lambda '{lambda_name}' invocation request sent.")
        return True
    except Exception as e:
        print(f"Thread {threading.get_ident()}: ERROR invoking Lambda '{lambda_name}': {e}")
        return False

def message_processing_task(delivery_tag, message_body_str, processing_lambda_name):
    """
    Task executed by a worker thread from the pool.
    It processes one message: invokes Lambda, then schedules Ack/Nack.
    """
    global pika_connection, pika_channel # Access global Pika objects

    print(f"Thread {threading.get_ident()}: Starting processing for message ID {delivery_tag}: {message_body_str[:60]}...")

    if invoke_processing_lambda(processing_lambda_name, message_body_str):
        print(f"Thread {threading.get_ident()}: Lambda invoked successfully for {delivery_tag}. Scheduling Ack.")
        if pika_connection and pika_connection.is_open and pika_channel and pika_channel.is_open:
            pika_connection.add_callback_threadsafe(lambda: pika_channel.basic_ack(delivery_tag=delivery_tag))
        else:
            print(f"Thread {threading.get_ident()}: Pika connection/channel closed. Cannot Ack message {delivery_tag}.")
    else:
        print(f"Thread {threading.get_ident()}: Lambda invocation failed for {delivery_tag}. Scheduling Nack (requeue=False).")
        if pika_connection and pika_connection.is_open and pika_channel and pika_channel.is_open:
            pika_connection.add_callback_threadsafe(lambda: pika_channel.basic_nack(delivery_tag=delivery_tag, requeue=False))
        else:
            print(f"Thread {threading.get_ident()}: Pika connection/channel closed. Cannot Nack message {delivery_tag}.")

def stream_operation(
    processing_lambda_name: str,
    max_concurrent_invokers: int,
    queue_name: str,
    rabbitmq_host: str,
    rabbitmq_user: str,
    rabbitmq_pass: str
):
    """
    Implements the primitive stream operation.
    - 'processing_lambda_name': Name of the AWS Lambda function to call.
    - 'max_concurrent_invokers': Max number of messages this script will process concurrently by invoking Lambdas.
    - 'queue_name': The RabbitMQ queue to consume from.
    - 'rabbitmq_host', 'rabbitmq_user', 'rabbitmq_pass': RabbitMQ connection details.
    """
    global pika_connection, pika_channel # Use global Pika objects

    # ThreadPoolExecutor to manage concurrent message processing tasks
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_invokers)

    try:
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        # Increased heartbeat for potentially long-idling connections
        params = pika.ConnectionParameters(
            host=rabbitmq_host,
            credentials=credentials,
            heartbeat=600,  # Heartbeat interval in seconds. If someone is idle for this long, the connection will be closed.
            blocked_connection_timeout=300 # Timeout for blocked connection
        )
        pika_connection = pika.BlockingConnection(params)
        pika_channel = pika_connection.channel()

        pika_channel.queue_declare(queue=queue_name, durable=True) # Ensure queue exists and is durable
        # QOS: Prefetch 'max_concurrent_invokers' messages. The client will buffer up to this many messages.
        # Each will be processed by a worker in the ThreadPoolExecutor when its callback is invoked.
        pika_channel.basic_qos(prefetch_count=max_concurrent_invokers)

        print(f"[*] Stream operation started for RabbitMQ queue '{queue_name}'.")
        print(f"[*] Max concurrent local invokers (maxfunc): {max_concurrent_invokers}.")
        print(f"[*] Target Lambda function: '{processing_lambda_name}'.")
        print("[*] Waiting for messages. Press CTRL+C to exit gracefully.")

        def on_message_callback(ch, method, properties, body):
            """Callback executed by Pika when a message is received."""
            if shutdown_event.is_set():
                # If shutting down, try to Nack and requeue the message so it's not lost.
                print(f"Shutdown initiated. Nacking and requeueing message ID {method.delivery_tag}.")
                try:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as e_nack:
                    print(f"Error Nacking message during shutdown: {e_nack}")
                return

            message_body_str = body.decode('utf-8') # Decode message body
            print(f"Pika I/O Thread: Received message ID {method.delivery_tag}. Submitting to processing pool.")
            # Submit the actual processing to the thread pool.
            # The Pika callback itself should be lightweight and non-blocking.
            executor.submit(
                message_processing_task,
                method.delivery_tag,
                message_body_str,
                processing_lambda_name
            )

        pika_channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)

        # Keep the main thread alive to run Pika's I/O loop (process_data_events)
        # This loop also allows checking the shutdown_event periodically.
        while not shutdown_event.is_set():
            try:
                # Process Pika events with a timeout.
                # This drives message delivery and execution of threadsafe callbacks.
                if pika_connection and pika_connection.is_open:
                    pika_connection.process_data_events(time_limit=1)  # Timeout of 1 second
                else:
                    print("Pika connection is closed. Exiting Pika event loop.")
                    break # Connection lost or closed, exit loop.
            except pika.exceptions.StreamLostError: # More specific than AMQPConnectionError for this context
                print("ERROR: RabbitMQ connection lost (StreamLostError). Attempting to stop.")
                break # Exit loop
            except pika.exceptions.AMQPHeartbeatTimeout:
                print("ERROR: RabbitMQ heartbeat timeout. Attempting to stop.")
                break # Exit loop
            except Exception as e_loop:
                print(f"ERROR in Pika event loop: {type(e_loop).__name__} - {e_loop}. Attempting to stop.")
                break # Exit on other unexpected errors

    except pika.exceptions.AMQPConnectionError as e_conn:
        print(f"CRITICAL ERROR: Could not connect to RabbitMQ at '{rabbitmq_host}'. Details: {e_conn}")
    except KeyboardInterrupt:
        print("\nCTRL+C detected by user. Initiating graceful shutdown...")
    except Exception as e_setup:
        print(f"CRITICAL ERROR during setup: {e_setup}")
    finally:
        print("Initiating shutdown sequence for stream operation...")
        shutdown_event.set() # Signal all threads and loops to stop

        # Stop consuming new messages from RabbitMQ
        if pika_channel and pika_channel.is_open:
            try:
                pika_channel.stop_consuming()
                print("Pika consumption stopped successfully.")
            except Exception as e_cancel:
                print(f"Info: Exception during consumer stop attempt: {e_cancel}")

        print("Shutting down ThreadPoolExecutor (waiting for active tasks to complete)...")
        executor.shutdown(wait=True) # Allow currently running tasks to finish
        print("ThreadPoolExecutor shut down.")

        if pika_connection and pika_connection.is_open:
            print("Closing RabbitMQ connection...")
            try:
                pika_connection.close()
                print("RabbitMQ connection closed.")
            except Exception as e_close:
                print(f"ERROR closing RabbitMQ connection: {e_close}")
        
        # Clear global references
        pika_channel = None
        pika_connection = None
        print("Stream operation has finished.")

# --- Example Usage ---
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Stream processor for RabbitMQ -> AWS Lambda")
    parser.add_argument('--function', help=f'AWS Lambda function name, default is {conf.LAMBDA_FUNCTION_NAME}', default=conf.LAMBDA_FUNCTION_NAME)
    parser.add_argument('--maxfunc', type=int, help='Max parallel message processing, default is 2', default=2)
    parser.add_argument('--queue', help=f'RabbitMQ queue name, default is {conf.RABBITMQ_QUEUE}', default=conf.RABBITMQ_QUEUE)

    args = parser.parse_args()
    # Configuration for the stream operation
    PROCESSING_LAMBDA_NAME = args.function
    MAX_CONCURRENT_INVOKERS = args.maxfunc
    QUEUE_NAME = args.queue

    print("Starting stream operation script...")
    stream_operation(
        processing_lambda_name=PROCESSING_LAMBDA_NAME,
        max_concurrent_invokers=MAX_CONCURRENT_INVOKERS,
        queue_name=QUEUE_NAME,
        rabbitmq_host=RABBITMQ_HOST,
        rabbitmq_user=RABBITMQ_USER,
        rabbitmq_pass=RABBITMQ_PASS
    )
    print("Stream operation script ended.")