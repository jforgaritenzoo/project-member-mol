from django.db import close_old_connections
import pika, os, json
from celery import shared_task
from dotenv import load_dotenv
from .models import TaskResult, ConsumedData
from time import time
import logging

load_dotenv()

logger = logging.getLogger(__name__)


@shared_task
def consume_data(data):
    close_old_connections()
    try:
        logger.info(f"Received data for consumption: {data}")
        ConsumedData.objects.create(
            queue_name=data["queue_name"], payload=data["payload"]
        )
        logger.info("Data successfully saved to the database.")
    except Exception as e:
        logger.error(f"Error in consume_data: {e}")
        raise


@shared_task
def consume_from_queue(queue_name):
    params = pika.URLParameters(os.getenv("CLOUDAMQP_URL"))
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    max_messages = 10  # Number of messages to process
    messages_consumed = 0
    timeout = 60  # Consume for 60 seconds
    start_time = time()

    def callback(ch, method, properties, body):
        nonlocal messages_consumed
        try:
            payload = json.loads(body.decode())
            consume_data.delay({"queue_name": queue_name, "payload": payload})
            messages_consumed += 1

            if messages_consumed >= max_messages or (time() - start_time) > timeout:
                channel.stop_consuming()
        except Exception as e:
            print(f"Error processing message: {e}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        connection.close()


@shared_task
def consume_from_queue_stagging(queue_name):

    hostKlik = os.getenv("RMQ_KLIK_HOST")
    user = os.getenv("RMQ_KLIK_USER")
    password = os.getenv("RMQ_KLIK_PASSWORD")
    vhost = os.getenv("RMQ_KLIK_VHOST")

    print(hostKlik, user, password, vhost)

    creds = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=hostKlik,
            credentials=creds,
            virtual_host=vhost,
            port=5672,
            socket_timeout=60,
        )
    )
    channel = connection.channel()

    max_messages = 10  # Number of messages to process
    messages_consumed = 0
    timeout = 60  # Consume for 60 seconds
    start_time = time()

    def callback(ch, method, properties, body):
        nonlocal messages_consumed
        try:
            payload = json.loads(body.decode())
            consume_data.delay({"queue_name": queue_name, "payload": payload})
            messages_consumed += 1

            if messages_consumed >= max_messages or (time() - start_time) > timeout:
                channel.stop_consuming()
        except Exception as e:
            print(f"Error processing message: {e}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        connection.close()


@shared_task
def add(x, y):
    return x + y


@shared_task
def process_data(data):
    # Process the data (replace this with your actual logic)
    processed_data = {"processed": True, "original_data": data}

    # Save result to the database
    TaskResult.objects.create(
        task_id=process_data.request.id,  # Save the Celery task ID
        result_data=json.dumps(processed_data),
    )
    return processed_data
