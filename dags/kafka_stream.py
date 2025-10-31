import uuid
import hashlib
import json
import time
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from confluent_kafka import Producer

#configurations
API_ENDPOINT = "https://randomuser.me/api/"
KAFKA_BOOTSTRAP_SERVERS = ['broker:29092']
KAFKA_TOPIC = 'streaming_data'
MAX_RECORDS = 1000
SLEEP_INTERVAL = 0.1 


default_args = {
    'owner': 'huybui04',
    'start_date': datetime(2025, 10, 27, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def get_data(url=API_ENDPOINT):
    max_retries = 5
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            if res.status_code == 200:
                res = res.json()['results'][0]
                return res
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to get data after {max_retries} attempts.")
                raise


def format_data(res):
    location = res['location']
    return {
        "id": str(uuid.uuid4()),
        "first_name": res['name']['first'],
        "last_name": res['name']['last'],
        "gender": res['gender'],
        "address": f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        "post_code": encrypt_zip(location['postcode']),  
        "email": res['email'],
        "user_name": res['login']['username'],
        "dob": res['dob']['date'],
        "registered_date": res['registered']['date'],
        "phone": res['phone'],
        "picture": res['picture']['medium']
    }

def encrypt_zip(zip_code):
    """Hashes the zip code using MD5 and returns its integer representation."""
    zip_str = str(zip_code)
    return int(hashlib.md5(zip_str.encode()).hexdigest(), 16)

def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS):
    """Creates and returns a Confluent Kafka Producer instance."""
    settings = {
        'bootstrap.servers': ','.join(servers),
        'client.id': 'airflow_producer'
    }
    return Producer(settings)


def delivery_status(err, msg):
    """Callback to report Kafka delivery status."""
    if err is not None:
        logging.error(f" Message delivery failed: {err}")
    else:
        logging.info(f" Message delivered to {msg.topic()} [partition {msg.partition()}]")


def stream_data():
    """Fetches, formats, and streams random user data to Kafka."""
    producer = configure_kafka()
    count = 0

    logging.info(" Starting to stream data to Kafka...")

    while count < MAX_RECORDS:
        try:
            res = get_data()
            formatted_data = format_data(res)

            producer.produce(
                topic=KAFKA_TOPIC,
                value=json.dumps(formatted_data).encode('utf-8'),
                callback=delivery_status
            )
            producer.poll(0)  # Trigger delivery report callbacks
            count += 1
            logging.info(f"Sent record {count}/{MAX_RECORDS}")

            time.sleep(SLEEP_INTERVAL)

        except Exception as e:
            logging.error(f" Error occurred while streaming: {e}")
            continue

    producer.flush()
    logging.info(" Completed streaming data to Kafka.")


with DAG(
    dag_id='project_streaming',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Stream RandomUser API data to Kafka using Confluent Kafka producer',
) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_task',
        python_callable=stream_data,
    )

    streaming_task
