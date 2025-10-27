import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'huybui04',
    'start_date':datetime(2025 , 10, 27, 0, 0)
}
def get_data():
    import requests
    import time
    import logging

    max_retries = 5
    retry_delay = 3
    for attemp in range(max_retries):
        try:
            res = requests.get("https://randomuser.me/api/", timeout=10)
            res.raise_for_status()
            res = res.json()
            res = res['results'][0]
            return res
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attemp+1}/{max_retries}: {e}")
            if attemp < max_retries -1 :
                time.sleep(retry_delay)
            else :
                logging.error(f"fail to get data after{max_retries} attempts")
                raise

def format_data(res):
    data ={}
    location = res['location']
    data['id'] =  str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"

    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['user_name'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone']=res['phone']
    data['picture'] = res['picture']['medium']
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms = 5000)
    count = 0
    max_records = 1000

    while count < max_records:
        try :
            res = get_data()
            formatted_data = format_data(res)

            producer.send('streaming_data', json.dumps(formatted_data).encode('utf-8'))
            count +=1
            logging.info(f'Sent user{count}/{max_records} ')


            time.sleep(0.1) # delay 100s
        except Exception as e:
            logging.error(f"An error occured : {e}")
            continue
    logging.info("Compeled streaming data")



with DAG (
    dag_id = 'project_streaming_api',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False,
) as dag :
    streaming_task = PythonOperator(
        task_id = 'streaming_task',
        python_callable = stream_data ,
    )