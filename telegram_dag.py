#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example use of Telegram operator.
"""
import logging
import requests
import json
import os
import subprocess
import os
from datetime import datetime
from __future__ import annotations
from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "example_telegram"

CONN_ID = "6426823081:AAH7jOLdVAPSCZZ0bzMN3Py7K19zwJAZWtw" #telegram bot connection id

SAVE_PATH = f"/opt/airflow/dags/files/"

def get_updates_telegram(**kwargs):
    """
    Get updates from telegram bot through request with telegram
    entrypoint and bot_token.
    Files saves in docker volumes, metadata in json
    """
    entry_point = kwargs['entrypoint']
    bot_token = kwargs['token']
    response = requests.get(entry_point + bot_token + f'/getUpdates')

    logging.info(f"Get updates")

    if response.status_code == 200:
        data = json.loads(response.text)

        if data['ok'] and len(data['result']) > 0:
            offset = data['result'][0]['update_id'] + 1
            requests.get(entry_point + bot_token + f'/getUpdates?offset={offset}')
            logging.info(f"Succesfully get data. Saving to file: {SAVE_PATH}data.json")
        with open(f'{SAVE_PATH}data.json', 'w') as f:
            json.dump(data['result'], f)
    else:
        raise ValueError(f'Unable get data')

def get_ocr():
    """
    Run tesseract ocr for received data.
    Check json and if exited read lines and run tesseract
    Files saves in docker volumes, metadata in json
    """
    with open(f'{SAVE_PATH}data.json', 'r') as f:
        data = json.load(f)
    files = []
    for line in data:
        name, from_id = line['path'].split('_')
        subprocess.run(['tesseract', f"{SAVE_PATH}{line['path']}", f"{SAVE_PATH}{name}", "-l", "eng+rus"])
        files.append({"id": f"{from_id}", "path": f"{name}"})

    with open(f'{SAVE_PATH}data.json', 'w') as f:
        json.dump(files, f)

def send_ocr_to_telegram(**kwargs):
    """
    Send recognition text to telegram.
    Check json and if exited send files from docker volume
    for users from json
    """
    entry_point = kwargs['entrypoint']
    bot_token = kwargs['token']
    with open(f'{SAVE_PATH}data.json', 'r') as f:
        data = json.load(f)

    for line in data:
        document = open(f"{SAVE_PATH}{line['path']}.txt", "rb")
        response = requests.post(entry_point + bot_token + "/sendDocument", data={'chat_id': line['id']}, files={'document': document})

def get_files_from_json(**kwargs):
    """
    Get files after check updates from telegram bot.
    Files gets through requests with /getFile command
    """
    entry_file_point = kwargs['entry_file_point']
    entry_point = kwargs['entrypoint']
    bot_token = kwargs['token']
    files = []
    with open(f'{SAVE_PATH}data.json', 'r') as f:
        data = json.load(f)

    for line in data:
        if 'photo' in line['message'].keys():
            file_id = line['message']['photo'][2]['file_id']
            r = requests.post(entry_point + bot_token + '/getFile?file_id=' + file_id)
            if r.status_code == 200:
                file_path = json.loads(r.text)['result']['file_path']
                file = requests.get(entry_file_point + bot_token + '/' + file_path)
                logging.info(f"Save photo to path: {SAVE_PATH}{line['message']['photo'][2]['file_unique_id']}_{line['message']['from']['id']}.{file_path.split('.')[-1]}")
                files.append({'path': f"{line['message']['photo'][2]['file_unique_id']}_{line['message']['from']['id']}.{file_path.split('.')[-1]}"})
                with open(f"{SAVE_PATH}{line['message']['photo'][2]['file_unique_id']}_{line['message']['from']['id']}.{file_path.split('.')[-1]}", 'wb') as f:
                    f.write(file.content)
        elif 'document' in line['message'].keys():
            file_id = line['message']['document']['file_id']
            r = requests.post(entry_point + bot_token + '/getFile?file_id=' + file_id)
            if r.status_code == 200:
                file_path = json.loads(r.text)['result']['file_path']
                file = requests.get(entry_file_point + bot_token + '/' + file_path)
                logging.info(f"Save document to path: {SAVE_PATH}{line['message']['document']['file_name']}_{line['message']['from']['id']}.{file_path.split('.')[-1]}")
                files.append({'path': f"{line['message']['document']['file_name']}_{line['message']['from']['id']}.{file_path.split('.')[-1]}"})
                with open(
                        f"{SAVE_PATH}{line['message']['document']['file_name']}_{line['message']['from']['id']}.{file_path.split('.')[-1]}",
                        'wb') as f:
                    f.write(file.content)

    with open(f'{SAVE_PATH}data.json', 'w') as f:
        json.dump(files, f)

default_arg = {
    'owner': 'mivankin',
    'email': 'mivankin@gmail.com',
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': 60, #seconds
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 16),
    'end_data': datetime(2022, 9, 17)
}

with DAG(DAG_ID,
         schedule_interval='0 7 * * *',
         max_active_runs=2,
         concurrency=4,
         default_args=default_arg,
         tags=["example"]) as dag:

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=get_updates_telegram,
        op_kwargs={
            'entrypoint': 'https://api.telegram.org/bot',
            'token': CONN_ID
        }
    )

    download_files_from_json = PythonOperator(
        task_id='download_files_from_json',
        python_callable=get_files_from_json,
        op_kwargs={
            'entrypoint': 'https://api.telegram.org/bot',
            'entry_file_point': 'https://api.telegram.org/file/bot',
            'token': CONN_ID
        }
    )

    get_and_save_ocr = PythonOperator(
        task_id='get_ocr',
        python_callable=get_ocr
    )

    send_ocr_to_telegram_by_id = PythonOperator(
        task_id='send_ocr_to_telegram',
        python_callable=send_ocr_to_telegram,
        op_kwargs={
            'entrypoint': 'https://api.telegram.org/bot',
            'token': CONN_ID
        }
    )
    # [END howto_operator_telegram]

download_data >> download_files_from_json >> get_and_save_ocr >> send_ocr_to_telegram_by_id
