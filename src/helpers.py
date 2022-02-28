import os
import requests
import hashlib
import csv
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

BASE_URL = "https://gateway.marvel.com:443/v1/public/{type}?ts={time_stamp}&limit={limit}&apikey={api_key}&hash={hash}"

def save_file(data, file_path):
    with open(file_path, "w", encoding="utf-8") as output_file:
        output_file.write(str(data))


def read_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.readline()
    return content


def save_checkpoint(offset):
    with open("checkpoint.txt", "w", encoding="utf-8") as checkpoint:
        checkpoint.write(str(offset))


def store_to_csv(data, entity_type):
    try:
        headers = data[0].keys()
    except Exception as e:
        print("ERROR ENCOUNTERED WHILE EXTRACTING THE HEADERS: ")
        print(e)
    try:
        with open('data/{type}.csv'.format(type=entity_type), 'a', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file, fieldnames=headers)
            # fc.writeheader()
            fc.writerows(data)
    except Exception as e:
        print("ERROR ENCOUNTERED WHILE TRYING TO SAVE YOUR DATA: ")
        print(e)


def read_checkpoint():
    try:
        with open("checkpoint.txt", "r", encoding="utf-8") as checkpoint:
            content = checkpoint.readline()
            if content:
                return content
            else:
                return 0
    except Exception as e:
        print(e)
        return 0


def check_entity_last_update(entity):
    df = pd.read_csv(f'data/{entity}.csv'.format(enitity=entity))
    last_date = df['date_modified'].max()

    return last_date

# for x in ['characters', 'creators', 'comics', 'events']:
#     print(x, check_entity_last_update(x))

def generate_url(type, limit):
    ts = datetime.now()
    ts = str(ts).replace(' ', '_')
    return BASE_URL.format(type=type, time_stamp=ts, limit=limit, api_key=os.environ['API_KEY2'],
                           hash=create_hash_for_login(ts))


def create_hash_for_login(ts):
    keys = str(os.environ['PRIVATE_KEY2'] + os.environ['API_KEY2'])
    hashed = hashlib.md5(str(ts).encode() + keys.encode())

    return hashed.hexdigest()


def retries_session():
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http