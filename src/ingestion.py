import csv
import os
import requests
import hashlib
import ast
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from comics import simplify_comics_data, simplify_comics_from_characters
from characters import simplify_character_data
from helpers import read_file

BASE_URL = "https://gateway.marvel.com:443/v1/public/{type}?ts={time_stamp}&limit={limit}&apikey={api_key}&hash={hash}"


def generate_url(type, limit):
    ts = datetime.now()
    ts = str(ts).replace(' ', '_')
    return BASE_URL.format(type=type, time_stamp=ts, limit=limit, api_key=os.environ['API_KEY'],
                           hash=create_hash_for_login(ts))


def create_hash_for_login(ts):
    keys = str(os.environ['PRIVATE_KEY'] + os.environ['API_KEY'])
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


def save_checkpoint(offset):
    with open("checkpoint.txt", "w", encoding="utf-8") as checkpoint:
        checkpoint.write(str(offset))


def read_checkpoint():
    try:
        with open("checkpoint.txt", "r", encoding="utf-8") as checkpoint:
            content = checkpoint.readline()
            if content:
                return content
            else:
                return 0
    except:
        return 0


def ingest_characters(limit=100):
    # generalise to ingest entity
    characters = []
    x = 0
    url = generate_url('comics')

    response = requests.get(url, params={'orderBy': 'title', 'offset': x})
    print(response.json())

    return characters


def ingest_comics(limit=100, offset=0):
    comics = []

    url = generate_url('characters', limit)
    http = retries_session()

    try:
        response = http.get(url, params={'orderBy': 'name', 'offset': offset})

        print("lenght is: ", len(response.json()['data']['results']))

        if not response.json()['data']['results']:
            print('no data anymore')
            return False

        comics.append(response.json())

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)

    return comics


def store_to_csv(data, type):
    headers = data[0].keys()
    with open('data/{type}.csv'.format(type=type), 'a', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file, fieldnames=headers)
        # fc.writeheader()
        fc.writerows(data)


# char = [simplify_character_data(character) for character in test[0]['data']['results']]
# store_all_characters(char)

def extract_and_save_comics_data(limit=100):
    count = 0
    offset = read_checkpoint()
    while True:
        comics = ingest_comics(limit=limit, offset=offset)
        save_checkpoint(offset)

        if not comics:
            break

        comics_simplified = [simplify_comics_data(x) for x in comics[0]['data']['results']]
        store_to_csv(comics_simplified, 'comics')
        print("request number", count)
        count += 1
        offset = offset + limit


def extract_and_save_characters_data(limit=100):
    count = 0
    offset = read_checkpoint()
    while True:
        characters = ingest_comics(limit=limit, offset=offset)
        save_checkpoint(offset)

        if not characters:
            break

        characters_simplified = [simplify_character_data(x) for x in characters[0]['data']['results']]
        store_to_csv(characters_simplified, 'characters')
        print("request number", count)
        count += 1
        offset = offset + limit


def read_test():

    with open("data/test.txt", "r", encoding="utf-8") as f:
        data = f.readline()

    http = retries_session()
    response = http.get(generate_url(type='characters', limit=100))
    print(response.json())


    # print(test[0]['data']['results'][0]['comics']['available'] == test[0]['data']['results'][0]['comics']['returned'])


def ingest_comics_from_characters(http, char_id, offset, limit):
    comics_list = []
    try:
        response = http.get(generate_url("characters/{id}/comics".format(id=char_id), limit=100), params={'orderBy': 'title', 'offset': offset})
        comics_list.append(response.json()['data']['results'])

        if response.json()['data']['count'] > limit:
            offset = offset + limit
            while True:
                response = http.get(generate_url("characters/{id}/comics".format(id=char_id), limit=100),
                                    params={'orderBy': 'title', 'offset': offset})

                if response.json()['data']['results']:
                    offset = offset + limit
                    comics_list.append(response.json()['data']['results'])
                else:
                    break
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)

    return comics_list



def extract_and_save_comics_from_characters(limit=100):
    ids = ast.literal_eval(read_file("data/characters_ids_for_comics_ingestion.txt"))
    http = retries_session()
    # offset = read_checkpoint()
    offset = 0
    for char_id in ids:
        comics = ingest_comics_from_characters(http=http, char_id=char_id, offset=offset, limit=100)

        comics_from_characters_simplified = [simplify_comics_from_characters(char_id, x) for x in comics[0]]
        store_to_csv(comics_from_characters_simplified, 'characters_in_comics')


# test
# comics_from_characters_simplified = [simplify_comics_from_characters('1009150', x) for x in l[0][0]['results']]
# for x in l[0]:
#     print(x.get('title'))