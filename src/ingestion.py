import csv
import os
import requests
import hashlib
import ast
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from comics import simplify_comics_data, simplify_comics_from_characters, simplify_comics_from_creators
from events import simplify_events_data, simplify_events_from_characters
from characters import simplify_character_data
from creators import simplify_creators_data
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

        print("length is: ", len(response.json()['data']['results']))

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
        print("Oops: Something Else", err)

    return comics


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

def ingest_events_from_characters(http, char_id, offset, limit):
    events_list = []
    try:
        response = http.get(generate_url("characters/{id}/events".format(id=char_id), limit=100), params={'orderBy': 'name', 'offset': offset})
        events_list.append(response.json()['data']['results'])
        print(char_id, 'first call has ', len(response.json()['data']['results']))
        if response.json()['data']['count'] > limit:
            offset = offset + limit
            while True:
                response = http.get(generate_url("characters/{id}/events".format(id=char_id), limit=100),
                                    params={'orderBy': 'name', 'offset': offset})
                print(char_id, 'second call has ', len(response.json()['data']['results']))
                if response.json()['data']['results']:
                    offset = offset + limit
                    events_list.append(response.json()['data']['results'])
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

    return events_list


def extract_and_save_events_from_characters(limit=100):
    ids = ast.literal_eval(read_file("data/characters_ids_for_events_ingestion.txt"))
    http = retries_session()
    # offset = read_checkpoint()
    offset = 0
    for char_id in ids:
        comics = ingest_events_from_characters(http=http, char_id=char_id, offset=offset, limit=100)

        events_from_characters_simplified = [simplify_events_from_characters(char_id, x) for x in comics[0]]
        store_to_csv(events_from_characters_simplified, 'characters_in_events_fetched')

## events

def ingest_events(limit=100, offset=0):
    events = []

    url = generate_url('events', limit)
    http = retries_session()

    try:
        response = http.get(url, params={'orderBy': 'name', 'offset': offset})

        print("length is: ", len(response.json()['data']['results']))

        if not response.json()['data']['results']:
            print('no data anymore')
            return False

        events.append(response.json())

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)

    return events


def extract_and_save_events_data(limit=100, offset=0):
    count = 0
    # offset = read_checkpoint()
    while True:
        events = ingest_events(limit=limit, offset=offset)
        # save_checkpoint(offset)
        print(events)
        if not events:
            break

        events_simplified = [simplify_events_data(x) for x in events[0]['data']['results']]
        store_to_csv(events_simplified, 'events')
        print("request number", count)
        count += 1
        offset = offset + limit


# creators


def ingest_creators(limit=100, offset=0):
    creators = []

    url = generate_url('creators', limit)
    http = retries_session()

    try:
        response = http.get(url, params={'orderBy': 'lastName', 'offset': offset})

        print("length is: ", len(response.json()['data']['results']))

        if not response.json()['data']['results']:
            print('no data anymore')
            return False

        creators.append(response.json())

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)

    return creators


def extract_and_save_creators_data(limit=100, offset=0):
    count = 0
    # offset = read_checkpoint()
    while True:
        creators = ingest_creators(limit=limit, offset=offset)
        # save_checkpoint(offset)
        if not creators:
            break

        creators_simplified = [simplify_creators_data(x) for x in creators[0]['data']['results']]
        store_to_csv(creators_simplified, 'creators')
        print("request number", count)
        count += 1
        offset = offset + limit



def ingest_comics_from_creators(http, creator_id, offset, limit):
    comics_list = []
    try:
        response = http.get(generate_url("creators/{id}/comics".format(id=creator_id), limit=100), params={'orderBy': 'title', 'offset': offset})
        comics_list.append(response.json()['data']['results'])
        print(creator_id, 'first call has ', len(response.json()['data']['results']))
        if response.json()['data']['total'] > limit:
            offset = offset + limit
            while True:
                response = http.get(generate_url("creators/{id}/comics".format(id=creator_id), limit=100),
                                    params={'orderBy': 'title', 'offset': offset})
                print(creator_id, 'second call has ', len(response.json()['data']['results']))
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


def extract_and_save_comics_from_creators(limit=100):
    ids = ast.literal_eval(read_file("data/creator_ids_for_comics_ingestion.txt"))

    http = retries_session()
    # offset = read_checkpoint()
    offset = 0
    for creator_id in ids:
        comics = ingest_comics_from_creators(http=http, creator_id=creator_id, offset=offset, limit=100)

        comics_from_creators_simplified = [simplify_comics_from_creators(creator_id, x) for x in comics[0]]
        print(comics_from_creators_simplified)
        store_to_csv(comics_from_creators_simplified, 'creators_in_comics_fetched')
        save_checkpoint(creator_id)

extract_and_save_comics_from_creators()