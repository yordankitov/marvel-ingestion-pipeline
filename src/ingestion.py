import pandas as pd
import requests
import ast
from datetime import datetime

from comics import simplify_comics_data, simplify_comics_from_characters, simplify_comics_from_creators
from events import simplify_events_data, simplify_events_from_characters
from characters import simplify_character_data
from creators import simplify_creators_data
from helpers import create_in_memory_csv, generate_url, retries_session, read_file, save_checkpoint, read_snowflake_checkpoint, store_to_csv
from upload_to_s3 import upload_file


def ingest_entity(limit, offset, entity, order_by, modified):

    url = generate_url(entity, limit)
    http = retries_session()
    another_request = None

    if modified:
        modified_since = modified
    else:
        modified_since = None

    try:
        response = http.get(url, params={'orderBy': order_by, 'offset': offset, 'modifiedSince': modified_since})

        print("length is: ", len(response.json()['data']['results']))

        print("total is ", response.json()['data']['count'])

        total_values = response.json()['data']['offset'] + response.json()['data']['count']
        if response.json()['data']['total'] == total_values:
            print('no data anymore')
            another_request = False
        else:
            another_request = True

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else:", err)

    return response.json(), another_request


def extract_and_save_comics_data(limit, offset, order_by, modified=None):
    count = 0
    # offset = read_checkpoint()
    while True:
        comics, another_request = ingest_entity(limit=limit, offset=offset, entity='comics',
                                                order_by=order_by, modified=modified)

        comics_simplified = [simplify_comics_data(x) for x in comics['data']['results']]
        store_to_csv(comics_simplified, 'comics')
        print("request number", count)
        count += 1
        offset = offset + limit
        if not another_request:
            break


def extract_and_save_characters_data(limit, offset, order_by, modified=None):
    count = 0
    offset = read_snowflake_checkpoint('characters')
    print(offset)
    # while True:
    #     characters, another_request = ingest_entity(limit=limit, offset=offset, entity='characters', order_by=order_by, modified=modified)
    #
    #     characters_simplified = [simplify_character_data(x) for x in characters['data']['results']]
    #     # store_to_csv(characters_simplified, 'characters')
    #     csv_string_object = create_in_memory_csv(characters_simplified)
    #
    #     upload_file('data/characters/characters-{date}.csv'.format(date=str(datetime.now()).replace(' ', '-')), csv_string_object)
    #     print("request number", count)
    #     count += 1
    #     offset = offset + limit
    #     # if not another_request:
    #     #     break
    #     break
# extract_and_save_characters_data(100, 0, 'modified')

def extract_and_save_events_data(limit, offset, order_by, modified=None):
    count = 0
    # offset = read_checkpoint()
    while True:
        events, another_request = ingest_entity(limit=limit, offset=offset, entity='events', order_by=order_by, modified=modified)

        events_simplified = [simplify_events_data(x) for x in events['data']['results']]
        headers = events_simplified[0].keys()

        csv_string_object = create_in_memory_csv(events_simplified, headers)
        upload_file('test.csv', csv_string_object)
        # store_to_csv(events_simplified, 'events')

        print("request number", count)
        count += 1
        offset = offset + limit
        if not another_request:
            break


def extract_and_save_creators_data(limit, offset, order_by, modified=None):
    count = 0
    # offset = read_checkpoint()
    while True:
        creators, another_request = ingest_entity(limit=limit, offset=offset, entity='creators', order_by=order_by, modified=modified)

        # creators_simplified = [simplify_creators_data(x) for x in creators['data']['results']]
        # store_to_csv(creators_simplified, 'creators')
        print("request number", count)
        count += 1
        offset = offset + limit

        if not another_request:
            break


def ingest_events_from_characters(http, char_id, offset, limit, modified=None):
    events_list = []

    if modified:
        modified_since = modified
    else:
        modified_since = None

    try:
        while True:
            response = http.get(generate_url("characters/{id}/events".format(id=char_id), limit=100),
                                params={'orderBy': 'modified', 'offset': offset, 'modifiedSince': modified_since})
            events_list.append(response.json()['data']['results'])
            print(char_id, 'first call has ', len(response.json()['data']['results']))

            total_values = response.json()['data']['offset'] + response.json()['data']['count']
            if response.json()['data']['total'] == total_values:
                offset = offset + limit
            else:
                break

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else:", err)

    return events_list


def extract_and_save_events_from_characters(limit):
    ids = ast.literal_eval(read_file("data/characters_ids_for_events_ingestion.txt"))
    http = retries_session()
    checkpoint = read_checkpoint("../checkpoints/character_id_for_ingesting_events.txt")
    start_index = None

    if checkpoint:
        try:
            start_index = ids.index(int(checkpoint))
        except ValueError:
            print('id is not found')
    else:
        start_index = 0

    for char_id in ids[start_index:]:
        comics = ingest_events_from_characters(http=http, char_id=char_id, offset=0, limit=limit)
        if comics:
            events_from_characters_simplified = [simplify_events_from_characters(char_id, y) for x in comics for y in x]
            store_to_csv(events_from_characters_simplified, 'characters_in_events_fetched')
            save_checkpoint(char_id, "../checkpoints/character_id_for_ingesting_events.txt")
        else:
            break


def ingest_comics_from_entity(http, entity_id, offset, limit, entity, modified=None):
    comics_list = []

    if modified:
        modified_since = modified
    else:
        modified_since = None

    try:
        while True:
            response = http.get(generate_url("{entity}/{id}/comics".format(entity=entity, id=entity_id), limit=limit),
                                params={'orderBy': 'modified', 'offset': offset, 'modifiedSince': modified_since})
            comics_list.append(response.json()['data']['results'])
            print(entity_id, 'first call has ', len(response.json()['data']['results']))

            total_values = response.json()['data']['offset'] + response.json()['data']['count']

            if response.json()['data']['total'] == total_values:
                break
            else:
                offset = offset + limit

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else:", err)

    return comics_list


def extract_and_save_comics_from_characters(limit):
    ids = ast.literal_eval(read_file("data/characters_ids_for_comics_ingestion-final.txt"))
    http = retries_session()
    checkpoint = read_checkpoint("../checkpoints/character_id_for_ingesting_comics.txt")
    start_index = None

    if checkpoint:
        try:
            start_index = ids.index(int(checkpoint))
        except ValueError:
            print('id is not found')
    else:
        start_index = 0

    for char_id in ids[start_index:]:

        comics = ingest_comics_from_entity(http=http, entity_id=char_id, offset=0, limit=limit, entity='characters')
        if comics:
            comics_from_characters_simplified = [simplify_comics_from_characters(char_id, y) for x in comics for y in x]
            store_to_csv(comics_from_characters_simplified, 'characters_in_comics-final')
            save_checkpoint(char_id, "../checkpoints/character_id_for_ingesting_comics.txt")
        else:
            break


def extract_and_save_comics_from_creators(limit):
    ids = ast.literal_eval(read_file("data/creator_ids_for_comics_ingestion.txt"))
    http = retries_session()
    checkpoint = read_checkpoint("../checkpoints/creator_id_for_ingesting_comics.txt")
    start_index = ids.index(checkpoint)
    for creator_id in ids[start_index:]:
        comics = ingest_comics_from_entity(http=http, entity_id=2053, offset=0, limit=limit, entity='creators')
        if comics:
            comics_from_creators_simplified = [simplify_comics_from_creators(creator_id, y) for x in comics for y in x]
            store_to_csv(comics_from_creators_simplified, 'creators_in_comics_fetched')
            save_checkpoint(creator_id, "../checkpoints/creator_id_for_ingesting_comics.txt")
        else:
            break

