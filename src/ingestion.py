import requests
import ast

from comics import simplify_comics_data, simplify_comics_from_characters, simplify_comics_from_creators
from events import simplify_events_data, simplify_events_from_characters
from characters import simplify_character_data
from creators import simplify_creators_data
from helpers import generate_url, retries_session, read_file, save_checkpoint, read_checkpoint, store_to_csv


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
        print("OOps: Something Else", err)

    return response.json(), another_request


def extract_and_save_comics_data(limit, offset, order_by, modified=None):
    count = 0
    # offset = read_checkpoint()
    while True:
        comics, another_request = ingest_entity(limit=limit, offset=offset, entity='comics', order_by=order_by, modified=modified)

        # comics_simplified = [simplify_comics_data(x) for x in comics[0]['data']['results']]
        # store_to_csv(comics_simplified, 'comics')
        print("request number", count)
        count += 1
        offset = offset + limit
        if not another_request:
            break


def extract_and_save_characters_data(limit, offset, order_by, modified=None):
    count = 0
    # offset = read_checkpoint()
    while True:
        characters, another_request = ingest_entity(limit=limit, offset=offset, entity='characters', order_by=order_by, modified=modified)

        # characters_simplified = [simplify_character_data(x) for x in characters[0]['data']['results']]
        # store_to_csv(characters_simplified, 'characters')
        print("request number", count)
        count += 1
        offset = offset + limit
        if not another_request:
            break


def extract_and_save_events_data(limit, offset, order_by, modified=None):
    count = 0
    # offset = read_checkpoint()
    while True:
        events, another_request = ingest_entity(limit=limit, offset=offset, entity='events', order_by=order_by, modified=modified)

        # events_simplified = [simplify_events_data(x) for x in events[0]['data']['results']]
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
            response = http.get(generate_url("characters/{id}/events".format(id=char_id), limit=100), params={'orderBy': 'modified', 'offset': offset, 'modifiedSince': modified_since})
            events_list.append(response.json()['data']['results'])
            print(char_id, 'first call has ', len(response.json()['data']['results']))

            total_values = response.json()['data']['offset'] + response.json()['data']['count']
            if response.json()['data']['total'] > total_values:
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


def ingest_comics_from_entity(http, entity_id, offset, limit, entity, modified=None):
    comics_list = []

    if modified:
        modified_since = modified
    else:
        modified_since = None

    try:
        while True:
            response = http.get(generate_url("{entity}/{id}/comics".format(entity=entity, id=entity_id), limit=limit), params={'orderBy': 'modified', 'offset': offset, 'modifiedSince': modified_since})
            comics_list.append(response.json()['data']['results'])
            print(entity_id, 'first call has ', len(response.json()['data']['results']))

            total_values = response.json()['data']['offset'] + response.json()['data']['count']

            if response.json()['data']['total'] > total_values:
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
        print("OOps: Something Else", err)

    return comics_list


def extract_and_save_comics_from_characters(limit):
    ids = ast.literal_eval(read_file("data/characters_ids_for_comics_ingestion.txt"))
    http = retries_session()
    # offset = read_checkpoint()
    offset = 0
    # for char_id in ids[:2]:
    comics = ingest_comics_from_entity(http=http, entity_id=1009212, offset=offset, limit=limit, entity='characters')

    comics_from_characters_simplified = [simplify_comics_from_characters(1009212, x) for x in comics[0]]
    # store_to_csv(comics_from_characters_simplified, 'characters_in_comics')
    print(comics_from_characters_simplified)


def extract_and_save_comics_from_creators(limit):
    ids = ast.literal_eval(read_file("data/creator_ids_for_comics_ingestion.txt"))
    http = retries_session()
    # offset = read_checkpoint()
    offset = 0
    start_index = ids.index(2053)
    for creator_id in ids[start_index:]:
        # print(creator_id)
        comics = ingest_comics_from_entity(http=http, entity_id=2053, offset=offset, limit=limit, entity='creators')

        comics_from_creators_simplified = [simplify_comics_from_creators(2053, x) for x in comics[0]]
        # store_to_csv(comics_from_creators_simplified, 'creators_in_comics_fetched')
        save_checkpoint(creator_id)

