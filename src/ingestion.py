import requests
from datetime import datetime
from io import StringIO

from src.srialisers import (
    simplify_comics_data,
    simplify_events_data,
    simplify_character_data,
    simplify_creators_data,
)
from src.helpers import create_in_memory_csv, generate_url, retries_session
from src.snowflake import get_last_date_from_table
from src.aws_s3 import upload_file


class APIClientError(RuntimeError):
    pass


class APIServerError(RuntimeError):
    pass


def ingest_entity(limit, offset, entity, order_by, modified):
    url = generate_url(entity, limit)
    http = retries_session()

    if modified:
        modified_since = modified
    else:
        modified_since = None

    # try:
    response = fetch_entity(http, modified_since, offset, order_by, url).json()

    another_request = check_for_another_request(response)

    # except requests.exceptions.HTTPError as errh:
    #     print("Http Error:", errh)
    # except requests.exceptions.ConnectionError as errc:
    #     print("Error Connecting:", errc)
    # except requests.exceptions.Timeout as errt:
    #     print("Timeout Error:", errt)
    # except requests.exceptions.RequestException as err:
    #     print("OOps: Something Else:", err)
    return response, another_request


def check_for_another_request(response):
    total_values = response["data"]["offset"] + response["data"]["count"]
    if response["data"]["total"] <= total_values:
        print("no data anymore")
        another_request = False
    else:
        another_request = True
    return another_request


def fetch_entity(http, modified_since, offset, order_by, url):
    response = http.get(
        url,
        params={
            "orderBy": order_by,
            "offset": offset,
            "modifiedSince": modified_since,
        },
    )
    if response.status_code in [400, 401, 403, 404, 409]:
        raise APIClientError(
            f"Marvel API failed with a client error code {response.status_code}"
        )
    elif response.status_code in [500, 501, 503, 504]:
        raise APIServerError(
            f"Marvel API failed with a server error code {response.status_code}"
        )
    return response


def extract_and_save_comics_data(limit, offset, order_by):
    modified = get_last_date_from_table("comics", "comics_id")

    main_output = StringIO()
    outputs = list()
    while True:
        comics, another_request = ingest_entity(
            limit=limit,
            offset=offset,
            entity="comics",
            order_by=order_by,
            modified=modified,
        )

        comics_simplified = [simplify_comics_data(x) for x in comics["data"]["results"]]
        csv_string_object = create_in_memory_csv(comics_simplified)
        outputs.append(csv_string_object)
        offset = offset + limit
        if not another_request:
            break
    main_output.write("".join([x for x in outputs]))
    upload_file(
        "data/comics/comics-{date}.csv".format(
            date=str(datetime.now()).replace(" ", "-")
        ),
        main_output.getvalue(),
    )


def extract_and_save_characters_data(limit, offset, order_by):
    modified = get_last_date_from_table("characters", "character_id")
    main_output = StringIO()
    outputs = list()
    while True:
        characters, another_request = ingest_entity(
            limit=limit,
            offset=offset,
            entity="characters",
            order_by=order_by,
            modified=modified,
        )
        characters_simplified = [
            simplify_character_data(x) for x in characters["data"]["results"]
        ]
        csv_string_object = create_in_memory_csv(characters_simplified)
        outputs.append(csv_string_object)
        offset = offset + limit
        if not another_request:
            break
    main_output.write("".join([x for x in outputs]))
    upload_file(
        "data/characters/characters-{date}.csv".format(
            date=str(datetime.now()).replace(" ", "-")
        ),
        main_output.getvalue(),
    )


def extract_and_save_events_data(limit, offset, order_by):
    modified = get_last_date_from_table("events", "event_id")
    main_output = StringIO()
    outputs = list()
    while True:
        events, another_request = ingest_entity(
            limit=limit,
            offset=offset,
            entity="events",
            order_by=order_by,
            modified=modified,
        )

        events_simplified = [simplify_events_data(x) for x in events["data"]["results"]]

        csv_string_object = create_in_memory_csv(events_simplified)
        outputs.append(csv_string_object)
        offset = offset + limit
        if not another_request:
            break
    main_output.write("".join([x for x in outputs]))
    upload_file(
        "data/events/events-{date}.csv".format(
            date=str(datetime.now()).replace(" ", "-")
        ),
        main_output.getvalue(),
    )


def extract_and_save_creators_data(limit, offset, order_by):
    count = 0
    modified = get_last_date_from_table("creators", "creator_id")
    print(modified)
    main_output = StringIO()
    outputs = list()
    while True:
        creators, another_request = ingest_entity(
            limit=limit,
            offset=offset,
            entity="creators",
            order_by=order_by,
            modified=modified,
        )

        creators_simplified = [
            simplify_creators_data(x) for x in creators["data"]["results"]
        ]
        csv_string_object = create_in_memory_csv(creators_simplified)
        outputs.append(csv_string_object)
        count += 1
        offset = offset + limit

        if not another_request:
            break

    main_output.write("".join([x for x in outputs]))
    upload_file(
        "data/creators/creators-{date}.csv".format(
            date=str(datetime.now()).replace(" ", "-")
        ),
        main_output.getvalue(),
    )


def ingest_events_from_characters(http, char_id, offset, limit, modified=None):
    events_list = []

    if modified:
        modified_since = modified
    else:
        modified_since = None

    try:
        while True:
            response = http.get(
                generate_url("characters/{id}/events".format(id=char_id), limit=100),
                params={
                    "orderBy": "modified",
                    "offset": offset,
                    "modifiedSince": modified_since,
                },
            )
            events_list.append(response.json()["data"]["results"])
            print(char_id, "first call has ", len(response.json()["data"]["results"]))

            total_values = (
                    response.json()["data"]["offset"] + response.json()["data"]["count"]
            )
            if response.json()["data"]["total"] <= total_values:
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

    return events_list


def ingest_comics_from_entity(http, entity_id, offset, limit, entity, modified=None):
    comics_list = []

    if modified:
        modified_since = modified
    else:
        modified_since = None

    try:
        while True:
            response = http.get(
                generate_url(
                    "{entity}/{id}/comics".format(entity=entity, id=entity_id),
                    limit=limit,
                ),
                params={
                    "orderBy": "modified",
                    "offset": offset,
                    "modifiedSince": modified_since,
                },
            )
            comics_list.append(response.json()["data"]["results"])

            total_values = (
                    response.json()["data"]["offset"] + response.json()["data"]["count"]
            )

            if response.json()["data"]["total"] <= total_values:
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
