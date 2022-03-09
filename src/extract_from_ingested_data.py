import pandas as pd
import ast
from io import StringIO
from datetime import datetime

from src.comics import simplify_comics_from_characters, simplify_comics_from_creators
from src.events import simplify_events_from_characters
from src.helpers import retries_session, create_in_memory_file, create_in_memory_csv
from src.aws_s3 import upload_file
from src.snowflake import get_table_data, get_last_id_from_table
from src.ingestion import ingest_comics_from_entity, ingest_events_from_characters


def extract_names(data_set: dict) -> list:
    """
    Extracts the name from the data received

    :param data_set: dictionary data from any entity
    :return: a list with names
    """
    return [d['name'] for d in data_set]


def create_dataframe_data(char_id: str, list_of_comics: list) -> dict:
    """
    Prepares data received to be saved as a pandas dataframe (data gets converted into a dict)

    :param char_id: character id
    :param list_of_comics: list of comics
    :return: dictionary
    """
    comics = []
    for x in list_of_comics:
        comics.append(x)

    return {'creator_id': char_id, 'comics_name': comics}


def save_dataframe(file_path: str, data: dict):
    """
    Receives pandas df which saves to a csv

    :param data: data in the form of a dataframe
    :param file_path: the directory and name of the file where the csv to be saved
    """
    df = pd.DataFrame(data)
    df.to_csv(file_path, mode='a', index=False, header=False)


def check_returned_data_entity(entity: str, looking_for: str):
    """
    Checks if the specified amount of data fetched for the entity is exhausted,
    if it's not, it will save the id of the main entity that will need
    further data ingestion,
    or it will pass the entity id and their respective list of sub entities to
    extract_sub_entity_from_entity function.

    :param entity: main entity name (character, creator). NOTE: must be in singular form
    :param looking_for: sub entity name (comics, events, etc.) NOTE: must be in plural form

    """
    df = pd.read_csv(f"data/{entity}s.csv")
    ids = []
    for index, row in df.iterrows():
        entity_id = row[f'{entity}_id']
        print(row)
        if int(row[f'available_{looking_for}']) > int(row[f'fetched_{looking_for}']):
            ids.append(entity_id)
        else:
            extract_sub_entity_from_entity(entity_id, row[f"list_of_{looking_for}"], looking_for, entity)

    if ids:
        # save_file(data=ids, file_path=f"data/{entity}s_ids_for_{looking_for}_ingestion-final.txt")
        stream_file = create_in_memory_file(ids)
        upload_file(f"data/{entity}s_ids_for_{looking_for}_ingestion-final.txt", stream_file)



def extract_sub_entity_from_entity(entity_id: str, data_set: list):
    """
    Receives an entity id and a list of sub entities.
    Creates a pandas df with that data and then saves it to a csv file.

    :param entity_id: id of the entity
    :param data_set: list of sub entities to match to the entity id
    """
    print('extracting now')
    data_list = extract_names(ast.literal_eval(data_set))
    # save_dataframe(data=create_dataframe_data(entity_id, data_list), file_path=f"data/{entity}s_in_{looking_for}-final.csv")
    csv_data = create_in_memory_csv(data=create_dataframe_data(entity_id, data_list))

    return csv_data


def extract_from_ingested_characters_comics_data(limit):
    http = retries_session()
    data = get_table_data('characters', 'character_id')
    main_output = StringIO()
    outputs = list()
    checkpoint = get_last_id_from_table('characters_in_comics', 'character_id')

    if checkpoint:
        index = next(i for i, (v, *_) in enumerate(data) if v == checkpoint)
        if index >= len(data):
            return 0
    else:
        index = 0
    print(index, len(data))
    print(checkpoint)
    print(len(data))

    for row in data[index:]:

        char_id = row[0]
        if int(row[4]) > int(row[5]):
            # data_row = extract_and_save_comics_from_characters(entity_id, 100)
            comics = ingest_comics_from_entity(http=http, entity_id=char_id, offset=0, limit=limit, entity='characters')
            comics_from_characters_simplified = [simplify_comics_from_characters(char_id, y) for x in comics for y in x]
            csv_string_object = create_in_memory_csv(comics_from_characters_simplified)
            outputs.append(csv_string_object)

        else:
            data_row = extract_sub_entity_from_entity(char_id, row[6])
            outputs.append(data_row)

    main_output.write(''.join([x for x in outputs]))
    upload_file(
        'data/characters-in-comics/characters-in-comics-{date}.csv'.format(date=str(datetime.now()).replace(' ', '-')),
        main_output.getvalue())


def extract_from_ingested_characters_events_data(limit):
    http = retries_session()
    data = get_table_data('characters', 'character_id')
    main_output = StringIO()
    outputs = list()
    checkpoint = get_last_id_from_table('characters_in_events', 'character_id')

    if checkpoint:
        index = next(i for i, (v, *_) in enumerate(data) if v == checkpoint)
        if index >= len(data):
            return 0
    else:
        index = 0
    print(index, len(data))
    print(checkpoint)
    print(len(data))

    for row in data[index:]:

        char_id = row[0]
        if int(row[7]) > int(row[8]):
            # data_row = extract_and_save_comics_from_characters(entity_id, 100)
            comics = ingest_events_from_characters(http=http, char_id=char_id, offset=0, limit=limit)
            events_from_characters_simplified = [simplify_events_from_characters(char_id, y) for x in comics for y in x]
            csv_string_object = create_in_memory_csv(events_from_characters_simplified)
            outputs.append(csv_string_object)

        else:
            data_row = extract_sub_entity_from_entity(char_id, row[9])
            outputs.append(data_row)

    main_output.write(''.join([x for x in outputs]))
    upload_file(
        'data/characters-in-events/characters-in-events-{date}.csv'.format(date=str(datetime.now()).replace(' ', '-')),
        main_output.getvalue())


def extract_from_ingested_creators_comics_data(limit):
    http = retries_session()
    data = get_table_data('creators', 'creator_id')
    main_output = StringIO()
    outputs = list()
    checkpoint = get_last_id_from_table('creators_in_comics', 'creator_id')

    if checkpoint:
        index = next(i for i, (v, *_) in enumerate(data) if v == checkpoint)
        if index >= len(data):
            return 0
    else:
        index = 0
    print(checkpoint)
    print(len(data))

    for row in data[index:]:

        creator_id = row[0]
        if int(row[7]) > int(row[8]):
            # data_row = extract_and_save_comics_from_characters(entity_id, 100)
            comics = ingest_comics_from_entity(http=http, entity_id=creator_id, offset=0, limit=limit, entity='creators')
            comics_from_creators_simplified = [simplify_comics_from_creators(creator_id, y) for x in comics for y in x]
            csv_string_object = create_in_memory_csv(comics_from_creators_simplified)
            outputs.append(csv_string_object)

        else:
            data_row = extract_sub_entity_from_entity(creator_id, row[9])
            outputs.append(data_row)

    main_output.write(''.join([x for x in outputs]))
    upload_file(
        'data/creators-in-comics/creators-in-comics-{date}.csv'.format(date=str(datetime.now()).replace(' ', '-')),
        main_output.getvalue())

