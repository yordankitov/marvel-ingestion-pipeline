import pandas as pd
import ast
from helpers import save_file


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


def save_dataframe(data: dict):
    """
    Receives pandas df which saves to a csv

    :param data: data in the form of a dataframe
    """
    df = pd.DataFrame(data)
    df.to_csv('data/creators_in_comics.csv', mode='a', index=False, header=False)


def extract_comics(char_id: str, comics: list):
    """
    Receives a character id and a list of comics.
    A pandas df is created with that data and then
    saved to a csv file.

    :param char_id: character id
    :param comics: list of comics

    """
    comics_list = extract_names(ast.literal_eval(comics))
    save_dataframe(create_dataframe_data(char_id, comics_list))


def check_returned_data():
    """
    Checks if the specified amount of data fetched for the entity is exhausted,
    if it's not, it will save the id of the character that will need
    further data ingestion

    """
    df = pd.read_csv("data/characters.csv")
    ids = []
    for index, row in df.iterrows():
        char_id = row['character_id']

        if int(row['available_comics']) > int(row['fetched_comics']):
            ids.append(char_id)
        else:
            extract_comics(char_id, row['list_of_comics'])

    if ids:
        save_file(data=ids, file_path="data/characters_ids_for_comics_ingestion.txt")


def extract_events(char_id, events):
    events_list = extract_names(ast.literal_eval(events))
    save_dataframe(create_dataframe_data(char_id, events_list))


def check_returned_data_for_events():
    df = pd.read_csv("data/characters.csv")
    ids = []
    for index, row in df.iterrows():
        char_id = row['character_id']

        if int(row['available_events']) > int(row['fetched_events']):
            ids.append(char_id)
        else:
            extract_events(char_id, row['list_of_events'])

    if ids:
        save_file(data=ids, file_path="data/characters_ids_for_events_ingestion.txt")


# generalise the functions here ############


def check_returned_data_for_comics():
    df = pd.read_csv("data/creators.csv")
    ids = []
    for index, row in df.iterrows():
        creator_id = row['creator_id']

        if int(row['available_comics']) > int(row['fetched_comics']):
            ids.append(creator_id)
        else:
            extract_comics_from_creators(creator_id, row['list_of_comics'])

    if ids:
        save_file(data=ids, file_path="data/creator_ids_for_comics_ingestion.txt")


def extract_comics_from_creators(creator_id, comics):
    comics_list = extract_names(ast.literal_eval(comics))
    save_dataframe(create_dataframe_data(creator_id, comics_list))

# check_returned_data_for_comics()