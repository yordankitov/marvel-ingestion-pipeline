import pandas as pd
import ast
from helpers import save_file, create_in_memory_file, create_in_memory_csv
from upload_to_s3 import upload_file


def extract_names(data_set: dict) -> list:
    """
    Extracts the name from the data received

    :param data_set: dictionary data from any entity
    :return: a list with names
    """
    return [d['name'] for d in data_set]

#

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



def extract_sub_entity_from_entity(entity_id: str, data_set: list, looking_for: str, entity: str):
    """
    Receives an entity id and a list of sub entities.
    Creates a pandas df with that data and then saves it to a csv file.

    :param entity_id: id of the entity
    :param data_set: list of sub entities to match to the entity id
    :param looking_for: name of the sub entity (comics, events etc.)
    :param entity: name of the main entity (character, creator etc.)

    """
    print('extracting now')
    data_list = extract_names(ast.literal_eval(data_set))
    # save_dataframe(data=create_dataframe_data(entity_id, data_list), file_path=f"data/{entity}s_in_{looking_for}-final.csv")
    csv_data = create_in_memory_csv(data = create_dataframe_data(entity_id, data_list))
    upload_file(f"data/{entity}s_in_{looking_for}-final.csv", csv_data)

