import pandas as pd


def check_entity_last_update(entity):
    df = pd.read_csv(f'data/{entity}.csv'.format(enitity=entity))
    last_date = df['date_modified'].max()

    # date will be edited in a way to be accepted by the api at a later date

    return last_date

