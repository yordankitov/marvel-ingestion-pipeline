from src.ingestion import extract_and_save_characters_data, extract_and_save_creators_data, extract_and_save_events_data, extract_and_save_comics_data
from src.extract_from_ingested_data import extract_from_ingested_characters_events_data, extract_from_ingested_characters_comics_data, extract_from_ingested_creators_comics_data
from src.importing_to_snowflake import *

# client = boto3.client('s3')
user = os.getenv('SNOW_USER')
password = os.getenv('SNOW_PASS')
db = os.getenv('DB')
account = os.getenv('ACCOUNT')
schema = os.getenv('SCHEMA')
role = os.getenv('ROLE')
wh = os.getenv('WH')


def characters():
    print('characters')
    try:
        extract_and_save_characters_data(limit=100, offset=0, order_by='modified')
        copy_stage_to_sf('characters')
        with snowflake_connection() as con:
            con.cursor().execute(create_characters_view(db, schema))
    except Exception as e:
        print(e)


def comics():
    print('comics')
    try:
        extract_and_save_comics_data(limit=100, offset=0, order_by='modified')
        copy_stage_to_sf('comics')
        with snowflake_connection() as con:
            con.cursor().execute(create_comics_view(db, schema))
    except Exception as e:
        print(e)


def creators():
    print('creators')
    try:
        extract_and_save_creators_data(limit=100, offset=0, order_by='modified')
        copy_stage_to_sf('creators')
        with snowflake_connection() as con:
            con.cursor().execute(create_creators_view(db, schema))
    except Exception as e:
        print(e)


def events():
    print('events')
    try:
        extract_and_save_events_data(limit=100, offset=0, order_by='modified')
        copy_stage_to_sf('events')
        with snowflake_connection() as con:
            con.cursor().execute(create_events_view(db, schema))
    except Exception as e:
        print(e)


def characters_in_comics():
    print('characters in comics')
    try:
        extract_from_ingested_characters_comics_data(100)
        copy_stage_to_sf('characters_in_comics')
        with snowflake_connection() as con:
            con.cursor().execute(create_characters_in_comics_view(db, schema))
    except Exception as e:
        print(e)


def characters_in_events():
    print('characters in events')
    try:
        extract_from_ingested_characters_events_data(100)
        copy_stage_to_sf('characters_in_events')
        with snowflake_connection() as con:
            con.cursor().execute(create_characters_in_events_view(db, schema))
    except Exception as e:
        print(e)


def creators_in_comics():
    print('creators in comics')
    try:
        extract_from_ingested_creators_comics_data(100)
        copy_stage_to_sf('creators_in_comics')
        with snowflake_connection() as con:
            con.cursor().execute(create_creators_in_comics_view(db, schema))
    except Exception as e:
        print(e)


def copy_stage_to_sf(table):
    try:
        with snowflake_connection() as con:
            copy_s3_stage_to_sf(con, db, schema, table)
    except Exception as e:
        print(e)


def main():
    characters()
    comics()
    events()
    creators()
    characters_in_comics()
    characters_in_events()
    creators_in_comics()


if __name__ == '__main__':
    main()