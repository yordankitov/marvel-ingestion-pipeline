import boto3
import os
from src.ingestion import extract_and_save_characters_data, extract_and_save_creators_data, extract_and_save_events_data, extract_and_save_comics_data, extract_and_save_comics_from_creators, extract_and_save_comics_from_characters, extract_and_save_events_from_characters
from src.extract_from_ingested_data import check_returned_data_entity
from src.upload_to_s3 import create_bucket
from src.helpers import check_entity_last_update
from src.importing_to_snowflake import snowflake_connection, copy_s3_stage_to_sf, read_table, create_views

client = boto3.client('s3')
user = os.getenv('SNOW_USER')
password = os.getenv('SNOW_PASS')
db = os.getenv('DB')
account = os.getenv('ACCOUNT')
schema = os.getenv('SCHEMA')
role = os.getenv('ROLE')
wh = os.getenv('WH')




def ingestion():
    extract_and_save_characters_data(limit=100, offset=0, order_by='modified', modified=read_table('characters'))
    # extract_and_save_creators_data(limit=100, offset=0, order_by='modified', modified=read_table('creators'))
    # extract_and_save_comics_data(limit=100, offset=0, order_by='modified', modified=read_table('comics'))
    # extract_and_save_events_data(limit=100, offset=0, order_by='modified', modified=read_table('events'))


def extraction_of_sub_entities_from_ingested_entities():
    check_returned_data_entity('creator', 'comics')
    check_returned_data_entity('character', 'comics')
    check_returned_data_entity('character', 'events')


def ingestion_of_sub_entities():
    extract_and_save_events_from_characters(100)
    extract_and_save_comics_from_characters(100)
    extract_and_save_comics_from_creators(100)


def upload_to_aws_s3():
    bucket = create_bucket('il-tapde-final-exercise-yordan')
    print(bucket)

def copy_stages_to_sf(table):
    try:
        with snowflake_connection() as con:
            copy_s3_stage_to_sf(con, db, schema, table)
    except Exception as e:
        print(e)

def create_views_on_sf():
    with snowflake_connection() as con:
        create_views(con, db, schema)

def main():
    ingestion()
    extraction_of_sub_entities_from_ingested_entities()
    ingestion_of_sub_entities()


if __name__ == '__main__':

    # ingestion()
    # copy_stages_to_sf('characters_in_events')
    create_views_on_sf()