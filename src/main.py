from ingestion import extract_and_save_characters_data, extract_and_save_creators_data, extract_and_save_events_data, extract_and_save_comics_data, extract_and_save_comics_from_creators, extract_and_save_comics_from_characters, extract_and_save_events_from_characters
from extract_from_ingested_data import check_returned_data_entity
from upload_to_s3 import create_bucket, upload_file
from helpers import check_entity_last_update


def get_checkpoint(entity):
    checkpoint = check_entity_last_update(entity)
    if checkpoint:
        return checkpoint
    else:
        return None

def ingestion():

    extract_and_save_characters_data(limit=100, offset=0, order_by='modified', modified=get_checkpoint('characters'))
    extract_and_save_creators_data(limit=100, offset=0, order_by='modified', modified=get_checkpoint('creators'))
    extract_and_save_comics_data(limit=100, offset=0, order_by='modified', modified=get_checkpoint('comics'))
    extract_and_save_events_data(limit=100, offset=0, order_by='modified', modified=get_checkpoint('events'))


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


def main():
    ingestion()
    extraction_of_sub_entities_from_ingested_entities()
    ingestion_of_sub_entities()


if __name__ == '__main__':
    upload_to_aws_s3()
