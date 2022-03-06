import snowflake.connector
import os
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

user = os.getenv('SNOW_USER')
password = os.getenv('SNOW_PASS')
db = os.getenv('DB')
account = os.getenv('ACCOUNT')
schema = os.getenv('DEV_SCHEMA')
role = os.getenv('ROLE')


def database_config(con, schema):
    """
    Creates the schema of the db
    :param con: The connection to the db
    :param schema: the schema used for the config
    :return:
    """
    create_schema = f"create schema if not exists {schema}"
    create_characters_table = """create TABLE if not exists characters (
    	CHARACTER_ID VARCHAR NOT NULL,
    	NAME VARCHAR,
    	DESCRIPTION VARCHAR,
    	DATE_MODIFIED VARCHAR,
    	AVAILABLE_COMICS NUMBER,
    	FETCHED_COMICS NUMBER,
    	LIST_OF_COMICS VARCHAR,
    	AVAILABLE_EVENTS NUMBER,
    	FETCHED_EVENTS NUMBER,
    	LIST_OF_EVENTS VARCHAR,
    	primary key (CHARACTER_ID)
    );"""

    create_creators_table = """create TABLE if not exists creators (
    	CREATOR_ID VARCHAR NOT NULL,
    	FIRST_NAME VARCHAR,
    	MIDDLE_NAME VARCHAR,
    	LAST_NAME VARCHAR,
    	SUFFIX VARCHAR,
    	FULL_NAME VARCHAR,
    	DATE_MODIFIED VARCHAR,
    	AVAILABLE_COMICS NUMBER,
    	FETCHED_COMICS NUMBER,
    	LIST_OF_COMICS VARCHAR,
    	AVAILABLE_STORIES NUMBER,
    	FETCHED_STORIES NUMBER,
    	LIST_OF_STORIES VARCHAR,
    	AVAILABLE_SERIES NUMBER,
    	FETCHED_SERIES NUMBER,
    	LIST_OF_SERIES VARCHAR,
    	AVAILABLE_EVENTS NUMBER,
    	FETCHED_EVENTS NUMBER,
    	LIST_OF_EVENTS VARCHAR,
    	primary key (CREATOR_ID)
    );"""

    create_events_table = """create TABLE if not exists events (
    	EVENT_ID VARCHAR NOT NULL,
    	TITLE VARCHAR,
    	DESCRIPTION VARCHAR,
    	DATE_MODIFIED VARCHAR,
    	AVAILABLE_CREATORS NUMBER,
    	FETCHED_CREATORS NUMBER,
    	LIST_OF_CREATORS VARCHAR,
    	AVAILABLE_STORIES NUMBER,
    	FETCHED_STORIES NUMBER,
    	LIST_OF_STORIES VARCHAR,
    	AVAILABLE_COMICS NUMBER,
    	FETCHED_COMICS NUMBER,
    	LIST_OF_COMICS VARCHAR,
    	AVAILABLE_SERIES NUMBER,
    	FETCHED_SERIES NUMBER,
    	LIST_OF_SERIES VARCHAR,
    	primary key (EVENT_ID)
    );"""

    create_comics_table = """create TABLE if not exists comics (
    	COMICS_ID VARCHAR NOT NULL,
    	DIGITAL_ID VARCHAR,
    	TITLE VARCHAR,
    	VARIANT_DESCRIPTION VARCHAR,
    	DESCRIPTION VARCHAR,
    	DATE_MODIFIED VARCHAR,
    	ISBN VARCHAR,
    	UPC VARCHAR,
    	DIAMOND_CODE VARCHAR,
    	EAN VARCHAR,
    	ISSN VARCHAR,
    	FORAMT VARCHAR,
    	PAGE_COUNT NUMBER,
    	PRINT_PRICE NUMBER,
    	AVAILABLE_SERIES NUMBER,
    	FETCHED_SERIES NUMBER,
    	LIST_OF_SERIES VARCHAR,
    	AVAILABLE_CREATORS NUMBER,
    	FETCHED_CREATORS NUMBER,
    	LIST_OF_CREATORS VARCHAR,
    	AVAILABLE_STORIES NUMBER,
    	FETCHED_STORIES NUMBER,
    	LIST_OF_STORIES VARCHAR,
    	AVAILABLE_EVENTS NUMBER,
    	FETCHED_EVENTS NUMBER,
    	LIST_OF_EVENTS VARCHAR,
    	primary key (COMICS_ID)
    );"""

    create_characters_comics_table = """create TABLE if not exists characters_in_comics (
    	CHARACTER_ID VARCHAR NOT NULL,
    	COMICS_ID VARCHAR NOT NULL,
    	COMICS_NAME VARCHAR,
    	foreign key (CHARACTER_ID) references characters (CHARACTER_ID),
    	foreign key (COMICS_ID) references comics (COMICS_ID)
    	);"""

    create_characters_events_table = """create TABLE if not exists characters_in_events (
      	CHARACTER_ID VARCHAR NOT NULL,
      	EVENT_ID VARCHAR NOT NULL,
      	EVENT_NAME VARCHAR,
      	foreign key (CHARACTER_ID) references characters (CHARACTER_ID),
      	foreign key (EVENT_ID) references events (EVENT_ID)
      	);"""

    create_creators_comics_table = """create TABLE if not exists creators_in_comics (
      	CREATOR_ID VARCHAR NOT NULL,
      	COMICS_ID VARCHAR NOT NULL,
      	COMICS_NAME VARCHAR,
      	foreign key (CREATOR_ID) references creators (CREATOR_ID),
      	foreign key (COMICS_ID) references comics (COMICS_ID)
      	);"""
    try:
        con.cursor().execute(create_schema)
        con.cursor().execute(create_characters_table)
        con.cursor().execute(create_creators_table)
        con.cursor().execute(create_comics_table)
        con.cursor().execute(create_events_table)
        con.cursor().execute(create_characters_comics_table)
        con.cursor().execute(create_characters_events_table)
        con.cursor().execute(create_creators_comics_table)
    except Exception as e:
        print(e)


def create_s3_stage_for_snowflake(con, db, schema):
    create_storage_integration = """create storage integration IF NOT EXISTS s3_integration_yordan
          type = external_stage
          storage_provider = s3
          enabled = true
          storage_aws_role_arn = 'arn:aws:iam::185827676115:role/il-tapde-role-yordan'
          storage_allowed_locations = ('s3://il-tapde-final-exercise-yordan/data/');"""

    create_file_format = """create or replace file format {db}.{schema}.csv_format_yordan
          type = csv
          field_delimiter = ','
          skip_header = 1
          null_if = ('NULL', 'null')
          empty_field_as_null = true
          error_on_column_count_mismatch = false
          field_optionally_enclosed_by = '"';""".format(db=db, schema=schema)

    create_stage = """ 
        create or replace stage {db}.{schema}.S3_STAGE
        URL = 's3://il-tapde-final-exercise-yordan/data/'
        storage_integration = s3_integration_yordan
        file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    try:
        con.cursor().execute(create_storage_integration)
        con.cursor().execute(create_file_format)
        con.cursor().execute(create_stage)


    except Exception as e:
        print(e)


def copy_s3_stage_to_sf(con, db, schema, entity):
    copy_data = """ copy into {db}.{schema}.{entity}
            from @{db}.{schema}.S3_STAGE
            file_format= {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema, entity=entity)

    try:
        con.cursor().execute(copy_data)

    except Exception as e:
        print(e)


def snowflake_connection(user, password, account, db, schema):
    """
    Establishes a connection to the db

    :return: Returns active connection
    """

    try:
        con = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=db,
            schema=schema,
            role=role
        )
        return con
    except Exception as e:
        print(e)




def copy_to_snowflake(con, file_path, abbreviation, schema, table):
    """
    Establishes connection to Snowflake, creates the appropriate structure and dumps the data from a selected local file

    :param con: The db it needs to be connected to
    :param file_path: The path of the file used to import the data

    """
    try:
        con.cursor().execute(
            "PUT file://{file_path} @%{abbreviation}".format(file_path=file_path, abbreviation=abbreviation)
        )
        # con.cursor().execute("PUT file://../brewery_data/breweries.csv @%breweries")

        con.cursor().execute(
            "COPY INTO {db}.{schema}.{table} FROM @%{abbreviation} "
            "file_format = (type = csv field_optionally_enclosed_by='\"' field_delimiter=',' SKIP_HEADER=1)".format(db=db, abbreviation=abbreviation, schema=schema, table=table)
        )
        con.cursor().execute("REMOVE @%{abbreviation}".format(abbreviation=abbreviation))
    except Exception as e:
        print(e)


def get_table_data_as_dataframe(table):
    url = URL(
        user=user,
        password=password,
        account=account,
        database=db,
        schema=schema,
        role=role
    )

    query = "select * from {table}".format(table=table)

    try:
        engine = create_engine(url)

        connection = engine.connect()
        df = pd.read_sql(query, connection)


        return df

    except Exception as e:
        print(e)
    finally:
        connection.close()


def test(table):
    engine_1 = create_engine(URL(
        user=user,
        password=password,
        account=account,
        database=db,
        schema=schema,
        role=role
    ))

    engine_1_con = engine_1.connect()
    try:

        engine_cur = engine_1_con.execute('SELECT * FROM {table};'.format(table=table))
        total_rows = engine_cur.rowcount
    except Exception as e:
        print(e)
    finally:
        engine_1_con.close()
        return total_rows

def t():
    tt = test('characters')
    return tt


def populate_db():
    """
    Main function that structures everything to be executed chronologically

    :param:
    :return:
    """
    con = snowflake_connection(user, password, account, db, schema)

    database_config(con, schema)

    for entity in ['characters', 'creators', 'comics', 'events', 'characters_in_comics', 'characters_in_events', 'creators_in_comics']:
        con.cursor().execute("truncate table {db}.{schema}.{entity}".format(db=db, schema=schema, entity=entity))
        copy_to_snowflake(con, f'data/{entity}.csv', entity, schema, entity)

    create_s3_stage_for_snowflake(con, db, schema)
    copy_s3_stage_to_sf(con, db, schema, 'characters')

    print(get_table_data_as_dataframe('characters'))

    con.close()

