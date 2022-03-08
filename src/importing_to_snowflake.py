import snowflake.connector
import os

user = os.getenv('SNOW_USER')
password = os.getenv('SNOW_PASS')
db = os.getenv('DB')
account = os.getenv('ACCOUNT')
schema = os.getenv('SCHEMA')
role = os.getenv('ROLE')
wh = os.getenv('WH')


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
    	COMICS_NAME VARCHAR,
    	foreign key (CHARACTER_ID) references characters (CHARACTER_ID),
    	foreign key (COMICS_ID) references comics (COMICS_ID)
    	);"""

    create_characters_events_table = """create TABLE if not exists characters_in_events (
      	CHARACTER_ID VARCHAR NOT NULL,
      	EVENT_NAME VARCHAR,
      	foreign key (CHARACTER_ID) references characters (CHARACTER_ID),
      	foreign key (EVENT_ID) references events (EVENT_ID)
      	);"""

    create_creators_comics_table = """create TABLE if not exists creators_in_comics (
      	CREATOR_ID VARCHAR NOT NULL,
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


def create_s3_stages_for_snowflake(con, db, schema):
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

    create_stage_characters = """ 
        create or replace stage {db}.{schema}.characters_stage
        URL = 's3://il-tapde-final-exercise-yordan/data/characters/'
        storage_integration = s3_integration_yordan
        file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    create_stage_comics = """ 
            create or replace stage {db}.{schema}.comics_stage
            URL = 's3://il-tapde-final-exercise-yordan/data/comics/'
            storage_integration = s3_integration_yordan
            file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    create_stage_creators = """ 
            create or replace stage {db}.{schema}.creators_stage
            URL = 's3://il-tapde-final-exercise-yordan/data/creators/'
            storage_integration = s3_integration_yordan
            file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    create_stage_events = """ 
            create or replace stage {db}.{schema}.events_stage
            URL = 's3://il-tapde-final-exercise-yordan/data/events/'
            storage_integration = s3_integration_yordan
            file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    create_stage_characters_in_comics = """ 
            create or replace stage {db}.{schema}.characters_in_comics_stage
            URL = 's3://il-tapde-final-exercise-yordan/data/characters-in-comics/'
            storage_integration = s3_integration_yordan
            file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    create_stage_characters_in_events = """ 
                create or replace stage {db}.{schema}.characters_in_events_stage
                URL = 's3://il-tapde-final-exercise-yordan/data/characters-in-events/'
                storage_integration = s3_integration_yordan
                file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    create_stage_creators_in_comics = """ 
                create or replace stage {db}.{schema}.creators_in_comics_stage
                URL = 's3://il-tapde-final-exercise-yordan/data/creators-in-comics/'
                storage_integration = s3_integration_yordan
                file_format = {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema)

    try:
        con.cursor().execute(create_storage_integration)
        con.cursor().execute(create_file_format)
        con.cursor().execute(create_stage_characters)
        con.cursor().execute(create_stage_characters_in_comics)
        con.cursor().execute(create_stage_characters_in_events)
        con.cursor().execute(create_stage_comics)
        con.cursor().execute(create_stage_events)
        con.cursor().execute(create_stage_creators_in_comics)
        con.cursor().execute(create_stage_creators)

    except Exception as e:
        print(e)


def create_characters_view(db, schema):
    characters_view = """create or replace view {db}.{schema}.characters_view as
                select CHARACTER_ID, NAME, DESCRIPTION, max(date_modified) as DATE_MODIFIED, AVAILABLE_COMICS, AVAILABLE_EVENTS
                from {db}.{schema}.characters group by CHARACTER_ID, NAME, DESCRIPTION, AVAILABLE_COMICS, FETCHED_COMICS, LIST_OF_COMICS, 
                AVAILABLE_EVENTS, FETCHED_EVENTS, LIST_OF_EVENTS;
        """.format(db=db, schema=schema)

    return characters_view


def create_comics_view(db, schema):
    comics_view = """create or replace view {db}.{schema}.comics_view as
               select COMICS_ID, DIGITAL_ID, TITLE, VARIANT_DESCRIPTION, DESCRIPTION, max(date_modified) as DATE_MODIFIED, ISBN, UPC, 
               DIAMOND_CODE, EAN, ISSN, PAGE_COUNT, PRINT_PRICE, AVAILABLE_SERIES, AVAILABLE_CREATORS, AVAILABLE_STORIES, AVAILABLE_EVENTS
               from {db}.{schema}.comics group by COMICS_ID, DIGITAL_ID, TITLE, VARIANT_DESCRIPTION, DESCRIPTION, DATE_MODIFIED, 
               ISBN, UPC, 
               DIAMOND_CODE, EAN, ISSN, PAGE_COUNT, PRINT_PRICE, AVAILABLE_SERIES, FETCHED_SERIES, LIST_OF_SERIES, AVAILABLE_CREATORS, 
               AVAILABLE_STORIES, FETCHED_STORIES, LIST_OF_STORIES, AVAILABLE_EVENTS, FETCHED_EVENTS, LIST_OF_EVENTS;
           """.format(db=db, schema=schema)

    return comics_view


def create_creators_view(db, schema):
    creators_view = """create or replace view {db}.{schema}.creators_view as
               select CREATOR_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, SUFFIX, FULL_NAME, max(date_modified) as DATE_MODIFIED, AVAILABLE_COMICS,
               AVAILABLE_STORIES, AVAILABLE_SERIES, AVAILABLE_EVENTS
               from {db}.{schema}.creators group by CREATOR_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, SUFFIX, FULL_NAME, DATE_MODIFIED, 
               AVAILABLE_COMICS, FETCHED_COMICS, LIST_OF_COMICS, AVAILABLE_STORIES, FETCHED_STORIES, LIST_OF_STORIES, AVAILABLE_SERIES, FETCHED_SERIES, 
               LIST_OF_SERIES, AVAILABLE_EVENTS, FETCHED_EVENTS, LIST_OF_EVENTS;
           """.format(db=db, schema=schema)

    return creators_view


def create_events_view(db, schema):
    events_view = """create or replace view {db}.{schema}.events_view as
               select EVENT_ID, TITLE, DESCRIPTION, max(date_modified) as DATE_MODIFIED, AVAILABLE_CREATORS, AVAILABLE_STORIES, AVAILABLE_COMICS,
               AVAILABLE_SERIES
               from {db}.{schema}.events group by EVENT_ID, TITLE, DESCRIPTION, DATE_MODIFIED, AVAILABLE_CREATORS,
               FETCHED_CREATORS, LIST_OF_CREATORS, AVAILABLE_STORIES, FETCHED_STORIES, LIST_OF_STORIES, AVAILABLE_COMICS, FETCHED_COMICS, 
               LIST_OF_COMICS, AVAILABLE_SERIES, FETCHED_SERIES, LIST_OF_SERIES;
               """.format(db=db, schema=schema)

    return events_view


def create_characters_in_comics_view(db, schema):
    characters_in_comics_view = """create or replace view {db}.{schema}.characters_in_comics_view as
               select distinct CHARACTER_ID, COMICS_NAME
               from {db}.{schema}.characters_in_comics group by CHARACTER_ID, COMICS_NAME""".format(db=db, schema=schema)

    return characters_in_comics_view


def create_characters_in_events_view(db, schema):
    characters_in_events_view = """create or replace view {db}.{schema}.characters_in_events_view as
                   select distinct CHARACTER_ID, EVENT_NAME
                   from {db}.{schema}.characters_in_events group by CHARACTER_ID, EVENT_NAME""".format(db=db, schema=schema)

    return characters_in_events_view


def create_creators_in_comics_view(db, schema):
    creators_in_comics_view = """create or replace view {db}.{schema}.creators_in_comics_view as
                  select distinct CREATOR_ID, COMICS_NAME
                  from {db}.{schema}.creators_in_comics group by CREATOR_ID, COMICS_NAME""".format(db=db, schema=schema)

    return creators_in_comics_view


def copy_s3_stage_to_sf(con, db, schema, entity):
    copy_data = """ copy into {db}.{schema}.{entity}
            from @{db}.{schema}.{entity}_stage
            file_format= {db}.{schema}.csv_format_yordan;""".format(db=db, schema=schema, entity=entity)

    try:
        con.cursor().execute(copy_data)

    except Exception as e:
        print(e)


def snowflake_connection():
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
            role=role,
            wharehouse=wh
        )
        return con
    except Exception as e:
        print(e)


def read_table(table):
    con = snowflake_connection()
    try:

        data = con.cursor().execute('select max(date_modified) from {db}.{schema}.{table}'
                                    .format(table=table, db=db, schema=schema)).fetchone()

        return data[0]
    except Exception as e:
        print(e)
    finally:
        con.close()

def get_last_id_from_table(table, entity_id):
    con = snowflake_connection()
    try:

        data = con.cursor().execute('select * from {db}.{schema}.{table} order by {entity_id} desc;'
                                    .format(table=table, db=db, schema=schema, entity_id=entity_id)).fetchall()

        return data[0][0]
    except Exception as e:
        print(e)
    finally:
        con.close()


def get_table_data(table, entity_id):
    con = snowflake_connection()
    data = 0
    try:

        data = con.cursor().execute('select * from {db}.{schema}.{table} order by {entity_id} asc'
                                    .format(table=table, db=db, schema=schema, entity_id=entity_id)).fetchall()

    except Exception as e:
        print(e)
    finally:
        con.close()

    return data

def get_last_date_from_table(table, entity_id):
    con = snowflake_connection()
    data = 0
    try:

        data = con.cursor().execute('select max(date_modified) from {db}.{schema}.{table}'
                                    .format(table=table, db=db, schema=schema, entity_id=entity_id)).fetchall()

    except Exception as e:
        print(e)
    finally:
        con.close()

    return data[0][0]

def populate_db():
    """
    Main function that structures everything to be executed chronologically

    :param:
    :return:
    """
    con = snowflake_connection()

    # database_config(con, schema)
    #
    # for entity in ['characters', 'creators', 'comics', 'events', 'characters_in_comics', 'characters_in_events', 'creators_in_comics']:
    #     con.cursor().execute("truncate table {db}.{schema}.{entity}".format(db=db, schema=schema, entity=entity))

    create_s3_stages_for_snowflake(con, db, schema)
    # copy_s3_stage_to_sf(con, db, schema, 'characters')

    con.close()

