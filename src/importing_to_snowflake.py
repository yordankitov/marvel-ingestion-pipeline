import snowflake.connector
import os

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
    except Exception as e:
        print(e)

    return con

def populate_db():
    """
    Main function that structures everything to be executed chronologically

    :param:
    :return:
    """
    con = snowflake_connection(user, password, account, db, schema)

    database_config(con, schema)

    con.close()

populate_db()