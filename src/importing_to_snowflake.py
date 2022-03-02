import snowflake.connector
import os

user = os.getenv('USER')
password = os.getenv('PASSWORD')
db = os.getenv('DATABASE')
account = os.getenv('ACCOUNT')
schema = os.getenv('SCHEMA')


def create_schema(con):
    """
    Creates the schema of the db
    :param con: The connection to the db
    :return:
    """
    create_db = "create database if not exists {db};".format(db=db)
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
    	primary key (ID)
    );"""

    try:
        con.cursor().execute(create_db)
        con.cursor().execute(create_characters_table)
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
    con = snowflake_connection()

    create_schema(con)

    con.close()