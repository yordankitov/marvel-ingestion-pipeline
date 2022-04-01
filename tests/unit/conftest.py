import os

import pytest
import snowflake.connector

from src.snowflake import create_characters_table_stmt

SNOWFLAKE_TEST_PARAMS = {
    "user": "TAPDE_USER_yordan",
    "role": "TAPDE_ROLE_YORDAN",
    "password": "Dragclimber0",
    "account": "infinitelambda.eu-west-1",
    "warehouse": "TAPDE_WH",
    "database": "TAPDE_DB_YORDAN"
}

# SNOWFLAKE_TEST_PARAMS = {
#     "user": "yordankitov",
#     "password": "Dragclimber0",
#     "db": "SNOWFLAKE_SAMPLE_DATA",
#     "schema": "WEATHER",
#     "account": "vz89800.eu-central-1",
#     "role": "ACCOUNTADMIN",
#     "wh": "COMPUTE_WH"
# }
@pytest.fixture(scope="session")
def marvel_schema():

    with snowflake.connector.connect(**SNOWFLAKE_TEST_PARAMS) as conn:
        schema_name = f"{conn.database}.DEV_SCHEMA_MARVEL_TEST"
        conn.cursor().execute(f"create schema if not exists {schema_name};")

    yield schema_name.lower()

    with snowflake.connector.connect(**SNOWFLAKE_TEST_PARAMS) as conn:
        conn.cursor().execute(f"drop schema {schema_name};")

@pytest.fixture(scope='session')
def characters_table(marvel_schema):
    with snowflake.connector.connect(**SNOWFLAKE_TEST_PARAMS) as conn:
        schema = marvel_schema
        conn.cursor().execute(f"use schema {schema}")
        conn.cursor().execute(create_characters_table_stmt())
        table_name = f"{schema}.characters"

    # dont need to yield as when the schema is dropped, everything inside is as well
    yield table_name

    with snowflake.connector.connect(**SNOWFLAKE_TEST_PARAMS) as conn:
        conn.cursor().execute(f"drop table {table_name}")

