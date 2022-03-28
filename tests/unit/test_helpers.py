import requests

from src.helpers import retries_session, create_in_memory_csv


def test_retries_session_returns_requests_session_object():
    session = retries_session()
    assert isinstance(session, requests.Session)


def test_create_in_memory_csv_contains_expected_data():
    data_to_be_transformed = [{"col1": "val1", "col2": "val2"}]
    expected_data = "val1,val2\n"
    result = create_in_memory_csv(data_to_be_transformed)
    assert expected_data == result
