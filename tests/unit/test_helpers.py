import requests

from src.helpers import retries_session, create_in_memory_csv, generate_url, create_in_memory_file


def test_retries_session_returns_requests_session_object():
    session = retries_session()
    assert isinstance(session, requests.Session)


def test_create_in_memory_csv_contains_expected_data():
    data_to_be_transformed = [{"col1": "val1", "col2": "val2"}]
    expected_data = "val1,val2\n"
    result = create_in_memory_csv(data_to_be_transformed)
    assert expected_data == result


def test_create_in_memory_file_contains_expected_data():
    data = [{"col1": "val1", "col2": "val2"}]
    expected_data = "[{'col1': 'val1', 'col2': 'val2'}]"
    result = create_in_memory_file(data)
    assert expected_data == result


def test_generate_url_returns_a_valid_url():
    expected = "https://gateway.marvel.com:443/v1/public/characters?ts=1&limit=100&apikey=eef4a51f95df748e9c1effea6cc244f2&hash=fdc2fde7473f59a1544c0471493d6e1a"
    actual = generate_url('characters', 100)

    assert expected == actual

