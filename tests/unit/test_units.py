import pytest
import responses
import requests
from mockito import when, mock, verify

from src.helpers import retries_session, generate_url, create_in_memory_csv
from src.ingestion import ingest_entity, APIUnreachableError, fetch_entity, check_for_another_request

url = "https://gateway.marvel.com:443/v1/public/characters?ts=1&limit=5&apikey=eef4a51f95df748e9c1effea6cc244f2&hash=fdc2fde7473f59a1544c0471493d6e1a&orderBy=modified&offset=0"


def test_retries_session_returns_requests_session_object():
    session = retries_session()
    assert isinstance(session, requests.Session)


def test_create_in_memory_csv_contains_expected_data():
    data_to_be_transformed = [{'col1': 'val1', 'col2': 'val2'}]
    expected_data = "val1,val2\n"
    result = create_in_memory_csv(data_to_be_transformed)
    assert expected_data == result
####################################################
# @pytest.mark.skip
# @pytest.mark.parametrize(
#     "http, modified_since, offset, order_by, url",
#     [(retries_session(), None, 0, "modified", generate_url('characters', 5))])
# def test_retries_session_raises_retry_error_for_reaching_max_retries(http, modified_since, offset, order_by, url):
#     mock_object = mock({"status_code": 429})
#     with pytest.raises(APIUnreachableError):
#     # with pytest.raises(requests.exceptions.RetryError):
#         with when(http).get(...).thenReturn(mock_object):
#             http.get(url)
#         # verify(http, times=6).get(url)
#     # print(err.status_code)

####################################################


@pytest.mark.parametrize("response", [{"status_code": 200, "data": {"total": 10, "offset": 0, "count": 4}}])
def test_ingest_entity_returns_true_for_another_request_when_more_data_entries_available(response):
    # response = {"status_code": 200, "data": {"total": 10, "offset": 0, "count": 4}}
    another_request = check_for_another_request(response)
    assert another_request is True

@pytest.mark.parametrize("response", [{"status_code": 200, "data": {"total": 10, "offset": 0, "count": 10}}])
def test_ingest_entity_returns_false_for_another_request_when_no_more_data_entries_available(response):
    # response = {"status_code": 200, "data": {"total": 10, "offset": 0, "count": 4}}
    another_request = check_for_another_request(response)
    assert another_request is False


@pytest.mark.parametrize(
    "http, modified_since, offset, order_by, url",
    [(retries_session(), None, 0, "modified", generate_url('characters', 5))])
@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409, 500, 501, 503, 504])
def test_fetching_entity_raises_api_unreachable_error_exception(
    http, modified_since, offset, order_by, url, status_code):

    code = {"status_code": status_code}
    mock_object = mock(code)

    with pytest.raises(APIUnreachableError) as err:
        with when(requests.Session).get(...).thenReturn(mock_object):
            fetch_entity(http, modified_since, offset, order_by, url)
    assert err.value.args[0] == f'Marvel API failed with a status code {status_code}'


@pytest.mark.parametrize(
    "http, modified_since, offset, order_by, url",
    [(retries_session(), None, 0, "modified", generate_url('characters', 5))])
def test_fetching_entity_with_successful_response(
    http, modified_since, offset, order_by, url):

    entity = fetch_entity(http, modified_since, offset, order_by, url)

    assert entity.json().get('code') == 200
    assert entity.json().get('data').get('count') == 5
    assert isinstance(entity.json().get('data').get('results'), list)








@responses.activate
def test_chars_mock():
    response_test = {"data": {"count": 10, "total": 20, "offset": 0, "results": [5]}}
    responses.add(responses.GET, url, json=response_test)
    expected_data = response_test["data"]["results"]
    expected_another_request = True

    character_results, another_request = ingest_entity(
        5, 0, "characters", "modified", None
    )

    assert expected_data == character_results["data"]["results"]
    assert expected_another_request == another_request


@responses.activate
def test_invalid_url_raises_exception():
    responses.add(responses.GET, url, body=Exception("..."))
    with pytest.raises(Exception):
        requests.get(
            "https://gateway.marvel.com:443/v1/c/characters?ts=1&limit=5&apikey=eef4a51f95df748e9c1effea6cc244f2&hash=fdc2fde7473f59a1544c0471493d6e1a&orderBy=modified&offset=0"
        )

