import pytest
import responses
import requests
from mockito import when, mock, verify

from src.helpers import retries_session, generate_url
from src.ingestion import ingest_entity


url = "https://gateway.marvel.com:443/v1/public/characters?ts=1&limit=5&apikey=eef4a51f95df748e9c1effea6cc244f2&hash=fdc2fde7473f59a1544c0471493d6e1a&orderBy=modified&offset=0"


def test_retries_session_returns_requests_session_object():
    session = retries_session()
    assert isinstance(session, requests.Session)


@pytest.mark.skip
def test_retries_session_raises_retry_error_for_reaching_max_retries():
    session = retries_session()
    with pytest.raises(requests.exceptions.RetryError):
        session.get("https://httpstat.us/429")


@pytest.mark.skip
@pytest.mark.parametrize(
    "limit, offset, entity, order_by, modified",
    [(100, 0, "characters", "modified", None)],
)
def test_fetching_raises_maximum_retries_exceeded_error(
    limit, offset, entity, order_by, modified
):
    code = {"status_code": 502}
    mock_object = mock(code)

    with when(requests).get(...).thenReturn(mock_object):
        with pytest.raises(requests.exceptions.RequestException):
            ingest_entity(limit, offset, entity, order_by, modified)
        verify(requests, times=5).get(...)


@pytest.mark.skip
@pytest.mark.parametrize(
    "limit, offset, entity, order_by, modified",
    [(1, 0, "characters", "modified", None)],
)
def test_fetching_characters_with_successful_response(
    limit, offset, entity, order_by, modified
):
    expected_response = {"a": "dictionary"}
    mock_response = mock({"json": lambda: expected_response})
    with when(requests).get(...).thenReturn(mock_response):
        ingested_entity = ingest_entity(limit, offset, entity, order_by, modified)
        assert ingested_entity == expected_response
        verify(requests, times=1).get(generate_url("characters", 1))


@responses.activate
def test_chars_mock():
    # url = requests.get(generate_url('characters', 5), params={'orderBy': 'modified', 'offset': 0, 'modifiedSince': None}).url
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
