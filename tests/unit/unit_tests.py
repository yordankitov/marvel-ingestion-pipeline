import pytest
import requests
from mockito import when, mock, verify

from src.helpers import retries_session, generate_url
from src.ingestion import ingest_entity

@pytest.mark.skip
def test_retries_session_returns_requests_session_object():
    session = retries_session()
    assert isinstance(session, requests.Session)

@pytest.mark.skip
def test_retries_session_raises_retry_error_for_reaching_max_retries():
    session = retries_session()
    with pytest.raises(requests.exceptions.RetryError):
        session.get('https://httpstat.us/429')



@pytest.mark.parametrize("limit, offset, entity, order_by, modified", [(100, 0, 'characters', 'modified', None)])
def test_fetching_raises_maximum_retries_exceeded_error(limit, offset, entity, order_by, modified):

    with when(requests).get(...).thenReturn(mock({"status_code": 502})):
        with pytest.raises(requests.exceptions.RequestException):
            ingest_entity(limit, offset, entity, order_by, modified)
        verify(requests, times=5).get(...)


@pytest.mark.skip
@pytest.mark.parametrize("limit, offset, entity, order_by, modified", [(1, 0, 'characters', 'modified', None)])
# its working but have to ask how to test the result
def test_fetching_weather_forecast_with_successful_response(limit, offset, entity, order_by, modified):
    expected_response = {"a": "dictionary"}
    mock_response = mock({"json": lambda: expected_response})
    with when(requests).get(...).thenReturn(mock_response):
        ingested_entity = ingest_entity(limit, offset, entity, order_by, modified)
        assert ingested_entity == expected_response
        verify(requests, times=1).get(generate_url('characters', 1))

