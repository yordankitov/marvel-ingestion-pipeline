import pytest
import requests
from src.helpers import retries_session


def test_retries_session_returns_requests_session_object():
    session = retries_session()
    assert isinstance(session, requests.Session)

def test_retries_session_raises_retry_error_for_reaching_max_retries():
    session = retries_session()
    with pytest.raises(requests.exceptions.RetryError):
        session.get('https://httpstat.us/429')