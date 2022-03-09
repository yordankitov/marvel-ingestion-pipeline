import os
import requests
import hashlib
import pandas as pd
from io import StringIO
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


BASE_URL = "https://gateway.marvel.com:443/v1/public/{type}?ts={time_stamp}&limit={limit}&apikey={api_key}&hash={hash}"


def generate_url(entity, limit):
    ts = 1
    return BASE_URL.format(
        type=entity,
        time_stamp=ts,
        limit=limit,
        api_key=os.environ["API_KEY2"],
        hash=create_hash_for_login(ts),
    )


def create_hash_for_login(ts):
    keys = str(os.environ["PRIVATE_KEY2"] + os.environ["API_KEY2"])
    hashed = hashlib.md5(str(ts).encode() + keys.encode())

    return hashed.hexdigest()


def retries_session():
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http


def create_in_memory_csv(data, headers=None):
    stream = StringIO()
    df = pd.DataFrame(data)
    df.to_csv(stream, mode="a", index=False, header=False)
    csv_string_object = stream.getvalue()

    return csv_string_object


def create_in_memory_file(data):
    stream = StringIO()
    stream.write(str(data))
    content = stream.getvalue()

    return content
