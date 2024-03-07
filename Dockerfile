FROM python:3.11-slim-buster

WORKDIR /

COPY pyproject.toml pyproject.toml

RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install
RUN poetry export -f requirements.txt --output requirements.txt


RUN pip install -r requirements.txt
COPY . .
