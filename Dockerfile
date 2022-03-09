FROM python:3.9.9

WORKDIR /

COPY pyproject.toml pyproject.toml

RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install
RUN poetry export -f requirements.txt --output requirements.txt

RUN ls
RUN pwd
RUN find -name 'requirements.txt'
#COPY build/requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "../src/main.py"]