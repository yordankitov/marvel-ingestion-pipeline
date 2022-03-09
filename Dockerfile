FROM python:3.9.9

WORKDIR /

COPY . .

RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install
RUN poe export_requirements

#RUN ls
#RUN pwd
#RUN find -name 'requirements.txt'
COPY ./requirements.txt requirements.txt
#RUN pip install -r requirements.txt

#CMD ["python", "../src/main.py"]