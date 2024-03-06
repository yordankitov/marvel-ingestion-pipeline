# Marvel ingestion pipeline

This is my first data engineering project done back in mid 2022. It is an ingestion pipeline that gets data from a REST 
API, serialises it, uploads it to AWS S3 as a data store and from there it gets uploaded to Snowflake. 

There's some things to be changed/added:
- [ ] simplify code
- [ ] add Airflow for orchestration
- [ ] add Sphinx conf
- [ ] add more docstring to create a more comprehensive documentation with Sphinx
- [ ] change gitlab ci to github actions
- [ ] get test coverage to at least 70%