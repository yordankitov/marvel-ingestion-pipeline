import pandas as pd
import ast
from helpers import save_file

id = 2864
test = "[{'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21175', 'name': 'Incredible Hercules (2008) #117'}, {'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21324', 'name': 'Incredible Hercules (2008) #118'}, {'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21505', 'name': 'Incredible Hercules (2008) #119'}, {'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21707', 'name': 'Incredible Hercules (2008) #120'}]"

t = [[1011334, 'Avengers: The Initiative (2007) #14'], [1011334, 'Avengers: The Initiative (2007) #14 (SPOTLIGHT VARIANT)'], [1011334, 'Avengers: The Initiative (2007) #15'], [1011334, 'Avengers: The Initiative (2007) #16'], [1011334, 'Avengers: The Initiative (2007) #17'], [1011334, 'Avengers: The Initiative (2007) #18'], [1011334, 'Avengers: The Initiative (2007) #18 (ZOMBIE VARIANT)'], [1011334, 'Avengers: The Initiative (2007) #19'], [1011334, 'Deadpool (1997) #44'], [1011334, 'Marvel Premiere (1972) #35'], [1011334, 'Marvel Premiere (1972) #36'], [1011334, 'Marvel Premiere (1972) #37']], [[1017100, 'FREE COMIC BOOK DAY 2013 1 (2013) #1'], [1017100, 'Hulk (2008) #53'], [1017100, 'Hulk (2008) #54'], [1017100, 'Hulk (2008) #55']], [[1010699, 'Dark Avengers (2012) #177'], [1010699, 'Dark Avengers (2012) #179'], [1010699, 'Dark Avengers (2012) #180'], [1010699, 'Dark Avengers (2012) #181'], [1010699, 'Dark Avengers (2012) #182'], [1010699, 'Dark Avengers (2012) #183'], [1010699, 'Hulk (2008) #43'], [1010699, 'Universe X (2000) #6'], [1010699, 'Universe X (2000) #7'], [1010699, 'Universe X (2000) #8'], [1010699, 'Universe X (2000) #9'], [1010699, 'Universe X (2000) #10'], [1010699, 'Universe X (2000) #11'], [1010699, 'Universe X (2000) #12']]


def extract_names(cell):
    return [d['name'] for d in cell]


def create_dataframe_data(char_id, list_of_comics):
    comics = []
    for x in list_of_comics:
        comics.append(x)
    # print(data)
    return {'charcter_id': char_id, 'comics_name': comics}


def save_dataframe(data):
    df = pd.DataFrame(data)
    df.to_csv('data/characters_in_comics.csv', mode='a', index=False, header=False)


def extract_comics(char_id, comics):
    comics_list = extract_names(ast.literal_eval(comics))
    save_dataframe(create_dataframe_data(char_id, comics_list))


def check_returned_comics():
    df = pd.read_csv("data/characters.csv")
    ids = []
    for index, row in df.iterrows():
        char_id = row['character_id']

        if int(row['available_comics']) > int(row['fetched_comics']):
            ids.append(char_id)
        else:
            extract_comics(char_id, row['list_of_comics'])

    if ids:
        save_file(data=ids, file_path="data/characters_ids_for_comics_ingestion.txt")
