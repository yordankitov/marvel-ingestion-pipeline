import pandas as pd
import csv
import ast

id = 2864
test = "[{'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21175', 'name': 'Incredible Hercules (2008) #117'}, {'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21324', 'name': 'Incredible Hercules (2008) #118'}, {'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21505', 'name': 'Incredible Hercules (2008) #119'}, {'resourceURI': 'http://gateway.marvel.com/v1/public/comics/21707', 'name': 'Incredible Hercules (2008) #120'}]"

t = [[1011334, 'Avengers: The Initiative (2007) #14'], [1011334, 'Avengers: The Initiative (2007) #14 (SPOTLIGHT VARIANT)'], [1011334, 'Avengers: The Initiative (2007) #15'], [1011334, 'Avengers: The Initiative (2007) #16'], [1011334, 'Avengers: The Initiative (2007) #17'], [1011334, 'Avengers: The Initiative (2007) #18'], [1011334, 'Avengers: The Initiative (2007) #18 (ZOMBIE VARIANT)'], [1011334, 'Avengers: The Initiative (2007) #19'], [1011334, 'Deadpool (1997) #44'], [1011334, 'Marvel Premiere (1972) #35'], [1011334, 'Marvel Premiere (1972) #36'], [1011334, 'Marvel Premiere (1972) #37']], [[1017100, 'FREE COMIC BOOK DAY 2013 1 (2013) #1'], [1017100, 'Hulk (2008) #53'], [1017100, 'Hulk (2008) #54'], [1017100, 'Hulk (2008) #55']], [[1010699, 'Dark Avengers (2012) #177'], [1010699, 'Dark Avengers (2012) #179'], [1010699, 'Dark Avengers (2012) #180'], [1010699, 'Dark Avengers (2012) #181'], [1010699, 'Dark Avengers (2012) #182'], [1010699, 'Dark Avengers (2012) #183'], [1010699, 'Hulk (2008) #43'], [1010699, 'Universe X (2000) #6'], [1010699, 'Universe X (2000) #7'], [1010699, 'Universe X (2000) #8'], [1010699, 'Universe X (2000) #9'], [1010699, 'Universe X (2000) #10'], [1010699, 'Universe X (2000) #11'], [1010699, 'Universe X (2000) #12']]

def read_file(file):
    with open(file, "r", encoding="utf-8") as f:
        content = f.readlines()
    return content


def check_for_further_calls_based_on_character_id(file):
    file
    type = ['comics', 'series', 'stories', 'events']
    further_urls = []
    for i in range(0, len(test[0]['data']['results'])):
        for x in type:
            result = test[0]['data']['results'][i][x]['available'] == test[0]['data']['results'][i][x]['returned']
            if not result:
                character_id = test[0]['data']['results'][i]['id']
                url = generate_url() + "/{id}/{type}".format(id=character_id, type=type)
                further_urls.append(url)
    print(further_urls)
    return further_urls


def extract_names(cell):
    return [d['name'] for d in cell]


def create_data(id, list_of_comics):
    comics = []
    for x in list_of_comics:
        comics.append(x)
    # print(data)
    return {'charcter_id': id, 'comics_name': comics}


def save_df(data):
    df = pd.DataFrame(data)
    df.to_csv('data/characters_in_comics.csv', mode='a', index=False, header=False)


def check_returned_comics():
    df = pd.read_csv("data/characters.csv")

    for index, row in df.iterrows():
        id = row['character_id']

        if int(row['available_comics']) > int(row['fetched_comics']):
            pass
            # print(id)
#             other logic here ...
        else:
            comics_list = extract_names(ast.literal_eval(row['list_of_comics']))
            save_df(create_data(id, comics_list))


check_returned_comics()

