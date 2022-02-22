def simplify_character_data(character):
    character_dict = {'character_id': character.get('id', None),
                      'name': character.get('name', None),
                      'description': character.get('description', None),
                      'date_modified': character.get('modified', None)
                      }

    print(character_dict)
    return character_dict

def ingest_characters(limit=100):
    characters = []
    x = 0
    url = generate_url('comics')
    # try:
    #     while True:
    response = requests.get(url, params={'orderBy': 'title', 'offset': x})
    print(response.json())
    #         x += 100
    #         if x == 100:
    #             break
    # except:
    #     print('oopsie')

    print(characters)
    return characters