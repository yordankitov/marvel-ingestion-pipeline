def simplify_character_data(character):
    character_dict = {'character_id': character.get('id', None),
                      'name': character.get('name', None),
                      'description': character.get('description', None),
                      'date_modified': character.get('modified', None)
                      }

    print(character_dict)
    return character_dict

