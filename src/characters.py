def simplify_character_data(character):
    character_dict = {'character_id': character.get('id', None),
                      'name': character.get('name', None),
                      'description': character.get('description', None),
                      'date_modified': character.get('modified', None),
                      'available_comics': character.get('comics').get('available'),
                      'fetched_comics': character.get('comics').get('returned'),
                      'list_of_comics': character.get('comics').get('items'),
                      'available_events': character.get('events').get('available'),
                      'fetched_events': character.get('events').get('returned'),
                      'list_of_events': character.get('events').get('items'),
                      }

    return character_dict

