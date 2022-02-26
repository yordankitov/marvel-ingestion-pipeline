def simplify_creators_data(creators):
    creators_dict = {'creator_id': creators.get('id', None),
                     'first_name': creators.get('firstName', None),
                     'middle_name': creators.get('middleName', None),
                     'last_name': creators.get('lastName', None),
                     'suffix': creators.get('suffix', None),
                     'full_name': creators.get('fullName', None),
                     'date_modified': creators.get('modified', None),
                     'available_comics': creators.get('comics').get('available', None),
                     'fetched_comics': creators.get('comics').get('returned', None),
                     'list_of_comics': creators.get('comics').get('items', None),
                     'available_stories': creators.get('stories').get('available', None),
                     'fetched_stories': creators.get('stories').get('returned', None),
                     'list_of_stories': creators.get('stories').get('items', None),
                     'available_series': creators.get('series').get('available', None),
                     'fetched_series': creators.get('series').get('returned', None),
                     'list_of_series': creators.get('series').get('items', None),
                     'available_events': creators.get('events').get('available', None),
                     'fetched_events': creators.get('events').get('returned', None),
                     'list_of_events': creators.get('events').get('items', None)
                     }

    return creators_dict
