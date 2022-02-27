def simplify_comics_data(comics):
    comics_dict = {'comics_id': comics.get('id', None),
                      'digital_id': comics.get('digitalId', None),
                      'title': comics.get('title', None),
                      'variant_description': comics.get('variantDescription', None),
                      'description': comics.get('description', None),
                      'date_modified': comics.get('modified', None),
                      'isbn': comics.get('isbn', None),
                      'upc': comics.get('upc', None),
                      'diamond_code': comics.get('diamondCode', None),
                      'ean': comics.get('ean', None),
                      'issn': comics.get('issn', None),
                      'format': comics.get('format', None),
                      'page_count': comics.get('pageCount'),
                      'print_price': comics.get('price', None),
                      'available_series': comics.get('series').get('available'),
                      'fetched_series': comics.get('series').get('returned'),
                      'list_of_series': comics.get('series').get('items'),
                      'available_creators': comics.get('creators').get('available'),
                      'fetched_creators': comics.get('creators').get('returned'),
                      'list_of_creators': comics.get('creators').get('items'),
                      'available_stories': comics.get('stories').get('available'),
                      'fetched_stories': comics.get('stories').get('returned'),
                      'list_of_stories': comics.get('stories').get('items'),
                      'available_events': comics.get('events').get('available'),
                      'fetched_events': comics.get('events').get('returned'),
                      'list_of_events': comics.get('events').get('items')
                      }

    return comics_dict


def simplify_comics_from_characters(char_id, comics):
    return {'character_id': char_id,
            'comics_name': comics.get('title', None)}


def simplify_comics_from_creators(creator_id, comics):
    return {'creator_id': creator_id,
            'comics_name': comics.get('title', None)}

# comics_id,digital_id,title,variant_description,description,date_modified,isbn,upc,diamond_code,ean,issn,format,page_count,print_price,available_series,fetched_series,list_of_series,available_creators,fetched_creators,list_of_creators,available_stories,fetched_stories,list_of_stories,available_events,fetched_events,list_of_events
