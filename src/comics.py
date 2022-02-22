def simplify_comics_data(comics):
    character_dict = {'comics_id': comics.get('id', None),
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
                      'print_price': comics.get('price', None)
                      }

    print(character_dict)
    return character_dict