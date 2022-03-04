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
                      'print_price': comics.get('price', None)[0].get('price'),
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


test = {
  "code": 200,
  "status": "Ok",
  "copyright": "© 2022 MARVEL",
  "attributionText": "Data provided by Marvel. © 2022 MARVEL",
  "attributionHTML": "<a href=\"http://marvel.com\">Data provided by Marvel. © 2022 MARVEL</a>",
  "etag": "fb0ae523e35db7eb188c384c31fabcdf030fa044",
  "data": {
    "offset": 0,
    "limit": 5,
    "total": 51300,
    "count": 5,
    "results": [
      {
        "id": 82967,
        "digitalId": 0,
        "title": "Marvel Previews (2017)",
        "issueNumber": 0,
        "variantDescription": "",
        "description": "",
        "modified": "2019-11-07T08:46:15-0500",
        "isbn": "",
        "upc": "75960608839302811",
        "diamondCode": "",
        "ean": "",
        "issn": "",
        "format": "",
        "pageCount": 112,
        "textObjects": [],
        "resourceURI": "http://gateway.marvel.com/v1/public/comics/82967",
        "urls": [
          {
            "type": "detail",
            "url": "http://marvel.com/comics/issue/82967/marvel_previews_2017?utm_campaign=apiRef&utm_source=d42af6b6ec93989a2e67e62f24735096"
          }
        ],
        "series": {
          "resourceURI": "http://gateway.marvel.com/v1/public/series/23665",
          "name": "Marvel Previews (2017 - Present)"
        },
        "variants": [
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82965",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82970",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82969",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/74697",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/72736",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/75668",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65364",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65158",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65028",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/75662",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/74320",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/73776",
            "name": "Marvel Previews (2017)"
          }
        ],
        "collections": [],
        "collectedIssues": [],
        "dates": [
          {
            "type": "onsaleDate",
            "date": "2099-10-30T00:00:00-0500"
          },
          {
            "type": "focDate",
            "date": "2019-10-07T00:00:00-0400"
          }
        ],
        "prices": [
          {
            "type": "printPrice",
            "price": 0
          }
        ],
        "thumbnail": {
          "path": "http://i.annihil.us/u/prod/marvel/i/mg/b/40/image_not_available",
          "extension": "jpg"
        },
        "images": [],
        "creators": {
          "available": 1,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82967/creators",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/creators/10021",
              "name": "Jim Nausedas",
              "role": "editor"
            }
          ],
          "returned": 1
        },
        "characters": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82967/characters",
          "items": [],
          "returned": 0
        },
        "stories": {
          "available": 2,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82967/stories",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/183698",
              "name": "cover from Marvel Previews (2017)",
              "type": "cover"
            },
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/183699",
              "name": "story from Marvel Previews (2017)",
              "type": "interiorStory"
            }
          ],
          "returned": 2
        },
        "events": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82967/events",
          "items": [],
          "returned": 0
        }
      },
      {
        "id": 82965,
        "digitalId": 0,
        "title": "Marvel Previews (2017)",
        "issueNumber": 0,
        "variantDescription": "",
        "description": "",
        "modified": "2019-08-21T17:11:27-0400",
        "isbn": "",
        "upc": "75960608839302611",
        "diamondCode": "JUL190068",
        "ean": "",
        "issn": "",
        "format": "",
        "pageCount": 152,
        "textObjects": [],
        "resourceURI": "http://gateway.marvel.com/v1/public/comics/82965",
        "urls": [
          {
            "type": "detail",
            "url": "http://marvel.com/comics/issue/82965/marvel_previews_2017?utm_campaign=apiRef&utm_source=d42af6b6ec93989a2e67e62f24735096"
          }
        ],
        "series": {
          "resourceURI": "http://gateway.marvel.com/v1/public/series/23665",
          "name": "Marvel Previews (2017 - Present)"
        },
        "variants": [
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82967",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82970",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82969",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/74697",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/72736",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/75668",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65364",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65158",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65028",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/75662",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/74320",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/73776",
            "name": "Marvel Previews (2017)"
          }
        ],
        "collections": [],
        "collectedIssues": [],
        "dates": [
          {
            "type": "onsaleDate",
            "date": "2099-08-28T00:00:00-0500"
          },
          {
            "type": "focDate",
            "date": "2019-08-05T00:00:00-0400"
          }
        ],
        "prices": [
          {
            "type": "printPrice",
            "price": 0
          }
        ],
        "thumbnail": {
          "path": "http://i.annihil.us/u/prod/marvel/i/mg/b/40/image_not_available",
          "extension": "jpg"
        },
        "images": [],
        "creators": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82965/creators",
          "items": [],
          "returned": 0
        },
        "characters": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82965/characters",
          "items": [],
          "returned": 0
        },
        "stories": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82965/stories",
          "items": [],
          "returned": 0
        },
        "events": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82965/events",
          "items": [],
          "returned": 0
        }
      },
      {
        "id": 82970,
        "digitalId": 0,
        "title": "Marvel Previews (2017)",
        "issueNumber": 0,
        "variantDescription": "",
        "description": "",
        "modified": "2020-02-07T09:35:32-0500",
        "isbn": "",
        "upc": "75960608839303111",
        "diamondCode": "",
        "ean": "",
        "issn": "",
        "format": "",
        "pageCount": 112,
        "textObjects": [],
        "resourceURI": "http://gateway.marvel.com/v1/public/comics/82970",
        "urls": [
          {
            "type": "detail",
            "url": "http://marvel.com/comics/issue/82970/marvel_previews_2017?utm_campaign=apiRef&utm_source=d42af6b6ec93989a2e67e62f24735096"
          },
          {
            "type": "purchase",
            "url": "http://comicstore.marvel.com/Marvel-Previews-0/digital-comic/52952?utm_campaign=apiRef&utm_source=d42af6b6ec93989a2e67e62f24735096"
          }
        ],
        "series": {
          "resourceURI": "http://gateway.marvel.com/v1/public/series/23665",
          "name": "Marvel Previews (2017 - Present)"
        },
        "variants": [
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82967",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82965",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/82969",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/74697",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/72736",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/75668",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65364",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65158",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/65028",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/75662",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/74320",
            "name": "Marvel Previews (2017)"
          },
          {
            "resourceURI": "http://gateway.marvel.com/v1/public/comics/73776",
            "name": "Marvel Previews (2017)"
          }
        ],
        "collections": [],
        "collectedIssues": [],
        "dates": [
          {
            "type": "onsaleDate",
            "date": "2099-01-29T00:00:00-0500"
          },
          {
            "type": "focDate",
            "date": "2020-01-06T00:00:00-0500"
          }
        ],
        "prices": [
          {
            "type": "printPrice",
            "price": 0
          }
        ],
        "thumbnail": {
          "path": "http://i.annihil.us/u/prod/marvel/i/mg/c/80/5e3d7536c8ada",
          "extension": "jpg"
        },
        "images": [
          {
            "path": "http://i.annihil.us/u/prod/marvel/i/mg/c/80/5e3d7536c8ada",
            "extension": "jpg"
          }
        ],
        "creators": {
          "available": 1,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82970/creators",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/creators/10021",
              "name": "Jim Nausedas",
              "role": "editor"
            }
          ],
          "returned": 1
        },
        "characters": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82970/characters",
          "items": [],
          "returned": 0
        },
        "stories": {
          "available": 1,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82970/stories",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/183704",
              "name": "cover from Marvel Previews (2017)",
              "type": "cover"
            }
          ],
          "returned": 1
        },
        "events": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/82970/events",
          "items": [],
          "returned": 0
        }
      },
      {
        "id": 331,
        "digitalId": 0,
        "title": "Gun Theory (2003) #4",
        "issueNumber": 4,
        "variantDescription": "",
        "description": "",
        "modified": "-0001-11-30T00:00:00-0500",
        "isbn": "",
        "upc": "5960605468-00111",
        "diamondCode": "",
        "ean": "",
        "issn": "",
        "format": "Comic",
        "pageCount": 0,
        "textObjects": [
          {
            "type": "issue_solicit_text",
            "language": "en-us",
            "text": "The phone rings, and killer-for-hire Harvey embarks on another hit. But nothing's going right this job. There's little room for error in the business of killing - so what happens when one occurs?\r\n32 PGS./ PARENTAL ADVISORY ...$2.50"
          }
        ],
        "resourceURI": "http://gateway.marvel.com/v1/public/comics/331",
        "urls": [
          {
            "type": "detail",
            "url": "http://marvel.com/comics/issue/331/gun_theory_2003_4?utm_campaign=apiRef&utm_source=d42af6b6ec93989a2e67e62f24735096"
          }
        ],
        "series": {
          "resourceURI": "http://gateway.marvel.com/v1/public/series/649",
          "name": "Gun Theory (2003)"
        },
        "variants": [],
        "collections": [],
        "collectedIssues": [],
        "dates": [
          {
            "type": "onsaleDate",
            "date": "2029-12-31T00:00:00-0500"
          },
          {
            "type": "focDate",
            "date": "-0001-11-30T00:00:00-0500"
          }
        ],
        "prices": [
          {
            "type": "printPrice",
            "price": 2.5
          }
        ],
        "thumbnail": {
          "path": "http://i.annihil.us/u/prod/marvel/i/mg/c/60/4bc69f11baf75",
          "extension": "jpg"
        },
        "images": [
          {
            "path": "http://i.annihil.us/u/prod/marvel/i/mg/c/60/4bc69f11baf75",
            "extension": "jpg"
          }
        ],
        "creators": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/331/creators",
          "items": [],
          "returned": 0
        },
        "characters": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/331/characters",
          "items": [],
          "returned": 0
        },
        "stories": {
          "available": 2,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/331/stories",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/2263",
              "name": "Interior #2263",
              "type": "interiorStory"
            },
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/65423",
              "name": "GUN THEORY 4 cover",
              "type": "cover"
            }
          ],
          "returned": 2
        },
        "events": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/331/events",
          "items": [],
          "returned": 0
        }
      },
      {
        "id": 183,
        "digitalId": 0,
        "title": "Startling Stories: The Incorrigible Hulk (2004) #1",
        "issueNumber": 1,
        "variantDescription": "",
        "description": "For Doctor Bruce Banner life is anything but normal. But what happens when two women get between him and his alter ego, the Incorrigible Hulk? Hulk confused! \r\nIndy superstar Peter Bagge (THE MEGALOMANIACAL SPIDER-MAN) takes a satirical jab at the Hulk mythos with a tale of dames, debauchery and destruction.\r\n32 PGS./MARVEL PSR...$2.99",
        "modified": "-0001-11-30T00:00:00-0500",
        "isbn": "",
        "upc": "5960605429-00811",
        "diamondCode": "",
        "ean": "",
        "issn": "",
        "format": "Comic",
        "pageCount": 0,
        "textObjects": [
          {
            "type": "issue_solicit_text",
            "language": "en-us",
            "text": "For Doctor Bruce Banner life is anything but normal. But what happens when two women get between him and his alter ego, the Incorrigible Hulk? Hulk confused! \r\nIndy superstar Peter Bagge (THE MEGALOMANIACAL SPIDER-MAN) takes a satirical jab at the Hulk mythos with a tale of dames, debauchery and destruction.\r\n32 PGS./MARVEL PSR...$2.99"
          }
        ],
        "resourceURI": "http://gateway.marvel.com/v1/public/comics/183",
        "urls": [
          {
            "type": "detail",
            "url": "http://marvel.com/comics/issue/183/startling_stories_the_incorrigible_hulk_2004_1?utm_campaign=apiRef&utm_source=d42af6b6ec93989a2e67e62f24735096"
          }
        ],
        "series": {
          "resourceURI": "http://gateway.marvel.com/v1/public/series/565",
          "name": "Startling Stories: The Incorrigible Hulk (2004)"
        },
        "variants": [],
        "collections": [],
        "collectedIssues": [],
        "dates": [
          {
            "type": "onsaleDate",
            "date": "2029-12-31T00:00:00-0500"
          },
          {
            "type": "focDate",
            "date": "-0001-11-30T00:00:00-0500"
          }
        ],
        "prices": [
          {
            "type": "printPrice",
            "price": 2.99
          }
        ],
        "thumbnail": {
          "path": "http://i.annihil.us/u/prod/marvel/i/mg/b/40/image_not_available",
          "extension": "jpg"
        },
        "images": [],
        "creators": {
          "available": 1,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/183/creators",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/creators/6291",
              "name": "Peter Bagge",
              "role": "penciller (cover)"
            }
          ],
          "returned": 1
        },
        "characters": {
          "available": 1,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/183/characters",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/characters/1009351",
              "name": "Hulk"
            }
          ],
          "returned": 1
        },
        "stories": {
          "available": 2,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/183/stories",
          "items": [
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/1891",
              "name": "Cover #1891",
              "type": "cover"
            },
            {
              "resourceURI": "http://gateway.marvel.com/v1/public/stories/1892",
              "name": "Interior #1892",
              "type": "interiorStory"
            }
          ],
          "returned": 2
        },
        "events": {
          "available": 0,
          "collectionURI": "http://gateway.marvel.com/v1/public/comics/183/events",
          "items": [],
          "returned": 0
        }
      }
    ]
  }
}

print(test['data']['results'][0].get('prices')[0].get('price'))