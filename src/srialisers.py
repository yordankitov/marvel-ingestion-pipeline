
def simplify_comics_data(comics):
    comics_dict = {
        "comics_id": comics.get("id", None),
        "digital_id": comics.get("digitalId", None),
        "title": comics.get("title", None),
        "variant_description": comics.get("variantDescription", None),
        "description": comics.get("description", None),
        "date_modified": comics.get("modified", None),
        "isbn": comics.get("isbn", None),
        "upc": comics.get("upc", None),
        "diamond_code": comics.get("diamondCode", None),
        "ean": comics.get("ean", None),
        "issn": comics.get("issn", None),
        "format": comics.get("format", None),
        "page_count": comics.get("pageCount"),
        "print_price": comics.get("prices", None)[0].get("price"),
        "available_series": comics.get("series").get("available"),
        "fetched_series": comics.get("series").get("returned"),
        "list_of_series": comics.get("series").get("items"),
        "available_creators": comics.get("creators").get("available"),
        "fetched_creators": comics.get("creators").get("returned"),
        "list_of_creators": comics.get("creators").get("items"),
        "available_stories": comics.get("stories").get("available"),
        "fetched_stories": comics.get("stories").get("returned"),
        "list_of_stories": comics.get("stories").get("items"),
        "available_events": comics.get("events").get("available"),
        "fetched_events": comics.get("events").get("returned"),
        "list_of_events": comics.get("events").get("items"),
    }

    return comics_dict


def simplify_comics_from_characters(char_id, comics):
    return {"character_id": char_id, "comics_name": comics.get("title", None)}


def simplify_comics_from_creators(creator_id, comics):
    return {"creator_id": creator_id, "comics_name": comics.get("title", None)}


def simplify_creators_data(creators):
    creators_dict = {
        "creator_id": creators.get("id", None),
        "first_name": creators.get("firstName", None),
        "middle_name": creators.get("middleName", None),
        "last_name": creators.get("lastName", None),
        "suffix": creators.get("suffix", None),
        "full_name": creators.get("fullName", None),
        "date_modified": creators.get("modified", None),
        "available_comics": creators.get("comics").get("available", None),
        "fetched_comics": creators.get("comics").get("returned", None),
        "list_of_comics": creators.get("comics").get("items", None),
        "available_stories": creators.get("stories").get("available", None),
        "fetched_stories": creators.get("stories").get("returned", None),
        "list_of_stories": creators.get("stories").get("items", None),
        "available_series": creators.get("series").get("available", None),
        "fetched_series": creators.get("series").get("returned", None),
        "list_of_series": creators.get("series").get("items", None),
        "available_events": creators.get("events").get("available", None),
        "fetched_events": creators.get("events").get("returned", None),
        "list_of_events": creators.get("events").get("items", None),
    }

    return creators_dict


def simplify_events_data(events):
    events_dict = {
        "event_id": events.get("id", None),
        "title": events.get("title", None),
        "description": events.get("description", None),
        "date_modified": events.get("modified", None),
        "available_creators": events.get("creators").get("available", None),
        "fetched_creators": events.get("creators").get("returned", None),
        "list_of_creators": events.get("creators").get("items", None),
        "available_stories": events.get("stories").get("available", None),
        "fetched_stories": events.get("stories").get("returned", None),
        "list_of_stories": events.get("stories").get("items", None),
        "available_comics": events.get("comics").get("available", None),
        "fetched_comics": events.get("comics").get("returned", None),
        "list_of_comics": events.get("comics").get("items", None),
        "available_series": events.get("series").get("available", None),
        "fetched_series": events.get("series").get("returned", None),
        "list_of_series": events.get("series").get("items", None),
    }

    return events_dict


def simplify_events_from_characters(char_id, events):
    return {"character_id": char_id, "comics_name": events.get("title", None)}


def simplify_character_data(character):
    character_dict = {
        "character_id": character.get("id", None),
        "name": character.get("name", None),
        "description": character.get("description", None),
        "date_modified": character.get("modified", None),
        "available_comics": character.get("comics").get("available"),
        "fetched_comics": character.get("comics").get("returned"),
        "list_of_comics": character.get("comics").get("items"),
        "available_events": character.get("events").get("available"),
        "fetched_events": character.get("events").get("returned"),
        "list_of_events": character.get("events").get("items"),
    }

    return character_dict
