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
