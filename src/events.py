def simplify_events_from_characters(char_id, events):
    return {'character_id': char_id,
            'comics_name': events.get('title', None)}