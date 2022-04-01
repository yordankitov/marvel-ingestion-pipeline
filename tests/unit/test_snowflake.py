
def test_schema_is_created(marvel_schema):
    assert marvel_schema == "tapde_db_yordan.dev_schema_marvel_test"

def test_characters_table_is_created(characters_table):
    assert characters_table == "tapde_db_yordan.dev_schema_marvel_test.characters"
