import python.main as _main


# These uuids have been changed to be different to those
# found in the tas_data.json file
test_category = {
    "name": "sushi rolls",
    "uuid": "078075db-cc9d-4585-bd2d-5646c93fa098"
}

test_kitchen = {
    "extTenantUUID": "f63p7d52-6fd8-4d56-b954-a9ecb14e350a",
    "name": "3 COLD",
    "uuid": "05aea7c2-cc13-74od-055v-068e608b3ba4"
}


# TODO: fix these test
def test_category_merge_query():
    output = _main.process_categories(test_category)

    # print(output)
    validator = f"""
            merge into categories as target
            using (select '{test_category["uuid"]}' as id) as source
            on target.id = source.id
            when matched then
                update set 
                    name = '{test_category["name"].replace("'","")}'

            when not matched then
                insert (id, name)
                values(
                    '{test_category["uuid"]}'
                    , '{test_category["name"].replace("'","")}'
                )
            ;
    """
    assert output == validator


def test_kitchen_merge_query():
    output = _main.process_kitchen_stations(test_kitchen)

    validator = f"""merge into kitchen_stations as target
            using (select '{test_kitchen["uuid"]}' as id) as source
            on target.id = source.id
            when matched then 
                update set
                    ext_tenant_uuid = '{test_kitchen["extTenantUUID"]}' 
                    , name = '{test_kitchen["name"]}'
            when not matched then
                insert (id, name, ext_tenant_uuid)
                values (
                    '{test_kitchen["uuid"]}'
                    , '{test_kitchen["name"].replace("'","")}'
                    , '{test_kitchen["extTenantUUID"]}'
                )
            ;
    """
    assert output == validator

