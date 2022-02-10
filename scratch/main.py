# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import httpx

from snowplow.query import get_table_definition
from snowplow.sfdc import get_object_definition

# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    #
    # with httpx.Client() as sfdc_client:
    #     obj_data = sfdc_client.get(
    #         "https://rehket-big-load-test.my.salesforce.com/services/data/v51.0/sobjects/account/describe",
    #         headers={
    #             "Accept": "application/json",
    #             "Authorization": "Bearer 00D5f000005vXXH!AQIAQCTwz6pxN.ZkH..CVUmsoZbF5goN3Nb8ZOE7T3WYw8rzv5fTnir734Jy0cLDOZV6HpK2aAZs0LN4aIaegOV9G2K9Mjlo",
    #         },
    #     )
    #
    # if obj_data.status_code != 200:
    #     raise RuntimeError(
    #         f"SFDC Request Failed: {obj_data.status_code}:{obj_data.content.decode()}"
    #     )
    #
    # account_data = SalesForceObject.parse_obj(obj_data.json())
    # print(account_data.get_non_compound_fields())

    print(get_table_definition(schema="PUBLIC", table="SFDC_LEAD").json(indent=2))
    print(get_object_definition("Lead").json(indent=2))