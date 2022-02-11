import os
import sys
import snowflake.connector
from snowplow.auth import get_salesforce_client
from snowplow.controller import TableObjectField, TableObject
from pydantic import parse_obj_as
from typing import List
import httpx


def get_object_definition(
    object: str,
    client: httpx.Client = get_salesforce_client(),
    api_version: str = "53.0",
    skip_fields: str = os.environ.get("SFDC_SKIP_FIELDS"),
    include_compound_fields: bool = False,
) -> TableObject:
    if skip_fields:
        skip_fields = [field.lower() for field in skip_fields.split(",")]
    else:
        skip_fields = []

    response = client.get(
        url=f"/services/data/v{api_version}/sobjects/{object}/describe/"
    )
    object_data = response.json()
    if include_compound_fields:
        compound_field_names = []
    else:
        compound_field_names = [field.get("compoundFieldName") for field in object_data.get("fields")]

    table = TableObject(
        name=object_data.get("name"),
        label=object_data.get("label"),
        fields=[
            TableObjectField(**field)
            for field in response.json().get("fields")
            if (field.get("name") not in skip_fields and field.get("name") not in compound_field_names)

        ],
        system="salesforce",
    )
    return table


if __name__ == "__main__":
    print(get_object_definition("Lead").json(indent=2))
