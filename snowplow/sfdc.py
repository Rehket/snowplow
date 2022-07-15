import os
from snowplow.auth import get_salesforce_client
from snowplow.controller import TableObjectField, TableObject
import httpx
from tenacity import (
    Retrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


def get_object_definition(
    object: str,
    client: httpx.Client = get_salesforce_client(),
    api_version: str = "53.0",
    skip_fields: str = os.environ.get("SFDC_SKIP_FIELDS", None),
    required_fields: str = os.environ.get("SFDC_REQUIRED_FIELDS", None),
    include_compound_fields: bool = False,
    max_attempts: int = os.getenv("SFDC_MAX_DOWNLOAD_ATTEMPTS", 20),
) -> TableObject:
    """
    Get SObject Metadata from SFDC to compare against SnowFlake.
    :param object: The Api name of the object to retrieve.
    :param client: A client authenticated against salesforce
    :param api_version: The api version to use when talking to salesforce.
    :param skip_fields: Fields to skip when building the object definition.
    :param include_compound_fields: Whether or not to include compound fields in the object definition.
    :param max_attempts: The maximum number of attempts before gining up.
    :return: The table object.
    """
    if skip_fields:
        skip_fields = [field.lower() for field in skip_fields.split(",")]
    else:
        skip_fields = []

    if required_fields:
        required_fields = [field.lower() for field in required_fields.split(",")]
    else:
        required_fields = []

    try:
        for attempt in Retrying(
            retry=retry_if_exception_type(httpx.ReadTimeout),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=4, max=60),
        ):
            with attempt:
                response = client.get(
                    url=f"/services/data/v{api_version}/sobjects/{object}/describe/"
                )
    finally:
        client.close()
    object_data = response.json()
    if include_compound_fields:
        compound_field_names = []
    else:
        compound_field_names = [
            field.get("compoundFieldName").lower()
            for field in object_data.get("fields")
            if field.get("compoundFieldName") is not None
        ]

    table = TableObject(
        name=object_data.get("name"),
        label=object_data.get("label"),
        fields=[
            TableObjectField(**field)
            for field in response.json().get("fields")
            if (
                field.get("name").lower() in required_fields
                or (
                    field.get("name").lower() not in skip_fields
                    and field.get("name").lower() not in compound_field_names
                )
            )
        ],
        system="salesforce",
    )
    return table


if __name__ == "__main__":
    print(get_object_definition("Lead").json(indent=2))
