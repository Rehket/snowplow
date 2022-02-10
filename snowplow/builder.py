import os

from snowplow.controller import TableObject, TableObjectField
from typing import List, Dict
import snowflake.connector
from snowplow.auth import get_snowflake_client, get_salesforce_client
import httpx

from snowplow.sfdc import get_object_definition
from snowplow.query import get_table_definition


def get_common_fields(
    first_object: TableObject, second_object: TableObject
) -> List[TableObjectField]:
    """
    Gets the common fields between two table object definitions.
    :param first_object: The first TableObject to compare.
    :param second_object: The second TableObject to compare.
    :return: The list of TableObjectFields that are shared between the two objects.
    """
    common_fields = first_object.to_field_name_set().intersection(
        second_object.to_field_name_set()
    )

    return [field for field in second_object.fields if field.name.upper() in common_fields]


def build_salesforce_query(
    salesforce_object_name: str,
    snowflake_table_prefix: str = "SFDC_",
    snowflake_table_postfix: str = "_OBJECT",
    snowflake_schema: str = os.environ.get("SNOWFLAKE_SCHEMA"),
    snowflake_client: snowflake.connector.SnowflakeConnection = get_snowflake_client(),
    salesforce_client: httpx.Client = get_salesforce_client(),
) -> Dict[str, str]:

    try:

        salesforce_table = get_object_definition(
            object=salesforce_object_name, client=salesforce_client
        )
        snowflake_table_name = (
            f"{snowflake_table_prefix}{salesforce_object_name}{snowflake_table_postfix}"
        )
        snowflake_table = get_table_definition(
            table=snowflake_table_name, schema=snowflake_schema, client=snowflake_client
        )

        # print(salesforce_table)
        common_field_list = get_common_fields(
            first_object=salesforce_table, second_object=snowflake_table
        )

        if len(common_field_list) < 1:
            raise RuntimeError(f"There are no common fields between Salesforce object: {salesforce_object_name} and snowflake table {snowflake_schema}.{snowflake_table_name}")

        query = (
            f"SELECT {','.join([field.name.upper() for field in common_field_list])} FROM {salesforce_object_name}"
        )
        salesforce_client.close()
        snowflake_client.close()
        return {
            "status": "success",
            "salesforce_object": salesforce_object_name,
            "snowflake_table": snowflake_table_name,
            "query": query,
        }
    except Exception as e:
        return {
            "status": "failed",
            "message": str(e),
        }
    finally:
        salesforce_client.close()
        snowflake_client.close()
