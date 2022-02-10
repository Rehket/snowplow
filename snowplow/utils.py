import json
import os
import sys
import traceback
import typer
import snowflake.connector
from typing import Dict
from sys import stdout
from time import sleep
from snowplow.auth import get_snowflake_client, get_salesforce_client
from snowplow.builder import get_common_fields, build_salesforce_query
from snowplow.sfdc import get_object_definition
from snowplow.query import get_table_definition
import httpx

util_app = typer.Typer()


@util_app.command()
def get_salesforce_query(
    salesforce_object_name: str,
    snowflake_table_prefix: str = "SFDC_",
    snowflake_table_postfix: str = "_OBJECT",
    snowflake_schema: str = os.environ.get("SNOWFLAKE_SCHEMA"),
) -> None:
    print(
        json.dumps(
            build_salesforce_query(
                salesforce_object_name=salesforce_object_name,
                snowflake_table_prefix=snowflake_table_prefix,
                snowflake_table_postfix=snowflake_table_postfix,
                snowflake_schema=snowflake_schema,
            )
        ),
        file=sys.stdout,
    )


@util_app.command()
def scratch() -> str:
    """
    Does Nothing

    :return:
    """
    pass
