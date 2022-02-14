import json
import os
import sys
import snowflake.connector
from snowplow.auth import get_snowflake_client
from snowplow.controller import TableObjectField, TableObject
from pydantic import parse_obj_as
from typing import List, Dict, Tuple, Union
import typer
import base64

query_app = typer.Typer()


def issue_query(
    query: str,
    query_args: Tuple,
    client: snowflake.connector.SnowflakeConnection = get_snowflake_client(),
) -> Union[List[tuple], List[dict]]:
    cs = client.cursor(snowflake.connector.DictCursor)
    try:
        cs.execute(query, query_args)
        result = cs.fetchall()
        cs.close()
        return result
    except Exception as e:
        print(e, sys.stderr)
    finally:
        cs.close()


def get_table_definition(
    table: str,
    schema: str = os.environ.get("SNOWFLAKE_SCHEMA"),
    client: snowflake.connector.SnowflakeConnection = get_snowflake_client(),
) -> TableObject:

    query = """
    SELECT
        ORDINAL_POSITION,
        COLUMN_NAME AS NAME,
        DATA_TYPE AS TYPE,
        CHARACTER_MAXIMUM_LENGTH AS LENGTH,
        NUMERIC_PRECISION AS PRECISION,
        NUMERIC_SCALE AS SCALE,
        IS_NULLABLE AS NILLABLE,
        COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA ILIKE %s -- PUT YOUR SCHEMA NAME HERE
    AND TABLE_NAME ILIKE %s  -- PUT YOUR TABLE NAME HERE
    ORDER BY ORDINAL_POSITION;
    """

    table_data = TableObject(
        name=table,
        schema=schema,
        fields=[],
        database=client.database,
        system="snowflake",
    )

    try:
        result = issue_query(query=query, query_args=(schema, table), client=client)
        table_data.fields = parse_obj_as(List[TableObjectField], result)
        return table_data
    finally:
        if not client.is_closed():
            client.close()


@query_app.command()
def run(query: str, base64_encoded: bool = False) -> Union[List[tuple], List[dict]]:

    if base64_encoded:
        query = base64.b64decode(query.encode()).decode()

    result = issue_query(query=query, query_args=())
    print(json.dumps(result, indent=2), file=sys.stdout)
    return result
