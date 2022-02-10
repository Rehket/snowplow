import typer
import sys
import json
import httpx
import snowflake.connector
from pathlib import Path
from snowplow.utils import util_app
from snowplow.query import query_app
from snowplow.auth import get_snowflake_client, get_salesforce_client


app = typer.Typer()
#
app.add_typer(util_app, name="utils")
app.add_typer(query_app, name="query")


@app.command()
def check(
    json_out: bool = typer.Option(
        True, help="Should the command output be printed to stdout as json?"
    )
):
    """
    Check the login capability for snowflake and salesforce.

    """
    client: httpx.Client
    snowflake_client: snowflake.connector.SnowflakeConnection
    try:
        client = get_salesforce_client()
        snowflake_client = get_snowflake_client()
    except Exception as e:
        print(json.dumps({"status": "failed", "message": e}), file=sys.stderr)
        raise typer.Exit(1)
    finally:
        if client:
            client.close()
        if snowflake_client:
            snowflake_client.close()
    print(
        json.dumps(
            {
                "status": "success",
                "message": "Successfully authenticated against Snowflake and Salesforce",
            }
        ),
        file=sys.stdout,
    )
    raise typer.Exit()


if __name__ == "__main__":

    app()
