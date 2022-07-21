from snowplow.sfdc import get_object_definition
from httpx import Client
import os
from unittest import mock


import pytest


TEST_CREDS = {
    "SFDC_USERNAME": "test@salesforce.com",
    "SFDC_PASSWORD": "some_Password_123",
}


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with mock.patch.dict(os.environ, SFDC_TEST_CREDS):
        yield


def test_get_object_definition(respx_mock, sfdc_cred, snowflake_cred):

    api_version = "53.0"
    object = "Account"

    respx_mock.get(
        f"https://test.salesforce.com/services/data/v{api_version}/sobjects/{object}/describe/"
    ).mock(return_value=httpx.Response(200))

    client = Client(base_url="https://test.salesforce.com")

    get_object_definition(object="Account", client=client, api_version=api_version)
