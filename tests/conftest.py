import os
import pytest
from unittest import mock
from airflow.models import Variable,Connection,DagBag

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="mock_key1234"):
        yield Variable.get("api_key")

@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_CHANNEL_HANDLE="MRCHEESE"):
        yield Variable.get("CHANNEL_HANDLE")

@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_user",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name"
    )

    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id= "POSTGRES_DB_YT_ELT")

@pytest.fixture()
def dagbag():
    yield DagBag()
