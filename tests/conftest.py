import pytest
from unittest.mock import MagicMock, patch

from src.config import MySQLConfig, ClickHouseConfig, ReplicationConfig, Settings, ReplicationMode
from src.mysql_client import MySQLClient, TableSchema, ColumnInfo
from src.clickhouse_client import ClickHouseClient
from src.schema_converter import SchemaConverter


@pytest.fixture
def mysql_config():
    """Fixture for MySQL configuration."""
    return MySQLConfig(
        MYSQL_HOST="localhost",
        MYSQL_PORT=3306,
        MYSQL_USER="test_user",
        MYSQL_PASSWORD="test_password",
        MYSQL_DATABASE="test_db",
    )


@pytest.fixture
def clickhouse_config():
    """Fixture for ClickHouse configuration."""
    return ClickHouseConfig(
        CLICKHOUSE_HOST="localhost",
        CLICKHOUSE_PORT=8123,
        CLICKHOUSE_USER="default",
        CLICKHOUSE_PASSWORD="",
        CLICKHOUSE_DATABASE="test_db",
    )


@pytest.fixture
def replication_config():
    """Fixture for Replication configuration."""
    return ReplicationConfig(
        REPLICATION_MODE=ReplicationMode.SNAPSHOT,
        REPLICATION_BATCH_SIZE=1000,
        REPLICATION_TABLES="",
        REPLICATION_DROP_EXISTING=False,
        REPLICATION_PARALLEL_TABLES=1,
    )


@pytest.fixture
def settings(mysql_config, clickhouse_config, replication_config):
    """Fixture for complete Settings."""
    return Settings(
        mysql=mysql_config,
        clickhouse=clickhouse_config,
        replication=replication_config,
    )


@pytest.fixture
def schema_converter():
    """Fixture for SchemaConverter."""
    return SchemaConverter()


@pytest.fixture
def sample_table_schema():
    """Fixture for a sample TableSchema."""
    return TableSchema(
        name="users",
        columns=[
            ColumnInfo(
                name="id",
                data_type="int",
                is_nullable=False,
                column_key="PRI",
                extra="auto_increment",
            ),
            ColumnInfo(
                name="email",
                data_type="varchar",
                is_nullable=False,
                column_key="UNI",
                extra="",
                character_maximum_length=255,
            ),
            ColumnInfo(
                name="name",
                data_type="varchar",
                is_nullable=True,
                column_key="",
                extra="",
                character_maximum_length=100,
            ),
            ColumnInfo(
                name="balance",
                data_type="decimal",
                is_nullable=True,
                column_key="",
                extra="",
                numeric_precision=10,
                numeric_scale=2,
            ),
            ColumnInfo(
                name="created_at",
                data_type="datetime",
                is_nullable=False,
                column_key="",
                extra="",
            ),
        ],
        primary_keys=["id"],
    )


@pytest.fixture
def sample_composite_key_schema():
    """Fixture for a TableSchema with composite primary key."""
    return TableSchema(
        name="order_items",
        columns=[
            ColumnInfo(
                name="order_id",
                data_type="int",
                is_nullable=False,
                column_key="PRI",
                extra="",
            ),
            ColumnInfo(
                name="product_id",
                data_type="int",
                is_nullable=False,
                column_key="PRI",
                extra="",
            ),
            ColumnInfo(
                name="quantity",
                data_type="int",
                is_nullable=False,
                column_key="",
                extra="",
            ),
        ],
        primary_keys=["order_id", "product_id"],
    )


@pytest.fixture
def mock_mysql_connection():
    """Fixture for mocked MySQL connection."""
    with patch("pymysql.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def mock_clickhouse_client():
    """Fixture for mocked ClickHouse client."""
    with patch("clickhouse_connect.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client
