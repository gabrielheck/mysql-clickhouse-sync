import pytest
from unittest.mock import MagicMock, patch, call

from src.mysql_client import MySQLClient, _validate_identifier as mysql_validate
from src.clickhouse_client import ClickHouseClient, _validate_identifier as ch_validate


class TestIdentifierValidation:
    """Tests for SQL identifier validation (SQL Injection prevention)."""

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_valid_simple_identifier(self, validate_func):
        """Test that simple valid identifiers pass validation."""
        assert validate_func("users", "table") == "users"
        assert validate_func("user_id", "column") == "user_id"
        assert validate_func("_private", "column") == "_private"

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_valid_identifier_with_numbers(self, validate_func):
        """Test identifiers with numbers."""
        assert validate_func("table1", "table") == "table1"
        assert validate_func("column_123", "column") == "column_123"

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_empty_identifier_raises(self, validate_func):
        """Test that empty identifiers raise ValueError."""
        with pytest.raises(ValueError, match="Empty"):
            validate_func("", "table")

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_sql_injection_attempt_semicolon(self, validate_func):
        """Test that SQL injection with semicolon is blocked."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_func("users; DROP TABLE users;--", "table")

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_sql_injection_attempt_quotes(self, validate_func):
        """Test that SQL injection with quotes is blocked."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_func("users' OR '1'='1", "table")
        with pytest.raises(ValueError, match="Invalid"):
            validate_func('users" OR "1"="1', "table")

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_sql_injection_attempt_comment(self, validate_func):
        """Test that SQL injection with comments is blocked."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_func("users--", "table")
        with pytest.raises(ValueError, match="Invalid"):
            validate_func("users/**/", "table")

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_identifier_starting_with_number(self, validate_func):
        """Test that identifiers starting with numbers are rejected."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_func("123table", "table")

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_identifier_with_spaces(self, validate_func):
        """Test that identifiers with spaces are rejected."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_func("my table", "table")

    @pytest.mark.parametrize("validate_func", [mysql_validate, ch_validate])
    def test_identifier_with_special_chars(self, validate_func):
        """Test that identifiers with special characters are rejected."""
        invalid_chars = ["@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "+", "="]
        for char in invalid_chars:
            with pytest.raises(ValueError, match="Invalid"):
                validate_func(f"table{char}name", "table")


class TestMySQLClient:
    """Tests for MySQLClient."""

    def test_init_validates_database_name(self, mysql_config):
        """Test that database name is validated at init."""
        # Valid database name should work
        client = MySQLClient(mysql_config)
        assert client.config.database == "test_db"

    def test_init_rejects_invalid_database_name(self):
        """Test that invalid database names are rejected at init."""
        from src.config import MySQLConfig

        with pytest.raises(ValueError, match="Invalid database name"):
            MySQLConfig(
                MYSQL_HOST="localhost",
                MYSQL_PORT=3306,
                MYSQL_USER="user",
                MYSQL_PASSWORD="pass",
                MYSQL_DATABASE="test; DROP DATABASE test;--",
            )
            MySQLClient(
                MySQLConfig(
                    MYSQL_HOST="localhost",
                    MYSQL_PORT=3306,
                    MYSQL_USER="user",
                    MYSQL_PASSWORD="pass",
                    MYSQL_DATABASE="test; DROP DATABASE test;--",
                )
            )

    def test_connect_creates_connection(self, mysql_config, mock_mysql_connection):
        """Test that connect() creates a database connection."""
        client = MySQLClient(mysql_config)
        client.connect()

        assert client._connection is not None

    def test_disconnect_closes_connection(self, mysql_config, mock_mysql_connection):
        """Test that disconnect() closes the connection."""
        client = MySQLClient(mysql_config)
        client.connect()
        client.disconnect()

        mock_mysql_connection.close.assert_called_once()
        assert client._connection is None

    def test_context_manager(self, mysql_config, mock_mysql_connection):
        """Test that client works as context manager."""
        with MySQLClient(mysql_config) as client:
            assert client._connection is not None

        mock_mysql_connection.close.assert_called_once()

    def test_get_row_count_validates_table_name(self, mysql_config, mock_mysql_connection):
        """Test that get_row_count validates table name."""
        client = MySQLClient(mysql_config)
        client.connect()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"cnt": 100}
        mock_mysql_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Valid table name
        count = client.get_row_count("users")
        assert count == 100

        # Invalid table name
        with pytest.raises(ValueError, match="Invalid table name"):
            client.get_row_count("users; DROP TABLE users;--")

    def test_fetch_data_batched_validates_identifiers(
        self, mysql_config, mock_mysql_connection
    ):
        """Test that fetch_data_batched validates table and column names."""
        client = MySQLClient(mysql_config)
        client.connect()

        # Invalid table name should raise
        with pytest.raises(ValueError, match="Invalid table name"):
            list(client.fetch_data_batched("users; DROP TABLE", 100, ["id"]))

        # Invalid column name should raise
        with pytest.raises(ValueError, match="Invalid column name"):
            list(client.fetch_data_batched("users", 100, ["id", "name; --"]))


class TestClickHouseClient:
    """Tests for ClickHouseClient."""

    def test_init_validates_database_name(self, clickhouse_config):
        """Test that database name is validated at init."""
        client = ClickHouseClient(clickhouse_config)
        assert client.config.database == "test_db"

    def test_connect_creates_client(self, clickhouse_config, mock_clickhouse_client):
        """Test that connect() creates a ClickHouse client."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        assert client._client is not None

    def test_disconnect_closes_client(self, clickhouse_config, mock_clickhouse_client):
        """Test that disconnect() closes the client."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()
        client.disconnect()

        mock_clickhouse_client.close.assert_called_once()
        assert client._client is None

    def test_table_exists_uses_parameterized_query(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that table_exists uses parameterized query."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        mock_result = MagicMock()
        mock_result.first_row = [1]
        mock_clickhouse_client.query.return_value = mock_result

        result = client.table_exists("users")

        assert result is True
        # Verify parameterized query was used
        call_args = mock_clickhouse_client.query.call_args
        assert "parameters" in call_args.kwargs
        assert call_args.kwargs["parameters"]["table"] == "users"

    def test_table_exists_rejects_invalid_table_name(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that table_exists rejects invalid table names."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        with pytest.raises(ValueError, match="Invalid table name"):
            client.table_exists("users; DROP TABLE users;--")

    def test_get_row_count_validates_table_name(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that get_row_count validates table name."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        mock_result = MagicMock()
        mock_result.first_row = [100]
        mock_clickhouse_client.query.return_value = mock_result

        # Valid table name
        count = client.get_row_count("users")
        assert count == 100

        # Invalid table name
        with pytest.raises(ValueError, match="Invalid table name"):
            client.get_row_count("users'; SELECT * FROM sensitive_data;--")

    def test_insert_data_validates_identifiers(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that insert_data validates table and column names."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        data = [(1, "test@email.com")]

        # Invalid table name
        with pytest.raises(ValueError, match="Invalid table name"):
            client.insert_data("users; DROP TABLE", ["id", "email"], data)

        # Invalid column name
        with pytest.raises(ValueError, match="Invalid column name"):
            client.insert_data("users", ["id", "email; --"], data)

    def test_insert_data_with_valid_data(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that insert_data works with valid data."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        data = [(1, "test@email.com"), (2, "user2@email.com")]
        result = client.insert_data("users", ["id", "email"], data)

        assert result == 2
        mock_clickhouse_client.insert.assert_called_once()

    def test_insert_data_empty_returns_zero(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that insert_data with empty data returns 0."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        result = client.insert_data("users", ["id", "email"], [])

        assert result == 0
        mock_clickhouse_client.insert.assert_not_called()

    def test_truncate_table_validates_table_name(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that truncate_table validates table name."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        with pytest.raises(ValueError, match="Invalid table name"):
            client.truncate_table("users; DROP DATABASE test_db;--")

    def test_create_database_validates_name(
        self, clickhouse_config, mock_clickhouse_client
    ):
        """Test that create_database validates database name."""
        client = ClickHouseClient(clickhouse_config)
        client.connect()

        # Should work with valid database name
        client.create_database()
        mock_clickhouse_client.command.assert_called_once()


