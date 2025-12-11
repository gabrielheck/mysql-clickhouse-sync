import pytest
from unittest.mock import MagicMock, patch, call

from src.replicator import Replicator
from src.mysql_client import TableSchema, ColumnInfo


class TestReplicator:
    """Tests for Replicator class."""

    @pytest.fixture
    def mock_mysql_client(self):
        """Create a mock MySQL client."""
        client = MagicMock()
        client.get_tables.return_value = ["users", "orders"]
        return client

    @pytest.fixture
    def mock_clickhouse_client(self):
        """Create a mock ClickHouse client."""
        return MagicMock()

    @pytest.fixture
    def replicator(
        self, settings, mock_mysql_client, mock_clickhouse_client, schema_converter
    ):
        """Create a Replicator instance with mocked dependencies."""
        return Replicator(
            settings=settings,
            mysql_client=mock_mysql_client,
            clickhouse_client=mock_clickhouse_client,
            schema_converter=schema_converter,
        )

    def test_get_tables_to_replicate_uses_configured_tables(
        self, replicator, settings
    ):
        """Test that configured tables are used when specified."""
        settings.replication.tables = "users,products"

        result = replicator.get_tables_to_replicate()

        assert result == ["users", "products"]

    def test_get_tables_to_replicate_fetches_all_when_not_configured(
        self, replicator, mock_mysql_client
    ):
        """Test that all tables are fetched when none are configured."""
        result = replicator.get_tables_to_replicate()

        assert result == ["users", "orders"]
        mock_mysql_client.get_tables.assert_called_once()

    def test_replicate_schema_creates_table(
        self, replicator, mock_clickhouse_client, sample_table_schema
    ):
        """Test that replicate_schema creates the table in ClickHouse."""
        replicator.replicate_schema(sample_table_schema)

        mock_clickhouse_client.execute_command.assert_called_once()
        call_sql = mock_clickhouse_client.execute_command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_sql
        assert "`users`" in call_sql

    def test_replicate_schema_drops_existing_when_configured(
        self, replicator, settings, mock_clickhouse_client, sample_table_schema
    ):
        """Test that existing table is dropped when drop_existing is True."""
        settings.replication.drop_existing = True

        replicator.replicate_schema(sample_table_schema)

        calls = mock_clickhouse_client.execute_command.call_args_list
        assert len(calls) == 2
        assert "DROP TABLE IF EXISTS" in calls[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in calls[1][0][0]

    def test_replicate_data_batched(
        self, replicator, mock_mysql_client, mock_clickhouse_client, sample_table_schema
    ):
        """Test that data is replicated in batches."""
        batch1 = [(1, "a@test.com", "Alice", 100.0, "2024-01-01")]
        batch2 = [(2, "b@test.com", "Bob", 200.0, "2024-01-02")]

        mock_mysql_client.fetch_data_batched.return_value = iter([batch1, batch2])
        mock_clickhouse_client.insert_data.return_value = 1  # Each batch returns 1 row

        rows = replicator.replicate_data(sample_table_schema)

        assert rows == 2
        assert mock_clickhouse_client.insert_data.call_count == 2

    def test_replicate_table_full_flow(
        self, replicator, mock_mysql_client, mock_clickhouse_client, sample_table_schema
    ):
        """Test the complete table replication flow."""
        mock_mysql_client.get_table_schema.return_value = sample_table_schema
        mock_mysql_client.get_row_count.return_value = 100
        mock_mysql_client.fetch_data_batched.return_value = iter(
            [[(i,) for i in range(100)]]
        )
        mock_clickhouse_client.get_row_count.return_value = 100
        mock_clickhouse_client.insert_data.return_value = 100

        result = replicator.replicate_table("users")

        assert result["table"] == "users"
        assert result["source_rows"] == 100
        assert result["target_rows"] == 100
        assert result["success"] is True

    def test_replicate_table_reports_failure_on_count_mismatch(
        self, replicator, mock_mysql_client, mock_clickhouse_client, sample_table_schema
    ):
        """Test that replication reports failure when row counts don't match."""
        mock_mysql_client.get_table_schema.return_value = sample_table_schema
        mock_mysql_client.get_row_count.return_value = 100
        mock_mysql_client.fetch_data_batched.return_value = iter([])
        mock_clickhouse_client.get_row_count.return_value = 50  # Mismatch

        result = replicator.replicate_table("users")

        assert result["success"] is False
        assert result["source_rows"] == 100
        assert result["target_rows"] == 50

    def test_run_sequential_processing(
        self, replicator, mock_mysql_client, mock_clickhouse_client, sample_table_schema
    ):
        """Test sequential table processing (parallel_tables=1)."""
        mock_mysql_client.get_tables.return_value = ["users", "orders"]
        mock_mysql_client.get_table_schema.return_value = sample_table_schema
        mock_mysql_client.get_row_count.return_value = 10
        mock_mysql_client.fetch_data_batched.return_value = iter([])
        mock_clickhouse_client.get_row_count.return_value = 10

        results = replicator.run(parallel_tables=1)

        assert len(results) == 2
        assert all(r["success"] for r in results)

    def test_run_creates_database(
        self, replicator, mock_mysql_client, mock_clickhouse_client, sample_table_schema
    ):
        """Test that run() creates the database first."""
        mock_mysql_client.get_tables.return_value = []

        replicator.run()

        mock_clickhouse_client.create_database.assert_called_once()

    def test_run_handles_table_errors_gracefully(
        self, replicator, mock_mysql_client, mock_clickhouse_client
    ):
        """Test that errors in one table don't stop others."""
        mock_mysql_client.get_tables.return_value = ["table1", "table2"]
        mock_mysql_client.get_table_schema.side_effect = [
            Exception("Schema error"),
            TableSchema(
                name="table2",
                columns=[
                    ColumnInfo(
                        name="id", data_type="int", is_nullable=False, column_key="PRI", extra=""
                    )
                ],
                primary_keys=["id"],
            ),
        ]
        mock_mysql_client.get_row_count.return_value = 0
        mock_mysql_client.fetch_data_batched.return_value = iter([])
        mock_clickhouse_client.get_row_count.return_value = 0

        results = replicator.run(parallel_tables=1)

        assert len(results) == 2
        assert results[0]["success"] is False
        assert "error" in results[0]
        assert results[1]["success"] is True

    def test_run_parallel_processing(
        self, replicator, mock_mysql_client, mock_clickhouse_client, sample_table_schema
    ):
        """Test parallel table processing (parallel_tables>1)."""
        mock_mysql_client.get_tables.return_value = ["t1", "t2", "t3"]
        mock_mysql_client.get_table_schema.return_value = sample_table_schema
        mock_mysql_client.get_row_count.return_value = 5
        mock_mysql_client.fetch_data_batched.return_value = iter([])
        mock_clickhouse_client.get_row_count.return_value = 5

        results = replicator.run(parallel_tables=3)

        assert len(results) == 3


class TestReplicatorConfig:
    """Tests for Replicator configuration handling."""

    @pytest.fixture
    def replicator(self, settings, schema_converter):
        """Create a Replicator with mocked clients."""
        return Replicator(
            settings=settings,
            mysql_client=MagicMock(),
            clickhouse_client=MagicMock(),
            schema_converter=schema_converter,
        )

    def test_batch_size_from_config(self, replicator, settings):
        """Test that batch size is read from config."""
        settings.replication.batch_size = 5000
        assert replicator.settings.replication.batch_size == 5000

    def test_tables_list_parsing(self, settings):
        """Test that tables list is parsed correctly."""
        settings.replication.tables = "users, orders , products"

        result = settings.replication.get_tables_list()

        assert result == ["users", "orders", "products"]

    def test_empty_tables_list(self, settings):
        """Test that empty tables list returns empty list."""
        settings.replication.tables = ""

        result = settings.replication.get_tables_list()

        assert result == []
