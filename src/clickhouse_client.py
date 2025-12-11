import re
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver import Client
import structlog

from src.config import ClickHouseConfig

logger = structlog.get_logger()

# Valid identifier pattern: alphanumeric and underscore only
_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, context: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifier to prevent injection.
    
    Raises ValueError if identifier contains invalid characters.
    """
    if not name:
        raise ValueError(f"Empty {context} is not allowed")
    
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid {context} '{name}': must contain only alphanumeric characters and underscores, "
            "and start with a letter or underscore"
        )
    
    return name


class ClickHouseClient:
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self._client: Client | None = None
        # Validate database name at init time
        _validate_identifier(config.database, "database name")

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(
            host=self.config.host,
            port=self.config.port,
            username=self.config.user,
            password=self.config.password,
            compress=True,  # Enable LZ4 compression
        )
        logger.info("Connected to ClickHouse", host=self.config.host)

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Disconnected from ClickHouse")

    @property
    def client(self) -> Client:
        if not self._client:
            raise RuntimeError("Not connected to ClickHouse")
        return self._client

    def create_database(self) -> None:
        db = _validate_identifier(self.config.database, "database name")
        self.client.command(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        logger.info("Database created/verified", database=db)

    def execute_command(self, sql: str) -> None:
        self.client.command(sql)

    def table_exists(self, table_name: str) -> bool:
        table = _validate_identifier(table_name, "table name")
        db = _validate_identifier(self.config.database, "database name")
        
        result = self.client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {table:String}",
            parameters={"db": db, "table": table},
        )
        return result.first_row[0] > 0

    def get_row_count(self, table_name: str) -> int:
        table = _validate_identifier(table_name, "table name")
        db = _validate_identifier(self.config.database, "database name")
        
        result = self.client.query(f"SELECT count() FROM `{db}`.`{table}`")
        return result.first_row[0]

    def insert_data(self, table_name: str, columns: list[str], data: list[tuple]) -> int:
        """Insert data using optimized tuple format (no dict conversion needed)."""
        if not data:
            return 0

        table = _validate_identifier(table_name, "table name")
        db = _validate_identifier(self.config.database, "database name")
        
        # Validate all column names
        validated_columns = [_validate_identifier(col, "column name") for col in columns]

        self.client.insert(
            table=f"`{db}`.`{table}`",
            data=data,
            column_names=validated_columns,
        )

        return len(data)

    def truncate_table(self, table_name: str) -> None:
        table = _validate_identifier(table_name, "table name")
        db = _validate_identifier(self.config.database, "database name")
        
        self.client.command(f"TRUNCATE TABLE `{db}`.`{table}`")
        logger.info("Table truncated", table=table)

    def __enter__(self) -> "ClickHouseClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

