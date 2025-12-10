from typing import Any

import clickhouse_connect
from clickhouse_connect.driver import Client
import structlog

from src.config import ClickHouseConfig

logger = structlog.get_logger()


class ClickHouseClient:
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self._client: Client | None = None

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
        self.client.command(f"CREATE DATABASE IF NOT EXISTS `{self.config.database}`")
        logger.info("Database created/verified", database=self.config.database)

    def execute_command(self, sql: str) -> None:
        self.client.command(sql)

    def table_exists(self, table_name: str) -> bool:
        result = self.client.query(
            f"SELECT count() FROM system.tables WHERE database = '{self.config.database}' AND name = '{table_name}'"
        )
        return result.first_row[0] > 0

    def get_row_count(self, table_name: str) -> int:
        result = self.client.query(f"SELECT count() FROM `{self.config.database}`.`{table_name}`")
        return result.first_row[0]

    def insert_data(self, table_name: str, columns: list[str], data: list[tuple]) -> int:
        """Insert data using optimized tuple format (no dict conversion needed)."""
        if not data:
            return 0

        self.client.insert(
            table=f"`{self.config.database}`.`{table_name}`",
            data=data,
            column_names=columns,
        )

        return len(data)

    def truncate_table(self, table_name: str) -> None:
        self.client.command(f"TRUNCATE TABLE `{self.config.database}`.`{table_name}`")
        logger.info("Table truncated", table=table_name)

    def __enter__(self) -> "ClickHouseClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

