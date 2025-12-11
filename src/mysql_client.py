import re
from dataclasses import dataclass
from typing import Iterator, Any

import pymysql
import pymysql.cursors
import structlog

from src.config import MySQLConfig

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


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool
    column_key: str
    extra: str
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None


@dataclass
class TableSchema:
    name: str
    columns: list[ColumnInfo]
    primary_keys: list[str]


class MySQLClient:
    def __init__(self, config: MySQLConfig):
        self.config = config
        self._connection: pymysql.Connection | None = None
        # Validate database name at init time
        _validate_identifier(config.database, "database name")

    def connect(self) -> None:
        self._connection = pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            cursorclass=pymysql.cursors.DictCursor,
            read_timeout=300,
            write_timeout=300,
        )
        logger.info("Connected to MySQL", host=self.config.host, database=self.config.database)

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from MySQL")

    @property
    def connection(self) -> pymysql.Connection:
        if not self._connection:
            raise RuntimeError("Not connected to MySQL")
        return self._connection

    def get_tables(self) -> list[str]:
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return [list(row.values())[0] for row in cursor.fetchall()]

    def get_table_schema(self, table_name: str) -> TableSchema:
        columns = []
        primary_keys = []

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_KEY,
                    EXTRA,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.config.database, table_name),
            )

            for row in cursor.fetchall():
                col = ColumnInfo(
                    name=row["COLUMN_NAME"],
                    data_type=row["DATA_TYPE"].lower(),
                    is_nullable=row["IS_NULLABLE"] == "YES",
                    column_key=row["COLUMN_KEY"],
                    extra=row["EXTRA"],
                    character_maximum_length=row["CHARACTER_MAXIMUM_LENGTH"],
                    numeric_precision=row["NUMERIC_PRECISION"],
                    numeric_scale=row["NUMERIC_SCALE"],
                )
                columns.append(col)

                if col.column_key == "PRI":
                    primary_keys.append(col.name)

        return TableSchema(name=table_name, columns=columns, primary_keys=primary_keys)

    def get_row_count(self, table_name: str) -> int:
        table = _validate_identifier(table_name, "table name")
        
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table}`")
            result = cursor.fetchone()
            return result["cnt"] if result else 0

    def fetch_data_batched(
        self, table_name: str, batch_size: int, columns: list[str]
    ) -> Iterator[list[tuple]]:
        """Fetch data using SSCursor for memory-efficient streaming."""
        table = _validate_identifier(table_name, "table name")
        validated_columns = [_validate_identifier(col, "column name") for col in columns]
        
        column_list = ", ".join(f"`{col}`" for col in validated_columns)

        # Use SSDictCursor for server-side streaming (doesn't buffer all rows)
        with self.connection.cursor(pymysql.cursors.SSDictCursor) as cursor:
            cursor.execute(f"SELECT {column_list} FROM `{table}`")

            batch = []
            for row in cursor:
                # Convert dict to tuple in column order for faster insertion
                batch.append(tuple(row[col] for col in columns))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if batch:
                yield batch

    def __enter__(self) -> "MySQLClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

