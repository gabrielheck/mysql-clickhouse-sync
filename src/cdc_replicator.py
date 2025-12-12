import json
import time
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
import structlog

from src.config import Settings
from src.mysql_client import MySQLClient
from src.clickhouse_client import ClickHouseClient
from src.schema_converter import SchemaConverter

logger = structlog.get_logger()


@dataclass
class BinlogPosition:
    file: str
    position: int
    timestamp: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BinlogPosition":
        return cls(**data)


class CDCReplicator:
    """
    Change Data Capture replicator using MySQL Binlog.

    Uses ReplacingMergeTree in ClickHouse with:
    - _version: timestamp for ordering versions
    - _deleted: soft delete flag (1 = deleted)
    """

    def __init__(
        self,
        settings: Settings,
        mysql_client: MySQLClient,
        clickhouse_client: ClickHouseClient,
        schema_converter: SchemaConverter,
    ):
        self.settings = settings
        self.mysql = mysql_client
        self.clickhouse = clickhouse_client
        self.converter = schema_converter
        self._stream: BinLogStreamReader | None = None
        self._position_file = Path(
            settings.replication.position_file or "/tmp/binlog_position.json"
        )
        self._tables_to_replicate: set[str] = set()
        self._table_schemas: dict[str, list[str]] = {}  # Cache: table -> column names.

    def _load_position(self) -> BinlogPosition | None:
        if not self._position_file.exists():
            return None
        try:
            data = json.loads(self._position_file.read_text())
            pos = BinlogPosition.from_dict(data)
            logger.info(
                "Loaded binlog position",
                file=pos.file,
                position=pos.position,
            )
            return pos
        except Exception as e:
            logger.warning("Failed to load position", error=str(e))
            return None

    def _save_position(self, position: BinlogPosition) -> None:
        self._position_file.parent.mkdir(parents=True, exist_ok=True)
        self._position_file.write_text(json.dumps(position.to_dict()))

    def _get_current_binlog_position(self) -> BinlogPosition:
        with self.mysql.connection.cursor() as cursor:
            cursor.execute("SHOW MASTER STATUS")
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    "Cannot get binlog position. Is binlog enabled?"
                )
            return BinlogPosition(
                file=row["File"],
                position=row["Position"],
                timestamp=time.time(),
            )

    def _create_binlog_stream(
        self, position: BinlogPosition | None = None
    ) -> BinLogStreamReader:
        mysql_settings = {
            "host": self.settings.mysql.host,
            "port": self.settings.mysql.port,
            "user": self.settings.mysql.user,
            "passwd": self.settings.mysql.password,
            "connect_timeout": 10,
            "read_timeout": 300,
            "write_timeout": 300,
        }

        kwargs = {
            "connection_settings": mysql_settings,
            "server_id": 100,
            "blocking": True,
            "resume_stream": position is not None,
            "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            "only_schemas": [self.settings.mysql.database],
            # Keep the binlog connection alive to avoid idle disconnects.
            # This reduces OperationalError reconnect warnings from
            # pymysql/mysql-replication.
            "heartbeat_interval": 5,
        }

        if self._tables_to_replicate:
            kwargs["only_tables"] = list(self._tables_to_replicate)

        if position:
            kwargs["log_file"] = position.file
            kwargs["log_pos"] = position.position

        return BinLogStreamReader(**kwargs)

    def _get_version_timestamp(self) -> int:
        return int(datetime.now().timestamp() * 1000000)

    def _get_table_columns(self, table: str) -> list[str]:
        """Get column names from cache or fetch from MySQL."""
        if table not in self._table_schemas:
            schema = self.mysql.get_table_schema(table)
            self._table_schemas[table] = [col.name for col in schema.columns]
        return self._table_schemas[table]

    def _process_write_event(self, event: WriteRowsEvent) -> int:
        table = event.table
        columns = self._get_table_columns(table)
        version = self._get_version_timestamp()

        rows = []
        for row in event.rows:
            # Extract values in column order
            values = list(row["values"].values())
            values.append(version)  # _version
            values.append(0)  # _deleted = 0
            rows.append(tuple(values))

        if rows:
            all_columns = columns + ["_version", "_deleted"]
            self.clickhouse.insert_data(table, all_columns, rows)

        return len(rows)

    def _process_update_event(self, event: UpdateRowsEvent) -> int:
        table = event.table
        columns = self._get_table_columns(table)
        version = self._get_version_timestamp()

        rows = []
        for row in event.rows:
            # Extract values in column order
            values = list(row["after_values"].values())
            values.append(version)  # _version
            values.append(0)  # _deleted = 0
            rows.append(tuple(values))

        if rows:
            all_columns = columns + ["_version", "_deleted"]
            self.clickhouse.insert_data(table, all_columns, rows)

        return len(rows)

    def _process_delete_event(self, event: DeleteRowsEvent) -> int:
        table = event.table
        columns = self._get_table_columns(table)
        version = self._get_version_timestamp()

        rows = []
        for row in event.rows:
            # Extract values in column order
            values = list(row["values"].values())
            values.append(version)  # _version
            values.append(1)  # _deleted = 1
            rows.append(tuple(values))

        if rows:
            all_columns = columns + ["_version", "_deleted"]
            self.clickhouse.insert_data(table, all_columns, rows)

        return len(rows)

    def _ensure_cdc_schema(self, tables: list[str]) -> None:
        """Create tables with CDC columns (_version, _deleted)."""
        self.clickhouse.create_database()

        for table_name in tables:
            schema = self.mysql.get_table_schema(table_name)

            # Cache column names for CDC event processing
            self._table_schemas[table_name] = [
                col.name for col in schema.columns
            ]

            if self.settings.replication.drop_existing:
                drop_sql = self.converter.generate_drop_table(
                    schema.name, self.settings.clickhouse.database
                )
                self.clickhouse.execute_command(drop_sql)

            create_sql = self.converter.generate_cdc_table(
                schema, self.settings.clickhouse.database
            )
            self.clickhouse.execute_command(create_sql)
            logger.info("Created CDC table", table=table_name)

    def initial_sync(self) -> None:
        """Perform initial full sync before starting CDC."""
        logger.info("Starting initial sync")

        # Get current binlog position BEFORE sync
        position = self._get_current_binlog_position()
        logger.info(
            "Captured binlog position for CDC",
            file=position.file,
            position=position.position,
        )

        tables = self._get_tables()

        # Create CDC-enabled tables
        self._ensure_cdc_schema(tables)

        # Copy existing data with CDC columns
        for table_name in tables:
            self._sync_table_with_cdc_columns(table_name)

        # Save position after successful sync
        self._save_position(position)
        logger.info("Initial sync completed")

    def _sync_table_with_cdc_columns(self, table_name: str) -> None:
        """Sync a single table adding CDC columns."""
        schema = self.mysql.get_table_schema(table_name)
        columns = [col.name for col in schema.columns]
        batch_size = self.settings.replication.batch_size
        version = self._get_version_timestamp()

        total_rows = 0
        for batch in self.mysql.fetch_data_batched(table_name, batch_size, columns):
            # Add _version and _deleted to each row
            rows_with_cdc = [row + (version, 0) for row in batch]
            all_columns = columns + ["_version", "_deleted"]
            self.clickhouse.insert_data(table_name, all_columns, rows_with_cdc)
            total_rows += len(batch)

        logger.info("Synced table", table=table_name, rows=total_rows)

    def _get_tables(self) -> list[str]:
        configured = self.settings.replication.get_tables_list()
        if configured:
            return configured
        return self.mysql.get_tables()

    def _load_table_schemas(self, tables: list[str]) -> None:
        """Pre-load and cache column names for all tables."""
        for table_name in tables:
            if table_name not in self._table_schemas:
                schema = self.mysql.get_table_schema(table_name)
                self._table_schemas[table_name] = [
                    col.name for col in schema.columns
                ]
        logger.info("Loaded table schemas", count=len(self._table_schemas))

    def run(self) -> None:
        """Run CDC replication continuously."""
        logger.info("Starting CDC replication")

        tables = self._get_tables()
        self._tables_to_replicate = set(tables)

        # Check if we need initial sync
        position = self._load_position()
        if position is None:
            self.initial_sync()
            position = self._load_position()
        else:
            # Load schemas for existing position (restart scenario)
            self._load_table_schemas(tables)

        logger.info(
            "Starting binlog stream",
            file=position.file if position else "current",
            position=position.position if position else 0,
        )

        events_processed = 0
        last_save_time = time.time()

        reconnect_delay_seconds = 1.0
        max_reconnect_delay_seconds = 30.0
        stopping = False

        while not stopping:
            self._stream = self._create_binlog_stream(position)

            try:
                for event in self._stream:
                    table = event.table

                    if table not in self._tables_to_replicate:
                        continue

                    if isinstance(event, WriteRowsEvent):
                        count = self._process_write_event(event)
                        logger.debug("INSERT", table=table, rows=count)
                    elif isinstance(event, UpdateRowsEvent):
                        count = self._process_update_event(event)
                        logger.debug("UPDATE", table=table, rows=count)
                    elif isinstance(event, DeleteRowsEvent):
                        count = self._process_delete_event(event)
                        logger.debug("DELETE", table=table, rows=count)

                    events_processed += 1

                    # Save position periodically (every 5 seconds)
                    if time.time() - last_save_time > 5:
                        pos = BinlogPosition(
                            file=self._stream.log_file,
                            position=self._stream.log_pos,
                            timestamp=time.time(),
                        )
                        self._save_position(pos)
                        position = pos
                        last_save_time = time.time()

                        if events_processed % 100 == 0:
                            logger.info(
                                "CDC progress",
                                events=events_processed,
                                binlog_file=pos.file,
                                binlog_pos=pos.position,
                            )

            except KeyboardInterrupt:
                logger.info("CDC stopped by user")
                stopping = True
            except (pymysql.err.OperationalError, OSError) as e:
                # MySQL connection dropped (e.g., network blip, idle timeout, MySQL restart).
                # We reconnect from the last known binlog position.
                logger.warning(
                    "Binlog stream disconnected; will reconnect",
                    error=str(e),
                    reconnect_delay_seconds=reconnect_delay_seconds,
                )
            except Exception as e:
                logger.exception("CDC replication failed", error=str(e))
                raise
            finally:
                if self._stream:
                    # Best-effort: persist last known position before closing.
                    try:
                        pos = BinlogPosition(
                            file=self._stream.log_file,
                            position=self._stream.log_pos,
                            timestamp=time.time(),
                        )
                        self._save_position(pos)
                        position = pos
                    except Exception as e:
                        logger.warning(
                            "Failed to persist binlog position during cleanup",
                            error=str(e),
                        )

                    try:
                        self._stream.close()
                    finally:
                        self._stream = None

            if stopping:
                logger.info(
                    "CDC stopped",
                    events_processed=events_processed,
                    final_position=position.position if position else None,
                )
                break

            # Backoff before reconnecting.
            time.sleep(reconnect_delay_seconds)
            reconnect_delay_seconds = min(
                max_reconnect_delay_seconds, reconnect_delay_seconds * 2
            )
