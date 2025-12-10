from concurrent.futures import ThreadPoolExecutor, as_completed

import structlog

from src.config import Settings
from src.mysql_client import MySQLClient, TableSchema
from src.clickhouse_client import ClickHouseClient
from src.schema_converter import SchemaConverter

logger = structlog.get_logger()


class Replicator:
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

    def get_tables_to_replicate(self) -> list[str]:
        configured_tables = self.settings.replication.get_tables_list()

        if configured_tables:
            return configured_tables

        return self.mysql.get_tables()

    def replicate_schema(self, schema: TableSchema) -> None:
        database = self.settings.clickhouse.database

        if self.settings.replication.drop_existing:
            drop_sql = self.converter.generate_drop_table(schema.name, database)
            self.clickhouse.execute_command(drop_sql)
            logger.info("Dropped existing table", table=schema.name)

        create_sql = self.converter.generate_create_table(schema, database)
        self.clickhouse.execute_command(create_sql)
        logger.info("Created table schema", table=schema.name)

    def replicate_data(self, schema: TableSchema) -> int:
        table_name = schema.name
        columns = [col.name for col in schema.columns]
        batch_size = self.settings.replication.batch_size

        total_rows = 0
        batch_count = 0

        for batch in self.mysql.fetch_data_batched(table_name, batch_size, columns):
            inserted = self.clickhouse.insert_data(table_name, columns, batch)
            total_rows += inserted
            batch_count += 1

            # Log progress every 10 batches to reduce log overhead
            if batch_count % 10 == 0:
                logger.info("Replication progress", table=table_name, rows=total_rows)

        return total_rows

    def replicate_table(self, table_name: str) -> dict:
        logger.info("Starting table replication", table=table_name)

        schema = self.mysql.get_table_schema(table_name)
        source_count = self.mysql.get_row_count(table_name)

        self.replicate_schema(schema)
        rows_inserted = self.replicate_data(schema)

        target_count = self.clickhouse.get_row_count(table_name)

        result = {
            "table": table_name,
            "source_rows": source_count,
            "rows_inserted": rows_inserted,
            "target_rows": target_count,
            "success": source_count == target_count,
        }

        logger.info("Table replication completed", **result)
        return result

    def run(self, parallel_tables: int = 1) -> list[dict]:
        """
        Run replication process.

        Args:
            parallel_tables: Number of tables to replicate in parallel.
                            Use 1 for sequential (safer for large tables).
                            Use > 1 for parallel (faster for many small tables).
        """
        logger.info("Starting replication process", parallel_tables=parallel_tables)

        self.clickhouse.create_database()

        tables = self.get_tables_to_replicate()
        logger.info("Tables to replicate", tables=tables, count=len(tables))

        results = []

        if parallel_tables <= 1:
            # Sequential processing
            for table_name in tables:
                try:
                    result = self.replicate_table(table_name)
                    results.append(result)
                except Exception as e:
                    logger.error("Failed to replicate table", table=table_name, error=str(e))
                    results.append({"table": table_name, "success": False, "error": str(e)})
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=parallel_tables) as executor:
                future_to_table = {
                    executor.submit(self._safe_replicate_table, table): table
                    for table in tables
                }

                for future in as_completed(future_to_table):
                    results.append(future.result())

        success_count = sum(1 for r in results if r.get("success"))
        logger.info(
            "Replication completed",
            total_tables=len(results),
            successful=success_count,
            failed=len(results) - success_count,
        )

        return results

    def _safe_replicate_table(self, table_name: str) -> dict:
        """Wrapper for parallel execution with error handling."""
        try:
            return self.replicate_table(table_name)
        except Exception as e:
            logger.error("Failed to replicate table", table=table_name, error=str(e))
            return {"table": table_name, "success": False, "error": str(e)}

