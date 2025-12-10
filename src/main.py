import sys
import structlog

from src.config import get_settings, ReplicationMode
from src.mysql_client import MySQLClient
from src.clickhouse_client import ClickHouseClient
from src.schema_converter import SchemaConverter

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


def run_snapshot_mode(settings, mysql_client, clickhouse_client, schema_converter):
    """Run one-time full replication."""
    from src.replicator import Replicator

    replicator = Replicator(
        settings=settings,
        mysql_client=mysql_client,
        clickhouse_client=clickhouse_client,
        schema_converter=schema_converter,
    )

    results = replicator.run(
        parallel_tables=settings.replication.parallel_tables
    )

    failed = [r for r in results if not r.get("success")]
    if failed:
        logger.error(
            "Some tables failed to replicate",
            failed_tables=[r["table"] for r in failed],
        )
        return 1

    logger.info("All tables replicated successfully")
    return 0


def run_cdc_mode(settings, mysql_client, clickhouse_client, schema_converter):
    """Run continuous CDC replication via binlog."""
    from src.cdc_replicator import CDCReplicator

    cdc = CDCReplicator(
        settings=settings,
        mysql_client=mysql_client,
        clickhouse_client=clickhouse_client,
        schema_converter=schema_converter,
    )

    cdc.run()
    return 0


def main() -> int:
    logger.info("MySQL to ClickHouse Replicator starting")

    try:
        settings = get_settings()
    except Exception as e:
        logger.error("Failed to load configuration", error=str(e))
        return 1

    mode = settings.replication.mode
    logger.info("Replication mode", mode=mode.value)

    mysql_client = MySQLClient(settings.mysql)
    clickhouse_client = ClickHouseClient(settings.clickhouse)
    schema_converter = SchemaConverter()

    try:
        with mysql_client, clickhouse_client:
            if mode == ReplicationMode.SNAPSHOT:
                return run_snapshot_mode(
                    settings, mysql_client, clickhouse_client, schema_converter
                )
            elif mode == ReplicationMode.CDC:
                return run_cdc_mode(
                    settings, mysql_client, clickhouse_client, schema_converter
                )
            else:
                logger.error("Unknown replication mode", mode=mode)
                return 1

    except Exception as e:
        logger.exception("Replication failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
