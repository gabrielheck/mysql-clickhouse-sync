import os
from enum import Enum
from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic import Field, model_validator


def read_secret_file(file_path: str) -> str:
    """Read content from a Docker secret file."""
    path = Path(file_path)
    if path.exists():
        return path.read_text().strip()
    return ""


class ReplicationMode(str, Enum):
    SNAPSHOT = "snapshot"  # One-time full copy
    CDC = "cdc"  # Continuous change data capture


class MySQLConfig(BaseSettings):
    host: str = Field(alias="MYSQL_HOST")
    port: int = Field(default=3306, alias="MYSQL_PORT")
    user: str = Field(alias="MYSQL_USER")
    password: str = Field(default="", alias="MYSQL_PASSWORD")
    database: str = Field(alias="MYSQL_DATABASE")

    class Config:
        env_prefix = ""
        extra = "ignore"

    @model_validator(mode="before")
    @classmethod
    def load_secrets(cls, values: dict) -> dict:
        """Load passwords from Docker secret files if *_FILE env vars are set."""
        password_file = os.environ.get("MYSQL_PASSWORD_FILE")
        if password_file:
            secret_value = read_secret_file(password_file)
            if secret_value:
                values["MYSQL_PASSWORD"] = secret_value
        return values


class ClickHouseConfig(BaseSettings):
    host: str = Field(alias="CLICKHOUSE_HOST")
    port: int = Field(default=8123, alias="CLICKHOUSE_PORT")
    user: str = Field(default="default", alias="CLICKHOUSE_USER")
    password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")
    database: str = Field(alias="CLICKHOUSE_DATABASE")

    class Config:
        env_prefix = ""
        extra = "ignore"

    @model_validator(mode="before")
    @classmethod
    def load_secrets(cls, values: dict) -> dict:
        """Load passwords from Docker secret files if *_FILE env vars are set."""
        password_file = os.environ.get("CLICKHOUSE_PASSWORD_FILE")
        if password_file:
            secret_value = read_secret_file(password_file)
            if secret_value:
                values["CLICKHOUSE_PASSWORD"] = secret_value
        return values


class ReplicationConfig(BaseSettings):
    mode: ReplicationMode = Field(
        default=ReplicationMode.SNAPSHOT, alias="REPLICATION_MODE"
    )
    batch_size: int = Field(default=50000, alias="REPLICATION_BATCH_SIZE")
    tables: str = Field(default="", alias="REPLICATION_TABLES")
    drop_existing: bool = Field(default=False, alias="REPLICATION_DROP_EXISTING")
    parallel_tables: int = Field(default=1, alias="REPLICATION_PARALLEL_TABLES")
    position_file: str = Field(
        default="/data/binlog_position.json", alias="REPLICATION_POSITION_FILE"
    )

    class Config:
        env_prefix = ""
        extra = "ignore"

    def get_tables_list(self) -> list[str]:
        if not self.tables:
            return []
        return [t.strip() for t in self.tables.split(",") if t.strip()]


class Settings(BaseSettings):
    mysql: MySQLConfig = Field(default_factory=MySQLConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    replication: ReplicationConfig = Field(default_factory=ReplicationConfig)


def get_settings() -> Settings:
    return Settings()
