from src.mysql_client import TableSchema, ColumnInfo

MYSQL_TO_CLICKHOUSE_TYPE_MAP = {
    "tinyint": "Int8",
    "smallint": "Int16",
    "mediumint": "Int32",
    "int": "Int32",
    "integer": "Int32",
    "bigint": "Int64",
    "float": "Float32",
    "double": "Float64",
    "decimal": "Decimal",
    "numeric": "Decimal",
    "bit": "UInt64",
    "bool": "Bool",
    "boolean": "Bool",
    "date": "Date",
    "datetime": "DateTime",
    "timestamp": "DateTime",
    "time": "String",
    "year": "UInt16",
    "char": "String",
    "varchar": "String",
    "binary": "String",
    "varbinary": "String",
    "tinyblob": "String",
    "blob": "String",
    "mediumblob": "String",
    "longblob": "String",
    "tinytext": "String",
    "text": "String",
    "mediumtext": "String",
    "longtext": "String",
    "enum": "String",
    "set": "String",
    "json": "String",
}


class SchemaConverter:
    def convert_column_type(self, column: ColumnInfo) -> str:
        mysql_type = column.data_type.lower()

        if mysql_type in ("decimal", "numeric"):
            precision = column.numeric_precision or 10
            scale = column.numeric_scale or 0
            ch_type = f"Decimal({precision}, {scale})"
        else:
            ch_type = MYSQL_TO_CLICKHOUSE_TYPE_MAP.get(mysql_type, "String")

        if column.is_nullable:
            ch_type = f"Nullable({ch_type})"

        return ch_type

    def generate_create_table(self, schema: TableSchema, database: str) -> str:
        columns_def = []

        for col in schema.columns:
            ch_type = self.convert_column_type(col)
            columns_def.append(f"    `{col.name}` {ch_type}")

        columns_sql = ",\n".join(columns_def)

        if schema.primary_keys:
            order_by = ", ".join(f"`{pk}`" for pk in schema.primary_keys)
        else:
            first_col = schema.columns[0].name if schema.columns else "tuple()"
            order_by = f"`{first_col}`" if schema.columns else "tuple()"

        create_sql = f"""
CREATE TABLE IF NOT EXISTS `{database}`.`{schema.name}`
(
{columns_sql}
)
ENGINE = MergeTree()
ORDER BY ({order_by})
""".strip()

        return create_sql

    def generate_drop_table(self, table_name: str, database: str) -> str:
        return f"DROP TABLE IF EXISTS `{database}`.`{table_name}`"

    def generate_cdc_table(self, schema: TableSchema, database: str) -> str:
        """
        Generate CREATE TABLE for CDC with ReplacingMergeTree.

        Adds:
        - _version: UInt64 for version ordering
        - _deleted: UInt8 for soft deletes (0=active, 1=deleted)

        Uses ReplacingMergeTree(_version) to keep only latest version.
        """
        columns_def = []

        for col in schema.columns:
            ch_type = self.convert_column_type(col)
            columns_def.append(f"    `{col.name}` {ch_type}")

        # Add CDC columns
        columns_def.append("    `_version` UInt64")
        columns_def.append("    `_deleted` UInt8")

        columns_sql = ",\n".join(columns_def)

        if schema.primary_keys:
            order_by = ", ".join(f"`{pk}`" for pk in schema.primary_keys)
        else:
            first_col = schema.columns[0].name if schema.columns else "tuple()"
            order_by = f"`{first_col}`" if schema.columns else "tuple()"

        create_sql = f"""
CREATE TABLE IF NOT EXISTS `{database}`.`{schema.name}`
(
{columns_sql}
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY ({order_by})
""".strip()

        return create_sql

    def generate_cdc_view(
        self, table_name: str, database: str, schema: TableSchema
    ) -> str:
        """
        Generate a VIEW that filters out deleted rows.
        Use this view for querying clean data.
        """
        columns = ", ".join(f"`{col.name}`" for col in schema.columns)

        return f"""
CREATE OR REPLACE VIEW `{database}`.`{table_name}_live` AS
SELECT {columns}
FROM `{database}`.`{table_name}` FINAL
WHERE _deleted = 0
""".strip()

