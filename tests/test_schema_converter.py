import pytest

from src.mysql_client import ColumnInfo, TableSchema
from src.schema_converter import SchemaConverter, MYSQL_TO_CLICKHOUSE_TYPE_MAP


class TestConvertColumnType:
    """Tests for SchemaConverter.convert_column_type method."""

    def test_basic_integer_types(self, schema_converter):
        """Test conversion of MySQL integer types."""
        test_cases = [
            ("tinyint", "Int8"),
            ("smallint", "Int16"),
            ("mediumint", "Int32"),
            ("int", "Int32"),
            ("integer", "Int32"),
            ("bigint", "Int64"),
        ]

        for mysql_type, expected_ch_type in test_cases:
            col = ColumnInfo(
                name="test_col",
                data_type=mysql_type,
                is_nullable=False,
                column_key="",
                extra="",
            )
            result = schema_converter.convert_column_type(col)
            assert result == expected_ch_type, f"Failed for {mysql_type}"

    def test_floating_point_types(self, schema_converter):
        """Test conversion of MySQL floating point types."""
        test_cases = [
            ("float", "Float32"),
            ("double", "Float64"),
        ]

        for mysql_type, expected_ch_type in test_cases:
            col = ColumnInfo(
                name="test_col",
                data_type=mysql_type,
                is_nullable=False,
                column_key="",
                extra="",
            )
            result = schema_converter.convert_column_type(col)
            assert result == expected_ch_type

    def test_decimal_with_precision(self, schema_converter):
        """Test conversion of decimal type with precision and scale."""
        col = ColumnInfo(
            name="price",
            data_type="decimal",
            is_nullable=False,
            column_key="",
            extra="",
            numeric_precision=10,
            numeric_scale=2,
        )
        result = schema_converter.convert_column_type(col)
        assert result == "Decimal(10, 2)"

    def test_decimal_without_precision(self, schema_converter):
        """Test conversion of decimal type with default precision."""
        col = ColumnInfo(
            name="amount",
            data_type="decimal",
            is_nullable=False,
            column_key="",
            extra="",
        )
        result = schema_converter.convert_column_type(col)
        assert result == "Decimal(10, 0)"

    def test_string_types(self, schema_converter):
        """Test conversion of MySQL string types."""
        string_types = [
            "char", "varchar", "text", "tinytext", 
            "mediumtext", "longtext", "enum", "set", "json",
        ]

        for mysql_type in string_types:
            col = ColumnInfo(
                name="test_col",
                data_type=mysql_type,
                is_nullable=False,
                column_key="",
                extra="",
            )
            result = schema_converter.convert_column_type(col)
            assert result == "String", f"Failed for {mysql_type}"

    def test_binary_types(self, schema_converter):
        """Test conversion of MySQL binary types."""
        binary_types = ["binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"]

        for mysql_type in binary_types:
            col = ColumnInfo(
                name="test_col",
                data_type=mysql_type,
                is_nullable=False,
                column_key="",
                extra="",
            )
            result = schema_converter.convert_column_type(col)
            assert result == "String", f"Failed for {mysql_type}"

    def test_datetime_types(self, schema_converter):
        """Test conversion of MySQL datetime types."""
        col_datetime = ColumnInfo(
            name="created_at", data_type="datetime", is_nullable=False, column_key="", extra=""
        )
        col_timestamp = ColumnInfo(
            name="updated_at", data_type="timestamp", is_nullable=False, column_key="", extra=""
        )
        col_date = ColumnInfo(
            name="birth_date", data_type="date", is_nullable=False, column_key="", extra=""
        )

        assert schema_converter.convert_column_type(col_datetime) == "DateTime"
        assert schema_converter.convert_column_type(col_timestamp) == "DateTime"
        assert schema_converter.convert_column_type(col_date) == "Date"

    def test_nullable_column(self, schema_converter):
        """Test that nullable columns are wrapped with Nullable()."""
        col = ColumnInfo(
            name="optional_field",
            data_type="varchar",
            is_nullable=True,
            column_key="",
            extra="",
        )
        result = schema_converter.convert_column_type(col)
        assert result == "Nullable(String)"

    def test_nullable_decimal(self, schema_converter):
        """Test nullable decimal type."""
        col = ColumnInfo(
            name="amount",
            data_type="decimal",
            is_nullable=True,
            column_key="",
            extra="",
            numeric_precision=18,
            numeric_scale=4,
        )
        result = schema_converter.convert_column_type(col)
        assert result == "Nullable(Decimal(18, 4))"

    def test_unknown_type_defaults_to_string(self, schema_converter):
        """Test that unknown types default to String."""
        col = ColumnInfo(
            name="weird_col",
            data_type="unknown_type",
            is_nullable=False,
            column_key="",
            extra="",
        )
        result = schema_converter.convert_column_type(col)
        assert result == "String"

    def test_boolean_types(self, schema_converter):
        """Test conversion of boolean types."""
        for mysql_type in ["bool", "boolean"]:
            col = ColumnInfo(
                name="is_active",
                data_type=mysql_type,
                is_nullable=False,
                column_key="",
                extra="",
            )
            result = schema_converter.convert_column_type(col)
            assert result == "Bool"


class TestGenerateCreateTable:
    """Tests for SchemaConverter.generate_create_table method."""

    def test_basic_table_creation(self, schema_converter, sample_table_schema):
        """Test basic table creation SQL generation."""
        sql = schema_converter.generate_create_table(sample_table_schema, "test_db")

        assert "CREATE TABLE IF NOT EXISTS `test_db`.`users`" in sql
        assert "`id` Int32" in sql
        assert "`email` String" in sql
        assert "`name` Nullable(String)" in sql
        assert "`balance` Nullable(Decimal(10, 2))" in sql
        assert "`created_at` DateTime" in sql
        assert "ENGINE = MergeTree()" in sql
        assert "ORDER BY (`id`)" in sql

    def test_composite_primary_key(self, schema_converter, sample_composite_key_schema):
        """Test table creation with composite primary key."""
        sql = schema_converter.generate_create_table(sample_composite_key_schema, "test_db")

        assert "ORDER BY (`order_id`, `product_id`)" in sql

    def test_table_without_primary_key(self, schema_converter):
        """Test table creation without primary key uses first column."""
        schema = TableSchema(
            name="logs",
            columns=[
                ColumnInfo(
                    name="message",
                    data_type="text",
                    is_nullable=False,
                    column_key="",
                    extra="",
                ),
                ColumnInfo(
                    name="timestamp",
                    data_type="datetime",
                    is_nullable=False,
                    column_key="",
                    extra="",
                ),
            ],
            primary_keys=[],
        )
        sql = schema_converter.generate_create_table(schema, "test_db")

        assert "ORDER BY (`message`)" in sql


class TestGenerateDropTable:
    """Tests for SchemaConverter.generate_drop_table method."""

    def test_drop_table_sql(self, schema_converter):
        """Test DROP TABLE SQL generation."""
        sql = schema_converter.generate_drop_table("users", "test_db")
        assert sql == "DROP TABLE IF EXISTS `test_db`.`users`"


class TestGenerateCDCTable:
    """Tests for SchemaConverter.generate_cdc_table method."""

    def test_cdc_table_has_version_and_deleted_columns(
        self, schema_converter, sample_table_schema
    ):
        """Test that CDC table includes _version and _deleted columns."""
        sql = schema_converter.generate_cdc_table(sample_table_schema, "test_db")

        assert "`_version` UInt64" in sql
        assert "`_deleted` UInt8" in sql

    def test_cdc_table_uses_replacing_merge_tree(
        self, schema_converter, sample_table_schema
    ):
        """Test that CDC table uses ReplacingMergeTree engine."""
        sql = schema_converter.generate_cdc_table(sample_table_schema, "test_db")

        assert "ENGINE = ReplacingMergeTree(_version)" in sql


class TestGenerateCDCView:
    """Tests for SchemaConverter.generate_cdc_view method."""

    def test_cdc_view_filters_deleted(self, schema_converter, sample_table_schema):
        """Test that CDC view filters out deleted rows."""
        sql = schema_converter.generate_cdc_view("users", "test_db", sample_table_schema)

        assert "CREATE OR REPLACE VIEW `test_db`.`users_live`" in sql
        assert "FROM `test_db`.`users` FINAL" in sql
        assert "WHERE _deleted = 0" in sql
        assert "`id`" in sql
        assert "`email`" in sql
        # Should not include CDC columns
        assert "_version" not in sql.split("SELECT")[1].split("FROM")[0]
        assert "_deleted" not in sql.split("SELECT")[1].split("FROM")[0]


class TestTypeMapCompleteness:
    """Tests to verify type mapping completeness."""

    def test_all_common_mysql_types_are_mapped(self):
        """Ensure all common MySQL types have mappings."""
        expected_types = [
            "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
            "float", "double", "decimal", "numeric",
            "bool", "boolean",
            "date", "datetime", "timestamp", "time", "year",
            "char", "varchar", "text", "tinytext", "mediumtext", "longtext",
            "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob",
            "enum", "set", "json",
        ]

        for mysql_type in expected_types:
            assert mysql_type in MYSQL_TO_CLICKHOUSE_TYPE_MAP, f"Missing mapping for {mysql_type}"
