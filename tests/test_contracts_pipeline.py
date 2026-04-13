import pytest
from pyspark.sql.types import StringType, StructField, StructType

from etl.contracts.pipeline import (
    clean_contracts_data,
    validate_contracts_data,
)


def test_clean_contracts_data_removes_duplicates(spark):
    """Test that duplicate rows are removed."""
    data = [
        ("0x123", "0xabc"),
        ("0x123", "0xabc"),
        ("0x456", "0xdef"),
    ]
    df = spark.createDataFrame(data, ["address", "bytecode"])

    result = clean_contracts_data(df)

    assert result.count() == 2


def test_clean_contracts_data_trims_whitespace(spark):
    """Test that extra spaces are removed from values."""
    data = [
        ("  0x123  ", "  0xabc  "),
        ("0x456", "0xdef"),
    ]
    df = spark.createDataFrame(data, ["address", "bytecode"])

    result = clean_contracts_data(df)
    rows = result.collect()

    assert rows[0]["address"] == "0x123"
    assert rows[0]["bytecode"] == "0xabc"


def test_clean_contracts_data_converts_to_string(spark):
    """Test that columns become StringType."""
    data = [
        (123, 456),
        (789, 101112),
    ]
    df = spark.createDataFrame(data, ["address", "bytecode"])

    result = clean_contracts_data(df)

    assert result.schema["address"].dataType == StringType()
    assert result.schema["bytecode"].dataType == StringType()


def test_clean_contracts_data_empty_dataframe(spark):
    """Test that cleaning an empty DataFrame works."""
    schema = StructType(
        [
            StructField("address", StringType(), nullable=True),
            StructField("bytecode", StringType(), nullable=True),
        ]
    )
    df = spark.createDataFrame([], schema=schema)

    result = clean_contracts_data(df)

    assert result.count() == 0


def test_validate_contracts_data_valid_schema(spark):
    """Test that valid schema passes without errors."""
    data = [
        ("0x123", "0xabc"),
        ("0x456", "0xdef"),
    ]
    df = spark.createDataFrame(data, ["address", "bytecode"])

    # This should not raise any error
    validate_contracts_data(df)


def test_validate_contracts_data_missing_column(spark):
    """Test that missing columns cause an error."""
    data = [
        ("0x123",),
        ("0x456",),
    ]
    df = spark.createDataFrame(data, ["address"])

    with pytest.raises(ValueError, match="missing"):
        validate_contracts_data(df)


def test_validate_contracts_data_wrong_types(spark):
    """Test that wrong column types cause an error."""
    data = [
        (123, 456),
    ]
    df = spark.createDataFrame(data, ["address", "bytecode"])

    with pytest.raises(ValueError, match="types"):
        validate_contracts_data(df)
