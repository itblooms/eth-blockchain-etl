import pytest
from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
    DateType,
    StructField,
    StructType,
)

from etl.blocks.pipeline import (
    clean_blocks_data,
    validate_blocks_data,
    enrich_blocks_data,
)


def test_clean_blocks_data_removes_duplicates(spark):
    """Test that duplicate rows are removed."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
        ("2026-01-02", 2, "0xdef", "0xminer2", 200.0, 400.0, 600, 40000, 35000, 20, 10, 1700000060),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_blocks_data(df)

    assert result.count() == 2


def test_clean_blocks_data_trims_whitespace(spark):
    """Test that extra spaces are removed from string values."""
    data = [
        ("2026-01-01", 1, "  0xabc  ", "  0xminer1  ", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
        ("2026-01-02", 2, "0xdef", "0xminer2", 200.0, 400.0, 600, 40000, 35000, 20, 10, 1700000060),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_blocks_data(df)
    rows = result.collect()

    assert rows[0]["hash"] == "0xabc"
    assert rows[0]["miner"] == "0xminer1"


def test_clean_blocks_data_converts_to_correct_types(spark):
    """Test that columns are cast to correct types."""
    data = [
        ("2026-01-01", "1", "0xabc", "0xminer1", "100", "200", "500", "30000", "25000", "10", "5", "1700000000"),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_blocks_data(df)

    assert result.schema["number"].dataType == LongType()
    assert result.schema["hash"].dataType == StringType()
    assert result.schema["miner"].dataType == StringType()
    assert result.schema["difficulty"].dataType == DoubleType()
    assert result.schema["total_difficulty"].dataType == DoubleType()
    assert result.schema["size"].dataType == LongType()
    assert result.schema["gas_limit"].dataType == LongType()
    assert result.schema["gas_used"].dataType == LongType()
    assert result.schema["base_fee_per_gas"].dataType == LongType()
    assert result.schema["transaction_count"].dataType == LongType()
    assert result.schema["timestamp"].dataType == TimestampType()
    assert result.schema["partition_date"].dataType == DateType()


def test_clean_blocks_data_creates_partition_date(spark):
    """Test that partition_date column is created from date."""
    data = [
        ("2026-01-15", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_blocks_data(df)
    rows = result.collect()

    assert "partition_date" in result.columns
    assert rows[0]["partition_date"] is not None


def test_clean_blocks_data_empty_dataframe(spark):
    """Test that cleaning an empty DataFrame works."""
    schema = StructType(
        [
            StructField("date", StringType(), nullable=True),
            StructField("number", LongType(), nullable=True),
            StructField("hash", StringType(), nullable=True),
            StructField("miner", StringType(), nullable=True),
            StructField("difficulty", DoubleType(), nullable=True),
            StructField("total_difficulty", DoubleType(), nullable=True),
            StructField("size", LongType(), nullable=True),
            StructField("gas_limit", LongType(), nullable=True),
            StructField("gas_used", LongType(), nullable=True),
            StructField("base_fee_per_gas", LongType(), nullable=True),
            StructField("transaction_count", LongType(), nullable=True),
            StructField("timestamp", LongType(), nullable=True),
        ]
    )
    df = spark.createDataFrame([], schema=schema)

    result = clean_blocks_data(df)

    assert result.count() == 0


def test_validate_blocks_data_valid_schema(spark):
    """Test that valid schema passes without errors."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)

    # This should not raise any error
    validate_blocks_data(cleaned_df)


def test_validate_blocks_data_missing_column(spark):
    """Test that missing columns cause an error."""
    data = [
        (1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]
    columns = [
        "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    with pytest.raises(ValueError, match="missing"):
        validate_blocks_data(df)


def test_validate_blocks_data_wrong_types(spark):
    """Test that wrong column types cause an error."""
    data = [
        ("2026-01-01", "not_a_number", "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "partition_date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    with pytest.raises(ValueError, match="types"):
        validate_blocks_data(df)


def test_validate_blocks_data_negative_transaction_count(spark):
    """Test that negative transaction_count raises an error."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, -1, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)

    with pytest.raises(ValueError, match="transaction_count"):
        validate_blocks_data(cleaned_df)


def test_validate_blocks_data_gas_used_exceeds_gas_limit(spark):
    """Test that gas_used > gas_limit raises an error."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 35000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)

    with pytest.raises(ValueError, match="gas_used.*gas_limit"):
        validate_blocks_data(cleaned_df)


def test_validate_blocks_data_base_fee_exceeds_gas_used(spark):
    """Test that base_fee_per_gas > gas_used raises an error."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 30000, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)

    with pytest.raises(ValueError, match="base_fee_per_gas"):
        validate_blocks_data(cleaned_df)


def test_enrich_blocks_data_adds_min_transaction_fee_percent(spark):
    """Test that enrichment adds min_transaction_fee_percent column."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)

    result = enrich_blocks_data(cleaned_df)

    assert "min_transaction_fee_percent" in result.columns


def test_enrich_blocks_data_calculates_fee_percent(spark):
    """Test that min_transaction_fee_percent is calculated correctly."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)
    result = enrich_blocks_data(cleaned_df)
    rows = result.collect()

    # base_fee_per_gas=10, transaction_count=5, gas_used=25000
    # expected = round(10 * 5 / 25000 * 100, 2) = round(0.2, 2) = 0.2
    assert rows[0]["min_transaction_fee_percent"] == pytest.approx(0.2, rel=1e-2)


def test_enrich_blocks_data_preserves_other_columns(spark):
    """Test that enrichment keeps all original columns."""
    data = [
        ("2026-01-01", 1, "0xabc", "0xminer1", 100.0, 200.0, 500, 30000, 25000, 10, 5, 1700000000),
    ]  # fmt: skip
    columns = [
        "date", "number", "hash", "miner", "difficulty", "total_difficulty",
        "size", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "timestamp",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_blocks_data(df)
    result = enrich_blocks_data(cleaned_df)

    for col in cleaned_df.columns:
        assert col in result.columns
