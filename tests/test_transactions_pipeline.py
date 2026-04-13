import pytest
from pyspark.sql.types import StringType, DoubleType, LongType, StructField, StructType

from etl.transactions.pipeline import (
    clean_transactions_data,
    validate_transactions_data,
)


def test_clean_transactions_data_removes_duplicates(spark):
    """Test that duplicate rows are removed."""
    data = [
        ("0xhash1", 1, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
        ("0xhash1", 1, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
        ("0xhash2", 2, "0xfrom2", "0xto2", 200.0, 40000, 2000, 35000, 60, 300, 2, 1, "0xcontract2", "0xblock2"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_transactions_data(df)

    assert result.count() == 2


def test_clean_transactions_data_trims_whitespace(spark):
    """Test that extra spaces are removed from string values."""
    data = [
        ("  0xhash1  ", 1, "  0xfrom1  ", "  0xto1  ", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "  0xcontract1  ", "  0xblock1  "),
        ("0xhash2", 2, "0xfrom2", "0xto2", 200.0, 40000, 2000, 35000, 60, 300, 2, 1, "0xcontract2", "0xblock2"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_transactions_data(df)
    rows = result.collect()

    assert rows[0]["hash"] == "0xhash1"
    assert rows[0]["from_address"] == "0xfrom1"
    assert rows[0]["to_address"] == "0xto1"
    assert rows[0]["receipt_contract_address"] == "0xcontract1"
    assert rows[0]["block_hash"] == "0xblock1"


def test_clean_transactions_data_converts_to_correct_types(spark):
    """Test that columns are cast to correct types."""
    data = [
        ("0xhash1", "1", "0xfrom1", "0xto1", "100", "30000", "1000", "25000", "50", "200", "2", "1", "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_transactions_data(df)

    assert result.schema["hash"].dataType == StringType()
    assert result.schema["num_sender_prior_transactions"].dataType == LongType()
    assert result.schema["from_address"].dataType == StringType()
    assert result.schema["to_address"].dataType == StringType()
    assert result.schema["value"].dataType == DoubleType()
    assert result.schema["gas"].dataType == LongType()
    assert result.schema["gas_price"].dataType == LongType()
    assert result.schema["receipt_gas_used"].dataType == LongType()
    assert result.schema["max_priority_fee_per_gas"].dataType == LongType()
    assert result.schema["max_fee_per_gas"].dataType == LongType()
    assert result.schema["transaction_type"].dataType == LongType()
    assert result.schema["receipt_status"].dataType == LongType()
    assert result.schema["receipt_contract_address"].dataType == StringType()
    assert result.schema["block_hash"].dataType == StringType()


def test_clean_transactions_data_renames_nonce_column(spark):
    """Test that nonce is renamed to num_sender_prior_transactions."""
    data = [
        ("0xhash1", 5, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    result = clean_transactions_data(df)

    assert "num_sender_prior_transactions" in result.columns
    assert "nonce" not in result.columns


def test_clean_transactions_data_empty_dataframe(spark):
    """Test that cleaning an empty DataFrame works."""
    schema = StructType(
        [
            StructField("hash", StringType(), nullable=True),
            StructField("nonce", LongType(), nullable=True),
            StructField("from_address", StringType(), nullable=True),
            StructField("to_address", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
            StructField("gas", LongType(), nullable=True),
            StructField("gas_price", LongType(), nullable=True),
            StructField("receipt_gas_used", LongType(), nullable=True),
            StructField("max_priority_fee_per_gas", LongType(), nullable=True),
            StructField("max_fee_per_gas", LongType(), nullable=True),
            StructField("transaction_type", LongType(), nullable=True),
            StructField("receipt_status", LongType(), nullable=True),
            StructField("receipt_contract_address", StringType(), nullable=True),
            StructField("block_hash", StringType(), nullable=True),
        ]
    )
    df = spark.createDataFrame([], schema=schema)

    result = clean_transactions_data(df)

    assert result.count() == 0


def test_validate_transactions_data_valid_schema(spark):
    """Test that valid schema passes without errors."""
    data = [
        ("0xhash1", 1, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_transactions_data(df)

    # This should not raise any error
    validate_transactions_data(cleaned_df)


def test_validate_transactions_data_missing_column(spark):
    """Test that missing columns cause an error."""
    data = [
        ("0xhash1", 1, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    with pytest.raises(ValueError, match="missing"):
        validate_transactions_data(df)


def test_validate_transactions_data_wrong_types(spark):
    """Test that wrong column types cause an error."""
    data = [
        ("0xhash1", "not_a_number", "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "num_sender_prior_transactions", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)

    with pytest.raises(ValueError, match="types"):
        validate_transactions_data(df)


def test_validate_transactions_data_negative_num_sender_prior_transactions(spark):
    """Test that negative num_sender_prior_transactions raises an error."""
    data = [
        ("0xhash1", -1, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_transactions_data(df)

    with pytest.raises(ValueError, match="prior transactions"):
        validate_transactions_data(cleaned_df)


def test_validate_transactions_data_non_positive_value(spark):
    """Test that non-positive value raises an error."""
    data = [
        ("0xhash1", 1, "0xfrom1", "0xto1", 0.0, 30000, 1000, 25000, 50, 200, 2, 1, "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_transactions_data(df)

    with pytest.raises(ValueError, match="value"):
        validate_transactions_data(cleaned_df)


def test_validate_transactions_data_priority_fee_exceeds_total_fee(spark):
    """Test that max_priority_fee_per_gas > max_fee_per_gas raises an error."""
    data = [
        ("0xhash1", 1, "0xfrom1", "0xto1", 100.0, 30000, 1000, 25000, 300, 200, 2, 1, "0xcontract1", "0xblock1"),
    ]  # fmt: skip
    columns = [
        "hash", "nonce", "from_address", "to_address", "value", "gas", "gas_price",
        "receipt_gas_used", "max_priority_fee_per_gas", "max_fee_per_gas",
        "transaction_type", "receipt_status", "receipt_contract_address", "block_hash",
    ]  # fmt: skip
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_transactions_data(df)

    with pytest.raises(ValueError, match="priority fee exceeds total fee"):
        validate_transactions_data(cleaned_df)
