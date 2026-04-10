from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, LongType, StructField, StructType
from etl.utils.logger import get_logger
from etl.utils.cleaning import ensure_schema

logger = get_logger(__name__)


def extract_transactions_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching transactions data from {s3_bucket_url}/transactions")
        transactions_df = spark.read.parquet(
            f"{s3_bucket_url}/transactions/date=2026-*-*", header=True
        ).select(
            F.col("hash"),
            F.col("nonce"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value"),
            F.col("gas"),
            F.col("gas_price"),
            F.col("receipt_gas_used"),
            F.col("max_priority_fee_per_gas"),
            F.col("max_fee_per_gas"),
            F.col("transacrion_type"),
            F.col("receipt_status"),
            F.col("receipt_contract_address"),
            F.col("block_hash"),
        )
        logger.info("Transactions data was successfully extracted!")
        return transactions_df
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/transactions/")
        raise
    except Exception as e:
        logger.error(f"Transactions data extraction failed with unexpected error: {e}")
        raise


def clean_blocks_data(df: DataFrame) -> DataFrame:
    logger.info("Casting column types and trimming stings...")
    transformed_df = df.drop_duplicates().select(
        F.trim(F.col("hash").cast(StringType())).alias("hash"),
        F.col("nonce").cast(LongType()).alias("num_sender_prior_transactions"),
        F.trim(F.col("from_address").cast(StringType())).alias("from_adress"),
        F.trim(F.col("to_address").cast(StringType())).alias("to_address"),
        F.col("value").cast(DoubleType()),
        F.col("gas").cast(LongType()),
        F.col("gas_price").cast(LongType()),
        F.col("receipt_gas_used").cast(LongType()),
        F.col("max_priority_fee_per_gas").cast(LongType()),
        F.col("max_fee_per_gas").cast(LongType()),
        F.col("transaction_type").cast(LongType()),
        F.col("receipt_status").cast(LongType()),
        F.trim(F.col("receipt_contract_address").cast(StringType())).alias(
            "receipt_contract_address"
        ),
        F.trim(F.col("block_hash").cast(StringType())).alias("block_hash"),
    )
    logger.info("Transactions data was successfully cleaned!")
    return transformed_df


def validate_transactions_data(df: DataFrame) -> None:
    expected_schema = StructType(
        [
            StructField("hash", StringType()),
            StructField("num_sender_prior_transactions", LongType()),
            StructField("from_address", StringType()),
            StructField("from_address", StringType()),
            StructField("value", DoubleType()),
            StructField("gas", LongType()),
            StructField("gas_price", LongType()),
            StructField("receipt_gas_used", LongType()),
            StructField("max_priority_fee_per_gas", LongType()),
            StructField("max_fee_per_gas", LongType()),
            StructField("transaction_type", LongType()),
            StructField("receipt_status", LongType()),
            StructField("receipt_contract_address", StringType()),
            StructField("block_hash", StringType()),
        ]
    )
    ensure_schema(
        df=df,
        expected_schema=expected_schema,
        table_name="transactions",
        logger=logger,
    )

    logger.info("Checking constraints...")
    checks_df = df.select(
        F.sum(F.when(F.col("num_sender_prior_transactions") < 0, 1).otherwise(0)).alias(
            "negative_num_sender_prior_transactions"
        ),
        F.sum(F.when(F.col("value") <= 0, 1).otherwise(0)).alias(
            "non_positive_value_violations"
        ),
        F.sum(
            F.when(F.col("max_fee_per_gas") < F.col("max_priority_fee_per_gas"), 1).otherwise(0)
        ).alias("priority_fee_exceeds_total_fee_violations"),
    ).collect()[0]

    if n := checks_df["negative_num_sender_prior_transactions"] > 0:
        exc = ValueError(
            f"For {n} rows in transactions table sender has less than 0 prior transactions"
        )
        logger.error(
            "Check for non-negative number of prior transactions has failed",
            exc_info=exc,
        )
        raise exc
    if n := checks_df["non_positive_value_violations"] > 0:
        exc = ValueError(f"For {n} rows in transactions table `value` is less than 0")
        logger.error("Check failed. Negative `value` encountered", exc_info=exc)
        raise exc
    if n := checks_df["priority_fee_exceeds_total_fee_violations"] > 0:
        exc = ValueError(f"For {n} rows in transactions table priority fee exceeds total fee")
        logger.error("Check failed. Total fee can't be less than prior fee", exc_info=exc)
        raise exc
    logger.info("All checkes have been successfully passed!")
