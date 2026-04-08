from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    LongType, 
    DoubleType, 
    StringType, 
    TimestampType, 
    DateType, 
    StructField,
    StructType
)
from etl.utils.logger import get_logger


logger = get_logger(__name__)

def extract_blocks_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching blocks data from {s3_bucket_url}/blocks...")
        blocks_df = spark.read.parquet(f"{s3_bucket_url}/blocks/date=2026-*-*", header=True) \
            .select(
                F.col("date").alias("partition_date"),
                F.col("number"),
                F.col("hash"),
                F.col("miner"), 
                F.col("difficulty"),
                F.col("total_difficulty"),
                F.col("size"),
                F.col("gas_limit"),
                F.col("gas_used"),
                F.col("base_fee_per_gas"),
                F.col("transaction_count"),
                F.col("timestamp")
            )
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/blocks/")
        raise
    except Exception as e:
        logger.error(f"Blocks data extraction failed with unexpected error: {e}")
        raise
    logger.info("Blocks data was successfully extracted!")
    return blocks_df

def clean_blocks_data(df: DataFrame) -> DataFrame:
    logger.info(f"Casting column types, trimming stings and formatting dates...")
    transformed_df = (
        df
        .drop_duplicates()
        .select(
            F.to_date(F.col("partition_date"), "yyyy-MM-dd"),
            F.col("number").cast(LongType()),
            F.trim(F.col("hash").cast(StringType())),
            F.trim(F.col("miner").cast(StringType())), 
            F.col("difficulty").cast(DoubleType()),
            F.col("total_difficulty").cast(DoubleType()),
            F.col("size").cast(LongType()),
            F.col("gas_limit").cast(LongType()),
            F.col("gas_used").cast(LongType()),
            F.col("base_fee_per_gas").cast(LongType()),
            F.col("transaction_count").cast(LongType()),
            F.to_timestamp(F.col("timestamp"))
        )
    )
    logger.info("Blocks data was successfully cleaned!")
    return transformed_df

def validate_blocks_data(df: DataFrame) -> None:
    expected_schema = StructType([
        StructField("partition_date", DateType()),
        StructField("number", StringType()),
        StructField("hash", StringType()),
        StructField("miner", StringType()),
        StructField("difficulty", DoubleType()),
        StructField("total_difficulty", DoubleType()),
        StructField("size", LongType()),
        StructField("gas_limit", LongType()),
        StructField("gas_used", LongType()),
        StructField("base_free_per_gas", LongType()),
        StructField("transaction_count", LongType()),
        StructField("timestamp", TimestampType())
    ])
    expected = {f.name: f.dataType for f in expected_schema.fields}
    actual = {f.name: f.dataType for f in df.schema.fields}
    logger.info("Ensuring blocks data schema...")

    missing_cols = set(expected) - set(actual)
    wrong_types = {k for k in actual if k in expected and actual[k] != expected[k]}

    if missing_cols:
        exc = ValueError(f"Blocks table schema is missing {missing_cols} columns")
        logger.error(f"Blocks table schema mismatch: {missing_cols} are missing", exc_info=exc)
        raise exc
    if wrong_types:
        exc = ValueError(f"Blocks table types are wrong. Incorrect fields {wrong_types}")
        logger.error(f"Blocks table schema types mismatch: {wrong_types}", exc_info=exc)
        raise exc
    logger.info("Blocks data schema is valid!")

    logger.info("Checking constraints...")
    checks_df = df.select(
        F.sum(F.when(F.col("transaction_count") <= 0, 1).otherwise(0)) \
            .alias("transaction_count_violations"),
        F.sum(F.when(F.col("gas_used") > F.col("gas_limit"), 1).otherwise(0)) \
            .alias("gas_used_exceeds_limit_violations"),
        F.sum(F.when(F.col("base_fee_per_gas") > F.col("gas_used"), 1).otherwise(0)) \
            .alias("base_fee_exceeds_gas_used_violations")
    )

    if n:=checks_df.collect()[0]["transaction_count"] > 0:
        exc = ValueError(f"For {n} rows in blocks table `transaction_count` <= 0")
        logger.error("Check for positive transactions count has failed", exc_info=exc)
        raise exc
    if n:=checks_df.collect()[0]["gas_used_exceeds_limit_violations"] > 0:
        exc = ValueError(f"For {n} rows in block table `gas_used` exceeds `gas_limit`")
        logger.error("Check failed. `gas_used` > `gas_limit`", exc_info=exc)
        raise exc
    if n:=checks_df.collect()[0]["base_fee_exceeds_gas_used_violations"] > 0:
        exc = ValueError(f"For {n} rows in blocks table `base_fee_per_gas` exceeds `gas_limit`")
        logger.error("Check failed. `base_fee_per_gas` > `gas_used`", exc_info=exc)
        raise exc
    logger.info("All checkes have been successfully passed!")
