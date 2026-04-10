from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
    DateType,
    StructField,
    StructType,
)
from etl.utils.logger import get_logger
from etl.utils.cleaning import ensure_schema


logger = get_logger(__name__)


def extract_blocks_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching blocks data from {s3_bucket_url}/blocks...")
        blocks_df = spark.read.parquet(
            f"{s3_bucket_url}/blocks/date=2026-*-*", header=True
        ).select(
            F.col("date"),
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
            F.col("timestamp"),
        )
        logger.info("Blocks data was successfully extracted!")
        return blocks_df
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/blocks/")
        raise
    except Exception as e:
        logger.error(f"Blocks data extraction failed with unexpected error: {e}")
        raise


def clean_blocks_data(df: DataFrame) -> DataFrame:
    logger.info("Casting column types, trimming stings and formatting dates...")
    transformed_df = df.drop_duplicates().select(
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("partition_date"),
        F.col("number").cast(LongType()),
        F.trim(F.col("hash").cast(StringType())).alias("hash"),
        F.trim(F.col("miner").cast(StringType())).alias("miner"),
        F.col("difficulty").cast(DoubleType()),
        F.col("total_difficulty").cast(DoubleType()),
        F.col("size").cast(LongType()),
        F.col("gas_limit").cast(LongType()),
        F.col("gas_used").cast(LongType()),
        F.col("base_fee_per_gas").cast(LongType()),
        F.col("transaction_count").cast(LongType()),
        F.to_timestamp(F.from_unixtime(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")).alias(
            "timestamp"
        ),
    )
    logger.info("Blocks data was successfully cleaned!")
    return transformed_df


def validate_blocks_data(df: DataFrame) -> None:
    expected_schema = StructType(
        [
            StructField("partition_date", DateType()),
            StructField("number", LongType()),
            StructField("hash", StringType()),
            StructField("miner", StringType()),
            StructField("difficulty", DoubleType()),
            StructField("total_difficulty", DoubleType()),
            StructField("size", LongType()),
            StructField("gas_limit", LongType()),
            StructField("gas_used", LongType()),
            StructField("base_fee_per_gas", LongType()),
            StructField("transaction_count", LongType()),
            StructField("timestamp", TimestampType()),
        ]
    )
    ensure_schema(
        df=df,
        expected_schema=expected_schema,
        table_name="blocks",
        logger=logger,
    )

    logger.info("Checking constraints...")
    checks_df = df.select(
        F.sum(F.when(F.col("transaction_count") < 0, 1).otherwise(0)).alias(
            "transaction_count_violations"
        ),
        F.sum(F.when(F.col("gas_used") > F.col("gas_limit"), 1).otherwise(0)).alias(
            "gas_used_exceeds_limit_violations"
        ),
        F.sum(F.when(F.col("base_fee_per_gas") > F.col("gas_used"), 1).otherwise(0)).alias(
            "base_fee_exceeds_gas_used_violations"
        ),
    ).collect()[0]

    if (n := checks_df["transaction_count_violations"]) > 0:
        exc = ValueError(f"For {n} rows in blocks table `transaction_count` < 0")
        logger.error("Check for non-negative transactions count has failed", exc_info=exc)
        raise exc
    if (n := checks_df["gas_used_exceeds_limit_violations"]) > 0:
        exc = ValueError(f"For {n} rows in block table `gas_used` exceeds `gas_limit`")
        logger.error("Check failed. `gas_used` > `gas_limit`", exc_info=exc)
        raise exc
    if (n := checks_df["base_fee_exceeds_gas_used_violations"]) > 0:
        exc = ValueError(f"For {n} rows in blocks table `base_fee_per_gas` exceeds `gas_limit`")
        logger.error("Check failed. `base_fee_per_gas` > `gas_used`", exc_info=exc)
        raise exc
    logger.info("All checkes have been successfully passed!")
