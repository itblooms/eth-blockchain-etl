from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, LongType, StructField, StructType
from etl.utils.logger import get_logger

logger = get_logger(__name__)

def extract_transactions_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching transactions data from {s3_bucket_url}/transactions")
        transactions_df = spark.read.parquet(f"{s3_bucket_url}/transactions/date=2026-*-*", header=True) \
            .select(
                F.col("hash"),
                F.col("nonce").alias("num_sender_prior_transactions"),
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
                F.col("block_hash")
            )
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/transactions/")
        raise
    except Exception as e:
        logger.error(f"Transactions data extraction failed with unexpected error: {e}")
        raise
    return transactions_df

def clean_blocks_data(df: DataFrame) -> DataFrame:
    logger.info(f"Casting column types and trimming stings...")
    transformed_df = (
        df
        .drop_duplicates()
        .select(
            F.trim(F.col("hash").cast(StringType())),
            F.col("num_sender_prior_transactions").cast(LongType()),
            F.trim(F.col("from_address").cast(StringType())), 
            F.trim(F.col("to_address").cast(StringType())), 
            F.col("value").cast(DoubleType()),
            F.col("gas").cast(LongType()),
            F.col("gas_price").cast(LongType()),
            F.col("receipt_gas_used").cast(LongType()),
            F.col("max_priority_fee_per_gas").cast(LongType()),
            F.col("max_fee_per_gas").cast(LongType()),
            F.col("receipt_status").cast(LongType()),
            F.trim(F.col("receipt_contract_address").cast(StringType())),
            F.trim(F.col("block_hash").cast(StringType()))
        )
    )
    logger.info("Transactions data was successfully cleaned!")
    return transformed_df