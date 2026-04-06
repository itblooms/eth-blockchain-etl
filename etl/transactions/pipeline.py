from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from etl.utils.logger import get_logger

logger = get_logger(__name__)

def extract_transactions_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching transactions data from {s3_bucket_url}/transactions")
        transactions_df = spark.read.parquet(f"{s3_bucket_url}/transactions/*", header=True) \
            .select(
                F.col("block_number"),
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
                F.col("receipt_contract_address")
            )
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/transactions/")
        raise
    except Exception as e:
        logger.error(f"Transactions data extraction failed with unexpected error: {e}")
        raise
    return transactions_df