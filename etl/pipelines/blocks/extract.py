from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from etl.utils.logger import get_logger


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
