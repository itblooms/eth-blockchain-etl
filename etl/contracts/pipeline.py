from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from etl.utils.logger import get_logger


logger = get_logger(__name__)

def extract_contracts_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching transactions data from {s3_bucket_url}/contracts")
        contracts_df = spark.read.parquet(f"{s3_bucket_url}/contracts/*", header=True) \
            .select(
                F.col("address"),
                F.col("bytecode"),
                F.col("block_number")
            )
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/contracts/")
        raise
    except Exception as e:
        logger.error(f"Contracts data extraction failed with unexpected error: {e}")
        raise
    return contracts_df