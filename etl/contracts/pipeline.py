from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType
from etl.utils.logger import get_logger


logger = get_logger(__name__)

def extract_contracts_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching contracts data from {s3_bucket_url}/contracts")
        contracts_df = spark.read.parquet(f"{s3_bucket_url}/contracts/date=2026-*-*", header=True) \
            .select(
                F.col("address"),
                F.col("bytecode")
            )
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/contracts/")
        raise
    except Exception as e:
        logger.error(f"Contracts data extraction failed with unexpected error: {e}")
        raise
    return contracts_df

def clean_contracts_data(df: DataFrame) -> DataFrame:
    logger.info(f"Casting column types and trimming stings...")
    transformed_df = (
        df
        .drop_duplicates()
        .select(
            F.trim(F.col("address").cast(StringType())),
            F.trim(F.col("bytecode").cast(StringType()))
        )
    )
    logger.info("Contracts data was successfully cleaned!")
    return transformed_df