from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType
from etl.utils.logger import get_logger
from etl.utils.cleaning import ensure_schema


logger = get_logger(__name__)

def extract_contracts_data(spark: SparkSession, s3_bucket_url: str) -> DataFrame:
    try:
        logger.info(f"Fetching contracts data from {s3_bucket_url}/contracts")
        contracts_df = spark.read.parquet(f"{s3_bucket_url}/contracts/date=2026-*-*", header=True) \
            .select(
                F.col("address"),
                F.col("bytecode")
            )
        logger.info("Contracts data was successfully extracted!")
        return contracts_df
    except FileNotFoundError:
        logger.error(f"Fetching failed. There's no files at {s3_bucket_url}/contracts/")
        raise
    except Exception as e:
        logger.error(f"Contracts data extraction failed with unexpected error: {e}")
        raise

def clean_contracts_data(df: DataFrame) -> DataFrame:
    logger.info(f"Casting column types and trimming stings...")
    transformed_df = (
        df
        .drop_duplicates()
        .select(
            F.trim(F.col("address").cast(StringType())).alias("address"),
            F.trim(F.col("bytecode").cast(StringType())).alias("bytecode")
        )
    )
    logger.info("Contracts data was successfully cleaned!")
    return transformed_df

def validate_contracts_data(df: DataFrame) -> None:
    expected_schema = StructType([
        StructField("address", StringType()),
        StructField("bytecode", StringType())
    ])
    ensure_schema(
        df=df, 
        expected_schema=expected_schema, 
        table_name="contracts", 
        logger=logger
    )