from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, DoubleType, StringType
from etl.utils.logger import get_logger


logger = get_logger(__name__)


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
