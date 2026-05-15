from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, LongType
from etl.utils.logger import get_logger


logger = get_logger(__name__)


def clean_transactions_data(df: DataFrame) -> DataFrame:
    logger.info("Casting column types and trimming stings...")
    transformed_df = df.drop_duplicates().select(
        F.trim(F.col("hash").cast(StringType())).alias("hash"),
        F.col("nonce").cast(LongType()).alias("num_sender_prior_transactions"),
        F.trim(F.col("from_address").cast(StringType())).alias("from_address"),
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
