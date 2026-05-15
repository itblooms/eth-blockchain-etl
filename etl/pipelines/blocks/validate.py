from pyspark.sql import DataFrame
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
