from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, LongType, StructField, StructType
from etl.utils.logger import get_logger
from etl.utils.cleaning import ensure_schema


logger = get_logger(__name__)


def validate_transactions_data(df: DataFrame) -> None:
    expected_schema = StructType(
        [
            StructField("hash", StringType()),
            StructField("num_sender_prior_transactions", LongType()),
            StructField("from_address", StringType()),
            StructField("from_address", StringType()),
            StructField("value", DoubleType()),
            StructField("gas", LongType()),
            StructField("gas_price", LongType()),
            StructField("receipt_gas_used", LongType()),
            StructField("max_priority_fee_per_gas", LongType()),
            StructField("max_fee_per_gas", LongType()),
            StructField("transaction_type", LongType()),
            StructField("receipt_status", LongType()),
            StructField("receipt_contract_address", StringType(), nullable=True),
            StructField("block_hash", StringType()),
        ]
    )
    ensure_schema(
        df=df,
        expected_schema=expected_schema,
        table_name="transactions",
        logger=logger,
    )

    logger.info("Checking constraints...")
    checks_df = df.select(
        F.sum(F.when(F.col("num_sender_prior_transactions") < 0, 1).otherwise(0)).alias(
            "negative_num_sender_prior_transactions"
        ),
        F.sum(F.when(F.col("value") <= 0, 1).otherwise(0)).alias(
            "non_positive_value_violations"
        ),
        F.sum(
            F.when(F.col("max_fee_per_gas") < F.col("max_priority_fee_per_gas"), 1).otherwise(0)
        ).alias("priority_fee_exceeds_total_fee_violations"),
    ).collect()[0]

    if n := checks_df["negative_num_sender_prior_transactions"] > 0:
        exc = ValueError(
            f"For {n} rows in transactions table sender has less than 0 prior transactions"
        )
        logger.error(
            "Check for non-negative number of prior transactions has failed",
            exc_info=exc,
        )
        raise exc
    if n := checks_df["non_positive_value_violations"] > 0:
        exc = ValueError(f"For {n} rows in transactions table `value` is less than 0")
        logger.error("Check failed. Negative `value` encountered", exc_info=exc)
        raise exc
    if n := checks_df["priority_fee_exceeds_total_fee_violations"] > 0:
        exc = ValueError(f"For {n} rows in transactions table priority fee exceeds total fee")
        logger.error("Check failed. Total fee can't be less than prior fee", exc_info=exc)
        raise exc
    logger.info("All checkes have been successfully passed!")
