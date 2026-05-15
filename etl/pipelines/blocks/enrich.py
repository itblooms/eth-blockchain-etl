from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from etl.utils.logger import get_logger


logger = get_logger(__name__)


def enrich_blocks_data(df: DataFrame) -> DataFrame:
    logger.info("Enriching blocks data...")
    enriched_df = df.withColumn(
        "min_transaction_fee_percent",
        F.round(
            (
                F.col("base_fee_per_gas").cast(DoubleType())
                * F.col("transaction_count")
                / F.col("gas_used")
                * 100
            ),
            2,
        ),
    )
    return enriched_df
