from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from etl.utils.logger import get_logger


logger = get_logger(__name__)


def clean_contracts_data(df: DataFrame) -> DataFrame:
    logger.info("Casting column types and trimming stings...")
    transformed_df = df.drop_duplicates().select(
        F.trim(F.col("address").cast(StringType())).alias("address"),
        F.trim(F.col("bytecode").cast(StringType())).alias("bytecode"),
    )
    logger.info("Contracts data was successfully cleaned!")
    return transformed_df
