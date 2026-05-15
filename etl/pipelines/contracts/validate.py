from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType
from etl.utils.logger import get_logger
from etl.utils.cleaning import ensure_schema


logger = get_logger(__name__)


def validate_contracts_data(df: DataFrame) -> None:
    expected_schema = StructType(
        [
            StructField("address", StringType()),
            StructField("bytecode", StringType()),
        ]
    )
    ensure_schema(
        df=df,
        expected_schema=expected_schema,
        table_name="contracts",
        logger=logger,
    )
