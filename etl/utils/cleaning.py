from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.logger import PySparkLogger


def ensure_schema(
    df: DataFrame, 
    expected_schema: StructType, 
    table_name: str, 
    logger: PySparkLogger
) -> None:
    expected = {f.name: f.dataType for f in expected_schema.fields}
    actual = {f.name: f.dataType for f in df.schema.fields}
    logger.info(f"Ensuring {table_name} data schema...")

    missing_cols = set(expected) - set(actual)
    wrong_types = {k for k in actual if k in expected and actual[k] != expected[k]}

    if missing_cols:
        exc = ValueError(f"{table_name} table schema is missing {missing_cols} columns")
        logger.error(f"{table_name} table schema mismatch: {missing_cols} are missing", exc_info=exc)
        raise exc
    if wrong_types:
        exc = ValueError(f"{table_name} table types are wrong. Incorrect fields {wrong_types}")
        logger.error(f"{table_name} table schema types mismatch: {wrong_types}", exc_info=exc)
        raise exc
    logger.info("{table_name} data schema is valid!")