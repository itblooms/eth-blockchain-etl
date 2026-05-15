from pyspark.sql import DataFrame
import os


def get_snowflake_options() -> dict[str, str]:
    return {
        "sfURL": os.environ["SF_ACCOUNT"] + ".snowflakecomputing.com",
        "sfUser": os.environ["SF_USER"],
        "sfPassword": os.environ["SF_PASSWORD"],
        "sfDatabase": os.environ["SF_DATABASE"],
        "sfSchema": os.environ["SF_SCHEMA"],
        "sfWarehouse": os.environ["SF_WAREHOUSE"],
        "sfRole": os.environ["SF_ROLE"],
    }


def load_into_snowflake(df: DataFrame, table: str, snowflake_options: dict[str, str]) -> None:
    df.write \
        .format("net.snowflake.spark.esnowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table.upper()) \
        .mode("append") \
        .save()  # fmt: skip
