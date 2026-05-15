from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "s3a://ethereum-data/spark-event-logs/")
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_4.1,"
            "net.snowflake:snowflake-jdbc:3.14.4",
        )
        .getOrCreate()
    )
