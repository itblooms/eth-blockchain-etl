from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "s3a://ethereum-data/spark-event-logs/")
        .getOrCreate()
    )
