from argparse import ArgumentParser
from etl.pipelines.transactions.extract import extract_transactions_data
from etl.pipelines.transactions.clean import clean_transactions_data
from etl.pipelines.transactions.validate import validate_transactions_data
from etl.utils.load_data import load_into_snowflake, get_snowflake_options
from etl.utils.spark import get_spark_session
from etl.utils.logger import get_logger


logger = get_logger("jobs.process_transactions")


def main():
    parser = ArgumentParser()
    parser.add_argument("--s3_bucket", type=str, required=True)
    args = parser.parse_args()
    sf_options = get_snowflake_options()

    spark = get_spark_session("eth-transactions")
    extracted_data = extract_transactions_data(spark, args.s3_bucket)
    clean_data = clean_transactions_data(extracted_data)
    validate_transactions_data(clean_data)
    load_into_snowflake(
        clean_data,
        table="TRANSACTIONS",
        snowflake_options=sf_options,
    )
    spark.stop()


if __name__ == "__main__":
    main()
