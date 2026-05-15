from argparse import ArgumentParser
from etl.pipelines.contracts.extract import extract_contracts_data
from etl.pipelines.contracts.clean import clean_contracts_data
from etl.pipelines.contracts.validate import validate_contracts_data
from etl.utils.load_data import load_into_snowflake, get_snowflake_options
from etl.utils.spark import get_spark_session
from etl.utils.logger import get_logger


logger = get_logger("jobs.process_contracts")


def main():
    parser = ArgumentParser()
    parser.add_argument("--s3_bucket", type=str, required=True)
    args = parser.parse_args()
    sf_options = get_snowflake_options()

    spark = get_spark_session("eth-contracts")
    extracted_data = extract_contracts_data(spark, args.s3_bucket)
    clean_data = clean_contracts_data(extracted_data)
    validate_contracts_data(clean_data)
    load_into_snowflake(
        clean_data,
        table="CONTRACTS",
        snowflake_options=sf_options,
    )
    spark.stop()


if __name__ == "__main__":
    main()
