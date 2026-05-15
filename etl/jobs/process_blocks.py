from argparse import ArgumentParser
from etl.pipelines.blocks.extract import extract_blocks_data
from etl.pipelines.blocks.clean import clean_blocks_data
from etl.pipelines.blocks.validate import validate_blocks_data
from etl.pipelines.blocks.enrich import enrich_blocks_data
from etl.utils.load_data import load_into_snowflake, get_snowflake_options
from etl.utils.spark import get_spark_session
from etl.utils.logger import get_logger


logger = get_logger("jobs.process_blocks")


def main():
    parser = ArgumentParser()
    parser.add_argument("--s3_bucket", type=str, required=True)
    args = parser.parse_args()
    sf_options = get_snowflake_options()

    spark = get_spark_session("eth-blocks")
    extracted_data = extract_blocks_data(spark, args.s3_bucket)
    clean_data = clean_blocks_data(extracted_data)
    validate_blocks_data(clean_data)
    enriched_data = enrich_blocks_data(clean_data)
    load_into_snowflake(
        enriched_data,
        table="BLOCKS",
        snowflake_options=sf_options,
    )
    spark.stop()


if __name__ == "__main__":
    main()
