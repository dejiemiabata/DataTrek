import logging
import os

import click
from pyspark.sql import SparkSession

from transformation.spark.python.catalog_config import create_glue_spark_session
from transformation.spark.python.convert_json_to_parquet import convert_json_to_parquet

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

spark_master_url = os.getenv("SPARK_MASTER_URL")


spark = create_glue_spark_session(
    spark_master_url=spark_master_url,
    app_name="CountryStateCityTransform",
)


@click.command(name="Spark Job to run json convertion to parquet.")
@click.option("--input_path", help="Input JSON file path")
@click.option("--output_path", help="Output Parquet file path")
def run_convert_json_to_parquet(input_path: str, output_path: str):
    try:
        convert_json_to_parquet(
            spark=spark,
            raw_data_path=input_path,
            preprocess_data_path=output_path,
        )
    except Exception as e:
        logging.error(f"An error occurred while converting json to parquet: {str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_convert_json_to_parquet()
