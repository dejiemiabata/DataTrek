import argparse
import logging
import os

import click
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

from transformation.spark.python.catalog_config import create_glue_spark_session
from transformation.spark.python.convert_json_to_parquet import convert_json_to_parquet
from transformation.spark.python.country_state_city_transform import (
    transform_city_data,
    transform_country_data,
    transform_state_and_city_data,
    transform_state_data,
)
from transformation.spark.python.write_to_catalog import write_to_catalog
from utils.datasets import Dataset

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

transformed_data_path = os.getenv("S3_LOCATION_TRANSFORMED_PATH")
spark_master_url = os.getenv("SPARK_MASTER_URL")

catalog_name = "glue_catalog"
database_name = "location_db"


spark = create_glue_spark_session(
    spark_master_url=spark_master_url,
    app_name="CountryStateCityTransform",
)


@click.command(name="Spark Job to Run ETL tasks.")
@click.option("--dataset", help="Dataset to process: country, state or city")
@click.option(
    "--preprocess_data_path",
    help="Path to the preprocessed Parquet data for each type of dataset",
)
@click.option("--population_data_path", help="Path to the population Parquet data")
def run_elt(dataset: str, preprocess_data_path: str, population_data_path: str):
    try:

        logger.info(
            f"Reading Parquet data from the preprocessed/intermediate path {preprocess_data_path}"
        )
        final_country_state_city_preprocessed_data_df: DataFrame = spark.read.parquet(
            preprocess_data_path
        )
        logger.info(f"Successfully read data in Parquet format {preprocess_data_path}")

        logger.info(
            f"Reading Parquet data from the preprocessed/intermediate path {population_data_path}"
        )
        final_country_population_preprocessed_data_df: DataFrame = spark.read.parquet(
            population_data_path
        )
        logger.info(
            f"Successfully read data in Parquet format from {population_data_path}"
        )

        if dataset == Dataset.COUNTRIES.value:
            logger.info("Starting data transformation...")

            transformed_df = transform_country_data(
                spark=spark,
                preprocess_data_df=final_country_state_city_preprocessed_data_df,
                population_data_df=final_country_population_preprocessed_data_df,
            )

            logger.info("Data transformation completed.")

            table_name = Dataset.COUNTRIES.value
            s3_path = f"{transformed_data_path}/countries"
            partition_cols = ["country_code"]

            logger.info(f"Writing {table_name} data to catalog...")

            write_to_catalog(
                df=transformed_df,
                catalog_name=catalog_name,
                table_name=table_name,
                s3_path=s3_path,
                database_name=database_name,
                partition_by=partition_cols,
            )
            logger.info(f"{table_name} data written successfully.")

        elif dataset == Dataset.STATES.value:
            transformed_df = transform_state_data(
                preprocess_data_df=final_country_state_city_preprocessed_data_df
            )
            table_name = Dataset.STATES.value
            s3_path = f"{transformed_data_path}/states"
            partition_cols = ["country_code", "state_code"]

            logger.info(f"Writing {table_name} data to catalog...")

            write_to_catalog(
                df=transformed_df,
                catalog_name=catalog_name,
                table_name=table_name,
                s3_path=s3_path,
                database_name=database_name,
                partition_by=partition_cols,
            )
            logger.info(f"{table_name} data written successfully.")

        elif dataset == Dataset.CITIES.value:
            transformed_df = transform_city_data(
                preprocess_data_df=final_country_state_city_preprocessed_data_df
            )
            table_name = Dataset.CITIES.value
            s3_path = f"{transformed_data_path}/cities"
            partition_cols = ["country_code", "state_code", "city_name"]

            logger.info(f"Writing {table_name} data to catalog...")

            write_to_catalog(
                df=transformed_df,
                catalog_name=catalog_name,
                table_name=table_name,
                s3_path=s3_path,
                database_name=database_name,
                partition_by=partition_cols,
            )
            logger.info(f"{table_name} data written successfully.")

        elif dataset == Dataset.STATES_CITIES.value:
            table_name = Dataset.STATES_CITIES.value

            logger.info(f"Starting {table_name} data transformation...")
            transformed_states_df, transformed_cities_df = (
                transform_state_and_city_data(
                    preprocess_data_df=final_country_state_city_preprocessed_data_df
                )
            )
            logger.info(f"{table_name} data transformation completed.")

            # Write states data to catalog
            logger.info(f"Writing states data to catalog...")
            write_to_catalog(
                df=transformed_states_df,
                catalog_name=catalog_name,
                table_name="states",
                s3_path=f"{transformed_data_path}/states",
                database_name=database_name,
                partition_by=["country_code", "state_code"],
            )
            logger.info(f"States data written successfully.")

            # Write cities data to catalog
            logger.info(f"Writing cities data to catalog...")
            write_to_catalog(
                df=transformed_cities_df,
                catalog_name=catalog_name,
                table_name="cities",
                s3_path=f"{transformed_data_path}/cities",
                database_name=database_name,
                partition_by=[
                    "country_code",
                    "state_code",
                ],
            )
            logger.info(f"Cities data written successfully.")

        else:
            logger.error(f"Invalid dataset specified: {dataset}")
            return

        logger.info(f"ELT job completed successfully for dataset: {dataset}")

        logger.info("Executing sample queries for validation...")

        if dataset == Dataset.COUNTRIES.value:

            spark.sql(
                f"SELECT * FROM {catalog_name}.{database_name}.countries WHERE country_name = 'Nigeria'"
            ).show()

        elif dataset == Dataset.STATES.value:
            spark.sql(
                f"SELECT * FROM {catalog_name}.{database_name}.states WHERE state_name = 'Georgia'"
            ).show()

        elif dataset == Dataset.CITIES.value:

            spark.sql(
                f"SELECT * FROM {catalog_name}.{database_name}.cities LIMIT 10"
            ).show()

        elif dataset == Dataset.STATES_CITIES.value:

            spark.sql(
                f"SELECT * FROM {catalog_name}.{database_name}.states WHERE state_name = 'Georgia'"
            ).show()

            spark.sql(
                f"SELECT * FROM {catalog_name}.{database_name}.cities LIMIT 10"
            ).show()

        else:
            return

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":

    run_elt()
