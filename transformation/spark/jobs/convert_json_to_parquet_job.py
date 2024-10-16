import logging
import os

from pyspark.sql import SparkSession

from transformation.spark.python.catalog_config import create_glue_spark_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

spark_master_url = os.getenv("SPARK_MASTER_URL")


raw_data_path = os.getenv("S3_LOCATION_RAW_PATH")
preprocess_data_path = os.getenv("S3_LOCATION_PREPROCESSED_PATH")

spark = create_glue_spark_session(
    spark_master_url=spark_master_url,
    app_name="CountryStateCityTransform",
)


def convert_json_to_parquet():

    raw_data_df = spark.read.option("multiLine", "true").json(raw_data_path)
    raw_data_df.write.mode("overwrite").parquet(preprocess_data_path)
    logger.info(
        f"Converted raw JSON data from {raw_data_path} to Parquet at {preprocess_data_path}"
    )
    spark.stop()


if __name__ == "__main__":
    convert_json_to_parquet()
