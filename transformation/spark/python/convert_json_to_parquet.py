import logging

from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def convert_json_to_parquet(
    spark: SparkSession, raw_data_path: str, preprocess_data_path: str
):

    raw_data_df = spark.read.option("multiLine", "true").json(raw_data_path)

    raw_data_df.write.mode("overwrite").parquet(preprocess_data_path)

    logger.info(
        f"Converted raw JSON data from {raw_data_path} to Parquet at {preprocess_data_path}"
    )
