import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

from transformation.spark.python.catalog_config import create_glue_spark_session
from transformation.spark.python.country_state_city import transform_country_data
from transformation.spark.python.write_to_catalog import write_to_catalog

load_dotenv()

raw_data_path = os.getenv("S3_LOCATION_RAW_PATH")
transformed_data_path = os.getenv("S3_LOCATION_TRANSFORMED_PATH")
catalog_name = "glue_catalog"
table_name = "countries"
database_name = "location_db"
s3_path = f"{transformed_data_path}/countries"


spark = create_glue_spark_session(
    transformed_data_path=transformed_data_path, app_name="CountryTransform"
)

raw_data_df: DataFrame = spark.read.option("multiLine", "true").json(raw_data_path)


# Transform the country data
transformed_df = transform_country_data(spark, raw_data_df)

# Write the transformed data to the Iceberg catalog
write_to_catalog(
    df=transformed_df,
    catalog_name=catalog_name,
    table_name=table_name,
    s3_path=s3_path,
    database_name=database_name,
)

spark.sql(
    f"SELECT * FROM {catalog_name}.{database_name}.{table_name} WHERE country_name = 'Nigeria'"
).show()

spark.stop()
