import os

from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession

load_dotenv()

raw_data_path = os.getenv("S3_LOCATION_RAW_PATH")
transformed_data_path = os.getenv("S3_LOCATION_TRANSFORMED_PATH")
table_name = "countries"


# Full url of the Nessie API endpoint to nessie
nessie_api_url_endpoint = "http://nessie:19120/api/v1"


# Nessie authentication type (NONE, BEARER, OAUTH2 or AWS)
auth_type = "NONE"


conf = SparkConf()


# Adding Iceberg and Nessie functionality
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
)


# Specify dependencies for Iceberge, Nessie and AWS Integration
conf.set(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.5.2,org.projectnessie:nessie-spark-3.2-extensions:0.95.0,org.apache.iceberg:iceberg-aws-bundle:1.5.2",
)

# Setup Nessie Catalog for Iceberg management
conf.set("spark.sql.catalog.nessie.uri", nessie_api_url_endpoint)
conf.set("spark.sql.catalog.nessie.ref", "main")
conf.set("spark.sql.catalog.nessie.authentication.type", auth_type)
conf.set(
    "spark.sql.catalog.nessie.catalog-impl",
    "org.apache.iceberg.nessie.NessieCatalog",
)
conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.nessie.warehouse", transformed_data_path)

spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()


# Very basic transformation testing spark + iceberg + nessie configuration
def transform_data(spark: SparkSession, raw_data_path: str):

    raw_data: DataFrame = spark.read.json(raw_data_path)
    transformed_df = raw_data.select(
        raw_data["id"].alias("country_id"),
        raw_data["name"].alias("country_name"),
        raw_data["latitude"].alias("country_latitude"),
        raw_data["longitude"].alias("country_longitude"),
    )
    return transformed_df


# Function to write data to Iceberg table in Parquet format
def write_to_iceberg(df: DataFrame):
    df.write.format("iceberg").mode("overwrite").save(
        f"nessie.development.{table_name}"
    )


if __name__ == "__main__":

    transformed_df = transform_data(spark, raw_data_path)

    write_to_iceberg(transformed_df, "countries")

    spark.sql(
        f"""SELECT * FROM nessie.development.{table_name} WHERE country_name = 'Nigeria' """
    ).show()

    spark.stop()
