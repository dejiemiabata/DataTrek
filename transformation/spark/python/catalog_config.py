import os

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession


# Creates and returns a SparkSession configured with Nessie (Catalog) and Iceberg (table format) for metadata management.
def create_nessie_spark_session(
    transformed_data_path: str, app_name: str
) -> SparkSession:

    nessie_api_url_endpoint = "http://nessie:19120/api/v1"
    auth_type = "NONE"

    conf = SparkConf()

    # Specify dependencies for Iceberg, Nessie, and AWS Integration
    conf.set(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.98.0,software.amazon.awssdk:bundle:2.20.131",
    )

    # Ensure Python <-> Java interactions are w/ pyarrow
    conf.set("spark.sql.execution.pyarrow.enabled", "true")

    # Adding Iceberg and Nessie functionality
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )

    # Setup Nessie Catalog for Iceberg management
    conf.set("spark.sql.catalog.nessie.uri", nessie_api_url_endpoint)
    conf.set("spark.sql.catalog.nessie.ref", "main")
    conf.set("spark.sql.catalog.nessie.authentication.type", auth_type)
    conf.set(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    conf.set("spark.sql.catalog.nessie.type", "nessie")
    conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.nessie.warehouse", transformed_data_path)

    # Create and return Spark session
    spark_session = (
        SparkSession.builder.config(conf=conf).appName(app_name).getOrCreate()
    )
    return spark_session


# Creates and returns a SparkSession configured with AWS Glue (metastore) and Iceberg (table format) for metadata management.
def create_glue_spark_session(
    app_name: str,
    spark_master_url: str,
) -> SparkSession:

    # Create and return Spark session
    spark_session = (
        SparkSession.builder.appName(app_name).master(spark_master_url).getOrCreate()
    )
    return spark_session
