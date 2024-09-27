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
    transformed_data_path: str, app_name: str, catalog_name: str = "glue_catalog"
) -> SparkSession:

    conf = SparkConf()

    # Specify dependencies for Iceberg and AWS Integration
    conf.set(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    )

    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Adding Iceberg functionality for Glue Catalog
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # Setup Glue Catalog for Iceberg management
    conf.set(
        f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
    )
    conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", transformed_data_path)
    conf.set(
        f"spark.sql.catalog.{catalog_name}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    conf.set(
        f"spark.sql.catalog.{catalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    )

    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.multipart.uploads.enabled", "true")

    # Create and return Spark session
    spark_session = (
        SparkSession.builder.config(conf=conf).appName(app_name).getOrCreate()
    )
    return spark_session
