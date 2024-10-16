def get_default_spark_conf(
    event_log_dir: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str = "us-east-1",
):

    return {
        # Specify dependencies for Iceberg and AWS Integration
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.6.1,org.antlr:antlr4-runtime:4.9.3,org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        # Adding Iceberg functionality for Glue Catalog
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.adaptive.enabled": "true",
        "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key,
        # "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.multipart.uploads.enabled": "true",
        "spark.hadoop.fs.s3a.endpoint": f"s3.{aws_region}.amazonaws.com",
        "spark.hadoop.fs.s3a.endpoint.region": aws_region,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": event_log_dir,
    }


def get_glue_catalog_conf():

    # Setup Glue Catalog for Iceberg management
    return {
        f"spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        f"spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }
