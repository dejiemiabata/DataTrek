from pyspark.sql import DataFrame


def write_to_catalog(
    df: DataFrame,
    catalog_name: str,
    table_name: str,
    s3_path: str,
    database_name: str,
):
    df.writeTo(f"{catalog_name}.{database_name}.{table_name}").using("iceberg").option(
        "path", s3_path
    ).tableProperty("write.format.default", "parquet").createOrReplace()
