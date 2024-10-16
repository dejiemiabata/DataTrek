from pyspark.sql import DataFrame, DataFrameWriterV2


def write_to_catalog(
    df: DataFrame,
    catalog_name: str,
    table_name: str,
    s3_path: str,
    database_name: str,
    partition_by: list = None,
):
    catalog_writer: DataFrameWriterV2 = (
        df.writeTo(f"{catalog_name}.{database_name}.{table_name}")
        .using("iceberg")
        .option("path", s3_path)
        .tableProperty("write.format.default", "parquet")
    )

    if partition_by and len(partition_by) > 0:
        catalog_writer = catalog_writer.partitionedBy(*partition_by)

    catalog_writer.createOrReplace()
