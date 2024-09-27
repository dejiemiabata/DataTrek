import os

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession


def transform_country_data(spark: SparkSession, raw_data_df: DataFrame):

    raw_data_df.createOrReplaceTempView("raw_country_data")

    sql = """

    SELECT id as country_id
        , name as country_name
        , iso3 as country_code
        , capital
        , currency
        , currency_name
        , currency_symbol
        , region
        , subregion
        , phone_code
        , emoji
        , CAST(latitude AS float) as latitude
        , CAST(longitude AS float) as longitude
        , current_timestamp() as created_at
        , current_timestamp() as last_updated_at

        from raw_country_data

    """
    transformed_df = spark.sql(sql)
    return transformed_df
