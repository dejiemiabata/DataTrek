import os

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, explode, explode_outer


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


def transform_state_data(raw_data_df: DataFrame):
    from pyspark.sql.functions import col, current_timestamp, explode

    # Explode states data
    exploded_states_df = raw_data_df.select(
        col("id").alias("country_id"),
        col("iso3").alias("country_code"),
        explode("states").alias("state"),
    )

    transformed_states_df = exploded_states_df.select(
        "country_id",
        "country_code",
        col("state.id").alias("state_id"),
        col("state.name").alias("state_name"),
        col("state.state_code").alias("state_code"),
        col("state.latitude").cast("float").alias("latitude"),
        col("state.longitude").cast("float").alias("longitude"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("last_updated_at"),
    )

    return transformed_states_df


def transform_city_data(raw_data_df: DataFrame):

    # Explode states
    exploded_states_df = raw_data_df.select(
        col("id").alias("country_id"),
        col("iso3").alias("country_code"),
        explode("states").alias("state"),
    )

    # Explode cities within states
    exploded_cities_df = exploded_states_df.select(
        "country_id",
        "country_code",
        col("state.id").alias("state_id"),
        col("state.name").alias("state_name"),
        col("state.state_code").alias("state_code"),
        explode("state.cities").alias("city"),
    )

    transformed_cities_df = exploded_cities_df.select(
        "country_id",
        "country_code",
        "state_id",
        "state_name",
        "state_code",
        col("city.id").alias("city_id"),
        col("city.name").alias("city_name"),
        col("city.latitude").cast("float").alias("latitude"),
        col("city.longitude").cast("float").alias("longitude"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("last_updated_at"),
    )

    return transformed_cities_df


def transform_state_and_city_data(raw_data_df: DataFrame):

    # Explode states
    exploded_states_df = raw_data_df.select(
        col("id").alias("country_id"),
        col("iso3").alias("country_code"),
        explode_outer("states").alias("state"),
    )

    transformed_states_df = exploded_states_df.select(
        "country_id",
        "country_code",
        col("state.id").alias("state_id"),
        col("state.name").alias("state_name"),
        col("state.state_code").alias("state_code"),
        col("state.latitude").cast("float").alias("latitude"),
        col("state.longitude").cast("float").alias("longitude"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("last_updated_at"),
    )

    # Explode cities within states
    exploded_cities_df = exploded_states_df.select(
        "country_id",
        "country_code",
        col("state.id").alias("state_id"),
        col("state.name").alias("state_name"),
        col("state.state_code").alias("state_code"),
        explode_outer("state.cities").alias("city"),
    )

    # Transform cities data
    transformed_cities_df = exploded_cities_df.select(
        "country_id",
        "country_code",
        "state_id",
        "state_name",
        "state_code",
        col("city.id").alias("city_id"),
        col("city.name").alias("city_name"),
        col("city.latitude").cast("float").alias("latitude"),
        col("city.longitude").cast("float").alias("longitude"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("last_updated_at"),
    )

    return transformed_states_df, transformed_cities_df
