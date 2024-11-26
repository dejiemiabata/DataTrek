import os

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, explode, explode_outer


def transform_country_data(
    spark: SparkSession, preprocess_data_df: DataFrame, population_data_df: DataFrame
) -> DataFrame:

    preprocess_data_df.createOrReplaceTempView("raw_country_data")
    population_data_df.createOrReplaceTempView("raw_country_population_data")
    source_timestamp: str = (
        "2020-08-23 16:26:00"  # timestamp recorded from https://github.com/kwzrd/pypopulation/blob/main/pypopulation/resources/countries.json
    )
    sql = f"""

    SELECT raw_country_data.id as country_id
        , raw_country_data.name as country_name
        , raw_country_data.iso3 as country_code
        , raw_country_data.capital
        , raw_country_data.currency
        , raw_country_data.currency_name
        , raw_country_data.currency_symbol
        , raw_country_data.region
        , raw_country_data.subregion
        , raw_country_data.phone_code
        , raw_country_data.emoji
        , CAST(raw_country_data.latitude AS float) as latitude
        , CAST(raw_country_data.longitude AS float) as longitude
        , raw_country_pop_data.Population as population
        , to_timestamp('{source_timestamp}') AS population_source_last_updated_at
        , current_timestamp() as created_at
        , current_timestamp() as last_updated_at

        FROM raw_country_data 
        LEFT JOIN raw_country_population_data raw_country_pop_data ON raw_country_data.iso3 = raw_country_pop_data.Alpha_3


    """
    transformed_df = spark.sql(sql)
    return transformed_df


def transform_state_data(preprocess_data_df: DataFrame):
    from pyspark.sql.functions import col, current_timestamp, explode

    # Explode states data
    exploded_states_df = preprocess_data_df.select(
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


def transform_city_data(preprocess_data_df: DataFrame):

    # Explode states
    exploded_states_df = preprocess_data_df.select(
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


def transform_state_and_city_data(preprocess_data_df: DataFrame):

    # Explode states
    exploded_states_df = preprocess_data_df.select(
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
