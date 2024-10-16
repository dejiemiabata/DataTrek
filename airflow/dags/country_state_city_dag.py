import os

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from default_args_dag import default_args
from default_spark_conf import get_default_spark_conf, get_glue_catalog_conf
from pendulum import datetime, duration

from airflow import DAG
from transformation.utils.datasets import Dataset

transformed_data_path = os.getenv("S3_LOCATION_TRANSFORMED_PATH")
raw_data_path = os.getenv("S3_LOCATION_RAW_PATH")
spark_master_url = os.getenv("SPARK_MASTER_URL")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
spark_event_log_dir = os.getenv("SPARK_EVENT_LOG_DIR")

default_spark_conf = get_default_spark_conf(
    event_log_dir=spark_event_log_dir,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)
glue_catalog_conf = get_glue_catalog_conf()
catalog_name = "glue_catalog"

base_job_specific_conf = {
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    f"spark.sql.catalog.{catalog_name}.warehouse": transformed_data_path,
    "spark.jars.excludes": "org.antlr:antlr-runtime",  # exclude old incompatible antlr version
}

state_city_job_specific_conf = {
    "spark.executor.cores": "2",
    "spark.executor.memory": "8g",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "4g",
    "spark.driver.memoryOverhead": "1g",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    f"spark.sql.catalog.{catalog_name}.warehouse": transformed_data_path,
    "spark.jars.excludes": "org.antlr:antlr-runtime",
}


base_spark_job_conf = {
    **default_spark_conf,
    **glue_catalog_conf,
    **base_job_specific_conf,
}
state_city_spark_job_conf = {
    **default_spark_conf,
    **glue_catalog_conf,
    **state_city_job_specific_conf,
}


with DAG(
    "country_state_city_transform_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    json_to_parquet_task = SparkSubmitOperator(
        application="transformation/spark/jobs/convert_json_to_parquet_job.py",
        conn_id="spark_default",
        task_id="convert_json_to_parquet",
        name="convert_json_to_parquet",
        conf=base_spark_job_conf,
        trigger_rule="all_done",
    )

    country_transform_task = SparkSubmitOperator(
        application="transformation/spark/jobs/country_state_city_transform_job.py",
        conn_id="spark_default",
        task_id=f"transform_countries",
        name=f"transform_countries",
        application_args=["--dataset", Dataset.COUNTRIES.value],
        conf=base_spark_job_conf,
        trigger_rule="all_done",
    )

    state_transform_task = SparkSubmitOperator(
        application="transformation/spark/jobs/country_state_city_transform_job.py",
        conn_id="spark_default",
        task_id=f"transform_states_cities",
        name=f"transform_states_cities",
        application_args=["--dataset", Dataset.STATES_CITIES.value],
        conf=state_city_spark_job_conf,
        trigger_rule="all_done",
    )

    (json_to_parquet_task >> country_transform_task >> state_transform_task)
