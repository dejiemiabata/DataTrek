from airflow.utils.dates import days_ago
from pendulum import datetime, duration

default_args = {
    "owner": "DataTrek",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": duration(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(hours=2),
}
