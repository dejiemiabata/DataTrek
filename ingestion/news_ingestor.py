import json
import logging
import os
import random
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, Optional

import dlt
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.pendulum import pendulum
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
from dotenv import load_dotenv
from requests import Response

from ingestion.news_api_config import news_api_config
from utils.country_codes import COUNTRY_CODES
from utils.news_topics import NEWS_TOPICS


class NewsIngestor:
    """A class to handle news data ingestion from the News Catcher API."""

    def __init__(self, api_name: str, query: str, default_lang: str = "en"):
        self.base_url = news_api_config[api_name]["api_base_url"]
        self.api_key = news_api_config[api_name]["api_key"]
        self.query = query
        self.default_lang = default_lang

        # Initialize logger
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=logging.DEBUG,
        )
        self.current_timestamp = pendulum.now("UTC").to_iso8601_string()
        self.custom_current_timestamp = pendulum.now("UTC").format("YYYY/MM/DD")

    def build_request_params(self) -> Dict[str, str]:
        """Dynamically build request parameters for the news catcher API."""
        # selected_countries = ",".join(
        #     random.sample(COUNTRY_CODES, 2)
        # )

        selected_country = random.choice(COUNTRY_CODES)

        # TODO ensure there's news from all countries
        selected_topic = random.choice(
            NEWS_TOPICS
        )  # TODO ensure there's news from each country per topic

        query = f'"{self.query}"'

        # add url-encoding to query, handling special characters
        encoded_query = urllib.parse.quote(query)

        self.logger.info(
            f"Parameters built: query='{query}', countries='{selected_country}', topic='{selected_topic}', default_lang = '{self.default_lang}'"
        )
        params = {
            "q": query,
            "lang": self.default_lang,
            "countries": selected_country,
            "topic": selected_topic,
            "sort_by": "rank",
            "page_size": 100,
            "page": 1,
        }
        query_string = urllib.parse.urlencode(params)
        full_url = f"{self.base_url}search?{query_string}"
        self.logger.info(f"Full API request URL: {full_url}")
        return {
            "params": params,
            "full_url": full_url,
        }

    def add_call_timestamp_to_newscatcher_api(
        self, response: Response, *args, **kwargs
    ) -> Response:
        """Add a call_timestamp field to each article in the response, enabling us track when the API request for a particular response was made."""

        self.logger.info("Adding call timestamp to the response.")

        # Check if the response content is empty or not
        if not response.content:
            self.logger.error("No response content found")
            return response

        try:
            payload: dict = response.json()
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON from response: {e}")
            return response
        else:

            # Ensure the payload contains the "articles" key
            if "articles" not in payload:
                self.logger.warning("No 'articles' key found in the response payload.")
                return response

            # finally, add the call timestamp
            call_timestamp = self.current_timestamp
            for record in payload.get("articles", []):
                record["call_timestamp"] = call_timestamp

            self.logger.info(f"Call timestamp {call_timestamp} added to the response.")

            modified_content: bytes = json.dumps(payload).encode("utf-8")
            response._content = modified_content

            return response

    def build_s3_path(
        self,
        country: str,
        topic: str,
        api_name: str = "newscatcher",
    ) -> str:
        """Build an S3 path for storing news data fetched from the News Catcher API."""

        current_date = self.custom_current_timestamp
        s3_path = f"{api_name}/{current_date}/{country}/{topic}/news_data.parquet"
        self.logger.info(f"Generated S3 path: {s3_path}")
        return s3_path


# TODO handle scenario where results don't get fetched and maybe don't load or parse to add call_timestamp ?

# TODO distinguish between "status": "ok" & "status": "No matches for your search." when parsing records of news articles


@dlt.resource(write_disposition="append", file_format="parquet", name="newscatcher")
def get_newscatcher_news(
    params: str, query: str, base_url: str, api_key: str
) -> Iterator[dict]:
    """Fetch news data from the News Catcher API."""

    print(f"Starting news ingestion for query: '{query}'")

    query_string = urllib.parse.urlencode(params)
    custom_path = f"search?{query_string}"

    yield from rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "auth": APIKeyAuth(
                    name="X-API-Key", api_key=api_key, location="header"
                ),
            },
            "resources": [
                {
                    "name": "news_articles",
                    "endpoint": {"path": custom_path},
                }
            ],
        }
    )


@dlt.source(name="newscatcher")
def ingest_newscatcher_news(
    params: Dict[str, str], query: str, base_url: str, api_key: str
) -> Any:
    """Ingest news data from the News Catcher API."""
    news_api.logger.info(f"Starting to ingest news data with parameters: {params}")

    # Yield each resource
    yield get_newscatcher_news(
        params=params,
        query=query,
        base_url=base_url,
        api_key=api_key,
    )


def load_newscatcher_news_data(
    query: str,
    base_url: str,
    params: Dict[str, str],
    api_key: str,
) -> None:
    """Load news data from the News Catcher API to an S3 bucket."""

    current_date = pendulum.now("UTC").format("YYYY/MM/DD")

    pipeline = dlt.pipeline(
        pipeline_name="newscatcher_data_pipeline",
        destination=dlt.destinations.filesystem(
            credentials=AwsCredentials(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            ),
            layout="{table_name}/{date}/{country}/{topic}/{load_id}.{file_id}.{ext}",  # s3 bucket file path
            extra_placeholders={
                "date": current_date,
                "country": country,
                "topic": topic,
            },
        ),
        # destination="filesystem",
        dataset_name="news",
    )

    # Run dlt pipeline
    load_info = pipeline.run(
        ingest_newscatcher_news(
            query=query,
            base_url=base_url,
            params=params,
            api_key=api_key,
        ),
    )

    # Log dlt pipeline info
    print(load_info)


if __name__ == "__main__":

    load_dotenv()
    s3_base_path = os.getenv("S3_NEWS_BASE_RAW_PATH", "")

    news_api = NewsIngestor(api_name="newscatcher", query="Random")
    query = news_api.query
    base_url = news_api.base_url
    api_key = news_api.api_key
    params_obj = news_api.build_request_params()

    params: Dict[str, str] = params_obj.get("params")
    country = params.get("countries")
    topic = params.get("topic")

    load_newscatcher_news_data(
        query=news_api.query,
        base_url=base_url,
        params=params,
        api_key=api_key,
    )
