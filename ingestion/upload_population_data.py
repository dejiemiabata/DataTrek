import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from utils.s3_manager import S3Manager


if __name__ == "__main__":
    load_dotenv()
    s3_manager = S3Manager()
    file_path = os.getenv("COUNTRY_POPULATION_DATA_FILE_PATH")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    destination_blob_name = os.getenv("COUNTRY_POPULATION_LOCATION_S3_BUCKET_PATH")

    s3_manager.upload_file(bucket_name, file_path, destination_blob_name)
