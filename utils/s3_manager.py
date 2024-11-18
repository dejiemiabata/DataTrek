import logging
from io import BytesIO, StringIO
from typing import IO, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.s3_client = self.session.client("s3")
        self.s3_resource = self.session.resource("s3")

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} exists.")
            return True
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info(f"Bucket {bucket_name} does not exist.")
                return False
            else:
                logger.error(f"Error checking if bucket exists: {e}")
                raise

    def create_bucket(
        self,
        bucket_name: str,
    ) -> None:
        try:

            self.s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} successfully created in region.")
        except ClientError as e:
            logger.error(f"Error creating bucket: {e}")
            raise

    # Upload file with provided file path
    def upload_file(self, bucket_name: str, file_path: str, s3_key: str) -> None:
        try:
            self.s3_client.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"File {file_path} uploaded to s3://{bucket_name}/{s3_key}.")
            # print(f"File {file_path} uploaded to s3://{bucket_name}/{s3_key}.")
        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            raise

    # Upload file obj
    def upload_fileobj(
        self, bucket_name: str, file_obj: StringIO | BytesIO, s3_key: str
    ) -> None:
        try:
            self.s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
            logger.info(f"File object uploaded to s3://{bucket_name}/{s3_key}.")
        except ClientError as e:
            logger.error(f"Error uploading file object: {e}")
            raise
    
    # Get object from s3 Bucket
    def get_object(self, bucket_name: str, s3_key: str, iterator=False) -> bytes:
        """
        
        :param iterator: allows for iterating over results, more-efficient, memory-wise. prevents loading entire file into memory

        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            data = (
                response["Body"].iter_lines() if iterator else response["Body"].read()
            )
        except ClientError as e:
            logger.error(f"Error getting object: {e}")
            raise
        else: 
            logger.info(f"Object s3://{bucket_name}/{s3_key} retrieved successfully.")
            return data

    # Create S3 Bucket if it doesn't exist
    def ensure_bucket_exists(
        self, bucket_name: str, region: Optional[str] = None
    ) -> None:
        if not self.bucket_exists(bucket_name):
            logger.info(f"Bucket {bucket_name} does not exist. Creating bucket...")
            self.create_bucket(bucket_name, region=region)
