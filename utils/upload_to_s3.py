import boto3
from botocore.exceptions import ClientError


def upload_to_s3(
    bucket_name: str,
    file_path: str,
    destination_blob_name: str,
):
    """Uploads a file to the S3 bucket."""

    # initialize s3 service with specified profile
    session = boto3.Session()
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"Bucket {bucket_name} does not exist. Creating bucket...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created.")
        else:
            raise e

    s3_resource.Bucket(bucket_name).upload_file(file_path, destination_blob_name)
