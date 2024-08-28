import os

from dotenv import load_dotenv
from google.cloud import storage


def upload_to_gcs(bucket_name: str, file_path: str, destinaton_blob_name: str):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()

    # Check if the bucket exists, create it if it does not
    bucket = storage_client.lookup_bucket(bucket_name)
    if bucket is None:
        print(f"Bucket {bucket_name} does not exist. Creating bucket...")
        bucket = storage_client.create_bucket(
            bucket_name,
        )
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    print(
        f"File {file_path} uploaded to {destination_blob_name} in bucket {bucket_name}."
    )


if __name__ == "__main__":
    load_dotenv()
    file_path = os.getenv("COUNTRY_DATA_FILE_PATH")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    destination_blob_name = os.getenv("LOCATION_GCS_BUCKET_PATH")

    upload_to_gcs(bucket_name, file_path, destination_blob_name)
