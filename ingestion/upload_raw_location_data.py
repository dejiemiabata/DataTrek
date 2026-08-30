import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage


UPLOAD_TARGETS = {
    "book-images": {
        "path_env": "PATH_TO_BOOK_IMAGES",
        "bucket": "book-images-2",
        "destination_prefix": "images",
    },
    "chess-gifs": {
        "path_env": "PATH_TO_CHESS_GIFS",
        "bucket": "chess-gifs",
        "destination_prefix": "images",
    },
    "travel-images": {
        "path_env": "PATH_TO_TRAVEL_IMAGES",
        "bucket": "travel-2",
        "destination_prefix": "",
    },
}

SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".gif"}


# Set up environment variables
def upload_to_gcs(bucket_name: str, file_path: str, destination_blob_name: str):
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


def make_blob_public(bucket_name, blob_name):
    """Makes a blob publicly accessible."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.make_public()

    print(f"Public URL: {blob.public_url}")
    return blob.public_url


def delete_bucket(bucket_name):
    """Deletes a bucket. The bucket must be empty."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()

    print(f"Bucket {bucket.name} deleted")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload local images to the configured Google Cloud Storage bucket."
    )
    parser.add_argument(
        "target",
        choices=[*UPLOAD_TARGETS, "all"],
        help="Which local directory and bucket to use, or 'all' to upload every configured target.",
    )
    parser.add_argument(
        "--prefix",
        help="Optional folder prefix for uploaded objects, such as 'yosemite'.",
    )
    return parser.parse_args()


def upload_directory(target_name: str, destination_prefix: str | None = None):
    target = UPLOAD_TARGETS[target_name]
    directory = os.getenv(target["path_env"])
    if not directory:
        raise ValueError(
            f"{target['path_env']} is not set. Set it to the local {target_name} directory."
        )

    root = Path(directory).expanduser()
    if not root.is_dir():
        raise ValueError(f"Directory does not exist: {root}")

    for file_path in sorted(root.rglob("*")):
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            print(f"{file_path.name} is not a valid image file, skipping.")
            continue

        relative_path = file_path.relative_to(root).as_posix()
        prefix = destination_prefix or target["destination_prefix"]
        destination_blob_name = relative_path
        if prefix:
            destination_blob_name = f"{prefix.strip('/')}/{relative_path}"

        upload_to_gcs(target["bucket"], str(file_path), destination_blob_name)
        public_url = make_blob_public(target["bucket"], destination_blob_name)
        print(f"Upload complete for {file_path.name}")
        print(f"Public URL for {file_path.name}: {public_url}")


if __name__ == "__main__":
    load_dotenv()
    args = parse_args()

    target_names = UPLOAD_TARGETS if args.target == "all" else (args.target,)
    for target_name in target_names:
        upload_directory(target_name, args.prefix)
