import os

from dotenv import load_dotenv
from google.cloud import storage


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


if __name__ == "__main__":
    load_dotenv()
    file_path = os.getenv("PATH_TO_BOOK_IMAGES")
    bucket_name = "book-images-2"

    for file in os.listdir(file_path):
        file_full_path = os.path.join(file_path, file)  # Obtain full path

        if os.path.isfile(file_full_path):  # Ignore directories found in file_path
            if file.endswith(".jpg") or file.endswith(".jpeg"):
                file_name = file
                destination_blob_name = f"images/{file_name}"

                upload_to_gcs(bucket_name, file_full_path, destination_blob_name)
                public_url = make_blob_public(
                    bucket_name, destination_blob_name
                )  # Make public and return url
                print(f"Upload complete for {file_name}")
                print(f"Public URL for {file_name}: {public_url}")

            else:
                print(f"{file} is not a valid image file.")
        else:
            print(f"{file} is a directory, skipping.")
