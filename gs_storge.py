# Imports the Google Cloud client library
from google.cloud import storage


# The name of the bucket
bucket_name = 'biodock-bucket'

def upload_to_bucket(blob_name, path_to_file, bucket_name):

    # Instantiates a client
    storage_client = storage.Client.from_service_account_json('creds.json')
    # Get bukcet object
    bucket = storage_client.get_bucket(bucket_name)
    # Get a blob object
    blob = bucket.blob(blob_name)
    # Upload file
    blob.upload_from_filename(path_to_file)

    return blob.public_url


def download_from_bucket(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client.from_service_account_json('creds.json')
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))

upload_to_bucket('cred', 'creds.json', bucket_name)
