import os
# Imports the Google Cloud client library
from google.cloud import storage


# The name of the bucket
bucket_name = 'biodock-bucket'


class BucketGCP(object):
    '''This class holds method to communicate with GCP bucket'''

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        # Instantiates a client
        storage_client = storage.Client()
        self.bucket = storage_client.get_bucket(bucket_name)


    def upload(self, blob_name, path_to_file):
        # Get a blob object
        blob = self.bucket.blob(blob_name)
        # Upload file
        blob.upload_from_filename(path_to_file)
        return blob.public_url


    def download_all(self, source_blob_name, destination_folder, delimiter="/"):
        blobs = self.bucket.list_blobs(prefix=source_blob_name)
        for blob in blobs:
            if not delimiter or not blob.name.endswith(delimiter):
                print (blob.name)
                file_path = os.path.join(destination_folder, os.path.basename(blob.name))
                self.download(blob.name, file_path)


    def download_all_recursively(self, source_blob_name, destination_folder):
        self.download_all(source_blob_name, destination_folder, delimiter=None)


    def download(self, source_blob_name, destination_file_name=""):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(source_blob_name)
        print ("NAME-%s" %source_blob_name)
        print ("***************************")
        print (blob.exists())
        blob.download_to_filename(destination_file_name)
        print('Blob {} downloaded to {}.'.format(
                source_blob_name,
                destination_file_name))
        return (True)


    def exists(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return (blob.exists())


