import fnmatch
import pika
from google.cloud import storage
import conn


routing_key = "run1_task1"
bucket_name = "biodock-bucket"
delimiter = "/"
source_blob_name = "bwaoutput"
ext = "bam"


connection = conn.get_amqp_conn()
print (connection)
channel = connection.channel()
print (channel)


# Instantiate a GS client
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix=source_blob_name)
for blob in blobs:
    if not delimiter or not blob.name.endswith(delimiter):
        filter = "*.%s" %(ext)
        if filter and fnmatch.fnmatch(blob.name, filter):
            print (blob.name)
            body = "gs://%s/%s" %(bucket_name, blob.name)
            conn.publish_to_amqp(channel, routing_key=routing_key, body=body)
