from gs_storage import *

bucket_name = "biodock-bucket"

bucket = BucketGCP(bucket_name)
bucket.download("bwaoutput/FullOutput_Sort.bam" , "test")
