from sparkjob import SparkJob

class SparkJobOnGKE(SparkJob):
    """
    This class have methods to submit spark jobs on GKE cluster
    """

    def __init__(self, master, app_name, class_path, class_args, jar_path, container_image, spark_conf):
        SparkJob.__init__(self, master, app_name, class_path, class_args, jar_path, container_image, spark_conf)

