import SparkJob

class SparkOnGKEJob(SparkJob):
    """
    This class have methods to submit spark jobs on GKE cluster
    """

    def __init__(self, master, app_name, class_path, class_args, jar_path, spark_conf, spark_gke_conf):
        self.spark_gke_conf = spark_gke_conf
        super().__init__(master, app_name, class_path, class_args, jar_path, spark_conf)


    def get_spark_conf_str():
        spark_gke_conf_str = "" 
        for k,v in self.spark_gke_conf:
            spark_gke_conf_str = " %s %s=%s %s" %("--conf", k, v, spark_gke_conf_str)
        return ("%s %s" %(super().get_spark_conf_str(), spark_gke_conf_str))


    def submit_job(self):
        spark_submit_cmd = self.get_spark_submit_cmd()
        spark_conf_args = self.get_spark_conf_str()
        cmd = [spark_submit_cmd,
                "--master",\
                self.master,\ 
                spark_conf_args,\ 
                "--class",\ 
                self.class_path,\
                self.jar_path,\
                self.class_args]
