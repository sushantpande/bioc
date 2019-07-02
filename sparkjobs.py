class SparkJob(object):
    """
    This class have methods to submit spark jobs.
    """

    def __init__(self, master, app_name, class_path, class_args, jar_path, spark_conf):
        self.master = master
        self.app_name = app_name
        self.class_path = class_path
        self.class_args = class_args
        self.jar_path = jar_path
        self.spark_conf = spark_conf


    def get_spark_conf_str():
        spark_conf_str = "" 
        for k,v in self.spark_conf:
            spark_conf_str = " %s %s=%s %s" %("--conf", k, v, spark_conf_str)
        return (spark_conf_str)


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
