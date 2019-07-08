import os
from utils import exec_cmd
from sparkconf import *


class SparkJob(object):
    """
    This class have methods to submit spark jobs.
    """

    def __init__(self, master, app_name, class_path, class_args, jar_path, container_image, spark_conf):
        self.master = master
        self.app_name = app_name
        self.class_path = class_path
        self.class_args = class_args
        self.jar_path = jar_path
        self.container_image = container_image
        self.spark_conf = spark_conf
        self.submit_cmd = None
        self.properties_file = None

    
    def get_k8s_namespace(self):
        return (self.spark_conf.get(SPARK_KUBERNETES_NAMESPACE, DEFAULT_NAMESPACE))


    def write_spark_properties_file(self):
        file = "%s.spark.propertes" %(self.app_name)
        with open(file, 'w+') as fh:
            for k,v in self.spark_conf.items():
                property = "%s %s\n" %(k, v)
                fh.write(property)

        self.properties_file = file


    def get_spark_properties_file(self):
        self.write_properties_file()
        return (self.properties_file)
        

    def get_spark_conf_str(self):
        spark_conf_str = "" 
        for k,v in self.spark_conf.items():
            spark_conf_str = " %s %s=%s %s" %("--conf", k, v, spark_conf_str)
        return (spark_conf_str)


    def set_spark_submit_cmd(self):
        sparkhome = os.environ.get("SPARK_HOME")
        sparkhome = "/home/snappyflow2018/spark-2.4.3-bin-hadoop2.7"
        if sparkhome is not None:
            self.submit_cmd = sparkhome +"/bin/spark-submit"
        else:
            self.submit_cmd = "spark-submit"


    def submit_job(self):
        self.set_spark_submit_cmd()
        self.write_spark_properties_file()
        cmd_str = "%s --master %s --properties-file %s --conf spark.kubernetes.container.image=%s --class %s %s %s &" %(self.submit_cmd, self.master, self.properties_file, self.container_image ,self.class_path, self.jar_path, self.class_args)
        print (cmd_str)
        cmd = [cmd_str]
        print(exec_cmd(cmd))
        return (cmd)
