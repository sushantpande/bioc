from sparkjob_gke import SparkJobOnGKE
from k8sjobmonitor import K8sJobMonitor
from k8spodmonitor import K8sPodMonitor
import utils

master = "k8s://https://35.202.169.42:443"
host = "https://35.202.169.42:443"
app_name = "bwa"
container_image = "docker.io/sushantpande/spark-gcp-con-py:orig9"
class_path = "com.github.sparkbwa.SparkBWA"
class_args = "-m -r -s --index gs://biodock-bucket/scaffolds -n 3   gs://biodock-bucket/evolved-6-R1.fastq gs://biodock-bucket/bwaoutputk8"
jar_path = "gs://biodock-bucket/SparkBWA-0.2.jar"
spark_conf = utils.load_spark_conf('properties.bkp') 

job = SparkJobOnGKE(master, app_name, class_path, class_args, jar_path, container_image, spark_conf)
job.submit_job()
namespace = job.get_k8s_namespace()
print (namespace)
monitor = K8sPodMonitor(host, app_name, namespace)
monitor.start()
