from sparkjob_gke import SparkJobOnGKE
from k8sjobmonitor import K8sJobMonitor
from k8spodmonitor import K8sPodMonitor
from k8sjob import K8sJob
import utils

master = "k8s://https://35.202.169.42:443"
host = "https://35.202.169.42:443"
app_name = "bwa"
container_image = "docker.io/sushantpande/spark-gcp-con-py:orig9"
class_path = "com.github.sparkbwa.SparkBWA"
class_args = "-m -r -s --index gs://biodock-bucket/scaffolds -n 3   gs://biodock-bucket/evolved-6-R1.fastq gs://biodock-bucket/bwaoutputk8"
jar_path = "gs://biodock-bucket/SparkBWA-0.2.jar"
spark_conf = utils.load_spark_conf('properties.bkp') 

#job = SparkJobOnGKE(master, app_name, class_path, class_args, jar_path, container_image, spark_conf)
#job.submit_job()
#namespace = job.get_k8s_namespace()
#print (namespace)
#monitor = K8sPodMonitor(host, app_name, namespace)
#monitor.start()

app_name = "gatk"
container_image = "docker.io/sushantpande/gatk:efs"
container_name = "somecontainer"
env_vars = {}
parallelism = 1
cmd = "ls -la"
image_pull_policy = "Always"
namespace = "default"
job1 = K8sJob(host, app_name, container_image, container_name, namespace, env_vars, parallelism, cmd, image_pull_policy) 
job1.kube_create_job_object()
job1.kube_run_job_object()
monitor1 = K8sJobMonitor(host, app_name, namespace)
monitor1.monitor()
