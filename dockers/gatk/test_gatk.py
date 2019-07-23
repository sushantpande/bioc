from k8sjob import K8sJob

host = "https://35.224.174.225:443"
app_name = "gatk"
container_image = "docker.io/sushantpande/gatk:test"
parallelism = 1
#cmd = "python /tmp/gatk.py"
cmd = "sleep 900"
image_pull_policy = "Always"
namespace = "default"
container_name = "gatk-container"
env_vars = {}
env_vars.update({'AMQP_SERVER': "34.67.117.117"}) 
env_vars.update({'AMQP_PORT': "5672"}) 
env_vars.update({'AMQP_USER': "rabbit"}) 
env_vars.update({'AMQP_PWD': "UUisZsH3VtcT"}) 
env_vars.update({'RUN_ID': "run1"})
env_vars.update({'TASK_ID': "task1"})
env_vars.update({'DATA_DIR': "gs://biodock-bucket"})
env_vars.update({'WORK_DIR': "/tmp"})
env_vars.update({'REF_FILE': "scaffolds.fasta"})
env_vars.update({'IN_QUEUE': "run1_task1"})
env_vars.update({'BOTO_CONFIG': "/tmp/.boto"})
env_vars.update({"GOOGLE_APPLICATION_CREDENTIALS" : "/mnt/spark-sa.json"})

job1 = K8sJob(host, app_name, container_image, container_name, namespace, env_vars, parallelism, cmd, image_pull_policy) 
job1.submit_job()
