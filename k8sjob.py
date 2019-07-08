import kubernetes
from kubernetes import client
import os
import sys
import yaml


class K8sJob(object):
    """
    This class has methods to submit non spark jobs to K8 cluster.
    For Spark jobs use SparkOnGKEJobs or SparkJobs classes.
    """

    def __init__(self, k8host, jobname, container_image, container_name, namespace, env_vars, parallelism, cmd, image_pull_policy):
        self.k8host= k8host
        self.jobname = jobname
        self.container_image = container_image
        self.container_name = container_name
        self.namespace = namespace
        self.env_vars = env_vars
        self.parallelism = parallelism
        self.cmd = cmd
        self.image_pull_policy = image_pull_policy
        kubernetes.config.load_kube_config()
        configuration = client.Configuration()
        configuration.host = self.k8host
        configuration.watch = True
        self.api_instance = client.BatchV1Api(client.ApiClient(configuration))
        self.body = None

    def kube_create_job_object(self):
        '''
        Create a k8 Job Object
        '''
        ''' Body is the object Body '''
        body = client.V1Job(api_version="batch/v1", kind="Job")
        ''' Configure Metadata '''
        body.metadata = client.V1ObjectMeta(namespace=self.namespace, name=self.jobname)
        ''' Add Status '''
        body.status = client.V1JobStatus()
        ''' Configure Template '''
        template = client.V1PodTemplate()
        template.template = client.V1PodTemplateSpec()
        ''' Passing Arguments in Env: '''
        env_list = []
        for env_name, env_value in self.env_vars.items():
            env_list.append(client.V1EnvVar(name=env_name, value=env_value))
        ''' Configure command and arguments '''
        command = ["/bin/bash"]
        args = ["-c"]
        args.append(self.cmd)
        print (len(args))
        print (args)
        ''' Configure security context and capabilities '''
        capabilities = client.V1Capabilities(add=["SYS_ADMIN"])
        security_context = client.V1SecurityContext(capabilities=capabilities)
        container = client.V1Container(name=self.container_name, image=self.container_image, env=env_list, command=command, args=args, security_context=security_context, image_pull_policy=self.image_pull_policy)
        template.template.spec = client.V1PodSpec(containers=[container], restart_policy='Never')
        ''' Configure Spec '''
        body.spec = client.V1JobSpec(parallelism=self.parallelism, ttl_seconds_after_finished=600, template=template.template)
        self.body = body


    def kube_run_job_object(self):
        try: 
           api_response = self.api_instance.create_namespaced_job("default", self.body, pretty=True)
           print(api_response)
        except kubernetes.client.rest.ApiException as e:
           print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
        return
