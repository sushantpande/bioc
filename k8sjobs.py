import kubernetes
from kubernetes import client
import os
import sys
import yaml


class K8sJobs(object):
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
        configuration = client.Configuration()
        configuration.host = self.k8host
        configuration.watch = True
        self.api_instance = client.BatchV1Api(client.ApiClient(configuration))
        self.body = None

    def kube_create_job_object():
        '''
        Create a k8 Job Object
        '''
        ''' Body is the object Body '''
        body = self.client.V1Job(api_version="batch/v1", kind="Job")
        ''' Configure Metadata '''
        body.metadata = self.client.V1ObjectMeta(namespace=self.namespace, name=self.jobname)
        ''' Add Status '''
        body.status = self.client.V1JobStatus()
        ''' Configure Template '''
        template = self.client.V1PodTemplate()
        template.template = client.V1PodTemplateSpec()
        ''' Passing Arguments in Env: '''
        env_list = []
        for env_name, env_value in self.env_vars.items():
            env_list.append(self.client.V1EnvVar(name=env_name, value=env_value))
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
        body.spec = self.client.V1JobSpec(parallelism=self.parallelism, ttl_seconds_after_finished=600, template=template.template)
        self.body = body


    def kube_run_job_object():
        try: 
           api_response = api_instance.create_namespaced_job("default", self.body, pretty=True)
           print(api_response)
        except kubernetes.client.rest.ApiException as e:
           print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
        return


    def kube_monitor_job_status():
        STATUS_COND_TYPE_COMPLETE = "Complete"
        STATUS_COND_STATUS_TRUE = "True"
        INTERESTED_EVENTS = ["MODIFIED", "DELETED"]
        api_response = None
        ''' Watch begins! '''
        w = kubernetes.watch.Watch()
        stream = w.stream(self.api_instance.list_namespaced_job, self.namespace)
        for event in stream:
            if event['object'].metadata.name == self.name:
                if event['type'] in INTERESTED_EVENTS:
                    status = event['object'].status
                    if not (status.failed):
                        if not (status.active):
                            conditions  = status.conditions[0]
                            if conditions.type == STATUS_COND_TYPE_COMPLETE and STATUS_COND_STATUS_TRUE == "True":
                                print ("Job (%s) status is (%s)" %(name, conditions.type))
                                #Call clean up here
                                break
                        else:
                            print ("Job (%s) has (%s) active nodes" %(self.name, status.active))
                    else:        
                        print ("Job (%s) has (%s) failed nodes" %(self.name, status.failed))
        return True


    def kube_delete_complete_jobs_pod():
        deleteoptions = client.V1DeleteOptions()
        api_pods = client.CoreV1Api()
        try:
            pods = api_pods.list_namespaced_pod(self.namespace,
                                                include_uninitialized=False,
                                                pretty=True,
                                                timeout_seconds=60)
        except kubernetes.client.rest.ApiException as e:
            print("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
            return False
        
        for pod in pods.items:
            print (pod)
            pod_job_name = pod.metadata.labels.get('job-name', '')
            if pod_job_name == self.jobname:
                podname = pod.metadata.name
                try:
                    api_response = api_pods.delete_namespaced_pod(podname, self.namespace, body=deleteoptions)
                    print (api_response)
                except kubernetes.client.rest.ApiException as e:
                    print ("Exception when calling CoreV1Api->delete_namespaced_pod: %s\n" % e)

        return True


    def kube_cleanup_complete_jobs():
        deleteoptions = client.V1DeleteOptions()
        try: 
            jobs = api_instance.list_namespaced_job(self.namespace,
                                                    include_uninitialized=False,
                                                    pretty=True,
                                                    timeout_seconds=60)
        except kubernetes.client.rest.ApiException as e:
            print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)
            return False
        for job in jobs.items:
            print(job)
            if self.jobname == job.metadata.name:
                jobstatus = job.status.conditions
                parallelism = job.spec.parallelism
                if jobstatus and job.status.succeeded == parallelism:
                    '''Clean up Job'''
                    print("Cleaning up Job: {}. Completed at: {}".format(self.jobname, job.status.completion_time))
                    try: 
                        api_response = api_instance.delete_namespaced_job(self.jobname, self.namespace, body=deleteoptions)
                        print(api_response)
                        '''API to delete pods''' 
                        kube_delete_complete_jobs_pod()
                    except kubernetes.client.rest.ApiException as e:
                        print("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
                else:
                    active = job.status.active
                    failed = job.status.failed
                    if jobstatus is None and (active > 0 or failed > 0):
                        print("Job: {} not cleaned up. Current status: active-{} failed-{}".format(self.jobname, active, failed))
        
        return True
