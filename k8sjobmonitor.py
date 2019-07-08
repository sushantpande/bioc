import kubernetes
from kubernetes import client
import os
import sys
import yaml


class K8sJobMonitor(object):
    """
    This class has methods to monitor Job on K8 cluster.
    """

    def __init__(self, k8host, jobname, namespace):
        self.k8host= k8host
        self.jobname = jobname
        self.namespace = namespace
        kubernetes.config.load_kube_config()
        configuration = client.Configuration()
        configuration.host = self.k8host
        configuration.watch = True
        configuration.debug = True
        self.api_instance = client.BatchV1Api(client.ApiClient(configuration))
        self.body = None


    def monitor(self):
        self.kube_monitor_job_status()


    def kube_monitor_job_status(self):
        STATUS_COND_TYPE_COMPLETE = "Complete"
        STATUS_COND_STATUS_TRUE = "True"
        INTERESTED_EVENTS = ["MODIFIED", "DELETED"]
        api_response = None
        ''' Watch begins! '''
        w = kubernetes.watch.Watch()
        print ("Starting a watch")
        stream = w.stream(self.api_instance.list_namespaced_job, self.namespace)
        for event in stream:
            print (event['object'].metadata.name)
            if event['object'].metadata.name == self.jobname:
                if event['type'] in INTERESTED_EVENTS:
                    status = event['object'].status
                    if not (status.failed):
                        if not (status.active):
                            conditions  = status.conditions[0]
                            if conditions.type == STATUS_COND_TYPE_COMPLETE and STATUS_COND_STATUS_TRUE == "True":
                                print ("Job (%s) status is (%s)" %(self.jobname, conditions.type))
                                #Call clean up here
                                break
                        else:
                            print ("Job (%s) has (%s) active nodes" %(self.jobname, status.active))
                    else:        
                        print ("Job (%s) has (%s) failed nodes" %(self.jobname, status.failed))
        return True


    def kube_delete_complete_jobs_pod(self):
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


    def kube_cleanup_complete_jobs(self):
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
