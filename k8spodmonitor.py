import kubernetes
from kubernetes import client
from multiprocessing import Process
import os
import sys
import yaml


class K8sPodMonitor(object):
    """
    This class has methods to monitor Pod on K8 cluster.
    """

    def __init__(self, k8host, name, namespace):
        self.k8host= k8host
        self.name = name
        self.namespace = namespace
        kubernetes.config.load_kube_config()
        configuration = client.Configuration()
        configuration.host = self.k8host
        configuration.watch = True
        configuration.debug = True
        self.api_instance = client.CoreV1Api(client.ApiClient(configuration))
        self.watch = None


    def start(self):
        self.monitor_process = Process(target=self.kube_monitor_pod_status())
        self.monitor_process.daemon = True
        self.monitor_process.start()


    def stop(self):
        if self.monitor_process:
            self.monitor_process.terminate()
    

    def kube_monitor_pod_status(self):
        INTERESTED_EVENTS = ["MODIFIED", "DELETED"]
        pods = []
        try:
            allpods = self.api_instance.list_namespaced_pod(self.namespace,
                                                include_uninitialized=False,
                                                pretty=True,
                                                timeout_seconds=60)
        except kubernetes.client.rest.ApiException as e:
            print("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
            return False

        for pod in allpods.items:
            print (pod)
            if self.name in pod.metadata.name:
                pods.append(pod.metadata.name)

        ''' Watch begins! '''
        w = kubernetes.watch.Watch()
        print ("Starting a watch")
        stream = w.stream(self.api_instance.list_namespaced_pod, self.namespace)
        for event in stream:
            if not pods:
                break
                #Call clean up here
            print (pods)
            name = event['object'].metadata.name
            if name in pods:
                if event['type'] in INTERESTED_EVENTS:
                    status = event['object'].status
                    print ("Pod (%s) status is (%s)" %(name, status.phase))
                    if status.phase == 'Succeeded' or status.phase == 'Failed' or status.phase == 'Pending':
                        pods.remove(name)
        return (True)


    def kube_delete_pod(self):
        deleteoptions = client.V1DeleteOptions()
        try:
            pods = api_instance.list_namespaced_pod(self.namespace,
                                                include_uninitialized=False,
                                                pretty=True,
                                                timeout_seconds=60)
        except kubernetes.client.rest.ApiException as e:
            print("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
            return False
        
        for pod in pods.items:
            print (pod)
            if self.name in pod.metadata.name:
                try:
                    api_response = api_pods.delete_namespaced_pod(podname, self.namespace, body=deleteoptions)
                    print (api_response)
                except kubernetes.client.rest.ApiException as e:
                    print ("Exception when calling CoreV1Api->delete_namespaced_pod: %s\n" % e)

        return True
