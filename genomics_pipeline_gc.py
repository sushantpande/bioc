from utils import exec_cmd


project_id = 'biodock'
sa = 'spark-bwa@biodock.iam.gserviceaccount.com'
zone = 'us-central1-a'

class GenomicsPipelineGC(object):
    def __init__(self, cmd, runner='', docker_image=''):
        self.cmd = cmd
        if not runner:
            self.runner = 'DataflowRunner'
        else:
            self.runner = runner
        self.docker_image = docker_image

    def submit_job(self):
        if not self.docker_image:
            pass
        else:
            cmd = "%s --runner %s --service_account_email %s --zone %s" %(self.cmd, self.runner, sa, zone)
            docker_cmd = ["docker run %s --project %s --zones %s '%s'" %(self.docker_image, project_id, zone, cmd)]
            exec_cmd(docker_cmd)


cmd = "vcf_to_bq --input_pattern gs://biodock-bucket/merge.vcf  --output_table biodock:biodock.tmp3  --temp_location gs://biodock-bucket/temp --job_name vcf-to-bigquery"
df = GenomicsPipelineGC(cmd, docker_image="gcr.io/gcp-variant-transforms/gcp-variant-transforms")
df.submit_job()
