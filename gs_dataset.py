# Imports the Google Cloud client library
from google.cloud import bigquery


# The name of the dataset
bucket_name = 'biodock-dataset'


class DatasetGS(object):
    '''This class holds method to communicate with GCP Big Query Dataset'''

    def __init__(self, dataset_name, project_id, creds_file_path):
        # Instantiates a client
        self.dataset_name = dataset_name
        self.project_id = project_id
        self.dataset_client = bigquery.Client(project=project_id).from_service_account_json(creds_file_path)


    def create_table(self, table_name, schema=[]):
        table_id = "%s.%s.%s" %(self.project_id, self.dataset_name, table_name)
        table = bigquery.Table(table_id, schema)
        table = self.dataset_client.create_table(table)
        print (table)
        return(table_id)


ds = DatasetGS('biodock', 'biodock', '../creds.json')
ds.create_table('temp')
