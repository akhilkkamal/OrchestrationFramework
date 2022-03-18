from api.IConfigurator import IConfigurator
from airflow.models import Pool, Variable
from constants import OrchestratorConstants as OC
import pandas as pd
import boto3
from airflow.hooks.base import BaseHook


class FileConfigurator(IConfigurator):

    def __init__(self, context):
        self._conn = BaseHook.get_connection(OC.METASTORE_CONN_ID)
        self._path = BaseHook.get_connection(OC.METASTORE_PATH)
        self._bucket_name = str(self._path).split('/')[0]
        self._key = str(self._path)[len(self._bucket_name):len(self._bucket_name)]

    def get_configuration(self):
        s3 = boto3.client('s3', aws_access_key_id=self._conn.login,
                          aws_secret_access_key=self._conn.get_password(),
                          region_name=self._conn.extra_config["region_name"])
        read_file = s3.get_object(self._bucket_name, self._key)
        df = pd.read_json(read_file['Body'])
        return df
