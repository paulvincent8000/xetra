'''Connector and methods to access s3'''

import logging
import os
import boto3

class S3BucketConnector():

    '''Class for interacting with s3 buckets'''

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str) -> None:
        '''
        Constructor for s3 bucket connector

        :param access_key: access key for s3
        :param secret_key: secret for accessing s3
        :param endpoint_url: endpoint url to s3
        :param bucket: s3 bucket name
        '''

        self.logger=logging.getLogger(name=__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session( aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID"),
                                aws_secret_access_key= os.environ.get("AWS_SECRET_ACCESS_KEY"))
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

        
    def list_files_in_prefix(self, prefix: str) -> list:
        '''
        List all files in the s3 bucket

        :param prefix: prefix of the s3 bucket to be filtered

        returns:
          files: list of all files in the selected s3 bucket
        '''
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self) -> None:
        ''''''
        pass

    def write_df_to_s3(self):
        ''''''
        pass

    def __str__(self):
        pass

# test connection
x = S3BucketConnector('','','https://xetra-1234-pv.s3.eu-central-1.amazonaws.com','xetra-1234-pv')