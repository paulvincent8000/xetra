'''Connector and methods to access s3'''

import logging
import os
from io import StringIO, BytesIO

import boto3
import pandas as pd

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException

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

        self._logger=logging.getLogger(name=__name__)
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

    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ',') -> pd.DataFrame:
        '''
        Read csv files from 3s bucket into a dataframe
        
        :param key: the key that identifies the csv file in the s3 bucket
        :param decoding: the decoding used in the csv file. Defaults to utf-8.
        :param sep: the delimiter used in the csv file. Defaults to comma.

        return:
          dataframe filled with contents of csv file
        '''

        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, delimiter=sep)
        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        '''
        Write contents of a dataframe to a bucket in s3
        '''

        if data_frame.empty:
            self._logger.info('The dataframe is empty. No file will be written.')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not '
        'supported to be written to s3.', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        '''
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        '''

        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

# test connection
x = S3BucketConnector('','','https://xetra-1234-pv.s3.eu-central-1.amazonaws.com','xetra-1234-pv')
