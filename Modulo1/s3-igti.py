import logging
import boto3
from botocore.exceptions import ClientError
import requests
import zipfile
import os
from pyspark.sql import SparkSession


_url = 'https://download.inep.gov.br'
_urlDir = '/microdados/'
_urlFile = 'microdados_enem_2020.zip'
_urlDownload = _url+_urlDir+_urlFile

_objectName = None
_bucketName = 'datalake-brunodaemon-501118535191'
_fileOptions = 'rb'
_fileDir = '/Users/brunodaemon/PycharmProjects/estudos/DADOS/'
_fileDirLake = _bucketName + '/raw-data/'
_bucketNameConsumerZone = _bucketName + '/consumer-zone/'
_fileDirLocalParquet1 = _fileDir + '/ITENS_PROVA_2020.csv.parquet/'
_fileDirLocalParquet2 = _fileDir + '/MICRODADOS_ENEM_2020.csv.parquet/'
_fileName = ''
_lakeService = 's3'
# _pdData = pandas.read_csv(_fileName)
_fileExtension = 'parquet'


def uploadParquetToS3(_fileDir):
    print(_fileDir)
    for v_file in os.listdir(_fileDir):
        if v_file.endswith(_fileExtension):
            v_file2 = _fileDir + v_file
            v_file3 = _bucketNameConsumerZone + v_file
            print('uploading ' + v_file + ' to ' + _bucketNameConsumerZone)
            upload_file(_fileName=v_file2, _bucketName=_bucketName, _objectName=v_file3)
            print(v_file + ' uploaded to ' + _bucketNameConsumerZone)


def processENEM2020_Parquet(_fileDir):
    spark = SparkSession.builder.getOrCreate()
    for v_file in os.listdir(_fileDir):
        v_file2 = _fileDir + v_file
        v_file3 = _fileDir + v_file + '.parquet'
        df = spark.read.csv(v_file2, header=True, sep=';', encoding='latin1')
        df.show(10)
        # df = spark.createDataFrame()
        df.write.parquet(v_file3)


def unZIPENEN(_fileUnzip):
    print('unZip start')
    zf = zipfile.ZipFile(_fileUnzip, 'r')
    zf.extractall()
    print('unZip end')


def getENENDataFromURL(_urlENEN, _fileENEN):
    response = requests.get(_urlENEN)
    print('nome do arquivo:', _urlDownload)
    print('response: ', response.headers)
    print('response: ', response.encoding)
    print('response: ', response.status_code)
    with open(_fileENEN, 'wb') as _myfile:
        _myfile.write(response.content)


def download_file(_fileName, _bucketName, _objectName):
    # Upload the file
    s3_client = boto3.client(_lakeService)
    try:
        s3_client.download_file(_bucketName, _objectName, _fileName)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(_fileName, _bucketName, _objectName):
    # Upload the file
    s3_client = boto3.client(_lakeService)
    try:
        s3_client.upload_file(_fileName, _bucketName, _objectName)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def bucketlist():
    # Retrieve the list of existing buckets
    s3 = boto3.client(_lakeService)
    response = s3.list_buckets()

    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')


if __name__ == "__main__":
    bucketlist()
    print('Getting ENEN2020 zip file')
    getENENDataFromURL(_urlENEN=_urlDownload, _fileENEN='microdados_enem_2020_v2.zip')
    print('unzipping ENEN2020 file')
    unZIPENEN('microdados_enem_2020_v2.zip')
    print('converting to parquet')
    processENEM2020_Parquet(_fileDir)
    print('uploading parquet to S3')
    # uploadParquetToS3(_fileDirLocalParquet1)
    uploadParquetToS3(_fileDirLocalParquet2)