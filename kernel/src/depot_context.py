import base64
import io
import json
import mimetypes
import os
import re
from itertools import islice

import boto3
from botocore.client import Config

import pyspark
from pyspark import SparkConf

S3_REGEX = r'^s3a://(.*?)/(.*?)$'

DOWNLOAD_CHUNK_SIZE = 2 ** 20

SAMPLE_ROWS = 15
SAMPLE_COLUMN_SIZE = 100

SAMPLE_BYTES = 1000

TYPE_MAPPINGS = {
    "string": "String",
    "int": "Integer",
    "bigint": "Long",
    "float": "Float",
    "double": "Double"
}


class PublishPayload:
    def __init__(self, touched_datasets, result_type):
        self.touched_datasets = touched_datasets
        self.result_type = result_type

    def _repr_depot_(self):
        return {'touched_datasets': self.touched_datasets, 'result_type': self.result_type}


class Executor:
    def read(self, location): raise NotImplementedError()
    def write(self, dataset, path): raise NotImplementedError()


class SparkExecutor(Executor):
    def __init__(self, depot_client, app_name='depot'):
        cluster_info = depot_client.cluster()
        spark_master = cluster_info['spark']['spark_master']
        keys = cluster_info['keys']

        conf = SparkConf()
        conf.set('spark.ui.enabled', 'false')
        conf.set('spark.jars.ivy', '/tmp/.ivy/')
        conf.set('spark.app.name', app_name)
        conf.set('spark.hadoop.security.authentication', 'simple')
        conf.set('spark.hadoop.security.authorization', 'false')
        conf.set('spark.hadoop.fs.s3.buffer.dir', '/tmp/.s3/')
        conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        conf.set('spark.hadoop.fs.s3a.access.key', keys['access_key'])
        conf.set('spark.hadoop.fs.s3a.secret.key', keys['secret_key'])
        conf.set('spark.hadoop.fs.s3a.endpoint', 's3.poc.aristotle.ucsb.edu:443')
        conf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
        conf.set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        conf.set('spark.hadoop.fs.s3a.signing-algorithm', 'S3SignerType')

        self.boto = boto3.client(
            's3',
            aws_access_key_id=keys['access_key'],
            aws_secret_access_key=keys['secret_key'],
            endpoint_url='http://s3.poc.aristotle.ucsb.edu:443',
            config=Config(
                signature_version='s3',
                s3={
                    'addressing_style': 'path',
                    'payload_signing_enabled': False
                }
            )
        )

        self.spark = pyspark.sql.SparkSession.builder \
            .master(f'spark://{spark_master}') \
            .config('spark.executor.instances', '1') \
            .config('spark.dynamicAllocation.enabled', 'true') \
            .config('spark.dynamicAllocation.minExecutors', '1') \
            .config('spark.dynamicAllocation.maxExecutors', '3') \
            .config('spark.executor.cores', '1') \
            .config('spark.executor.memory', '1g') \
            .config(conf=conf) \
            .getOrCreate()

    def upload(self, path, fp):
        (bucket, key) = re.match(S3_REGEX, path).groups()
        self.boto.upload_fileobj(fp, bucket, key)

    def download(self, location, target_dir):
        files = {}
        for path in location['path']:
            (bucket, prefix) = re.match(S3_REGEX, path).groups()
            response = self.boto.list_objects(Bucket=bucket, Prefix=prefix)
            for content in response.get('Contents', []):
                key = content.get('Key')
                filename = os.path.basename(key)
                target_file = f'{target_dir}/{filename}'
                os.makedirs(os.path.dirname(target_file), exist_ok=True)
                self.boto.download_file(bucket, key, target_file)
                files[filename] = target_file
        return files

    def read(self, location):
        paths = location['path']
        if len(paths) > 0:
            return self.spark.read.format(location['format']).schema(location['schema']).load(paths)
        else:
            return self.spark.createDataFrame([], location['schema'])

    def write(self, dataset, path):
        print(f'writing to {path}')
        dataset.write.format('parquet').mode('overwrite').save(path)


class DepotContext:
    def initialize(self): pass
    def raw(self, dataset: str): raise NotImplementedError
    def table(self, dataset: str): raise NotImplementedError()
    def publish(self, payload): raise NotImplementedError()


class TransformContext(DepotContext):
    def __init__(self, depot_client, executor, entity, tag, version, target_path):
        self.depot_client = depot_client
        self.executor = executor
        self.segment = self.depot_client.locate_version(entity, tag, version)
        self.target_path = target_path

    def raw(self, dataset: str):
        (entity, tag) = dataset.split('/')
        lower_inputs = {k.lower(): v for k, v in self.segment['inputs'].items()}
        location = lower_inputs[tag.lower()]
        target_dir = f'.data/{entity}/{tag}'
        os.makedirs(target_dir, 0o700, exist_ok=True)
        return self.executor.download(location, target_dir)

    def table(self, dataset: str):
        (entity, tag) = dataset.split('/')
        lower_inputs = {k.lower(): v for k, v in self.segment['inputs'].items()}
        location = lower_inputs[tag.lower()]
        return self.executor.read(location)

    def publish(self, payload):
        samples = []
        rows = 0
        if isinstance(payload, pyspark.sql.DataFrame):
            self.executor.write(payload, self.target_path)
            rows = payload.count()
            samples = list(map(lambda r: list(map(lambda v: str(v)[0:SAMPLE_COLUMN_SIZE], r.asDict().values())), payload.take(SAMPLE_ROWS)))
        elif isinstance(payload, dict):
            for key, data in payload.items():
                if isinstance(data, bytes):
                    size = len(data)
                    type = 'application/octet-stream'
                    src = io.BytesIO(data)
                else:
                    size = os.path.getsize(data)
                    type = mimetypes.MimeTypes().guess_type(data)[0]
                    src = open(data, 'rb')
                with src as fp:
                    samples.append([key, str(size), type, base64.b64encode(fp.read(SAMPLE_BYTES)).decode('ascii')])
                    fp.seek(0)
                    self.executor.upload(f'{self.target_path}/{key}', fp)
        else:
            raise Exception('Unrecognized payload type. dict(str, bytes), dict(str, str), or pyspark.sql.DataFrame required')
        with open('.outputs', 'a+') as f:
            result = {
                'rows': rows,
                'sample': samples
            }
            f.write(json.dumps(result))
            f.write('\n')


class ExploreContext(DepotContext):
    def __init__(self, depot_client, executor):
        self.depot_client = depot_client
        self.executor = executor
        self.datasets = set()

    def initialize(self):
        self.datasets.clear()

    def raw(self, dataset: str):
        (entity, tag) = dataset.split('/')
        location = self.depot_client.locate_dataset(entity, tag)['self']
        self.datasets.add(dataset)
        target_dir = f'.data/{entity}/{tag}'
        os.makedirs(target_dir, 0o700, exist_ok=True)
        return self.executor.download(location, target_dir)

    def table(self, dataset: str):
        (entity, tag) = dataset.split('/')
        location = self.depot_client.locate_dataset(entity, tag)['self']
        self.datasets.add(dataset)
        return self.executor.read(location)

    def publish(self, payload):
        if isinstance(payload, pyspark.sql.DataFrame):
            depot_schema = {
                'type': 'Table',
                'columns': [{'name': f.name, 'column_type': TYPE_MAPPINGS[f.dataType.simpleString()]} for f in payload.schema.fields]
            }
        elif isinstance(payload, dict):
            depot_schema = {
                'type': 'Raw'
            }
        else:
            raise Exception('Unrecognized payload type. dict(str, bytes), dict(str, str), or pyspark.sql.DataFrame required')
        return PublishPayload(self.datasets, depot_schema)
