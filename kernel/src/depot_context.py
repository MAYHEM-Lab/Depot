import json

import pyspark
from pyspark import SparkConf

sample_size = 15
sample_truncate = 100


class PublishPayload:
    def __init__(self, touched_datasets, result_type):
        self.touched_datasets = touched_datasets
        self.result_type = result_type

    def _repr_depot_(self):
        return {'touched_datasets': self.touched_datasets, 'result_type': self.result_type}


class Executor:
    def read(self, location):
        raise NotImplementedError()

    def write(self, dataset, path):
        raise NotImplementedError()


class SparkExecutor(Executor):
    def __init__(self, depot_client):
        cluster_info = depot_client.cluster()
        spark_master = cluster_info['spark']['spark_master']
        keys = cluster_info['keys']

        conf = SparkConf()
        conf.set('spark.hadoop.fs.s3.buffer.dir', '/tmp/')
        conf.set('spark.hadoop.fs.s3n.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
        conf.set('spark.hadoop.fs.s3n.awsAccessKeyId', keys['access_key'])
        conf.set('spark.hadoop.fs.s3n.awsSecretAccessKey', keys['secret_key'])

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
    def initialize(self):
        pass

    def table(self, entity: str, tag: str):
        raise NotImplementedError()

    def publish(self, df):
        raise NotImplementedError()


class TransformContext(DepotContext):
    def __init__(self, depot_client, executor, entity, tag, version, target_path):
        self.depot_client = depot_client
        self.executor = executor
        self.segment = self.depot_client.locate_version(entity, tag, version)
        self.target_path = target_path

    def table(self, entity, tag):
        print(self.segment)
        lower_inputs = {k.lower(): v for k, v in self.segment['inputs'].items()}
        location = lower_inputs[tag.lower()]
        return self.executor.read(location)

    def publish(self, df):
        self.executor.write(df, self.target_path)
        rows = df.count()
        sample = list(map(lambda r: list(map(lambda v: str(v)[0:sample_truncate], r.asDict().values())), df.take(sample_size)))
        with open('.depot/outputs', 'a+') as f:
            payload = {
                'rows': rows,
                'sample': sample
            }
            f.write(json.dumps(payload))
            f.write('\n')


class ExploreContext(DepotContext):
    def __init__(self, depot_client, executor):
        self.depot_client = depot_client
        self.executor = executor
        self.datasets = set()

    def initialize(self):
        self.datasets.clear()

    def table(self, entity: str, tag: str):
        location = self.depot_client.locate_dataset(entity, tag)['self']
        self.datasets.add(entity + '/' + tag)
        return self.executor.read(location)

    def publish(self, df):
        return PublishPayload(self.datasets, self.depot_client.as_depot_schema(df.schema))
