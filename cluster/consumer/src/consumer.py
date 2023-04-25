import threading
import json
import tornado.ioloop
from nbclient import NotebookClient
from tornado.options import define, options
from tornado.web import Application
from tornado.web import RequestHandler
import requests
import boto3
from botocore.client import Config
from io import BytesIO
import re
import os
from confluent_kafka import Consumer
S3_REGEX = r'^s3a://(.*?)/(.*?)$'
from depot_client import DepotClient
from nbparam import execute_notebook
from tornado.ioloop import IOLoop
import uuid
import logging

async def consume(options, depot_client, consumer):
    while True:
        msg=consumer.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=[int(msg.value().decode('utf-8'))]
        r = requests.get(
            f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/datasets/topics/{options.topic}',
            headers={'access_key': options.depot_access_key}
        )
        print(r)
        datasets = r.json()
        for dataset in datasets:
            dataset_id = dataset["dataset_id"]
            notebook_tag = dataset["notebook_tag"]
            print(dataset_id)
            r = requests.get(
                f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/notebooks/{notebook_tag}/contents',
                headers={'access_key': options.depot_access_key}
            )
            contents = r.json()
            print(contents)
            if (contents.get('cells')) and len(contents['cells'])>0:
                #call manager to create new segment and send the path of the created segment
                #IMP NOTE: notebook <> data mapping needs to be done, so while creating this dataset info,
                # store the notebook ID in streaming_data_notebook dataset with data-tag and notebook tag info.
                # And then during this notebooks execution, call this table and get the dataset tag name
                r = requests.post(
                    f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/datasets/{dataset_id}/segment',
                    headers={'access_key': options.depot_access_key}
                )
                print(r)
                if r.ok:
                    print(r.json())
                    bucket = r.json().get('bucket')
                    key = r.json().get('root')
                    version = int(key.split("/")[1])
                    dataset_tag = r.json().get('dataset_tag')
                    path = f"s3a://{bucket}/{key}"
                    entity = cluster_info["owner"]["name"]
                    #pass this path in the depot kernel object in "streaming" string while executing nb
                    print(f"executing notebook {notebook_tag}")
                    sandbox_id = uuid.uuid4().hex
                    await execute_notebook(entity, notebook_tag, dataset_tag, dataset_id, version, path, depot_client, contents, data, sandbox_id)
                else:
                    print("Error while executing notebook")
                #depot_context (StreamContext) will then upload the segment directly onto this path, and it will also be shown on the UI as it was created earlier

if __name__ == '__main__':
    define('port', default=9992, help='port to listen on')
    define('depot-endpoint', help='Depot API server endpoint')
    define('depot-access-key', help='Depot cluster access key')
    define('topic', help='Consumer topic to subscribe on')

    tornado.options.parse_command_line()

    logger = logging.getLogger('depot.consumer')
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.setLevel(logging.INFO)

    if options.topic is None:
        print("consumer topic is needed, exiting")
        exit(1)

    depot_client = DepotClient(options.depot_endpoint, options.depot_access_key)
    cluster_info = depot_client.cluster()
    keys = cluster_info['keys']
    boto = boto3.client(
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

    consumer =  Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'latest'})
    consumer.subscribe(["messenger"])

    IOLoop.current().spawn_callback(consume, options, depot_client, consumer)
    IOLoop.current().start()

