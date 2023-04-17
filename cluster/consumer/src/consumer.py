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

S3_REGEX = r'^s3a://(.*?)/(.*?)$'


from depot_client import DepotClient

map = dict()

class Consumer:
    def __init__(self, topic):
        self.topic = topic

    def consume(self):
        while True:
            msg=self.executor.consumer.poll(1.0) #timeout
            if msg is None:
                continue
            if msg.error():
                return Exception('Error: {}'.format(msg.error()))
                continue
            data=msg.value().decode('utf-8')

            for notebook in map[topic]:
                nb_client = NotebookClient()
                #retrieve process_func
                #thread = threading.Thread(target=process_func,
                #                  args=(data,))
                #thread.daemon = True  # so you can quit the demo program easily :)
                #thread.start()

    def register_notebook(self, notebook_id):
        if map[self.topic] is None:
            map[self.topic] = []
            map[self.topic].append(notebook_id)
        else:
            map[self.topic].append(notebook_id)

class ConsumerHandler(RequestHandler):

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def write_error(self, status_code, **kwargs):
        reply = {'message': 'Unknown error'}
        message = kwargs.get('message')
        exc_info = kwargs.get('exc_info')

        if message:
            reply['message'] = message
        elif exc_info:
            reply['message'] = str(exc_info[1])
        self.finish(json.dumps(reply))

    async def post(self):
        payload = json.loads(self.request.body)
        topic = payload["topic"]
        consumer = Consumer(topic)
        consumer.register_notebook(topic)
        while True:
            thread = threading.Thread(target=consumer.consume,
                                      args=())
            thread.daemon = True  # so you can quit the demo program easily :)
            thread.start()


if __name__ == '__main__':
    define('port', default=9992, help='port to listen on')
    define('depot-endpoint', help='Depot API server endpoint')
    define('depot-access-key', help='Depot cluster access key')

    tornado.options.parse_command_line()
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

    r = requests.get(
        f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/notebooks/topic/',
        headers={'access_key': options.depot_access_key}
    )
    info = r.json()
    for k in info:
        print(k["tag"])
        r = requests.get(
            f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/notebooks/{k["tag"]}/topic//contents',
            headers={'access_key': options.depot_access_key}
        )
        contents = r.json()
        # Serializing json
        json_object = json.dumps(contents, indent=4)

        # Writing to sample.json
        with open(f"executable-{k['tag']}.ipynb", "w") as outfile:
            outfile.write(json_object)



