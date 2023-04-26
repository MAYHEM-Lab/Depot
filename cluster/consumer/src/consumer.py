import tornado.ioloop
from tornado.options import define, options
import requests
import boto3
from botocore.client import Config
from confluent_kafka import Consumer
from depot_client import DepotClient
from nbparam import execute_notebook
from tornado.ioloop import IOLoop
import uuid
import logging
from tornado.web import Application
from tornado.web import RequestHandler
import json
import threading

S3_REGEX = r'^s3a://(.*?)/(.*?)$'
consumer_list = []
async def consume(options, depot_client, consumer, topic, window_length):
    data = []
    while True:
        msg=consumer.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        print(f"received message for consumer: {consumer}")
        if len(data) == int(window_length):
            r = requests.get(
                f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/datasets/topics/{topic}',
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
        elif len(data)<window_length:
            data.append(int(msg.value().decode('utf-8')))
        else:
            data=[]

def start_consuming(topic, window_length, bootstrap_server, group_id, auto_offset_reset):
    consumer = Consumer({'bootstrap.servers':bootstrap_server, 'group.id':group_id,'auto.offset.reset':auto_offset_reset})
    consumer.subscribe([topic])
    ioloop = IOLoop()
    ioloop.current(instance=False).spawn_callback(consume, options, depot_client, consumer, topic, window_length)
    ioloop.start()



class ConsumeHandler(RequestHandler):
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
        print("received post request")
        payload = json.loads(self.request.body)
        topic = payload['topic']
        bootstrap_server = payload['bootstrap_server']
        group_id = "python.consumer"
        auto_offset_reset = "latest"
        window_length=payload["window"]
        if topic not in consumer_list:
            consumer_list.append(topic)
            threading.Thread(target = start_consuming, args = (topic, window_length, bootstrap_server, group_id, auto_offset_reset)).start()
            return self.finish({"success": "true"})
        else:
            self.write_error(500, message="topic already in consumption")
if __name__ == '__main__':
    define('port', default=9992, help='port to listen on')
    define('depot-endpoint', help='Depot API server endpoint')
    define('depot-access-key', help='Depot cluster access key')

    tornado.options.parse_command_line()

    logger = logging.getLogger('depot.consumer')
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.setLevel(logging.INFO)

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

    logger.info(f'Started consumer for cluster {cluster_info["owner"]["name"]}/{cluster_info["cluster"]["tag"]}')
    #add application handler to handle /consume requests
    app = Application([
        ('/consume', ConsumeHandler),
    ])
    app.listen(options.port)
    IOLoop.current().start()
    #on receiving /consume {"topic": "", "bootstrap server":, "group id":"", "auto offsetreset:""} request, check if the consumer is already running, and if not, run the new consumer

