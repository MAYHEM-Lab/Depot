import tornado.ioloop
from tornado.options import define, options
import requests
import boto3
from botocore.client import Config
from confluent_kafka import Consumer, DeserializingConsumer, TopicPartition
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

async def consume(options, depot_client, notebook_tag, dataset_tag, dataset_id, consumer, topic, window_length, bootstrap_server):
    data = []

    while True:
        msg=consumer.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        print(f"received message for consumer: {consumer}")
        if len(data) == 0:
            print(f"starting offset: {msg.offset()}")
            start_offset = msg.offset()
        if len(data) == int(window_length):
            end_offset = msg.offset()
            # r = requests.get(
            #     f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/datasets/topics/{topic}',
            #     headers={'access_key': options.depot_access_key}
            # )
            # print(r)
            # datasets = r.json()
            # dataset_id = dataset["dataset_id"]
            # notebook_tag = dataset["notebook_tag"]
            # print(dataset_id)
            print(dataset_tag)
            r = requests.get(
                f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/datasets/{dataset_tag}/notebook',
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
                        headers={'access_key': options.depot_access_key},
                        json = {'notebook_tag':str(notebook_tag), 'end_offset':int(end_offset), 'start_offset':int(start_offset), 'topic':str(topic), 'partition':int(0), 'bootstrap_server': str(bootstrap_server)},
                    )
                print(r)
                if r.ok:
                    print(r.json())
                    segment_id = r.json().get('segment_id')
                    dataset_tag = r.json().get('dataset_tag')
                    segment_version = r.json().get('segment_version')
                    entity = cluster_info["owner"]["name"]
                    #pass this path in the depot kernel object in "streaming" string while executing nb
                    print(f"executing notebook {notebook_tag}")
                    sandbox_id = uuid.uuid4().hex
                    print(data)
                    print(f"start_offset: {start_offset}, end_offset: {end_offset}")
                    await execute_notebook(depot_client, data, entity, notebook_tag, dataset_id, segment_id, segment_version, contents, sandbox_id, False)
                    print("executed notebook")
                else:
                    print("Error while executing notebook")
                    #depot_context (StreamContext) will then upload the segment directly onto this path, and it will also be shown on the UI as it was created earlier
                data=[]
        elif len(data)<window_length:
            data.append(int(msg.value().decode('utf-8')))
            print(len(data))

def start_consuming(topic, notebook_tag, window_length, dataset_tag, dataset_id, bootstrap_server, group_id, auto_offset_reset):
    consumer = Consumer({'bootstrap.servers':bootstrap_server, 'group.id':group_id,'auto.offset.reset':auto_offset_reset})
    consumer.subscribe([topic])
    print(f"Subscribed to topic {topic} with group id {group_id}")
    ioloop = IOLoop()
    ioloop.current(instance=False).spawn_callback(consume, options, depot_client, notebook_tag, dataset_tag, dataset_id, consumer, topic, window_length, bootstrap_server)
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
        try:
            print("received post request")
            payload = json.loads(self.request.body)
            topic = payload['topic']
            bootstrap_server = payload['bootstrap_server']
            window_length = payload['window']
            dataset_tag = payload["dataset_tag"]
            dataset_id = payload["dataset_id"]
            notebook_tag = payload["notebook_tag"]
            group_id = f"python-consumer-{dataset_id}/{notebook_tag}"
            auto_offset_reset = "latest"
            consumer_list.append(topic)
            threading.Thread(target = start_consuming, args = (topic, notebook_tag, window_length, dataset_tag, dataset_id, bootstrap_server, group_id, auto_offset_reset)).start()
            return self.finish({"success": "true"})
        except Exception as ex:
            return self.write_error(500, exc_info=ex)

class AnnounceHandler(RequestHandler):
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
        try:
            print("received post request for announce")
            payload = json.loads(self.request.body)
            topic = payload['topic']
            start_offset = payload['start_offset']
            end_offset = payload['end_offset']
            notebook_id = payload['notebook_tag']
            segment_id = payload['segment_id']
            segment_version = payload['segment_version']
            dataset_id = payload["dataset_id"]
            bootstrap_server = payload["bootstrap_server"]
            group = f"python-consumer-{dataset_id}/{notebook_id}"
            data = []
            for i in range(start_offset,end_offset):
                conf = {'bootstrap.servers': bootstrap_server,
                        'group.id': group,
                        'auto.offset.reset': 'earliest',
                        'isolation.level':'read_committed',
                        }
                consumer = DeserializingConsumer(conf)
                offset = int(i)
                partition = 0
                partition = TopicPartition(topic, partition, offset)
                consumer.assign([partition])
                consumer.seek(partition)
                #  Read the message
                message = consumer.poll()
                print("Output: " + str(message.partition()) + " " + str(message.offset()), message.value())
                data.append(int(message.value()))
                print(data)
            r = requests.get(
                f'{options.depot_endpoint}/api/entity/{cluster_info["owner"]["name"]}/notebooks/{notebook_id}/contents',
                headers={'access_key': options.depot_access_key}
            )
            contents = r.json()
            print(contents)
            if (contents.get('cells')) and len(contents['cells'])>0:
                #call manager to create new segment and send the path of the created segment
                #IMP NOTE: notebook <> data mapping needs to be done, so while creating this dataset info,
                # store the notebook ID in streaming_data_notebook dataset with data-tag and notebook tag info.
                # And then during this notebooks execution, call this table and get the dataset tag name
                entity = cluster_info["owner"]["name"]
                 #pass this path in the depot kernel object in "streaming" string while executing nb
                print(f"executing notebook {notebook_id}")
                sandbox_id = uuid.uuid4().hex
                await execute_notebook(depot_client, data, entity, notebook_id, dataset_id, segment_id, segment_version, contents, sandbox_id, True)
            else:
                print("Error while executing notebook")
                #depot_context (StreamContext) will then upload the segment directly onto this path, and it will also be shown on the UI as it was created earlier
            return self.finish({"success": "true"})
        except Exception as ex:
            return self.write_error(500, exc_info=ex)

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
        ('/announce', AnnounceHandler)
    ])
    app.listen(options.port)
    IOLoop.current().start()
    #on receiving /consume {"topic": "", "bootstrap server":, "group id":"", "auto offsetreset:""} request, check if the consumer is already running, and if not, run the new consumer