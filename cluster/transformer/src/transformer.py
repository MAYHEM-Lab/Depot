#!/usr/bin/env python3

import json
import logging
import os
import shutil

import nbformat
import tornado.ioloop
from jupyter_client.ioloop import AsyncIOLoopKernelManager
from jupyter_client.kernelspec import KernelSpec, KernelSpecManager
from nbclient import NotebookClient
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.queues import Queue
from tornado.web import Application
from tornado.web import RequestHandler

from depot_client import DepotClient

logger = logging.getLogger('depot.transformer')
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger.setLevel(logging.INFO)

transform_queue = Queue(maxsize=5)

SANDBOX_DIR = '/opt/Depot/sandbox'


def build_kernel_spec(depot_endpoint: str, depot_access_key: str, transform: str):
    return KernelSpec(**{
        'argv': [
            '/opt/depot-kernel/depot_kernel.py',
            '-f',
            '{connection_file}',
            '--depot-endpoint',
            depot_endpoint,
            '--depot-access-key',
            depot_access_key,
            '--depot-transform',
            transform
        ],
        'display_name': 'Python 3 (depot)',
        'language': 'python',
        'metadata': {
            'debugger': True
        }
    })


class DepotKernelSpecManager(KernelSpecManager):
    def set_spec(self, spec):
        self.spec = spec

    def find_kernel_specs(self): return {}

    def get_all_specs(self): return {}

    def get_kernel_spec(self, kernel_name, **kwargs):
        return self.spec


static_kernel_specs = DepotKernelSpecManager()


class DepotKernelManager(AsyncIOLoopKernelManager):
    kernel_spec_manager = static_kernel_specs
    autorestart = False


async def transform_worker(client):
    while True:
        item = await transform_queue.get()
        content, entity, tag, version, transform_id, path = item
        sandbox_path = f'{SANDBOX_DIR}/{transform_id}'

        try:
            logger.info(f'Starting transformation {transform_id} to generate segment [{entity}/{tag}@{version}]')

            if not os.path.exists(sandbox_path):
                os.makedirs(sandbox_path)
                os.makedirs(f'{sandbox_path}/.depot')

            new_spec = build_kernel_spec(
                client.depot_destination,
                client.access_key,
                f'{entity},{tag},{version},{path}'
            )
            static_kernel_specs.set_spec(new_spec)
            nb_client = NotebookClient(
                nbformat.v4.to_notebook_json(content, minor=4),
                kernel_manager_class=DepotKernelManager,
                shutdown_kernel='immediate',
                timeout=60,
                kernel_name='depot',
                resources={'metadata': {'path': sandbox_path}}
            )
            await nb_client.async_execute()

            with open(f'{sandbox_path}/.depot/outputs', 'r') as f:
                for line in f.readlines():
                    payload = json.loads(line)
                    rows = payload['rows']
                    sample = payload['sample']
                    client.commit_segment(entity, tag, version, path, rows, sample)
                logger.info(f'Successfully created segment [{entity}/{tag}@{version}]')
        except Exception as ex:
            logger.exception(f'Error while transforming [{entity}/{tag}@{version}]', exc_info=ex)
            try:
                client.fail_segment(entity, tag, version, str(ex))
            except Exception as ex2:
                logger.exception(f'Error while failing segment [{entity}/{tag}@{version}]', exc_info=ex2)
        finally:
            shutil.rmtree(sandbox_path)
            transform_queue.task_done()


class TransformHandler(RequestHandler):
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
        content = payload['content']
        entity = payload['entity']
        tag = payload['tag']
        version = payload['version']
        transform_id = payload['id']
        path = payload['path']

        logger.info(f'Enqueueing transformation {transform_id} to generate segment [{entity}/{tag}@{version}]')
        IOLoop.current().add_callback(transform_queue.put_nowait, (content, entity, tag, version, transform_id, path))
        return self.finish({'artifact_id': transform_id})


if __name__ == '__main__':
    define('port', default=9994, help='port to listen on')
    define('depot-endpoint', help='Depot API server endpoint')
    define('depot-access-key', help='Depot cluster access key')

    tornado.options.parse_command_line()
    depot_client = DepotClient(options.depot_endpoint, options.depot_access_key)

    app = Application([
        ('/transform', TransformHandler),
    ])
    app.listen(options.port)
    IOLoop.current().spawn_callback(transform_worker, depot_client)
    IOLoop.current().start()
