import nbclient
import nbformat
import logging
from nbparameterise import (
    extract_parameters, replace_definitions, parameter_values
)
from jupyter_client.ioloop import AsyncIOLoopKernelManager
from jupyter_client.kernelspec import KernelSpec, KernelSpecManager
from nbclient import NotebookClient
import json

logger = logging.getLogger('depot.consumer')
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger.setLevel(logging.INFO)

def build_kernel_spec(depot_endpoint: str, depot_access_key: str, streaming_id:str):
    return KernelSpec(**{
        'argv': [
            '/opt/depot-kernel/depot_kernel.py',
            '-f',
            '{connection_file}',
            '--depot-endpoint',
            depot_endpoint,
            '--depot-access-key',
            depot_access_key,
            '--depot-streaming',
            streaming_id
        ],
        'display_name': 'Python 3 (depot)',
        'language': 'python',
        'metadata': {
            'debugger': True,
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


def execute_notebook(tag, path, client, contents, data, sandbox_id):
    try:
        nb = nbformat.reads(json.dumps(contents), as_version=4)
        orig_parameters = extract_parameters(nb)
        params = parameter_values(orig_parameters, data=data)
        streaming_string = f"{sandbox_id},{tag},{path}"
        new_spec = build_kernel_spec(
            client.depot_destination,
            client.access_key,
            streaming_string
        )
        print(params)
        static_kernel_specs.set_spec(new_spec)
        #  Make a notebook object with these definitions
        new_nb = replace_definitions(nb, params)
        nb_client = NotebookClient(
        new_nb,
        kernel_manager_class=DepotKernelManager,
        shutdown_kernel='graceful',
        timeout=180,
        kernel_name='depot'
        )
        nb_client.execute(cleanup_kc=False)

        logger.info(f'Successfully executed')
    except Exception as ex:
        logger.exception("Error while transforming", exc_info=ex)