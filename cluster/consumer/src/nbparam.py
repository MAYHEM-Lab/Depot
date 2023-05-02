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

def build_kernel_spec(depot_endpoint: str, depot_access_key: str, id:str, announce_context):
    if announce_context:
        return KernelSpec(**{
            'argv': [
                '/opt/depot-kernel/depot_kernel.py',
                '-f',
                '{connection_file}',
                '--depot-endpoint',
                depot_endpoint,
                '--depot-access-key',
                depot_access_key,
                '--depot-announce_streaming',
                id
            ],
            'display_name': 'Python 3 (depot)',
            'language': 'python',
            'metadata': {
            'debugger': True,
            }
        })
    else:
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
                id
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


async def execute_notebook(client, data, entity, dataset_tag, tag, dataset_id, segment_id, segment_version, contents, sandbox_id, announce_context):
    try:
        nb = nbformat.reads(json.dumps(contents), as_version=4)
        orig_parameters = extract_parameters(nb)
        params = parameter_values(orig_parameters, data=data)
        nb_client = None
        context_string = f"{sandbox_id},{entity},{tag},{dataset_id},{segment_id}"
        new_spec = build_kernel_spec(client.depot_destination, client.access_key, context_string, announce_context)
        static_kernel_specs.set_spec(new_spec)
        #  Make a notebook object with these definitions
        new_nb = replace_definitions(nb, params)
        print(params)
        nb_client = NotebookClient(
            new_nb,
            kernel_manager_class=DepotKernelManager,
            shutdown_kernel='graceful',
            timeout=180,
            kernel_name='depot'
        )
        await nb_client.async_execute(cleanup_kc=False)
        logger.info(f'Successfully executed notebook')
    except Exception as ex:
        logger.exception(f'Error while transforming [{entity}/{dataset_tag}@{segment_id}]', exc_info=ex)
        try:
            client.fail_segment(entity, dataset_tag, segment_version, type(ex).__name__, str(ex))
        except Exception as ex2:
            logger.exception(f'Error while failing segment [{entity}/{dataset_tag}@{segment_id}]', exc_info=ex2)
    finally:
        if nb_client:
            try:
                await nb_client._async_cleanup_kernel()
            except Exception as ex:
                logger.exception(f'Failed to clean up kernel context', exc_info=ex)