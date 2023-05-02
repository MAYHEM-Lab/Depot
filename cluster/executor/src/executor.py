#!/usr/bin/env python3

import faulthandler
import multiprocessing

import nest_asyncio
from jupyter_client import MultiKernelManager
from jupyter_client.kernelspec import KernelSpecManager, KernelSpec
from jupyter_server.serverapp import ServerApp
from jupyter_server.services.kernels.handlers import MainKernelHandler, KernelHandler, KernelActionHandler, ZMQChannelsHandler
from jupyter_server.services.kernels.kernelmanager import MappingKernelManager
from jupyter_server.services.sessions.handlers import SessionHandler, SessionRootHandler
from jupyterlab.handlers.extension_manager_handler import ExtensionManager
from jupyterlab.labapp import LabPathApp
from jupyterlab_server import LabServerApp
from traitlets import Unicode, Instance

from depot_client import DepotClient

def _jupyter_server_extension_points():
    return [{'module': __name__, 'app': DepotServerApp}]

def _jupyter_labextension_paths():
    return [{
        'src': 'labextension',
        'dest': '@jupyter-widgets/jupyterlab-manager'
    }]

faulthandler.enable(all_threads=True)


def build_kernel_spec(depot_endpoint: str, depot_access_key: str):
    return KernelSpec(**{
        'argv': [
            '/opt/depot-kernel/depot_kernel.py',
            '-f',
            '{connection_file}',
            '--depot-endpoint',
            depot_endpoint,
            '--depot-access-key',
            depot_access_key
        ],
        'display_name': 'Python 3 (depot)',
        'language': 'python',
        'metadata': {
            'debugger': True
        }
    })


class DepotKernelSpecManager(KernelSpecManager):
    spec = Instance(KernelSpec)

    def find_kernel_specs(self): return {}

    def get_all_specs(self): return {}

    def get_kernel_spec(self, kernel_name, **kwargs):
        return self.spec


class DepotServerApp(LabServerApp):
    load_other_extensions = True
    open_browser = False
    name = 'depot_notebook_server'
    app_name = 'Depot Notebook Server'

    MultiKernelManager.default_kernel_name = 'depot'
    MultiKernelManager.shared_context = True

    depot_endpoint = Unicode().tag(config=True)
    access_key = Unicode().tag(config=True)
    aliases = {
        'depot-endpoint': 'DepotServerApp.depot_endpoint',
        'depot-access-key': 'DepotServerApp.access_key'
    }

    ServerApp.xsrf_cookies = False
    ServerApp.disable_check_xsrf = True
    ServerApp.token = ''

    ServerApp.log_level = 'DEBUG'
    ServerApp.port_default_value = 9998
    ServerApp.answer_yes = True
    ServerApp.ip = '0.0.0.0'

    ServerApp.default_services = []

    MappingKernelManager.cull_idle_timeout = 120
    MappingKernelManager.cull_interval = 60
    MappingKernelManager.kernel_info_timeout=180

    ServerApp.allow_root = True
    ServerApp.allow_remote_access = True
    ServerApp.allow_origin = '*'
    ServerApp.kernel_spec_manager_class = DepotKernelSpecManager

    def initialize_handlers(self):
        print(self.extra_labextensions_path)
        print(self.labextensions_path)
        print(self.labextensions_url)
        DepotKernelSpecManager.spec = build_kernel_spec(self.depot_endpoint, self.access_key)
        depot_client = DepotClient(self.depot_endpoint, self.access_key)
        cluster = depot_client.cluster()
        self.log.info(f'Started executor for cluster {cluster["owner"]["name"]}/{cluster["cluster"]["tag"]}')

        multiprocessing.set_start_method('spawn')

        session_id_regex = r'(?P<session_id>\w+-\w+-\w+-\w+-\w+)'

        kernel_id_regex = r"(?P<kernel_id>\w+-\w+-\w+-\w+-\w+)"
        kernel_action_regex = r"(?P<action>restart|interrupt)"

        self.handlers.extend(
            [
                (r'/api/kernels', MainKernelHandler),
                (r'/api/kernels/%s' % kernel_id_regex, KernelHandler),
                (r'/api/kernels/%s/%s' % (kernel_id_regex, kernel_action_regex), KernelActionHandler),
                (r'/api/kernels/%s/channels' % kernel_id_regex, ZMQChannelsHandler),

                (r'/api/sessions/%s' % session_id_regex, SessionHandler),
                (r'/api/sessions', SessionRootHandler)
            ]
        )


if __name__ == '__main__':
    nest_asyncio.apply()
    DepotServerApp.launch_instance()
