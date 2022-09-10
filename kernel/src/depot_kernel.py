#!/usr/bin/env python3
import atexit
import multiprocessing
import os
import random
import shutil
import subprocess
import uuid

from IPython.core.formatters import *
from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelapp import IPKernelApp, kernel_aliases
from ipykernel.zmqshell import ZMQInteractiveShell
from pyspark.sql import SparkSession
from traitlets import Unicode, ObjectName, Instance
from traitlets.config import Application

from depot_client import DepotClient
from depot_context import SparkExecutor, TransformContext, ExploreContext, DepotContext


class DepotFormatter(BaseFormatter):
    format_type = Unicode('application/depot-publish')
    print_method = ObjectName('_repr_depot_')
    _return_type = dict


class DepotInteractiveShell(ZMQInteractiveShell):
    ast_node_interactivity = 'last_expr_or_assign'

    formatter_classes = [
        PlainTextFormatter,
        HTMLFormatter,
        MarkdownFormatter,
        SVGFormatter,
        PNGFormatter,
        PDFFormatter,
        JPEGFormatter,
        LatexFormatter,
        JSONFormatter,
        JavascriptFormatter,
        DepotFormatter
    ]
    d = {}
    for cls in formatter_classes:
        f = cls()
        d[f.format_type] = f
    DisplayFormatter.formatters = d

    def run_cell(self, *args, **kwargs):
        return super(DepotInteractiveShell, self).run_cell(*args, **kwargs)


class DepotKernel(IPythonKernel):
    depot_context = Instance(DepotContext)
    spark_session = Instance(SparkSession)
    shell_class = DepotInteractiveShell

    def __init__(self, **kwargs):
        IPythonKernel.user_ns = {
            'depot': self.depot_context,
            'spark': self.spark_session
        }
        super(DepotKernel, self).__init__(**kwargs)


class DepotKernelApp(IPKernelApp):
    kernel_class = DepotKernel
    transform = Unicode('').tag(config=True)
    default_extensions = []
    client = Instance(DepotClient)

    aliases = dict(kernel_aliases)

    def initialize(self, argv=None):
        self.parse_command_line(argv)

        if len(self.transform) > 0:
            _, entity, tag, version, path = self.transform.split(',')
            executor = SparkExecutor(self.client, f'transform:{entity}/{tag}/{version}')
            depot_ctx = TransformContext(self.client, executor, entity, tag, int(version), path)
        else:
            executor = SparkExecutor(self.client, f'explore')
            depot_ctx = ExploreContext(self.client, executor)

        DepotKernel.depot_context = depot_ctx
        DepotKernel.spark_session = executor.spark
        super(DepotKernelApp, self).initialize(argv)


def start_kernel(uid, dir, conn, transform, client, envs):
    IPKernelApp.connection_file = conn
    IPKernelApp.connection_dir = dir
    IPKernelApp.ipython_dir = dir
    DepotKernelApp.client = client
    DepotKernelApp.transform = transform

    for k, v in envs.items():
        os.environ[k] = v
    os.chdir(dir)
    os.setgid(uid)
    os.setuid(uid)
    DepotKernelApp.launch_instance([])


class DepotKernelLauncher(Application):
    connection_file = Unicode().tag(config=True)
    depot_endpoint = Unicode().tag(config=True)
    access_key = Unicode().tag(config=True)
    transform = Unicode().tag(config=True)

    aliases = {
        'depot-endpoint': 'DepotKernelLauncher.depot_endpoint',
        'depot-access-key': 'DepotKernelLauncher.access_key',
        'depot-transform': 'DepotKernelLauncher.transform',
        'f': 'DepotKernelLauncher.connection_file'
    }

    def start(self):
        multiprocessing.set_start_method('spawn')

        sandbox_id = uuid.uuid4().hex

        if len(self.transform) > 0:
            sandbox_id = self.transform.split(',')[0]

        sandbox_uid = random.randint(2001, 2 ** 16 - 10)
        sandbox_dir = f'/sandbox/{sandbox_id}'
        sandbox_conn = f'{sandbox_dir}/.connection'

        with open('/etc/passwd', 'a') as f:
            f.write(f'{sandbox_id}:x:{sandbox_uid}:{sandbox_uid}:{sandbox_id}:{sandbox_dir}:/bin/false\n')

        def cleanup():
            shutil.rmtree(sandbox_dir)
            subprocess.run(['userdel', '-f', sandbox_id], capture_output=True)

        os.makedirs(sandbox_dir, mode=0o700, exist_ok=True)
        atexit.register(cleanup)

        os.chown(sandbox_dir, sandbox_uid, sandbox_uid)

        parent_conn = self.connection_file
        os.chown(parent_conn, sandbox_uid, sandbox_uid)
        os.link(parent_conn, sandbox_conn)

        mpl_cfg = f'{sandbox_dir}/.matplotlib'
        envs = {'MPLCONFIGDIR': mpl_cfg}

        client = DepotClient(self.depot_endpoint, self.access_key)
        p = multiprocessing.Process(target=start_kernel, args=(sandbox_uid, sandbox_dir, sandbox_conn, self.transform, client, envs))
        p.daemon = True
        p.start()
        try:
            p.join()
        except:
            pass


if __name__ == "__main__":
    DepotKernelLauncher.launch_instance()
