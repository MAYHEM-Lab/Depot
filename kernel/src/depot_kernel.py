#!/usr/bin/env python3

from IPython.core.formatters import DisplayFormatter, BaseFormatter, PlainTextFormatter
from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelapp import IPKernelApp, kernel_aliases
from ipykernel.zmqshell import ZMQInteractiveShell
from pyspark.sql import SparkSession
from traitlets import Unicode, ObjectName, Instance

from depot_client import DepotClient
from depot_context import SparkExecutor, TransformContext, ExploreContext, DepotContext


class DepotFormatter(BaseFormatter):
    format_type = Unicode('application/depot-publish')
    print_method = ObjectName('_repr_depot_')
    _return_type = dict


class DepotInteractiveShell(ZMQInteractiveShell):
    ast_node_interactivity = 'last_expr_or_assign'

    DisplayFormatter.formatters = {
        'application/depot-publish': DepotFormatter(),
        'text/plain': PlainTextFormatter()
    }

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


kernel_aliases.update({
    'depot-endpoint': 'DepotKernelApp.depot_endpoint',
    'depot-access-key': 'DepotKernelApp.access_key',
    'depot-transform': 'DepotKernelApp.transform'
})


class DepotKernelApp(IPKernelApp):
    kernel_class = DepotKernel
    depot_endpoint = Unicode().tag(config=True)
    access_key = Unicode().tag(config=True)
    transform = Unicode('').tag(config=True)
    default_extensions = []

    aliases = dict(kernel_aliases)

    def initialize(self, argv=None):
        self.parse_command_line(argv)

        client = DepotClient(self.depot_endpoint, self.access_key)
        executor = SparkExecutor(client)

        if len(self.transform) > 0:
            entity, tag, version, path = self.transform.split(',')
            depot_ctx = TransformContext(client, executor, entity, tag, int(version), path)
        else:
            depot_ctx = ExploreContext(client, executor)

        DepotKernel.depot_context = depot_ctx
        DepotKernel.spark_session = executor.spark
        super(DepotKernelApp, self).initialize(argv)


if __name__ == "__main__":
    DepotKernelApp.launch_instance()
