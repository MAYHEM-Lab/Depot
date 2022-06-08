#!/usr/bin/env python3

import logging

import tornado.ioloop
from tornado import ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import define, options
from tornado.web import RequestHandler
from tornado.websocket import WebSocketHandler, websocket_connect

from depot_client import DepotClient

logger = logging.getLogger('depot.notebook.master')
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger.setLevel(logging.INFO)


class ClusterForwarderMixin:
    def initialize(self, client):
        self.client = client

    async def forwarding(self, protocol, server_req):
        request_parts = server_req.uri.split('/', 3)
        entity = request_parts[1]
        cluster = request_parts[2]

        cluster_info = self.client.cluster(entity, cluster)
        notebook_master = cluster_info['notebook']['notebook_master']

        req = HTTPRequest(protocol + '://' + notebook_master + '/' + request_parts[3])
        req.allow_nonstandard_methods = True
        req.method = server_req.method
        req.headers = server_req.headers
        req.body = server_req.body
        return req


class SocketProxy(ClusterForwarderMixin, WebSocketHandler):
    def initialize(self, client):
        super(SocketProxy, self).initialize(client)
        self.conn = None

    def check_origin(self, origin: str) -> bool:
        return True

    async def get(self, *args, **kwargs):
        req = await self.forwarding('ws', self.request)
        subprotocol_header = self.request.headers.get('Sec-WebSocket-Protocol')
        if subprotocol_header:
            subprotocols = [s.strip() for s in subprotocol_header.split(',')]
        else:
            subprotocols = []
        self.conn = await websocket_connect(req, subprotocols=subprotocols)
        await super(SocketProxy, self).get(*args, **kwargs)

    async def on_message(self, message):
        if self.conn:
            binary = self.selected_subprotocol == 'v1.kernel.websocket.jupyter.org'
            await self.conn.write_message(message, binary=binary)
        else:
            self.close()

    @property
    def ping_interval(self):
        return 60

    def on_close(self):
        if self.conn:
            self.conn.close()

    async def open(self):
        async def proxy_loop():
            while True:
                msg = await self.conn.read_message()
                if msg is None:
                    break
                if self.ws_connection:
                    binary = self.ws_connection.selected_subprotocol == 'v1.kernel.websocket.jupyter.org'
                    await self.ws_connection.write_message(msg, binary=binary)
                else:
                    self.close()
            self.close()

        ioloop.IOLoop.current().spawn_callback(proxy_loop)

    def select_subprotocol(self, subprotocols):
        preferred_protocol = 'v1.kernel.websocket.jupyter.org'
        return preferred_protocol if preferred_protocol in subprotocols else None


class RouterHandler(ClusterForwarderMixin, RequestHandler):
    def initialize(self, client):
        super(RouterHandler, self).initialize(client)

    async def proxy(self):
        req = await self.forwarding('http', self.request)
        resp = await AsyncHTTPClient().fetch(req, raise_error=False)
        self.set_status(resp.code)
        for k, v in resp.headers.get_all():
            if k != 'Content-Security-Policy':
                self.add_header(k, v)
        if len(resp.body) > 0:
            self.write(resp.body)

    async def get(self):
        await self.proxy()

    async def patch(self):
        await self.proxy()

    async def post(self):
        await self.proxy()

    async def delete(self):
        await self.proxy()


if __name__ == '__main__':
    define('port', default=9995, help='port to listen on')
    define('depot-endpoint', help='Depot API server endpoint')
    define('depot-access-key', help='Depot admin access key')

    tornado.options.parse_command_line()
    d_client = DepotClient(options.depot_endpoint, options.depot_access_key)

    app = tornado.web.Application([
        (r'/.*/api/kernels/.*/channels', SocketProxy, dict(client=d_client)),
        (r'/.*', RouterHandler, dict(client=d_client)),
    ])
    app.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
