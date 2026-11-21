"""Manage the Sidecar's NDN route and server lifecycle."""
import asyncio
import logging
import os
import signal

from ndn_transport import make_app
from package_server import start_package_server
from request_handler import FunctionRequestHandler
from runtime_client import FunctionRuntimeClient

LOG = logging.getLogger(__name__)


class SidecarServer:
    def __init__(self):
        self.app = make_app()
        self.tasks = set()
        self.prefix = None
        self.registered = False
        self.stopping = False
        self.runtime_client = FunctionRuntimeClient(
            os.getenv('FUNCTION_RUNTIME', 'ndn'), os.getenv('GRPC_PORT', '50051'))
        self.handler = FunctionRequestHandler(self.app, self.runtime_client)

    def on_interest(self, name, param, app_param):
        task = asyncio.create_task(self.handler.handle(name, param, self.prefix, app_param))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def register(self):
        # Register the NDN route once the runtime accepts gRPC requests.
        await self.runtime_client.wait_until_ready()
        if not await self.app.register(self.prefix, self.on_interest):
            raise RuntimeError(f'NFD rejected function prefix: {self.prefix}')
        self.registered = True
        LOG.info('Function prefix registered: %s', self.prefix)

    async def unregister(self):
        """Remove the function route from NFD before its transport is closed."""
        if not self.registered:
            return True
        try:
            if not await self.app.unregister(self.prefix):
                LOG.error('NFD rejected function prefix removal: %s', self.prefix)
                return False
        except Exception:
            LOG.exception('Could not remove function prefix from NFD: %s', self.prefix)
            return False
        self.registered = False
        LOG.info('Function prefix unregistered: %s', self.prefix)
        return True

    async def shutdown(self):
        if self.stopping:
            return
        self.stopping = True
        await self.unregister()
        self.app.shutdown()

    def request_shutdown(self):
        asyncio.get_running_loop().create_task(self.shutdown())

    async def _run(self, prefix):
        self.prefix = prefix
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.request_shutdown)

        package_server = None
        try:
            package_server = await start_package_server(os.environ['FUNCTION_CODE_PATH'])
            await self.app.main_loop(after_start=self.register())
        finally:
            if self.registered:
                await self.unregister()
            self.runtime_client.close()
            if package_server is not None:
                await package_server.stop(grace=2)

    def run(self, prefix):
        asyncio.run(self._run(prefix))
