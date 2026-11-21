"""Coordinate Function Pod creation, startup monitoring and deletion."""
import asyncio
import logging

from opentelemetry import trace

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class FunctionDeployer:
    def __init__(self, backend, source_loader, notifier):
        self.backend = backend
        self.source_loader = source_loader
        self.notifier = notifier
        self.active = set()
        self.lock = asyncio.Lock()

    async def apply(self, request, parent):
        if request['type'] == 'CREATE':
            result = await self.deploy_and_wait(request, parent)
            registerer_prefix = request.get('registerer_prefix')
            if registerer_prefix:
                await self.notifier.notify(registerer_prefix, result)
        else:
            async with self.lock:
                await self.delete(request['name'], parent)

    async def deploy_and_wait(self, request, parent):
        try:
            async with self.lock:
                await self.create(request, parent)
            return await asyncio.to_thread(self.backend.wait_for_placement_result, request['name'])
        except Exception as exc:
            LOG.exception('Function deployment failed: %s', request['name'])
            return {
                'name': request['name'], 'node_name': self.backend.node_name,
                'phase': 'Unknown', 'reason': 'DeploymentError', 'message': str(exc),
            }

    async def create(self, request, parent):
        prefix = request['name']
        runtime = request['runtime']
        requirements = request.get('requirements', [])
        with tracer.start_as_current_span('seed.create', context=parent, kind=trace.SpanKind.SERVER,
                                         attributes={'ndn.function.name': prefix}):
            source = await self.source_loader.load(request)
            LOG.info('Received %s function: %s', runtime, prefix)
            pod_name = await asyncio.to_thread(
                self.backend.create, prefix, source['code'], runtime, requirements,
                zip_content=source.get('zip_content'), source_file=source.get('source_file'))
        if pod_name is not None:
            await asyncio.to_thread(
                self.backend.upload_function, prefix, pod_name,
                code=source['code'], zip_content=source.get('zip_content'))
        self.active.add(prefix)

    async def delete(self, prefix, parent):
        with tracer.start_as_current_span('seed.delete', context=parent, kind=trace.SpanKind.SERVER,
                                         attributes={'ndn.function.name': prefix}):
            await asyncio.to_thread(self.backend.delete, prefix)
            self.active.discard(prefix)
