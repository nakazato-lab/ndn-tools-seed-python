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

    async def apply(self, request):
        if request['type'] == 'CREATE':
            result = await self.deploy_and_wait(request)
            registerer_prefix = request.get('registerer_prefix')
            if registerer_prefix:
                await self.notifier.notify(registerer_prefix, result)
        else:
            async with self.lock:
                await self.delete(request['name'])

    async def deploy_and_wait(self, request):
        try:
            async with self.lock:
                await self.create(request)
            parent_span = trace.get_current_span()
            with tracer.start_as_current_span('seed.wait.func_start', attributes={
                    'ndn.function.name': request['name']}) as span:
                result = await asyncio.to_thread(self.backend.wait_for_placement_result, request['name'])
                span.set_attribute('k8s.pod.phase', result.get('phase', 'Unknown'))
                if (result.get('phase') != 'Running'
                        or result.get('logs') or result.get('log_errors') or result.get('startup_errors')):
                    reason = result.get('reason') or 'Function startup failed'
                    span.set_status(trace.StatusCode.ERROR, reason)
                    parent_span.set_status(trace.StatusCode.ERROR, reason)
                return result
        except Exception as exc:
            span = trace.get_current_span()
            span.record_exception(exc)
            span.set_status(trace.StatusCode.ERROR, str(exc))
            LOG.exception('Function deployment failed: %s', request['name'])
            return {
                'name': request['name'], 'node_name': self.backend.node_name,
                'phase': 'Unknown', 'reason': 'DeploymentError', 'message': str(exc),
            }

    async def create(self, request):
        prefix = request['name']
        runtime = request['runtime']
        requirements = request.get('requirements', [])
        with tracer.start_as_current_span('seed.create', attributes={'ndn.function.name': prefix}):
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

    async def delete(self, prefix):
        with tracer.start_as_current_span('seed.delete', attributes={'ndn.function.name': prefix}):
            await asyncio.to_thread(self.backend.delete, prefix)
            self.active.discard(prefix)
