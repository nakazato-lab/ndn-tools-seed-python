import asyncio
import json
import logging
from pathlib import Path
from uuid import uuid4

from ndn.encoding import Name
from ndn.types import InterestTimeout
from segmented_node_wrapper import SegmentedNodeWrapper
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def parse_request(raw):
    doc = json.loads(bytes(raw).decode('utf-8'))
    if not isinstance(doc, dict):
        raise ValueError('ApplicationParameters must be a JSON object')
    if doc.get('type') not in ('CREATE', 'DELETE'):
        raise ValueError('type must be CREATE or DELETE')
    name = doc.get('name')
    if not isinstance(name, str) or not name.strip():
        raise ValueError('name must be a nonempty string')
    name = Name.to_str(Name.normalize('/' + name.strip('/'))) ## /oo/xx に統一する
    if name == '/':
        raise ValueError('name cannot be the root prefix')
    code = doc.get('content')
    code_ref = doc.get('code_ref')
    runtime = doc.get('runtime')
    requirements = doc.get('requirements', [])
    zip_ref = doc.get('zip_content')
    source_file = doc.get('source')
    if doc['type'] == 'CREATE':
        if zip_ref is None:
            if code_ref is not None:
                if not isinstance(code_ref, str) or not code_ref:
                    raise ValueError('code_ref must be a nonempty name')
            elif not isinstance(code, str) or not code.strip() or '\x00' in code:
                raise ValueError('content must be nonempty text without NUL characters')
        if runtime not in ('ndn', 'python'):
            raise ValueError("runtime must be 'ndn' or 'python'")
        if not isinstance(requirements, list) or any(
                not isinstance(requirement, str) or not requirement.strip()
                or '\x00' in requirement or '\n' in requirement or '\r' in requirement
                for requirement in requirements):
            raise ValueError('requirements must be a list of nonempty single-line strings')
        if runtime != 'python' and requirements:
            raise ValueError('requirements are supported only for the python runtime')
        if zip_ref is not None:
            if not isinstance(zip_ref, str) or not zip_ref.strip():
                raise ValueError('zip_content must be a nonempty name')
            if (not isinstance(source_file, str) or not source_file.strip()
                    or source_file in ('.', '..') or Path(source_file).name != source_file):
                raise ValueError('source must be a filename when zip_content is provided')
    doc['name'] = name
    return doc



async def accept_command(_name, _signature):
    # Match the C++ seed: unsigned Manager commands are accepted. python-ndn
    # separately checks the ParametersSha256Digest before invoking this validator.
    return True


class SeedServer:
    def __init__(self, app, backend, prefix, freshness=0, final=False):
        self.app = app
        self.backend = backend
        self.prefix = prefix
        self.freshness = freshness
        self.final = final
        self.fetcher = SegmentedNodeWrapper(app)
        self.publisher = SegmentedNodeWrapper(app, serve=True)
        self.active = set()
        self.tasks = set()
        self.lock = asyncio.Lock()
        self.registered = False

    async def register(self):
        if not await self.app.register(self.prefix, self.on_interest, validator=accept_command):
            raise RuntimeError(f'NFD rejected prefix registration: {self.prefix}')
        self.registered = True
        LOG.info('Registered Seed prefix: %s', self.prefix)

    async def unregister(self):
        """Remove this Seed's route from NFD while its face is still alive."""
        if not self.registered:
            return True
        try:
            if not await self.app.unregister(self.prefix):
                LOG.error('NFD rejected Seed prefix removal: %s', self.prefix)
                return False
        except Exception:
            LOG.exception('Could not remove Seed prefix from NFD: %s', self.prefix)
            return False
        self.registered = False
        LOG.info('Unregistered Seed prefix: %s', self.prefix)
        return True

    def _send_response(self, name, content):
        options = {'freshness_period': self.freshness}
        if self.final:
            options['final_block_id'] = name[-1]
        # Echo the entire received Name, including the parameters digest.
        try:
            self.app.put_data(name, content=content.encode(), **options)
        except Exception:
            LOG.exception('Could not send Seed response')

    def on_interest(self, name, _param, app_param):
        task = asyncio.create_task(self.handle(name, app_param))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def handle(self, name, app_param):
        if app_param is None:
            LOG.error('Seed request is missing ApplicationParameters: %s', Name.to_str(name))
            self._send_response(name, 'ACK\n')
            return

        try:
            request = parse_request(app_param)
            parent = TraceContextTextMapPropagator().extract(request, context=Context())
        except Exception as exc:
            LOG.exception('Invalid Seed request')
            self._send_response(name, f'Error: {exc}')
            return

        self._send_response(name, 'ACK\n')

        try:
            await self._apply_request(request, parent)
        except Exception:
            LOG.exception('Seed %s failed for %s', request['type'], request['name'])

    async def _apply_request(self, request, parent):
        if request['type'] == 'CREATE':
            result = await self._deploy_and_wait(request, parent)
            registerer_prefix = request.get('registerer_prefix')
            if registerer_prefix:
                await self._notify_placement_result(registerer_prefix, result)
        else:
            async with self.lock:
                await self._delete_function(request['name'], parent)

    async def _deploy_and_wait(self, request, parent):
        try:
            async with self.lock:
                await self._deploy_function(request, parent)
            return await asyncio.to_thread(self.backend.wait_for_placement_result, request['name'])
        except Exception as exc:
            LOG.exception('Function deployment failed: %s', request['name'])
            return {
                'name': request['name'], 'node_name': self.backend.node_name,
                'phase': 'Unknown', 'reason': 'DeploymentError', 'message': str(exc),
            }

    async def _deploy_function(self, request, parent):
        prefix = request['name']
        with tracer.start_as_current_span('seed.create', context=parent, kind=trace.SpanKind.SERVER,
                                         attributes={'ndn.function.name': prefix}):
            pod_name, package = await self._create_function(request)
        if pod_name is not None:
            await asyncio.to_thread(
                self.backend.finish_create, prefix, pod_name,
                code=package['code'], zip_content=package.get('zip_content'))
        self.active.add(prefix)

    async def _delete_function(self, prefix, parent):
        with tracer.start_as_current_span('seed.delete', context=parent, kind=trace.SpanKind.SERVER,
                                         attributes={'ndn.function.name': prefix}):
            await asyncio.to_thread(self.backend.delete, prefix)
            self.active.discard(prefix)

    async def _notify_placement_result(self, registerer_prefix, result):
        result_ref = await self.publisher.publish(
            f'{self.prefix}/results/{uuid4().hex}',
            json.dumps(result, ensure_ascii=False).encode('utf-8'))
        LOG.info('Notifying Registerer %s: result_ref=%s', registerer_prefix, result_ref)
        try:
            await self.app.express_interest(
                registerer_prefix, app_param=json.dumps({'result_ref': result_ref}).encode('utf-8'),
                must_be_fresh=True, can_be_prefix=False, lifetime=1000)
        except InterestTimeout:
            # Result notifications are one-way; Registerer does not return a Data ACK.
            pass

    async def _create_function(self, request):
        prefix = request['name']
        runtime = request['runtime']
        requirements = request.get('requirements', [])
        zip_ref = request.get('zip_content')
        package = {}
        if zip_ref:
            with tracer.start_as_current_span('seed.fetch.package', attributes={'ndn.name': zip_ref}):
                package['zip_content'] = await self.fetcher.fetch(zip_ref, must_be_fresh=True)
            package['source_file'] = request['source']
            code = None
        else:
            code = await self._load_code(request.get('content'), request.get('code_ref'))
        LOG.info('Received %s function: %s', runtime, prefix)
        pod_name = await asyncio.to_thread(self.backend.create, prefix, code, runtime, requirements, **package)
        package['code'] = code
        return pod_name, package

    async def _load_code(self, code, code_ref):
        if code_ref is not None:
            try:
                with tracer.start_as_current_span('seed.fetch.code', attributes={'ndn.name': code_ref}):
                    code = (await self.fetcher.fetch(code_ref, must_be_fresh=True)).decode('utf-8')
            except UnicodeDecodeError as exc:
                raise ValueError('function code must be UTF-8 text') from exc
        if not isinstance(code, str) or not code.strip() or '\x00' in code:
            raise ValueError('content must be nonempty text without NUL characters')
        return code
