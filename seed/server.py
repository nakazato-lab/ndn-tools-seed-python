import asyncio
import json
import logging
from pathlib import Path

from ndn.encoding import Name
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
    return doc['type'], name, code, code_ref, runtime, requirements, zip_ref, source_file



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
            async with self.lock:
                content = ('\n'.join(sorted(self.active)) + '\n') if self.active else 'no server created\n'
            self._send_response(name, content)
            return

        try:
            operation, prefix, code, code_ref, runtime, requirements, zip_ref, source_file = parse_request(app_param)
            parent = TraceContextTextMapPropagator().extract(json.loads(bytes(app_param)), context=Context())
        except Exception as exc:
            LOG.exception('Invalid Seed request')
            self._send_response(name, f'Error: {exc}')
            return

        self._send_response(name, 'ACK\n')

        try:
            with tracer.start_as_current_span(f'seed.{operation.lower()}', context=parent, kind=trace.SpanKind.SERVER,
                                             attributes={'ndn.function.name': prefix}):
                async with self.lock:
                    if operation == 'CREATE':
                        await self._create_function(prefix, code, code_ref, runtime, requirements, zip_ref, source_file)
                        self.active.add(prefix)
                    elif operation == 'DELETE':
                        await asyncio.to_thread(self.backend.delete, prefix)
                        self.active.discard(prefix)
        except Exception:
            LOG.exception('Seed %s failed for %s', operation, prefix)

    async def _create_function(self, prefix, code, code_ref, runtime, requirements, zip_ref, source_file):
        package = {}
        if zip_ref:
            with tracer.start_as_current_span('seed.fetch.package', attributes={'ndn.name': zip_ref}):
                package['zip_content'] = await self.fetcher.fetch(zip_ref, must_be_fresh=True)
            package['source_file'] = source_file
            code = None
        else:
            code = await self._load_code(code, code_ref)
        LOG.info('Received %s function: %s', runtime, prefix)
        await asyncio.to_thread(self.backend.create, prefix, code, runtime, requirements, **package)

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
