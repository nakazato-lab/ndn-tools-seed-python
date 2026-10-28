import asyncio
import json
import logging

from ndn.encoding import Name
from segmented_node_wrapper import SegmentedNodeWrapper

LOG = logging.getLogger(__name__)
SEED_CODE_PREFIX = '/manager/functions'


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
    if doc['type'] == 'CREATE':
        if code_ref is not None:
            if not isinstance(code_ref, str) or not code_ref:
                raise ValueError('code_ref must be a nonempty name')
        elif not isinstance(code, str) or not code.strip() or '\x00' in code:
            raise ValueError('content must be nonempty text without NUL characters')
        if runtime not in ('ndn', 'python'):
            raise ValueError("runtime must be 'ndn' or 'python'")
    return doc['type'], name, code, code_ref, runtime



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
        self.fetcher = SegmentedNodeWrapper(app, SEED_CODE_PREFIX, serve=False)
        self.active = set()
        self.tasks = set()
        self.lock = asyncio.Lock()

    async def register(self):
        await self.fetcher.start()
        if not await self.app.register(self.prefix, self.on_interest, validator=accept_command):
            raise RuntimeError(f'NFD rejected prefix registration: {self.prefix}')
        LOG.info('Registered Seed prefix: %s', self.prefix)

    def on_interest(self, name, _param, app_param):
        task = asyncio.create_task(self.handle(name, app_param))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def handle(self, name, app_param):
        try:
            operation = None
            if app_param is not None:
                operation, prefix, code, code_ref, runtime = parse_request(app_param)
            async with self.lock:
                if operation == 'CREATE':
                    if code_ref is not None:
                        try:
                            code = (await self.fetcher.fetch(
                                code_ref, must_be_fresh=True)).decode('utf-8')
                        except UnicodeDecodeError as exc:
                            raise ValueError('function code must be UTF-8 text') from exc
                    if not isinstance(code, str) or not code.strip() or '\x00' in code:
                        raise ValueError('content must be nonempty text without NUL characters')
                    LOG.info('Received %s function: %s', runtime, prefix)
                    await asyncio.to_thread(self.backend.create, prefix, code, runtime)
                    self.active.add(prefix)
                elif operation == 'DELETE':
                    await asyncio.to_thread(self.backend.delete, prefix)
                    self.active.discard(prefix)
                content = ('\n'.join(sorted(self.active)) + '\n') if self.active else 'no server created\n'
        except Exception as exc:
            LOG.exception('Seed request failed')
            content = f'Error: {exc}'
        options = {'freshness_period': self.freshness}
        if self.final:
            options['final_block_id'] = name[-1]
        # Echo the entire received Name, including the parameters digest.
        try:
            self.app.put_data(name, content=content.encode(), **options)
        except Exception:
            LOG.exception('Could not send Seed response')
