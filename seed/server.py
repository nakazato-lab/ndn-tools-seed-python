import asyncio
import json
import logging

from ndn.encoding import Name

LOG = logging.getLogger(__name__)


def parse_request(raw):
    doc = json.loads(bytes(raw).decode('utf-8'))
    if not isinstance(doc, dict):
        raise ValueError('ApplicationParameters must be a JSON object')
    if doc.get('type') not in ('CREATE', 'DELETE'):
        raise ValueError('type must be CREATE or DELETE')
    name = doc.get('name')
    if not isinstance(name, str) or not name.strip():
        raise ValueError('name must be a nonempty string')
    name = Name.to_str(Name.normalize('/' + name.lstrip('/')))
    if name == '/':
        raise ValueError('name cannot be the root prefix')
    if doc['type'] == 'CREATE':
        code = doc.get('content')
        if not isinstance(code, str):
            raise ValueError('content must be a string')
        # Pass source text through unchanged; language validation belongs to
        # the function runtime. content_type is ignored, as in the C++ Seed.
    return doc['type'], name, doc.get('content')


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
        self.active = set()
        self.tasks = set()
        self.lock = asyncio.Lock()

    async def register(self):
        if not await self.app.register(self.prefix, self.on_interest, validator=accept_command):
            raise RuntimeError(f'NFD rejected prefix registration: {self.prefix}')
        LOG.info('Registered Seed prefix: %s', self.prefix)

    def on_interest(self, name, _param, app_param):
        task = asyncio.create_task(self.handle(name, app_param))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def handle(self, name, app_param):
        try:
            async with self.lock:
                if app_param is not None:
                    operation, prefix, code = parse_request(app_param)
                    if operation == 'CREATE':
                        LOG.info('Received function: %s\n%s', prefix, code)
                        await asyncio.to_thread(self.backend.create, prefix, code)
                        self.active.add(prefix)
                    else:
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
