"""Load function source code or a ZIP from a registration request."""
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


class FunctionSourceLoader:
    def __init__(self, fetcher):
        self.fetcher = fetcher

    async def load(self, request):
        zip_ref = request.get('zip_content')
        if zip_ref:
            return {
                'code': None,
                'zip_content': await self.load_package(zip_ref),
                'source_file': request['source'],
            }
        return {'code': await self.load_code(request.get('content'), request.get('code_ref'))}

    async def load_package(self, zip_ref):
        with tracer.start_as_current_span('seed.fetch.package', attributes={'ndn.name': zip_ref}):
            return await self.fetcher.fetch(zip_ref, must_be_fresh=True)

    async def load_code(self, code, code_ref):
        if code_ref is not None:
            try:
                with tracer.start_as_current_span('seed.fetch.code', attributes={'ndn.name': code_ref}):
                    code = (await self.fetcher.fetch(code_ref, must_be_fresh=True)).decode('utf-8')
            except UnicodeDecodeError as exc:
                raise ValueError('function code must be UTF-8 text') from exc
        if not isinstance(code, str) or not code.strip() or '\x00' in code:
            raise ValueError('content must be nonempty text without NUL characters')
        return code
