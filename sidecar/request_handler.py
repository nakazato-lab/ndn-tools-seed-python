"""Translate NDN function Interests into execution requests and result references."""
import asyncio
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

import grpc
import function_pb2
from ndn.encoding import Name
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from segmented_node_wrapper import SegmentedNodeWrapper

from lib.ndn_utils import (
    decode_and_remove_metadata_from_interest, decode_json_string_argument,
    extract_function_name, parse_function_args,
)

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class FunctionRequestHandler:
    def __init__(self, app, runtime_client):
        self.app = app
        self.runtime_client = runtime_client
        self.publisher = SegmentedNodeWrapper(app, serve=True)

    async def handle(self, name, _param, prefix, app_param=None):
        try:
            original = Name.normalize(name)
            original_str = Name.to_str(original)
            LOG.info('Received Interest: %s', original_str)
            if original == Name.normalize(prefix + '/code'):
                self.send_source(name, original_str)
                return
            parameters = json.loads(bytes(app_param)) if app_param else {}
            parent = TraceContextTextMapPropagator().extract(parameters)
            with tracer.start_as_current_span('sidecar.handler', context=parent, kind=trace.SpanKind.SERVER,
                                             attributes={'ndn.name': original_str, 'ndn.function.name': prefix}):
                request = await self.build_request(original, parameters)
                LOG.info('Execution request: %s; forwarding to Function container via gRPC, args=%r',
                         original_str, list(request.args))
                with tracer.start_as_current_span('sidecar.grpc.execute_func', kind=trace.SpanKind.CLIENT):
                    carrier = {}
                    TraceContextTextMapPropagator().inject(carrier)
                    content, reference_key = await asyncio.to_thread(
                        self.runtime_client.execute, request, list(carrier.items()))
                await self.publish_result(name, prefix, content, reference_key)
        except Exception as exc:
            LOG.exception('Function request failed')
            detail = exc.details() if isinstance(exc, grpc.RpcError) else str(exc)
            self.app.put_data(name, content=f'Error: {detail}'.encode(), freshness_period=0)

    def send_source(self, name, original_str):
        LOG.info('Code request: %s; serving source without executing the function', original_str)
        source = Path(os.getenv('FUNCTION_CODE_PATH', '/app/func.ndn')).read_bytes()
        self.app.put_data(name, content=source, freshness_period=0)
        LOG.info('Returned function code: name=%s bytes=%d', original_str, len(source))

    async def build_request(self, original, parameters):
        decoded_interest = decode_and_remove_metadata_from_interest(original)
        raw_args = parse_function_args(decoded_interest)
        # Unquoted NDN names are deferred Interests, resolved by the Sidecar.
        interest_args = [arg for arg in raw_args if arg.startswith('/')]
        args = [decode_json_string_argument(arg) for arg in raw_args if arg and not arg.startswith('/')]
        image_ref = parameters.get('image_ref')
        if image_ref in args:
            args.remove(image_ref)
        request = function_pb2.FunctionRequest(name=extract_function_name(original), args=args)
        if interest_args:
            request.binary_input = await self.fetch_interest_image(interest_args[0])
        elif image_ref:
            with tracer.start_as_current_span('sidecar.fetch.arg_image', attributes={'ndn.name': image_ref}):
                request.binary_input = await SegmentedNodeWrapper(self.app).fetch(image_ref, must_be_fresh=True)
        return request

    async def fetch_interest_image(self, name):
        with tracer.start_as_current_span('sidecar.fetch.arg_image', kind=trace.SpanKind.CLIENT,
                                         attributes={'ndn.name': name}):
            carrier = {}
            TraceContextTextMapPropagator().inject(carrier)
            _, _, content = await self.app.express_interest(
                name, app_param=json.dumps(carrier).encode('utf-8') if carrier else None,
                must_be_fresh=True, can_be_prefix=True, lifetime=20000)
            raw_content = bytes(content)
            if raw_content.startswith(b'Error:'):
                raise RuntimeError(f'{name} failed: {raw_content.decode("utf-8", errors="replace")}')
            image_ref = json.loads(raw_content)['image_ref']
            return await SegmentedNodeWrapper(self.app).fetch(image_ref, must_be_fresh=True)

    async def publish_result(self, name, prefix, content, reference_key):
        if not content:
            self.app.put_data(name, content=b'', freshness_period=0)
            return
        result_name = f'{prefix}/result/{uuid4().hex}'
        with tracer.start_as_current_span('sidecar.publish.result', attributes={
                'ndn.name': result_name, 'ndn.content.bytes': len(content)}):
            reference = await self.publisher.publish(result_name, content)
        self.app.put_data(name, content=json.dumps({reference_key: reference}).encode('utf-8'), freshness_period=0)
