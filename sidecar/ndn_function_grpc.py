"""Forward NDN function requests to the gRPC runtime on the same Pod."""
import asyncio
import json
import logging
import os
import signal
from uuid import uuid4
from pathlib import Path
import grpc
from ndn.encoding import Name
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import function_pb2
import function_pb2_grpc
from segmented_node_wrapper import SegmentedNodeWrapper
from ndn_transport import make_app
from tracing import startup_context
from lib.ndn_utils import extract_function_args, extract_function_name

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class NDNFunction:
    def __init__(self):
        self.app = make_app()
        self.tasks = set()
        self.prefix = None
        self.registered = False
        self.publisher = SegmentedNodeWrapper(self.app, serve=True)
        port = os.getenv('GRPC_PORT', '50051')
        self.channel = grpc.insecure_channel(
            f'localhost:{port}', options=[('grpc.max_receive_message_length', -1),
                                         ('grpc.max_send_message_length', -1)])
        self.stub = function_pb2_grpc.FunctionRuntimeStub(self.channel)

    async def handle(self, name, param, prefix, app_param=None):
        try:
            original = Name.normalize(name)
            original_str = Name.to_str(original)
            LOG.info('Received Interest: %s', Name.to_str(name))
            if original == Name.normalize(prefix + '/code'):
                LOG.info('Code request: %s; serving source without executing the function', original_str)
                # ndnc clients fetch source before deciding where to execute.
                source = Path(os.getenv('FUNCTION_CODE_PATH', '/app/func.ndn')).read_bytes()
                self.app.put_data(name, content=source, freshness_period=0)
                LOG.info('Returned function code: name=%s bytes=%d', original_str, len(source))
                return
            args = [arg for arg in extract_function_args(original) if arg]
            parameters = json.loads(bytes(app_param)) if app_param else {}
            # Restore the parent context from traceparent and tracestate in ApplicationParameters.
            # Other keys, such as image_ref, are ignored and parameters is left unchanged.
            parent = TraceContextTextMapPropagator().extract(parameters)
            with tracer.start_as_current_span('sidecar.handler', context=parent, kind=trace.SpanKind.SERVER,
                                             attributes={'ndn.name': original_str, 'ndn.function.name': prefix}):
                image_ref = parameters.get('image_ref')
                # The image name is for readability; the bytes are passed through stdin.
                if image_ref in args:
                    args.remove(image_ref)
                request = function_pb2.FunctionRequest(name=extract_function_name(original), args=args)
                if image_ref:
                    # Fetch the image argument from ndnc running on the Invoker
                    # and pass it to the Function via gRPC.
                    with tracer.start_as_current_span('sidecar.fetch.arg_image', attributes={'ndn.name': image_ref}):
                        request.binary_input = await SegmentedNodeWrapper(self.app).fetch(image_ref, must_be_fresh=True)
                LOG.info('Execution request: %s; forwarding to Function container via gRPC, args=%r',
                         original_str, args)
                # Keep the NDN loop available for data fetches and nested calls.
                with tracer.start_as_current_span('sidecar.grpc.execute_func', kind=trace.SpanKind.CLIENT):
                    carrier = {}
                    TraceContextTextMapPropagator().inject(carrier)
                    response = await asyncio.to_thread(
                        self.stub.ExecuteFunction, request, timeout=20, metadata=list(carrier.items()))
                if response.HasField('binary_result'):
                    content = response.binary_result
                    reference_key = 'image_ref'
                else:
                    content = response.result.encode('utf-8')
                    reference_key = 'result_ref'
                # The wrapper requires nonempty content; preserve empty execution results.
                if not content:
                    self.app.put_data(name, content=b'', freshness_period=0)
                    return
                result_name = f'{prefix}/result/{uuid4().hex}'
                # Publish the execution result and return its reference to the Invoker.
                with tracer.start_as_current_span('sidecar.publish.result', attributes={
                        'ndn.name': result_name, 'ndn.content.bytes': len(content)}):
                    reference = await self.publisher.publish(result_name, content)
                self.app.put_data(name, content=json.dumps({reference_key: reference}).encode('utf-8'), freshness_period=0)
        except Exception as exc:
            LOG.exception('Function request failed')
            detail = exc.details() if isinstance(exc, grpc.RpcError) else str(exc)
            self.app.put_data(name, content=f'Error: {detail}'.encode(), freshness_period=0)

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

    async def _run(self, prefix):
        self.prefix = prefix
        loop = asyncio.get_running_loop()
        stopping = False

        async def shutdown():
            nonlocal stopping
            if stopping:
                return
            stopping = True
            await self.unregister()
            self.app.shutdown()

        def request_shutdown():
            loop.create_task(shutdown())

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, request_shutdown)

        def on_interest(name, param, app_param):
            task = asyncio.create_task(self.handle(name, param, prefix, app_param))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

        async def started():
            with tracer.start_as_current_span('sidecar.register', context=startup_context(),
                                             attributes={'ndn.function.name': prefix}):
                if not await self.app.register(prefix, on_interest):
                    raise RuntimeError(f'NFD rejected function prefix: {prefix}')
                self.registered = True
                LOG.info('Function prefix registered: %s', prefix)

        try:
            await self.app.main_loop(after_start=started())
        finally:
            if self.registered:
                await self.unregister()
            self.channel.close()

    def run(self, prefix):
        asyncio.run(self._run(prefix))
