"""Forward NDN function requests to the gRPC runtime on the same Pod."""
import asyncio
import logging
import os
from pathlib import Path
import grpc
from ndn.encoding import Name, Component
import function_pb2
import function_pb2_grpc
from ndn_transport import make_app
from lib.ndn_utils import (
    SEGMENT_SIZE, extract_first_level_args, extract_my_function_name,
    is_function_request, get_original_name,
)

LOG = logging.getLogger(__name__)


class NDNFunction:
    def __init__(self):
        self.app = make_app()
        self.segmented_data = {}
        self.tasks = set()
        port = os.getenv('GRPC_PORT', '50051')
        self.channel = grpc.insecure_channel(f'localhost:{port}')
        self.stub = function_pb2_grpc.FunctionRuntimeStub(self.channel)

    async def handle(self, name, param, prefix, data_request_handler):
        try:
            original = get_original_name(name)
            original_str = Name.to_str(original)
            LOG.info('Received Interest: %s', Name.to_str(name))
            if Component.get_type(name[-1]) == Component.TYPE_SEGMENT:
                segment = Component.to_number(name[-1])
                packets = self.segmented_data[original_str]
                self.app.put_raw_packet(packets[segment])
                LOG.info('Returned cached Data segment: name=%s segment=%d', original_str, segment)
                return
            if original == Name.normalize(prefix + '/code'):
                LOG.info('Code request: %s; serving source without executing the function', original_str)
                # ndnc clients fetch source before deciding where to execute.
                source = Path(os.getenv('FUNCTION_CODE_PATH', '/app/func.ndn')).read_bytes()
                self.app.put_data(name, content=source, freshness_period=0)
                LOG.info('Returned function code: name=%s bytes=%d', original_str, len(source))
                return
            if is_function_request(original):
                args = [arg for arg in extract_first_level_args(original) if arg]
                request = function_pb2.FunctionRequest(name=extract_my_function_name(original), args=args)
                LOG.info('Execution request: %s; forwarding to Function container via gRPC, args=%r',
                         original_str, args)
                # Keep the NDN loop available for data fetches and nested calls.
                response = await asyncio.to_thread(self.stub.ExecuteFunction, request, timeout=20)
                content = response.result.encode()
                LOG.info('Function execution completed: name=%s result=%r', original_str, response.result)
            else:
                content = data_request_handler(Name.to_str(name)).encode()
            if not param.can_be_prefix:
                # ndn-compiler uses exact-name Interests for remote execution.
                self.app.put_data(name, content=content, freshness_period=0)
                LOG.info('Returned result Data: name=%s bytes=%d', original_str, len(content))
                return
            count = max(1, (len(content) + SEGMENT_SIZE - 1) // SEGMENT_SIZE)
            packets = [self.app.prepare_data(
                original + [Component.from_segment(i)],
                content=content[i * SEGMENT_SIZE:(i + 1) * SEGMENT_SIZE],
                freshness_period=10000, final_block_id=Component.from_segment(count - 1),
            ) for i in range(count)]
            self.segmented_data[original_str] = packets
            self.app.put_raw_packet(packets[0])
            LOG.info('Returned first result Data segment: name=%s segments=%d bytes=%d',
                     original_str, count, len(content))
        except Exception as exc:
            LOG.exception('Function request failed')
            detail = exc.details() if isinstance(exc, grpc.RpcError) else str(exc)
            self.app.put_data(name, content=f'Error: {detail}'.encode(), freshness_period=0)

    def run(self, prefix, data_request_handler):
        prefix = Name.to_str(Name.normalize(prefix)).rstrip('/')

        def on_interest(name, param, _app_param):
            task = asyncio.create_task(self.handle(name, param, prefix, data_request_handler))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

        async def started():
            if not await self.app.register(prefix, on_interest):
                raise RuntimeError(f'NFD rejected function prefix: {prefix}')
            LOG.info('Function prefix registered: %s', prefix)

        try:
            self.app.run_forever(after_start=started())
        finally:
            self.channel.close()
