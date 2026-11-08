"""gRPC bridge to `ndnc run source.ndn ARG ...`."""
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
import grpc
import function_pb2
import function_pb2_grpc
from ndn_runtime import DeployedFunction
from package_receiver import PackageReceiver

LOG = logging.getLogger(__name__)


class FunctionRuntimeServicer(PackageReceiver, function_pb2_grpc.FunctionRuntimeServicer):
    def __init__(self, code_path, package_required=False):
        PackageReceiver.__init__(self, code_path, package_required)
        source = Path(code_path).read_text(encoding='utf-8')
        self.function = DeployedFunction.load(source, code_path=code_path)
        LOG.info('Loaded .ndn source from %s (%d bytes)', code_path, len(source.encode('utf-8')))

    async def ExecuteFunction(self, request, context):
        if not self.package_ready:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Function package has not been uploaded')
        try:
            remaining = context.time_remaining()
            async with asyncio.timeout(min(remaining, 20) if remaining is not None else 20):
                result = await self.function.execute(list(request.args))
            if isinstance(result, bytes):
                LOG.info('Executed %s: binary result (%d bytes)', request.name, len(result))
                return function_pb2.FunctionResponse(binary_result=result)
            LOG.info('Executed %s: result=%r', request.name, result)
            return function_pb2.FunctionResponse(result=result)
        except TimeoutError:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, 'Function execution timed out')
        except Exception as exc:
            LOG.exception('Function execution failed')
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))


async def serve(port, code_path):
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', -1)])
    function_pb2_grpc.add_FunctionRuntimeServicer_to_server(
        FunctionRuntimeServicer(code_path, package_required=os.getenv('FUNCTION_PACKAGE_REQUIRED') == '1'), server)
    if not server.add_insecure_port(f'0.0.0.0:{port}'):
        raise RuntimeError(f'Cannot bind gRPC port {port}')
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await server.start()
    LOG.info('ndnc CLI runtime listening on %s', port)
    try:
        await stop.wait()
    finally:
        await server.stop(grace=2)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve(
        sys.argv[1] if len(sys.argv) > 1 else '50051',
        sys.argv[2] if len(sys.argv) > 2 else os.getenv('FUNCTION_CODE_PATH', '/app/func.ndn')))
