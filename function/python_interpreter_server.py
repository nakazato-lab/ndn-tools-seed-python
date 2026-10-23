"""gRPC bridge that directly executes deployed Python scripts."""
import asyncio
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path

import grpc
import function_pb2
import function_pb2_grpc

LOG = logging.getLogger(__name__)


class PythonFunctionServicer(function_pb2_grpc.FunctionRuntimeServicer):
    def __init__(self, code_path):
        self.code_path = code_path
        LOG.info('Loaded .py source from %s (%d bytes)', code_path, Path(code_path).stat().st_size)

    async def ExecuteFunction(self, request, context):
        try:
            completed = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, self.code_path, *request.args],
                capture_output=True,
                text=True,
                errors='replace',
                timeout=20,
                check=True,
            )
            result = completed.stdout.rstrip('\n')
            LOG.info('Executed %s: result=%r', request.name, result)
            return function_pb2.FunctionResponse(result=result)
        except subprocess.TimeoutExpired:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, 'Function execution timed out')
        except subprocess.CalledProcessError as exc:
            LOG.exception('Function execution failed')
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, exc.stderr.strip())
        except Exception as exc:
            LOG.exception('Function execution failed')
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))


async def serve(port, code_path):
    server = grpc.aio.server()
    function_pb2_grpc.add_FunctionRuntimeServicer_to_server(PythonFunctionServicer(code_path), server)
    if not server.add_insecure_port(f'0.0.0.0:{port}'):
        raise RuntimeError(f'Cannot bind gRPC port {port}')
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await server.start()
    LOG.info('Python runtime listening on %s', port)
    try:
        await stop.wait()
    finally:
        await server.stop(grace=2)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve(
        sys.argv[1] if len(sys.argv) > 1 else '50051',
        sys.argv[2] if len(sys.argv) > 2 else os.getenv('FUNCTION_CODE_PATH', '/app/func.py')))
