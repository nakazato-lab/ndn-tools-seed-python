"""gRPC bridge that directly executes deployed Python scripts."""
import asyncio
import os
import signal
import subprocess
import sys

import grpc
import function_pb2
import function_pb2_grpc

class PythonFunctionServicer(function_pb2_grpc.FunctionRuntimeServicer):
    def __init__(self, code_path):
        self.code_path = code_path

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
            return function_pb2.FunctionResponse(
                result=completed.stdout.rstrip('\n'))
        except subprocess.TimeoutExpired:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, 'Function execution timed out')
        except subprocess.CalledProcessError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, exc.stderr.strip())
        except Exception as exc:
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
    try:
        await stop.wait()
    finally:
        await server.stop(grace=2)


if __name__ == '__main__':
    asyncio.run(serve(
        sys.argv[1] if len(sys.argv) > 1 else '50051',
        sys.argv[2] if len(sys.argv) > 2 else os.getenv('FUNCTION_CODE_PATH', '/app/func.py')))
