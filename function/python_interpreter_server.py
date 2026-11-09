"""gRPC bridge that directly executes deployed Python scripts."""
import asyncio
import logging
import os
import signal
import subprocess
import sys

import grpc
import function_pb2
import function_pb2_grpc
from package_receiver import PackageReceiver
from opentelemetry import trace
from tracing import setup_tracing, function_span

LOG = logging.getLogger(__name__)


class PythonFunctionServicer(PackageReceiver, function_pb2_grpc.FunctionRuntimeServicer):
    def __init__(self, code_path, package_required=False):
        PackageReceiver.__init__(self, code_path, package_required)
        if not package_required:
            self.load_source()

    def load_source(self):
        LOG.info('Loaded .py source from %s (%d bytes)', self.code_path, self.code_path.stat().st_size)

    async def ExecuteFunction(self, request, context):
        with function_span(request, context):
            if not self.package_ready:
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Function package has not been uploaded')
            try:
                with trace.get_tracer(__name__).start_as_current_span('function.python'):
                    completed = await asyncio.to_thread(
                        subprocess.run,
                        [sys.executable, self.code_path, *request.args],
                        # Pass the image argument to the Python script through stdin.
                        input=request.binary_input if request.HasField('binary_input') else None,
                        capture_output=True,
                        timeout=20,
                        check=True,
                    )
                if request.HasField('binary_input'):
                    # Return the processed image written to stdout by the Python script.
                    return function_pb2.FunctionResponse(binary_result=completed.stdout)
                try:
                    result = completed.stdout.decode('utf-8').rstrip('\n')
                except UnicodeDecodeError:
                    LOG.info('Executed %s: binary result=%d bytes', request.name, len(completed.stdout))
                    return function_pb2.FunctionResponse(binary_result=completed.stdout)
                LOG.info('Executed %s: result=%r', request.name, result)
                return function_pb2.FunctionResponse(result=result)
            except subprocess.TimeoutExpired:
                await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, 'Function execution timed out')
            except subprocess.CalledProcessError as exc:
                LOG.exception('Function execution failed')
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, exc.stderr.decode('utf-8', errors='replace').strip())
            except Exception as exc:
                LOG.exception('Function execution failed')
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))


async def serve(port, code_path):
    provider = setup_tracing('function')
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', -1)])
    try:
        runtime = PythonFunctionServicer(code_path, package_required=os.getenv('FUNCTION_PACKAGE_REQUIRED') == '1')
        function_pb2_grpc.add_FunctionRuntimeServicer_to_server(runtime, server)
        if not server.add_insecure_port(f'0.0.0.0:{port}'):
            raise RuntimeError(f'Cannot bind gRPC port {port}')
        await server.start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        LOG.info('Python runtime listening on %s', port)
        try:
            await stop.wait()
        finally:
            await server.stop(grace=2)
    finally:
        if provider is not None:
            provider.shutdown()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve(
        sys.argv[1] if len(sys.argv) > 1 else '50051',
        sys.argv[2] if len(sys.argv) > 2 else os.getenv('FUNCTION_CODE_PATH', '/app/func.py')))
