"""Persistent gRPC server that calls a deployed Python execute(input_bytes, args) function."""
import asyncio
import logging
import importlib.util
import os
import signal
import sys
from pathlib import Path

import grpc
import function_pb2
import function_pb2_grpc
from opentelemetry import trace
from tracing import setup_tracing, function_span

LOG = logging.getLogger(__name__)


class PythonFunctionServicer(function_pb2_grpc.FunctionRuntimeServicer):
    def __init__(self, code_path):
        self.code_path = Path(code_path).resolve()
        self.function = None

    def load_source(self):
        source_dir = str(self.code_path.parent)
        if source_dir not in sys.path:
            sys.path.insert(0, source_dir)
        spec = importlib.util.spec_from_file_location('deployed_function', self.code_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        if not callable(getattr(module, 'execute', None)):
            raise ValueError('Python function must define execute(input_bytes, args)')
        self.function = module.execute
        LOG.info('Loaded Python function from %s', self.code_path)

    def execute(self, input_bytes, args):
        with trace.get_tracer(__name__).start_as_current_span('function.python'):
            if self.function is None:
                self.load_source()
            return self.function(input_bytes, args)

    async def Execute(self, request, context):
        with function_span(request, context):
            # Check whether a regular file exists at this path.
            if self.function is None and not self.code_path.is_file():
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Function source has not been deployed')
            try:
                input_bytes = request.binary_input if request.HasField('binary_input') else None
                result = self.execute(input_bytes, list(request.args))
                if isinstance(result, bytes):
                    return function_pb2.ExecutionResult(binary_result=result)
                if not isinstance(result, str) and result is not None:
                    raise TypeError('execute() must return bytes, str, or None')
                return function_pb2.ExecutionResult(result=result or '')
            except Exception as exc:
                LOG.exception('Function execution failed')
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))


async def serve(port, code_path):
    provider = setup_tracing('function')
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', -1)])
    try:
        runtime = PythonFunctionServicer(code_path)
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
