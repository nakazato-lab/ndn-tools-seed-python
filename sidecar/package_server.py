"""Receive function code or a ZIP into the Pod's shared code volume."""
import asyncio
import io
from pathlib import Path
import zipfile

import grpc
import function_pb2
import function_pb2_grpc


class SidecarPackageReceiver(function_pb2_grpc.FunctionDeploymentServicer):
    def __init__(self, code_path, ready_path='/app/shared/package-ready'):
        self.code_path = Path(code_path).resolve()
        self.ready_path = Path(ready_path)

    async def UploadPackage(self, request, context):
        try:
            await asyncio.to_thread(self.write_package, request)
        except (ValueError, OSError, zipfile.BadZipFile) as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return function_pb2.PackageResponse()

    def write_package(self, request):
        self.code_path.parent.mkdir(parents=True, exist_ok=True)
        content = request.WhichOneof('content')
        if content == 'zip_content':
            with zipfile.ZipFile(io.BytesIO(request.zip_content)) as archive:
                archive.extractall(self.code_path.parent)
        elif content == 'code':
            self.code_path.write_text(request.code, encoding='utf-8')
        else:
            raise ValueError('Function code or ZIP is required')
        if not self.code_path.is_file():
            raise ValueError(f'Function source not found in package: {self.code_path.name}')
        # Both runtimes start only after all function files have been written.
        self.ready_path.touch()


async def start_package_server(code_path):
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', -1)])
    function_pb2_grpc.add_FunctionDeploymentServicer_to_server(SidecarPackageReceiver(code_path), server)
    if not server.add_insecure_port('0.0.0.0:50052'):
        raise RuntimeError('Cannot bind package upload port 50052')
    await server.start()
    return server
