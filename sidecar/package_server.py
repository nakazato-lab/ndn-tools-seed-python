"""Receive function code or a ZIP into the Pod's shared code volume."""
import asyncio
import io
from pathlib import Path
import tempfile
import zipfile

import grpc
import function_pb2
import function_pb2_grpc


class SidecarPackageReceiver(function_pb2_grpc.FunctionDeploymentServicer):
    def __init__(self, code_path):
        self.code_path = Path(code_path).resolve()

    async def UploadPackage(self, request, context):
        try:
            await asyncio.to_thread(self.write_package, request)
        except (ValueError, OSError, zipfile.BadZipFile) as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return function_pb2.PackageResponse()

    def write_package(self, request):
        destination = self.code_path.parent
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Prevent the Function from reading files while they are being written.
        with tempfile.TemporaryDirectory(prefix='.function-', dir=destination.parent) as temporary:
            directory = Path(temporary)
            content = request.WhichOneof('content')
            if content == 'zip_content':
                with zipfile.ZipFile(io.BytesIO(request.zip_content)) as archive:
                    archive.extractall(directory)
            elif content == 'code':
                (directory / self.code_path.name).write_text(request.code, encoding='utf-8')
            else:
                raise ValueError('Function code or ZIP is required')
            if not (directory / self.code_path.name).is_file():
                raise ValueError(f'Function source not found in package: {self.code_path.name}')
            # Publish the source and all attachments together after writing finishes.
            directory.rename(destination)


async def start_package_server(code_path):
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', -1)])
    function_pb2_grpc.add_FunctionDeploymentServicer_to_server(SidecarPackageReceiver(code_path), server)
    if not server.add_insecure_port('0.0.0.0:50052'):
        raise RuntimeError('Cannot bind package upload port 50052')
    await server.start()
    return server
