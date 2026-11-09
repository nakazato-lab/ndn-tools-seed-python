import asyncio
import io
from pathlib import Path
import zipfile

import grpc
import function_pb2
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


class PackageReceiver:
    def __init__(self, code_path, package_required=False):
        self.code_path = Path(code_path).resolve()
        self.package_ready = not package_required

    async def UploadPackage(self, request, context):
        parent = TraceContextTextMapPropagator().extract(dict(context.invocation_metadata()), context=Context())
        with trace.get_tracer(__name__).start_as_current_span(
                'function.unpack.package', context=parent, kind=trace.SpanKind.SERVER):
            self.package_ready = False
            try:
                await asyncio.to_thread(self.unpack, request.zip_content)
            except (ValueError, OSError, zipfile.BadZipFile) as exc:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            self.package_ready = True
            return function_pb2.PackageResponse()

    def unpack(self, zip_content):
        # code_path comes from FUNCTION_CODE_PATH set by Seed when creating the Pod, not from gRPC.
        destination = self.code_path.parent
        # Open the received ZIP bytes and extract all files into the function file's parent directory.
        with zipfile.ZipFile(io.BytesIO(zip_content)) as archive:
            archive.extractall(destination)
        self.load_source()
