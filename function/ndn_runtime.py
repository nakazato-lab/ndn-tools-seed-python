"""Execute the published ndnc CLI without importing its parser or interpreter."""
import asyncio
import base64
import logging
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeployedFunction:
    source: str
    code_path: Path | None = None

    @classmethod
    def load(cls, source, code_path=None):
        # Deployment only stores source. ndnc validates it when executed.
        return cls(source, Path(code_path).resolve() if code_path is not None else None)

    async def execute(self, args):
        # Each invocation owns its file, output and process, including during
        # concurrent execution and redeployment.
        with tempfile.TemporaryDirectory(prefix='ndnc-function-') as directory:
            path = self.code_path
            if path is None:
                path = Path(directory) / 'function.ndn'
                path.write_text(self.source, encoding='utf-8')
            result_path = Path(directory) / 'result.json'
            # No shell: arguments are passed literally, including leading '-'.
            with trace.get_tracer(__name__).start_as_current_span('function.ndnc') as span:
                environment = os.environ.copy()
                environment['OTEL_SERVICE_NAME'] = 'ndn-interpreter'
                carrier = {}
                TraceContextTextMapPropagator().inject(carrier)
                environment.update({key.upper(): value for key, value in carrier.items()})
                process = await asyncio.create_subprocess_exec(
                    'ndnc', 'run', '--result-file', str(result_path), str(path), '--', *args,
                    env=environment,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await process.communicate()
                finally:
                    # gRPC cancellation and execution timeouts must stop ndnc too.
                    if process.returncode is None:
                        try:
                            process.kill()
                        except ProcessLookupError:
                            pass
                        await process.communicate()
                span.set_attribute('process.exit.code', process.returncode)
                if process.returncode != 0:
                    span.set_status(trace.Status(trace.StatusCode.ERROR, 'ndnc execution failed'))
            error = stderr.decode('utf-8', errors='replace').strip()
            if process.returncode != 0:
                raise RuntimeError(f'ndnc run failed (exit {process.returncode}): {error}')
            if error:
                LOG.info('ndnc stderr: %s', error)
            if stdout:
                LOG.info('ndnc stdout: %s', stdout.decode('utf-8', errors='replace').rstrip('\n'))
            result = json.loads(result_path.read_text(encoding='utf-8'))
            if isinstance(result, dict) and result.get('encoding') == 'base64':
                return base64.b64decode(result['data'], validate=True)
            return '' if result is None else str(result)
