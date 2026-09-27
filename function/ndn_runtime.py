"""Execute the published ndnc CLI without importing its parser or interpreter."""
import asyncio
import logging
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeployedFunction:
    source: str

    @classmethod
    def load(cls, source):
        # Deployment only stores source. ndnc validates it when executed.
        return cls(source)

    async def execute(self, args):
        # Each invocation owns its file, output and process, including during
        # concurrent execution and redeployment.
        with tempfile.TemporaryDirectory(prefix='ndnc-function-') as directory:
            path = Path(directory) / 'function.ndn'
            path.write_text(self.source, encoding='utf-8')
            result_path = Path(directory) / 'result.json'
            # No shell: arguments are passed literally, including leading '-'.
            process = await asyncio.create_subprocess_exec(
                'ndnc', 'run', '--result-file', str(result_path), str(path), '--', *args,
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
            error = stderr.decode('utf-8', errors='replace').strip()
            if process.returncode != 0:
                raise RuntimeError(f'ndnc run failed (exit {process.returncode}): {error}')
            if error:
                LOG.info('ndnc stderr: %s', error)
            if stdout:
                LOG.info('ndnc stdout: %s', stdout.decode('utf-8', errors='replace').rstrip('\n'))
            result = json.loads(result_path.read_text(encoding='utf-8'))
            return '' if result is None else str(result)
