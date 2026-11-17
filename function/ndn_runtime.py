"""Execute the published ndnc CLI without importing its parser or interpreter."""
import asyncio
import logging
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
import psutil

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeployedFunction:
    source: str

    @classmethod
    def load(cls, source):
        # Deployment only stores source. ndnc validates it when executed.
        return cls(source)

    async def monitor_memory(self,process,interval=0.05):
        # あるPIDに対してpsutilによってndnc起動中のメモリ値を0.05sのインターバルで取得し、最高値を返す。
        peak_rss = 0
        try:
            ndnc_process = psutil.Process(process.pid)# psutilがpidを捕まえてprocessオブジェクトを作れるか。
        except psutil.NoSuchProcess:
            return peak_rss
        while process.returncode is None: # ndncが終了しない限り
            try:
                rss = ndnc_process.memory_info().rss
                peak_rss = max(peak_rss, rss)
            except psutil.NoSuchProcess:
                break

            await asyncio.sleep(interval)

        return peak_rss


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

            monitor_task = asyncio.create_task(self.monitor_memory(process))  # このタスク(A)実行中でも、ndnc側のタスク（B）を止めない。

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

            peak_rss = await monitor_task
            LOG.info('ndnc peak memory usage: %.2f MiB', peak_rss / 1024 / 1024)

            return '' if result is None else str(result)
