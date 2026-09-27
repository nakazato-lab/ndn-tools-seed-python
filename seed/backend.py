"""Kubernetes backend; all blocking calls run outside the NDN event loop."""
import hashlib
import os
import re
import time

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


def resource_name(prefix):
    slug = re.sub(r'[^a-z0-9-]+', '-', prefix.lower()).strip('-')[:40] or 'function'
    return f"func-{slug}-{hashlib.sha256(prefix.encode()).hexdigest()[:12]}"


class KubernetesBackend:
    def __init__(self, namespace, node_name, function_image, sidecar_image, api=None, transport=None):
        self.namespace = namespace
        self.node_name = node_name
        self.function_image = function_image
        self.sidecar_image = sidecar_image
        self.transport = transport or os.getenv("NDN_CLIENT_TRANSPORT", "unix:///run/nfd.sock")
        if api is None:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            api = client.CoreV1Api()
        self.api = api

    def _wait_until_deleted(self, read, name, kind):
        """Wait until Kubernetes returns 404 for a resource."""
        deadline = time.monotonic() + 60
        while True:
            try:
                read(name, self.namespace, _request_timeout=10)
            except ApiException as exc:
                if exc.status == 404:
                    return
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{kind} {name} was not deleted within 60 seconds')
            time.sleep(0.25)

    def _delete_existing(self, name):
        """Delete the existing function Pod, when present."""
        # Delete the existing function Pod before recreating it.
        try:
            self.api.delete_namespaced_pod(name, self.namespace, _request_timeout=10)
        except ApiException as exc:
            if exc.status != 404:
                raise
        else:
            self._wait_until_deleted(self.api.read_namespaced_pod, name, 'Pod')

    def create(self, prefix, code):
        if not isinstance(code, str) or not code.strip() or '\x00' in code:
            raise ValueError('code must be nonempty text without NUL characters')
        name = resource_name(prefix)
        metadata = {
            'name': name,
            'labels': {'app': 'edge-function', 'managed-by': 'ndn-seed-python'},
            'annotations': {'ndn-prefix': prefix},
        }
        # CREATE replaces a prior deployment of the same NDN prefix.
        self._delete_existing(name)
        mounts = [{'name': 'code', 'mountPath': '/app/func.ndn', 'subPath': 'func.ndn', 'readOnly': True},
                  {'name': 'shared', 'mountPath': '/app/shared'}]
        spec = {
            'restartPolicy': 'Never',
            # Write the received code before either application container starts.
            'initContainers': [{
                'name': 'write-function-code',
                'image': self.function_image,
                'command': ['python3', '-c',
                            "import os; from pathlib import Path; "
                            "Path('/code/func.ndn').write_text(os.environ['FUNCTION_CODE'], encoding='utf-8')"],
                # Escape Kubernetes $(VAR) expansion so source text stays literal.
                'env': [{'name': 'FUNCTION_CODE', 'value': code.replace('$', '$$')}],
                'volumeMounts': [{'name': 'code', 'mountPath': '/code'}],
            }],
            'containers': [
                {'name': 'function', 'image': self.function_image,
                 'command': ['python3', 'interpreter_server.py', '50051', '/app/func.ndn'],
                 'env': [{'name': 'NDN_CLIENT_TRANSPORT', 'value': self.transport}],
                 'volumeMounts': mounts.copy()},
                {'name': 'sidecar', 'image': self.sidecar_image,
                 'command': ['/bin/bash', '-ec'],
                 'args': ['exec python3 main.py "$1"', 'seed-sidecar', prefix],
                 'env': [{'name': 'NDN_CLIENT_TRANSPORT', 'value': self.transport}],
                 'volumeMounts': mounts.copy()},
            ],
            'volumes': [
                {'name': 'code', 'emptyDir': {}},
                {'name': 'shared', 'emptyDir': {}},
            ],
        }
        if self.transport.startswith('unix://'):
            spec['volumes'].append({
                'name': 'nfd', 'hostPath': {'path': '/var/run/nfd-k8s', 'type': 'Directory'},
            })
            for container in spec['containers']:
                container['volumeMounts'].append({'name': 'nfd', 'mountPath': '/run'})
        if self.node_name:
            spec['nodeName'] = self.node_name
        self.api.create_namespaced_pod(self.namespace, {
            'apiVersion': 'v1', 'kind': 'Pod', 'metadata': metadata, 'spec': spec,
        }, _request_timeout=10)

    def delete(self, prefix):
        # Deleting the Pod also removes its code volume (emptyDir).
        self._delete_existing(resource_name(prefix))
