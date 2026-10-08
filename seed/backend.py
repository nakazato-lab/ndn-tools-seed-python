"""Kubernetes backend; all blocking calls run outside the NDN event loop."""
import hashlib
import os
import re

from kubernetes import client, config


def get_pod_name(prefix, node_name):
    # e.g. /average → average
    slug = re.sub(r'[^a-z0-9-]+', '-', prefix.lower()).strip('-')[:40] or 'function'
    identity = prefix + "\0" + node_name
    return f"func-{slug}-{hashlib.sha256(identity.encode()).hexdigest()[:12]}"


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

    def _find_function_pod(self, prefix):
        pods = self.api.list_namespaced_pod(self.namespace).items
        # e.g. annotations: ndn-prefix: /average
        for pod in pods:
            annotations = pod.metadata.annotations or {}
            if annotations.get('ndn-prefix') == prefix and pod.spec.node_name == self.node_name:
                return pod
        return None

    def _delete_old_function_pods(self, prefix):
        pods = self.api.list_namespaced_pod(self.namespace).items
        for pod in pods:
            annotations = pod.metadata.annotations or {}
            if annotations.get('ndn-prefix') == prefix and pod.spec.node_name != self.node_name:
                self.api.delete_namespaced_pod(pod.metadata.name, self.namespace)

    def _get_pod_manifest(self, prefix, code):
        name = get_pod_name(prefix, self.node_name)
        metadata = {
            'name': name,
            'labels': {'app': 'edge-function', 'managed-by': 'ndn-seed-python'},
            'annotations': {'ndn-prefix': prefix},
        }
        mounts = [{'name': 'code', 'mountPath': '/app/func.ndn', 'subPath': 'func.ndn', 'readOnly': True},
                  {'name': 'shared', 'mountPath': '/app/shared'}]
        spec = {
            'restartPolicy': 'Never',
            'initContainers': [{
                'name': 'write-function-code',
                'image': self.function_image,
                'command': ['python3', '-c',
                            "import os; from pathlib import Path; "
                            "Path('/code/func.ndn').write_text(os.environ['FUNCTION_CODE'], encoding='utf-8')"],
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
        spec['nodeName'] = self.node_name
        return {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': metadata,
            'spec': spec,
        }

    def create(self, prefix, code):
        if not self.node_name:
            raise ValueError('NODE_NAME is required to place a function')
        if self._find_function_pod(prefix) is not None:
            return
        self.api.create_namespaced_pod(
            self.namespace, self._get_pod_manifest(prefix, code), _request_timeout=10)
        self._delete_old_function_pods(prefix)

    def delete(self, prefix):
        pod = self._find_function_pod(prefix)
        if pod is not None:
            self.api.delete_namespaced_pod(
                pod.metadata.name, self.namespace)
