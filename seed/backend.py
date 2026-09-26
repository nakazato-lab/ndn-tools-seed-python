"""Kubernetes backend; all blocking calls run outside the NDN event loop."""
import hashlib
import re

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


def resource_name(prefix):
    slug = re.sub(r'[^a-z0-9-]+', '-', prefix.lower()).strip('-')[:40] or 'function'
    return f"seed-{slug}-{hashlib.sha256(prefix.encode()).hexdigest()[:12]}"


class KubernetesBackend:
    def __init__(self, namespace, node_name, image, api=None):
        self.namespace = namespace
        self.node_name = node_name
        self.image = image
        if api is None:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            api = client.CoreV1Api()
        self.api = api

    def create(self, prefix, code):
        name = resource_name(prefix)
        metadata = {
            'name': name,
            'labels': {'app': 'edge-function', 'managed-by': 'ndn-seed-python'},
            'annotations': {'ndn-prefix': prefix},
        }
        # Do not silently overwrite a running function. DELETE first to replace it.
        self.api.create_namespaced_config_map(self.namespace, {
            'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': metadata,
            'data': {'func.py': code},
        }, _request_timeout=10)
        mounts = [{'name': 'code', 'mountPath': '/app/func.py', 'subPath': 'func.py'},
                  {'name': 'shared', 'mountPath': '/app/shared'}]
        spec = {
            'restartPolicy': 'Never',
            'containers': [
                {'name': 'function', 'image': self.image,
                 'command': ['python3', 'interpreter_server.py', '50051'],
                 'volumeMounts': mounts},
                {'name': 'sidecar', 'image': self.image,
                 'command': ['/bin/bash', '-ec'],
                 'args': ['python3 deploy.py 50051 /app/func.py; exec python3 main.py "$1" "$2"',
                          'seed-sidecar', prefix, self.namespace],
                 'env': [{'name': 'NDN_CLIENT_TRANSPORT', 'value': 'unix:///run/nfd.sock'}],
                 'volumeMounts': mounts + [{'name': 'nfd', 'mountPath': '/run'}]},
            ],
            'volumes': [
                {'name': 'code', 'configMap': {'name': name}},
                {'name': 'shared', 'emptyDir': {}},
                {'name': 'nfd', 'hostPath': {'path': '/var/run/nfd-k8s', 'type': 'Directory'}},
            ],
        }
        if self.node_name:
            spec['nodeName'] = self.node_name
        try:
            self.api.create_namespaced_pod(self.namespace, {
                'apiVersion': 'v1', 'kind': 'Pod', 'metadata': metadata, 'spec': spec,
            }, _request_timeout=10)
        except Exception:
            # ConfigMap was created by this request, so it is safe to roll it back.
            self.api.delete_namespaced_config_map(name, self.namespace, _request_timeout=10)
            raise

    def delete(self, prefix):
        name = resource_name(prefix)
        for delete in (self.api.delete_namespaced_pod, self.api.delete_namespaced_config_map):
            try:
                delete(name, self.namespace, _request_timeout=10)
            except ApiException as exc:
                if exc.status != 404:
                    raise
