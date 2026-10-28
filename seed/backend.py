"""Kubernetes backend; all blocking calls run outside the NDN event loop."""
import hashlib
import json
import os
import re

from kubernetes import client, config


def get_pod_name(prefix, node_name):
    # e.g. /average → average
    slug = re.sub(r'[^a-z0-9-]+', '-', prefix.lower()).strip('-')[:40] or 'function'
    identity = prefix + "\0" + node_name
    return f"func-{slug}-{hashlib.sha256(identity.encode()).hexdigest()[:12]}"


class KubernetesBackend:
    def __init__(self, namespace, node_name, ndn_function_image, sidecar_image, python_function_image,
                 api=None, transport=None):
        self.namespace = namespace
        self.node_name = node_name
        self.ndn_function_image = ndn_function_image
        self.python_function_image = python_function_image
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

    def _get_pod_manifest(self, prefix, code, runtime, requirements=()):
        if runtime not in ('ndn', 'python'):
            raise ValueError(f'Unsupported function runtime: {runtime}')
        filename = 'func.ndn' if runtime == 'ndn' else 'func.py'
        code_path = f'/app/{filename}'
        runtime_image = self.ndn_function_image if runtime == 'ndn' else self.python_function_image
        name = get_pod_name(prefix, self.node_name)
        metadata = {
            'name': name,
            'labels': {'app': 'edge-function', 'managed-by': 'ndn-seed-python'},
            'annotations': {'ndn-prefix': prefix},
        }
        mounts = [{'name': 'code', 'mountPath': code_path, 'subPath': filename, 'readOnly': True},
                  {'name': 'shared', 'mountPath': '/app/shared'},
                  {'name': 'nfd-endpoint', 'mountPath': '/etc/ndn-config'}]
        if runtime == 'python':
            mounts.append({'name': 'python-deps', 'mountPath': '/opt/function-deps', 'readOnly': True})
        spec = {
            'restartPolicy': 'Never',
            'initContainers': [
                {
                    'name': 'nfd-address',
                    'image': 'ghcr.io/nakazato-lab/ndn-dhcp/ndn-dhcp-client:latest',
                    'imagePullPolicy': 'Always',
                    'env': [{'name': 'NODE_NAME', 'valueFrom': {
                        'fieldRef': {'fieldPath': 'spec.nodeName'}}}],
                    'volumeMounts': [{'name': 'nfd-endpoint', 'mountPath': '/etc/ndn-config'}],
                },
                {
                    'name': 'write-function-code',
                    'image': runtime_image,
                    'command': ['python3', '-c',
                                "import json, os; from pathlib import Path; "
                                f"Path('/code/{filename}').write_text(os.environ['FUNCTION_CODE'], encoding='utf-8'); "
                                "Path('/code/requirements.txt').write_text("
                                "'\\n'.join(json.loads(os.environ['FUNCTION_REQUIREMENTS'])) + '\\n', encoding='utf-8')"],
                    'env': [
                        {'name': 'FUNCTION_CODE', 'value': code.replace('$', '$$')},
                        {'name': 'FUNCTION_REQUIREMENTS', 'value': json.dumps(list(requirements))},
                    ],
                    'volumeMounts': [{'name': 'code', 'mountPath': '/code'}],
                },
            ],
            'containers': [
                {'name': 'function', 'image': runtime_image,
                 # Resolve the NFD selected for this Pod by its DHCP init
                 # container instead of inheriting the Seed Pod's transport.
                 'command': ['/bin/sh', '-ec'],
                 'args': [
                     'while [ ! -f /etc/ndn-config/ADDRESS ] || '
                     '[ "$(cat /etc/ndn-config/ADDRESS)" = "not available yet" ]; do sleep 2; done; '
                     'export NDN_CLIENT_TRANSPORT="tcp4://$(cat /etc/ndn-config/ADDRESS)"; '
                     'exec python3 '
                     f"{'ndn_interpreter_server.py' if runtime == 'ndn' else 'python_interpreter_server.py'} "
                     f'50051 {code_path}',
                 ],
                 'env': [{'name': 'FUNCTION_CODE_PATH', 'value': code_path}],
                 'volumeMounts': mounts.copy()},
                {'name': 'sidecar', 'image': self.sidecar_image,
                 'command': ['/bin/bash', '-ec'],
                 'args': [
                     'while [ ! -f /etc/ndn-config/ADDRESS ] || '
                     '[ "$(cat /etc/ndn-config/ADDRESS)" = "not available yet" ]; do sleep 2; done; '
                     'export NDN_CLIENT_TRANSPORT="tcp4://$(cat /etc/ndn-config/ADDRESS)"; '
                     'exec python3 main.py "$1"',
                     'seed-sidecar', prefix,
                 ],
                 'env': [{'name': 'FUNCTION_CODE_PATH', 'value': code_path}],
                 'volumeMounts': mounts.copy()},
            ],
            'volumes': [
                {'name': 'code', 'emptyDir': {}},
                {'name': 'shared', 'emptyDir': {}},
                {'name': 'nfd-endpoint', 'emptyDir': {}},
            ],
        }
        if runtime == 'python':
            spec['initContainers'].append({
                'name': 'install-python-dependencies',
                'image': runtime_image,
                'command': ['/bin/sh', '-ec'],
                'args': [
                    'python3 -m pip install --no-cache-dir --target /opt/function-deps '
                    '-r /code/requirements.txt',
                ],
                'volumeMounts': [
                    {'name': 'code', 'mountPath': '/code', 'readOnly': True},
                    {'name': 'python-deps', 'mountPath': '/opt/function-deps'},
                ],
            })
            spec['containers'][0]['env'].append({
                'name': 'PYTHONPATH', 'value': '/opt/function-deps',
            })
            spec['volumes'].append({'name': 'python-deps', 'emptyDir': {}})
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

    def create(self, prefix, code, runtime, requirements=()):
        if not self.node_name:
            raise ValueError('NODE_NAME is required to place a function')
        if self._find_function_pod(prefix) is not None:
            return
        self.api.create_namespaced_pod(
            self.namespace, self._get_pod_manifest(prefix, code, runtime, requirements), _request_timeout=10)
        self._delete_old_function_pods(prefix)

    def delete(self, prefix):
        pod = self._find_function_pod(prefix)
        if pod is not None:
            self.api.delete_namespaced_pod(
                pod.metadata.name, self.namespace)
