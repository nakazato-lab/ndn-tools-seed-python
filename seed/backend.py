"""Kubernetes backend; all blocking calls run outside the NDN event loop."""
import hashlib
import json
import logging
import os
import re
import time

import grpc
import function_pb2
import function_pb2_grpc
from kubernetes import client, config
from opentelemetry import trace

tracer = trace.get_tracer(__name__)
LOG = logging.getLogger(__name__)
MAX_LOG_BYTES = 64 * 1024


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
        pods = self.api.list_namespaced_pod(self.namespace, _request_timeout=10).items
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

    def _get_pod_manifest(self, prefix, code, runtime, requirements=(), zip_content=None, source_file=None):
        if runtime not in ('ndn', 'python'):
            raise ValueError(f'Unsupported function runtime: {runtime}')
        filename = 'func.ndn' if runtime == 'ndn' else 'func.py'
        if zip_content is not None:
            filename = source_file
        code_path = f'/app/code/{filename}'
        runtime_image = self.ndn_function_image if runtime == 'ndn' else self.python_function_image
        name = get_pod_name(prefix, self.node_name)
        metadata = {
            'name': name,
            'labels': {'app': 'edge-function', 'managed-by': 'ndn-seed-python'},
            'annotations': {'ndn-prefix': prefix},
        }
        mounts = [{'name': 'code', 'mountPath': '/app/code', 'readOnly': True},
                  {'name': 'shared', 'mountPath': '/app/shared'},
                  {'name': 'nfd-endpoint', 'mountPath': '/etc/ndn-config'}]
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
                                f"Path({('/code/' + filename)!r}).write_text(os.environ['FUNCTION_CODE'], encoding='utf-8') "
                                "if 'FUNCTION_CODE' in os.environ else None; "
                                "Path('/code/requirements.txt').write_text("
                                "'\\n'.join(json.loads(os.environ['FUNCTION_REQUIREMENTS'])) + '\\n', encoding='utf-8')"],
                    'env': [
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
                     '50051 "$FUNCTION_CODE_PATH"',
                 ],
                 'env': [{'name': 'FUNCTION_CODE_PATH', 'value': code_path.replace('$', '$$')}],
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
                 'env': [{'name': 'FUNCTION_CODE_PATH', 'value': code_path.replace('$', '$$')}],
                 'volumeMounts': mounts.copy()},
            ],
            'volumes': [
                {'name': 'code', 'emptyDir': {}},
                {'name': 'shared', 'emptyDir': {}},
                {'name': 'nfd-endpoint', 'emptyDir': {}},
            ],
        }
        if zip_content is None:
            spec['initContainers'][1]['env'].append({'name': 'FUNCTION_CODE', 'value': code.replace('$', '$$')})
        for container in spec['containers']:
            container['env'].append({
                'name': 'OTEL_EXPORTER_OTLP_ENDPOINT',
                'value': os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://jaeger.jaeger.svc.cluster.local:4318'),
            })
        if zip_content is not None:
            function = spec['containers'][0]
            function['env'].append({'name': 'FUNCTION_PACKAGE_REQUIRED', 'value': '1'})
            function['volumeMounts'][0] = {'name': 'code', 'mountPath': '/app/code'}
        if runtime == 'python' and requirements:
            spec['initContainers'].append({
                'name': 'install-python-dependencies',
                'image': runtime_image,
                'command': ['/bin/sh', '-ec'],
                'args': [
                    'python3 -m pip install --no-cache-dir --target /code/site-packages '
                    '-r /code/requirements.txt',
                ],
                'volumeMounts': [
                    {'name': 'code', 'mountPath': '/code'},
                ],
            })
            spec['containers'][0]['env'].append({
                'name': 'PYTHONPATH', 'value': '/app/code/site-packages',
            })
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

    @tracer.start_as_current_span('seed.deploy')
    def create(self, prefix, code, runtime, requirements=(), zip_content=None, source_file=None):
        trace.get_current_span().set_attribute('ndn.function.name', prefix)
        trace.get_current_span().set_attribute('k8s.node.name', self.node_name or '')
        if not self.node_name:
            raise ValueError('NODE_NAME is required to place a function')
        if self._find_function_pod(prefix) is not None:
            trace.get_current_span().add_event('seed.pod.reused')
            return
        manifest = self._get_pod_manifest(prefix, code, runtime, requirements, zip_content, source_file)
        with tracer.start_as_current_span('seed.create.pod', attributes={'k8s.pod.name': manifest['metadata']['name']}):
            self.api.create_namespaced_pod(self.namespace, manifest, _request_timeout=10)
        return manifest['metadata']['name']

    def finish_create(self, prefix, pod_name, zip_content=None):
        try:
            if zip_content is not None:
                self._upload_package(pod_name, zip_content)
        except Exception:
            try:
                self.api.delete_namespaced_pod(pod_name, self.namespace, _request_timeout=10)
            except Exception:
                LOG.exception('Could not delete Function Pod after package upload failure: %s', pod_name)
            raise
        self._delete_old_function_pods(prefix)

    def wait_for_placement_result(self, prefix, timeout=120):
        try:
            pod = self._find_function_pod(prefix)
        except Exception as exc:
            return {
                'name': prefix, 'node_name': self.node_name, 'phase': 'Unknown',
                'reason': 'PodStatusUnavailable', 'message': str(exc),
            }
        if pod is None:
            return {
                'name': prefix, 'node_name': self.node_name, 'phase': 'Unknown',
                'reason': 'PodNotFound', 'message': 'Function Pod not found',
            }
        return self._monitor_function_pod(prefix, pod, timeout)

    def _monitor_function_pod(self, prefix, pod, timeout):
        pod_name = pod.metadata.name
        pod_uid = pod.metadata.uid
        deadline = time.monotonic() + timeout
        while True:
            try:
                pod = self.api.read_namespaced_pod(pod_name, self.namespace, _request_timeout=10)
            except Exception as exc:
                return {
                    'name': prefix, 'pod_name': pod_name, 'node_name': self.node_name, 'phase': 'Unknown',
                    'reason': 'PodStatusUnavailable', 'message': str(exc),
                }
            # Stop monitoring if the original Pod is replaced or being deleted.
            if pod.metadata.uid != pod_uid or pod.metadata.deletion_timestamp:
                result = {
                    'name': prefix, 'pod_name': pod_name, 'node_name': self.node_name,
                    'phase': 'Unknown', 'reason': 'PodReplacedOrDeleted',
                    'message': 'Function Pod was replaced or deleted during startup',
                }
                return result
            containers_with_execution_failure = self._containers_with_execution_failure(pod)
            # Pending may be normal startup; wait until the deadline.
            if (not containers_with_execution_failure
                    and pod.status.phase == 'Pending' and time.monotonic() < deadline):
                time.sleep(1)
                continue

            return self.collect_placement_result(prefix, pod)

    def collect_placement_result(self, prefix, pod):
        result = {
            'name': prefix, 'pod_name': pod.metadata.name, 'node_name': self.node_name,
            'phase': pod.status.phase or 'Unknown',
        }
        containers_with_execution_failure = self._containers_with_execution_failure(pod)
        if containers_with_execution_failure:
            self._collect_container_error_logs(pod.metadata.name, containers_with_execution_failure, result)
        elif pod.status.phase == 'Running':
            return result
        else:
            self._collect_events(pod.metadata.uid, result)
        return result

    def _containers_with_execution_failure(self, pod):
        # fields for pod.status: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodStatus.md
        statuses = (pod.status.init_container_statuses or []) + (pod.status.container_statuses or [])
        containers_with_execution_failure = []
        for status in statuses:
            if self._has_container_execution_failure(status):
                containers_with_execution_failure.append(status)
        return containers_with_execution_failure

    def _has_container_execution_failure(self, status):
        # fields for ContainerStatus: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ContainerStatus.md
        state = status.state
        if state is None:
            return False
        # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#container-state-waiting
        if state.waiting and state.waiting.reason == 'CrashLoopBackOff':
            return True
        # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-behavior-comparison
        if state.terminated and state.terminated.exit_code != 0:
            return True
        return False

    def _collect_container_error_logs(self, pod_name, containers, result):
        logs = {}
        errors = {}
        remaining = MAX_LOG_BYTES
        for status in containers:
            if remaining <= 0:
                break
            # Use previous logs for CrashLoopBackOff, current logs for terminated containers.
            if status.state.waiting is not None:
                previous = True # https://kubernetes.io/docs/tasks/debug/debug-application/debug-running-pod/#examine-pod-logs
            else:
                previous = False
            try:
                text = self.api.read_namespaced_pod_log(
                    pod_name, self.namespace, container=status.name, previous=previous,
                    tail_lines=100, limit_bytes=remaining, _request_timeout=2)
                raw = text.encode('utf-8')[-remaining:]
                logs[status.name] = raw.decode('utf-8', errors='replace')
                remaining -= len(raw)
            except Exception as exc:
                errors[status.name] = str(exc)
        if logs:
            result['logs'] = logs
        if errors:
            result['log_errors'] = errors

    def _collect_events(self, pod_uid, result):
        try:
            events = self.api.list_namespaced_event(
                self.namespace, field_selector=f'involvedObject.uid={pod_uid}', _request_timeout=5).items
            result['events'] = [
                {'type': event.type, 'reason': event.reason, 'message': event.message, 'count': event.count}
                for event in events
            ]
        except Exception as exc:
            result['event_error'] = str(exc)

    def _upload_package(self, pod_name, zip_content):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pod = self.api.read_namespaced_pod(pod_name, self.namespace, _request_timeout=10)
            if self._containers_with_execution_failure(pod) or pod.status.phase in ('Failed', 'Succeeded'):
                return
            address = pod.status.pod_ip
            if address:
                target = f'[{address}]:50051' if ':' in address else f'{address}:50051'
                with grpc.insecure_channel(target) as channel:
                    stub = function_pb2_grpc.FunctionRuntimeStub(channel)
                    stub.UploadPackage(
                        function_pb2.PackageRequest(zip_content=zip_content),
                        timeout=max(0, deadline - time.monotonic()), wait_for_ready=True)
                return
            time.sleep(1)
        raise TimeoutError(f'Timed out waiting for function Pod: {pod_name}')

    def delete(self, prefix):
        pod = self._find_function_pod(prefix)
        if pod is not None:
            self.api.delete_namespaced_pod(pod.metadata.name, self.namespace)
