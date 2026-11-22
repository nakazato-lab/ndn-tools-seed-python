"""Kubernetes backend; all blocking calls run outside the NDN event loop."""
import logging
import os
import time

import grpc
import function_pb2
import function_pb2_grpc
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .pod_manifest import PodManifestBuilder
from .pod_monitor import FunctionPodMonitor

tracer = trace.get_tracer(__name__)
LOG = logging.getLogger(__name__)


class KubernetesBackend:
    def __init__(self, namespace, node_name, ndn_function_image, sidecar_image, python_function_image,
                 api=None, transport=None):
        self.namespace = namespace
        self.node_name = node_name
        self.transport = transport or os.getenv("NDN_CLIENT_TRANSPORT", "unix:///run/nfd.sock")
        if api is None:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            api = client.CoreV1Api()
        self.api = api
        self.manifest_builder = PodManifestBuilder(
            node_name, ndn_function_image, sidecar_image, python_function_image, self.transport)
        self.monitor = FunctionPodMonitor(api, namespace, node_name)

    def _find_function_pod(self, prefix):
        pods = self.api.list_namespaced_pod(self.namespace, _request_timeout=10).items
        # e.g. annotations: ndn-prefix: /average
        for pod in pods:
            annotations = pod.metadata.annotations or {}
            if annotations.get('ndn-prefix') == prefix and pod.spec.node_name == self.node_name:
                return pod
        return None

    def _delete_old_function_pods_if_exists(self, prefix):
        pods = self.api.list_namespaced_pod(self.namespace).items
        old_pods = []
        for pod in pods:
            annotations = pod.metadata.annotations or {}
            if annotations.get('ndn-prefix') == prefix:
                old_pods.append(pod)
        if not old_pods:
            return
        for pod in old_pods:
            self.api.delete_namespaced_pod(pod.metadata.name, self.namespace)
            self._wait_for_pod_deletion(pod.metadata.name)

    def _wait_for_pod_deletion(self, pod_name, timeout=120):
        deadline = time.monotonic() + timeout
        while True:
            try:
                self.api.read_namespaced_pod(pod_name, self.namespace, _request_timeout=10)
            except ApiException as exc:
                if exc.status == 404:
                    return
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(f'Timed out waiting for old Function Pod deletion: {pod_name}')
            time.sleep(1)

    @tracer.start_as_current_span('seed.deploy')
    def create(self, prefix, code, runtime, requirements=(), zip_content=None, source_file=None):
        trace.get_current_span().set_attribute('ndn.function.name', prefix)
        trace.get_current_span().set_attribute('k8s.node.name', self.node_name or '')
        if not self.node_name:
            raise ValueError('NODE_NAME is required to place a function')
        if zip_content is None:
            source_file = None
        manifest = self.manifest_builder.build(prefix, runtime, requirements, source_file)
        self._delete_old_function_pods_if_exists(prefix)
        with tracer.start_as_current_span('seed.create.pod', attributes={'k8s.pod.name': manifest['metadata']['name']}):
            self.api.create_namespaced_pod(self.namespace, manifest, _request_timeout=10)
        return manifest['metadata']['name']

    @tracer.start_as_current_span('seed.upload')
    def upload_function(self, prefix, pod_name, code=None, zip_content=None):
        trace.get_current_span().set_attribute('ndn.function.name', prefix)
        trace.get_current_span().set_attribute('k8s.pod.name', pod_name)
        try:
            self._upload_package(pod_name, code=code, zip_content=zip_content)
        except Exception:
            try:
                self.api.delete_namespaced_pod(pod_name, self.namespace, _request_timeout=10)
            except Exception:
                LOG.exception('Could not delete Function Pod after package upload failure: %s', pod_name)
            raise

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
        return self.monitor.wait_for_result(prefix, pod, timeout)

    def _upload_package(self, pod_name, code=None, zip_content=None):
        if zip_content is not None:
            request = function_pb2.PackageRequest(zip_content=zip_content)
        else:
            request = function_pb2.PackageRequest(code=code)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pod = self.api.read_namespaced_pod(pod_name, self.namespace, _request_timeout=10)
            if self.monitor.containers_with_execution_failure(pod) or pod.status.phase in ('Failed', 'Succeeded'):
                return
            address = pod.status.pod_ip
            if address:
                target = f'[{address}]:50052' if ':' in address else f'{address}:50052'
                with grpc.insecure_channel(target) as channel:
                    stub = function_pb2_grpc.FunctionDeploymentStub(channel)
                    with tracer.start_as_current_span('seed.upload.package', kind=trace.SpanKind.CLIENT,
                                                     attributes={'k8s.pod.name': pod_name}):
                        carrier = {}
                        TraceContextTextMapPropagator().inject(carrier)
                        stub.UploadPackage(
                            request, metadata=list(carrier.items()),
                            timeout=max(0, deadline - time.monotonic()), wait_for_ready=True)
                return
            time.sleep(1)
        raise TimeoutError(f'Timed out waiting for function Pod: {pod_name}')

    def delete(self, prefix):
        pod = self._find_function_pod(prefix)
        if pod is not None:
            self.api.delete_namespaced_pod(pod.metadata.name, self.namespace)
