"""Monitor Function startup and collect container logs or Pod events."""
import time

from opentelemetry import trace

tracer = trace.get_tracer(__name__)

MAX_LOG_BYTES = 64 * 1024


class FunctionPodMonitor:
    def __init__(self, api, namespace, node_name):
        self.api = api
        self.namespace = namespace
        self.node_name = node_name

    def wait_for_result(self, prefix, pod, timeout):
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
            containers_with_execution_failure = self.containers_with_execution_failure(pod)
            # Pending may be normal startup; wait until the deadline.
            if (not containers_with_execution_failure
                    and pod.status.phase == 'Pending' and time.monotonic() < deadline):
                time.sleep(1)
                continue

            return self.collect_result(prefix, pod)

    def collect_result(self, prefix, pod):
        result = {
            'name': prefix, 'pod_name': pod.metadata.name, 'node_name': self.node_name,
            'phase': pod.status.phase or 'Unknown',
        }
        containers_with_execution_failure = self.containers_with_execution_failure(pod)
        if containers_with_execution_failure:
            self._collect_container_error_logs(pod.metadata.name, containers_with_execution_failure, result)
        elif pod.status.phase == 'Running':
            return result
        else:
            self._collect_events(pod.metadata.uid, result)
        return result

    def containers_with_execution_failure(self, pod):
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

    @tracer.start_as_current_span('seed.collect.logs')
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
                span = trace.get_current_span()
                span.record_exception(exc)
                span.set_status(trace.StatusCode.ERROR, str(exc))
                errors[status.name] = str(exc)
        if logs:
            result['logs'] = logs
        if errors:
            result['log_errors'] = errors

    @tracer.start_as_current_span('seed.collect.events')
    def _collect_events(self, pod_uid, result):
        try:
            events = self.api.list_namespaced_event(
                self.namespace, field_selector=f'involvedObject.uid={pod_uid}', _request_timeout=5).items
            result['events'] = [
                {'type': event.type, 'reason': event.reason, 'message': event.message, 'count': event.count}
                for event in events
            ]
        except Exception as exc:
            span = trace.get_current_span()
            span.record_exception(exc)
            span.set_status(trace.StatusCode.ERROR, str(exc))
            result['event_error'] = str(exc)
