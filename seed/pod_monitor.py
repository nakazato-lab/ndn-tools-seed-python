"""Monitor Function startup and collect container logs or Pod events."""
import time

from opentelemetry import trace

tracer = trace.get_tracer(__name__)

MAX_LOG_BYTES = 64 * 1024
CONTAINER_STARTUP_FAILURE_REASONS = frozenset({
    'ImagePullBackOff',
    'ErrImageNeverPull',
    'InvalidImageName',
    'CreateContainerConfigError',
    'CreateContainerError',
    'RunContainerError',
})


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
            has_failure = self.has_container_failure(pod)
            # Pending may be normal startup; wait until the deadline.
            if (not has_failure
                    and pod.status.phase == 'Pending' and time.monotonic() < deadline):
                time.sleep(1)
                continue

            return self.collect_result(prefix, pod)

    def collect_result(self, prefix, pod):
        result = {
            'name': prefix, 'pod_name': pod.metadata.name, 'node_name': self.node_name,
            'phase': pod.status.phase or 'Unknown',
        }
        execution_failures = self.containers_with_execution_failure(pod)
        startup_failures = self.containers_with_startup_failure(pod)
        if execution_failures:
            self._collect_container_error_logs(pod.metadata.name, execution_failures, result)
        if startup_failures:
            self._collect_startup_errors(pod.metadata.uid, startup_failures, result)
        elif result.get('log_errors') or (not execution_failures and pod.status.phase != 'Running'):
            self._collect_events(pod.metadata.uid, result)
        return result

    def _all_container_statuses(self, pod):
        # fields for pod.status: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodStatus.md
        if pod.status is None:
            return []
        return (pod.status.init_container_statuses or []) + (pod.status.container_statuses or [])

    def has_container_failure(self, pod):
        return bool(
            self.containers_with_execution_failure(pod)
            or self.containers_with_startup_failure(pod)
        )

    def containers_with_execution_failure(self, pod):
        return [
            status for status in self._all_container_statuses(pod)
            if self._has_container_execution_failure(status)
        ]

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

    def containers_with_startup_failure(self, pod):
        return [
            status for status in self._all_container_statuses(pod)
            if self._has_container_startup_failure(status)
        ]

    def _has_container_startup_failure(self, status):
        state = status.state
        if state is None or state.waiting is None:
            return False
        # Exclude ContainerCreating, PodInitializing, and transient ErrImagePull.
        return state.waiting.reason in CONTAINER_STARTUP_FAILURE_REASONS

    def _collect_startup_errors(self, pod_uid, containers, result):
        startup_errors = {}
        for status in containers:
            waiting = status.state.waiting
            startup_errors[status.name] = {
                'reason': waiting.reason,
                'message': waiting.message or '',
            }
        if startup_errors:
            result['startup_errors'] = startup_errors
            result.setdefault('reason', containers[0].state.waiting.reason)
        self._collect_events(pod_uid, result)

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
