"""OpenTelemetry setup shared by Seed, Sidecar and Function runtimes."""
import os
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


def setup_tracing(service_name):
    if not os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
        return None
    provider = TracerProvider(resource=Resource.create({
        'service.name': os.getenv('OTEL_SERVICE_NAME', service_name),
    }))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider


@contextmanager
def function_span(request, context):
    # Resume the Sidecar's request span received through gRPC metadata.
    parent = TraceContextTextMapPropagator().extract(dict(context.invocation_metadata()), context=Context())
    with trace.get_tracer(__name__).start_as_current_span(
            'function.execute', context=parent, kind=trace.SpanKind.SERVER,
            attributes={'ndn.function.name': request.name}):
        yield


def startup_context():
    # Startup context comes from the environment set by Seed during Pod creation.
    return TraceContextTextMapPropagator().extract({
        'traceparent': os.getenv('TRACEPARENT', ''),
        'tracestate': os.getenv('TRACESTATE', ''),
    }, context=Context())
