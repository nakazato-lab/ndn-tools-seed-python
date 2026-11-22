"""Publish placement results and notify the Registerer of their location."""
import json
import logging
from uuid import uuid4

from ndn.types import InterestTimeout
from segmented_node_wrapper import SegmentedNodeWrapper
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class PlacementResultNotifier:
    def __init__(self, app, prefix):
        self.app = app
        self.prefix = prefix
        self.publisher = SegmentedNodeWrapper(app, serve=True)

    async def notify(self, registerer_prefix, result):
        with tracer.start_as_current_span('seed.notify.registerer', kind=trace.SpanKind.PRODUCER,
                                         attributes={'ndn.name': registerer_prefix}):
            with tracer.start_as_current_span('seed.publish.registerer'):
                result_ref = await self.publisher.publish(
                    f'{self.prefix}/results/{uuid4().hex}',
                    json.dumps(result, ensure_ascii=False).encode('utf-8'))
            LOG.info('Notifying Registerer %s: result_ref=%s', registerer_prefix, result_ref)
            notification = {'result_ref': result_ref}
            TraceContextTextMapPropagator().inject(notification)
            try:
                await self.app.express_interest(
                    registerer_prefix, app_param=json.dumps(notification).encode('utf-8'),
                    must_be_fresh=True, can_be_prefix=False, lifetime=1000)
            except InterestTimeout:
                # Result notifications are one-way; Registerer does not return a Data ACK.
                pass
