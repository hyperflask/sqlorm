# Modified from opentelemetry-instrumentation-sqlalchemy
from collections.abc import Sequence
from typing import Collection

from wrapt import wrap_function_wrapper as _w

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import get_meter
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.trace import get_tracer

import sqlorm

from .tracer import (
    EngineTracer,
    _wrap_connect,
    _wrap_engine_init,
)


class SQLORMInstrumentor(BaseInstrumentor):
    """An instrumentor for SQLORM
    See `BaseInstrumentor`
    """

    def _instrument(self, **kwargs):
        """Instruments SQLORM engine creation methods and the engine
        if passed as an argument.

        Args:
            **kwargs: Optional arguments
                ``engine``: a SQLORM engine instance
                ``engines``: a list of SQLORM engine instances
                ``tracer_provider``: a TracerProvider, defaults to global
                ``meter_provider``: a MeterProvider, defaults to global
                ``enable_commenter``: bool to enable sqlcommenter, defaults to False
                ``commenter_options``: dict of sqlcommenter config, defaults to {}

        Returns:
            An instrumented engine if passed in as an argument or list of instrumented engines, None otherwise.
        """
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(
            __name__,
            sqlorm.__version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        meter_provider = kwargs.get("meter_provider")
        meter = get_meter(
            __name__,
            sqlorm.__version__,
            meter_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        connections_usage = meter.create_up_down_counter(
            name=MetricInstruments.DB_CLIENT_CONNECTIONS_USAGE,
            unit="connections",
            description="The number of connections that are currently in state described by the state attribute.",
        )

        enable_commenter = kwargs.get("enable_commenter", False)
        commenter_options = kwargs.get("commenter_options", {})

        _w(
            "sqlorm.engine",
            "Engine.__init__",
            _wrap_engine_init(
                tracer, connections_usage, enable_commenter, commenter_options
            ),
        )
        _w(
            "sqlorm.engine",
            "Engine._connect",
            _wrap_connect(tracer),
        )
        if kwargs.get("engine") is not None:
            return EngineTracer(
                tracer,
                kwargs.get("engine"),
                connections_usage,
                kwargs.get("enable_commenter", False),
                kwargs.get("commenter_options", {}),
            )
        if kwargs.get("engines") is not None and isinstance(
            kwargs.get("engines"), Sequence
        ):
            return [
                EngineTracer(
                    tracer,
                    engine,
                    connections_usage,
                    kwargs.get("enable_commenter", False),
                    kwargs.get("commenter_options", {}),
                )
                for engine in kwargs.get("engines")
            ]

        return None

    def _uninstrument(self, **kwargs):
        unwrap(sqlorm.Engine, "__init__")
        unwrap(sqlorm.Engine, "_connect")
        EngineTracer.remove_all_event_listeners()
