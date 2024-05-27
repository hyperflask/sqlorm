# Modified from opentelemetry-instrumentation-sqlalchemy
import re

from opentelemetry import trace
from opentelemetry.instrumentation.sqlcommenter_utils import _add_sql_comment
from opentelemetry.instrumentation.utils import _get_opentelemetry_values
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode

from ..engine import Engine, Transaction, connect_via_engine


def _normalize_vendor(vendor):
    """Return a canonical name for a type of database."""
    if not vendor:
        return "db"  # should this ever happen?

    if "sqlite" in vendor:
        return "sqlite"

    if "postgres" in vendor or vendor == "psycopg2":
        return "postgresql"

    return vendor


def _wrap_engine_init(
    tracer, connections_usage, enable_commenter=False, commenter_options=None
):
    def _wrap_engine_init_internal(func, instance, args, kwargs):
        func(instance, *args, **kwargs)
        EngineTracer(
            tracer,
            instance,
            connections_usage,
            enable_commenter,
            commenter_options,
        )

    return _wrap_engine_init_internal


def _wrap_connect(tracer):
    # pylint: disable=unused-argument
    def _wrap_connect_internal(func, instance, args, kwargs):
        with tracer.start_as_current_span(
            "connect", kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                span.set_attribute(
                    SpanAttributes.DB_SYSTEM, _normalize_vendor(instance.dbapi.__name__)
                )
            return func(*args, **kwargs)

    return _wrap_connect_internal


class EngineTracer:
    _remove_event_listener_params = []

    def __init__(
        self,
        tracer,
        engine,
        connections_usage,
        enable_commenter=False,
        commenter_options=None,
    ):
        self.tracer = tracer
        self.connections_usage = connections_usage
        self.vendor = _normalize_vendor(engine.dbapi.__name__)
        self.enable_commenter = enable_commenter
        self.commenter_options = commenter_options if commenter_options else {}
        self._leading_comment_remover = re.compile(r"^/\*.*?\*/")

        self._register_event_listener(engine, Engine.connected, self._pool_connect)
        self._register_event_listener(engine, Engine.pool_checkout, self._pool_checkout)
        self._register_event_listener(engine, Engine.pool_checkin, self._pool_checkin)
        self._register_event_listener(engine, Engine.disconnected, self._pool_disconnect)
        self._register_event_listener(engine, Transaction.before_execute, self._before_execute)
        self._register_event_listener(engine, Transaction.after_execute, _after_execute)
        self._register_event_listener(engine, Transaction.handle_error, _handle_error)

    def _add_idle_to_connection_usage(self, value):
        self.connections_usage.add(
            value,
            attributes={
                "state": "idle",
            },
        )

    def _add_used_to_connection_usage(self, value):
        self.connections_usage.add(
            value,
            attributes={
                "state": "used",
            },
        )

    def _pool_connect(self, engine, conn):
        self._add_idle_to_connection_usage(1)

    def _pool_checkout(self, engine, conn):
        self._add_idle_to_connection_usage(-1)
        self._add_used_to_connection_usage(1)

    def _pool_checkin(self, engine, conn):
        self._add_used_to_connection_usage(-1)
        self._add_idle_to_connection_usage(1)

    def _pool_disconnect(self, engine, conn):
        self._add_idle_to_connection_usage(-1)

    @classmethod
    def _register_event_listener(cls, engine, signal, func):
        func = connect_via_engine(engine, signal, func)
        cls._remove_event_listener_params.append((signal, func))

    @classmethod
    def remove_all_event_listeners(cls):
        for (signal, func) in cls._remove_event_listener_params:
            signal.disconnect(func)
        cls._remove_event_listener_params.clear()

    def _operation_name(self, db_name, statement):
        parts = []
        if isinstance(statement, str):
            # otel spec recommends against parsing SQL queries. We are not trying to parse SQL
            # but simply truncating the statement to the first word. This covers probably >95%
            # use cases and uses the SQL statement in span name correctly as per the spec.
            # For some very special cases it might not record the correct statement if the SQL
            # dialect is too weird but in any case it shouldn't break anything.
            # Strip leading comments so we get the operation name.
            parts.append(
                self._leading_comment_remover.sub("", statement).split()[0]
            )
        if db_name:
            parts.append(db_name)
        if not parts:
            return self.vendor
        return " ".join(parts)

    def _before_execute(
        self, tx, stmt, params, many
    ):
        attrs = {}
        span = self.tracer.start_span(
            self._operation_name(None, stmt),
            kind=trace.SpanKind.CLIENT,
        )
        with trace.use_span(span, end_on_exit=False):
            if span.is_recording():
                span.set_attribute(SpanAttributes.DB_STATEMENT, stmt)
                span.set_attribute(SpanAttributes.DB_SYSTEM, self.vendor)
                for key, value in attrs.items():
                    span.set_attribute(key, value)
            if self.enable_commenter:
                commenter_data = {
                    "db_driver": self.vendor,
                    # Driver/framework centric information.
                    "db_framework": "sqlorm",
                }

                if self.commenter_options.get("opentelemetry_values", True):
                    commenter_data.update(**_get_opentelemetry_values())

                # Filter down to just the requested attributes.
                commenter_data = {
                    k: v
                    for k, v in commenter_data.items()
                    if self.commenter_options.get(k, True)
                }

                stmt = _add_sql_comment(stmt, **commenter_data)

        tx._otel_span = span

        return stmt, params


# pylint: disable=unused-argument
def _after_execute(tx, cursor, stmt, params, many):
    span = getattr(tx, "_otel_span", None)
    if span is None:
        return

    span.end()


def _handle_error(tx, cursor, stmt, params, many, exc):
    span = getattr(tx, "_otel_span", None)
    if span is None:
        return

    if span.is_recording():
        span.set_status(
            Status(
                StatusCode.ERROR,
                str(exc),
            )
        )
    span.end()
