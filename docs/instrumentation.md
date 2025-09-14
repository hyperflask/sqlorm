# Instrumentation

SQLORM can be instrumented using OpenTelemetry.

It supports automatic instrumentation or can be enabled manually:

```py
from sqlorm.opentelemetry import SQLORMInstrumentor

SQLORMInstrumentor().instrument(engine=engine)
```

You can optionally configure SQLORM instrumentation to enable sqlcommenter which enriches the query with contextual information.

```py
SQLORMInstrumentor().instrument(enable_commenter=True, commenter_options={})
```