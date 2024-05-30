from importlib import import_module
from contextlib import contextmanager
import threading
import logging
import inspect
import urllib.parse
import functools
from blinker import Namespace
from .sql import render, ParametrizedStmt
from .resultset import ResultSet, CompositeResultSet, CompositionMap
from .mapper import Mapper, HydrationMap, HydratedResultSet


_signals = Namespace()


class Engine:
    """A connection factory
    Let's you create sessions and connections.

    When used as a context, will start a session and provide a transaction
    which will auto commit unless an error is raised.

        engine = Engine.from_uri("sqlite://:memory:")

        with engine as tx:
            tx.execute()

        with engine.session() as sess:
            with sess as tx:
                tx.execute()

    """

    connected = _signals.signal("connected")
    pool_checkin = _signals.signal("pool-checkin")
    pool_checkout = _signals.signal("pool-checkout")
    disconnected = _signals.signal("disconnected")

    @classmethod
    def from_uri(cls, uri, **kwargs):
        module, args, _kwargs = parse_uri(uri)
        _kwargs.update(kwargs)
        engine = cls.from_dbapi(module, *args, **_kwargs)
        engine.connection_factory.uri = uri
        return engine

    @classmethod
    def from_dbapi(cls, dbapi, *connect_args, **kwargs):
        logger = kwargs.pop("logger", None)
        logger_level = kwargs.pop("logger_level", "debug")
        pool = kwargs.pop("pool", True)
        max_pool_conns = kwargs.pop("max_pool_conns", 10)

        if isinstance(dbapi, str):
            try:
                dbapi = import_module(f"sqlorm.drivers.{dbapi}")
            except Exception:
                dbapi = import_module(dbapi)

        def connect(dbapi):
            return dbapi.connect(*connect.args, **connect.kwargs)

        connect.args = connect_args
        connect.kwargs = kwargs

        return Engine(dbapi, connect, logger, logger_level, pool, max_pool_conns)

    def __init__(
        self,
        dbapi,
        connection_factory,
        logger=None,
        logger_level="debug",
        pool=True,
        max_pool_conns=10,
    ):
        self.dbapi = dbapi
        self.connection_factory = connection_factory
        self.logger = logging.getLogger("sqlorm") if logger is True else logger
        self.logger_level = logger_level
        self.pool = [] if pool else False
        self.active_conns = []
        self.max_pool_conns = max_pool_conns

    def connect(self, from_pool=True):
        if not from_pool or self.pool is False:
            return self._connect()

        if self.pool:
            conn = self.pool.pop(0)
            if self.logger:
                getattr(self.logger, self.logger_level)("Re-using connection from pool")
            self.pool_checkout.send(self, conn=conn)
        elif not self.max_pool_conns or len(self.active_conns) < self.max_pool_conns:
            conn = self._connect()
        else:
            raise EngineError("Max number of connections reached")

        self.active_conns.append(conn)
        return conn
    
    def _connect(self):
        if self.logger:
            getattr(self.logger, self.logger_level)("Creating new connection")
        conn = self.connection_factory(self.dbapi)
        self.connected.send(self, conn=conn)
        return conn

    def disconnect(self, conn, force=False):
        if force or self.pool is False:
            self._close(conn)
        elif conn in self.active_conns:
            self.active_conns.remove(conn)
            if self.logger:
                getattr(self.logger, self.logger_level)("Returning connection to pool")
            self.pool.append(conn)
            self.pool_checkin.send(self, conn=conn)
        else:
            raise EngineError("Cannot close connection which is not part of pool")
        
    def _close(self, conn):
        if self.logger:
            getattr(self.logger, self.logger_level)("Closing connection")
        conn.close()
        self.disconnected.send(self, conn=conn)

    def disconnect_all(self):
        if self.pool is False:
            return
        if self.logger:
            getattr(self.logger, self.logger_level)("Closing all connections from pool")
        for conn in self.pool + self.active_conns:
            conn.close()
            self.disconnected.send(self, conn=conn)
        self.pool = []
        self.active_conns = []

    def make_session(self, **kwargs):
        kwargs["engine"] = self
        kwargs["dbapi_conn"] = None
        kwargs["auto_close_conn"] = True
        session_class = getattr(self, "session_class", Session)
        return session_class(**kwargs)

    @contextmanager
    def session(self, **kwargs):
        session = self.make_session(**kwargs)
        try:
            with session_context(session):
                yield session
        finally:
            session.close()

    def __enter__(self):
        if session_context.top:
            return session_context.top.__enter__()
        session = self.make_session()
        return session.__enter__()

    def __exit__(self, exc_type, exc_value, exc_tb):
        session = session_context.top
        session.__exit__(exc_type, exc_value, exc_tb)
        if not session.transactions:
            session.close()


class EngineError(Exception):
    pass


class _SessionContext:
    """A per-thread stack of Session to keep track of the current session"""

    def __init__(self):
        self.locals = threading.local()

    @property
    def stack(self):
        if not hasattr(self.locals, "stack"):
            self.locals.stack = []
        return self.locals.stack

    @property
    def top(self):
        if self.stack:
            return self.stack[-1]

    def push(self, session):
        self.stack.append(session)

    def pop(self):
        self.stack.pop()

    @contextmanager
    def __call__(self, session):
        self.push(session)
        try:
            yield session
        finally:
            self.pop()


session_context = _SessionContext()


def get_current_session():
    return session_context.top


@contextmanager
def ensure_session(engine=None):
    """A context to ensure that a session is available"""
    if session_context.top:
        yield session_context.top
    elif engine:
        with engine.session(virtual_tx=True) as sess:
            yield sess
    else:
        raise MissingSessionError()


@contextmanager
def ensure_transaction(engine=None, virtual=True):
    """A context to ensure that a session is available and provide a transaction"""
    with ensure_session(engine) as sess:
        ctx = sess.virtual_tx_ctx() if virtual else sess
        with ctx as tx:
            yield tx


def transaction(engine=None):
    return ensure_transaction(engine, virtual=False)


class Session:
    """Wraps a DBAPI Connection object

    sess = Session(conn)
    with sess as tx:
        tx.execute()
        # commit
    sess.close()

    sess = Session(conn, virtual_tx=True)
    with sess as tx:
        tx.execute()
    sess.commit()
    sess.close()
    """

    before_commit = _signals.signal("before-commit")
    after_commit = _signals.signal("after-commit")
    before_rollback = _signals.signal("before-rollback")
    after_rollback = _signals.signal("after-rollback")

    def __init__(
        self,
        dbapi_conn=None,
        auto_close_conn=None,
        virtual_tx=False,
        logger=None,
        logger_level=None,
        engine=None,
    ):
        if not dbapi_conn and not engine:
            raise SessionError("Session(): dbapi_conn or engine must be provided")
        if engine and logger is None:
            logger = engine.logger
        if engine and logger_level is None:
            logger_level = engine.logger_level
        elif logger_level is None:
            logger_level = logging.DEBUG
        self.engine = engine
        self.conn = dbapi_conn
        self.auto_close_conn = not bool(dbapi_conn) if auto_close_conn is None else auto_close_conn
        self.virtual_tx = virtual_tx
        self.logger = logger
        self.logger_level = logger_level
        self.transactions = []
        self.ended = False

    def connect(self):
        if not self.conn:
            self.conn = self.engine.connect()
        return self.conn

    def commit(self):
        if self.ended:
            raise SessionEndedError()
        if not self.conn:
            return
        if _signal_rv(self.before_commit.send(self)) is False:
            return
        if self.logger:
            getattr(self.logger, self.logger_level)("COMMIT")
        self.conn.commit()
        self.after_commit.send(self)

    def rollback(self):
        if self.ended:
            raise SessionEndedError()
        if not self.conn:
            return
        if _signal_rv(self.before_rollback.send(self)) is False:
            return
        if self.logger:
            getattr(self.logger, self.logger_level)("ROLLBACK")
        self.conn.rollback()
        self.after_rollback.send(self)

    def close(self):
        if self.conn:
            self.rollback()
            if self.auto_close_conn:
                if self.engine:
                    self.engine.disconnect(self.conn)
                else:
                    self.conn.close()
                self.conn = None
        self.ended = True

    @contextmanager
    def virtual_tx_ctx(self):
        tx = self.__enter__(True)
        try:
            yield tx
            self.__exit__(None, None, None)
        except Exception as e:
            self.__exit__(type(e), e, None)
            raise

    def __enter__(self, virtual=False):
        if self.ended:
            raise SessionEndedError()
        if not self.transactions:
            session_context.push(self)
        virtual = virtual or self.virtual_tx or bool(self.transactions)
        transaction_class = getattr(self, "transaction_class", Transaction)
        self.transactions.append(transaction_class(self, virtual))
        return self.transactions[-1]

    def __exit__(self, exc_type, exc_value, exc_tb):
        tx = self.transactions.pop()
        tx.__exit__(exc_type, exc_value, exc_tb)
        if not self.transactions:
            session_context.pop()

    @property
    def transaction(self):
        if self.transactions:
            return self.transactions[-1]

    @property
    def in_transaction(self):
        return bool(self.transactions)


class SessionError(Exception):
    pass


class MissingSessionError(SessionError):
    pass


class SessionStartedError(SessionError):
    pass


class SessionEndedError(SessionError):
    pass


class Transaction:
    default_composite_separator = "__"

    before_execute = _signals.signal("before-execute")
    after_execute = _signals.signal("after-execute")
    handle_error = _signals.signal("handle-error")

    def __init__(self, session, virtual=False):
        self.session = session
        self.virtual = virtual
        self.ended = False

    def commit(self):
        if self.ended:
            raise TransactionEndedError()
        if not self.virtual:
            self.session.commit()
        self.ended = True

    def rollback(self):
        if self.ended:
            raise TransactionEndedError()
        if not self.virtual:
            self.session.rollback()
        self.ended = True

    def cursor(self, stmt=None, params=None):
        if self.ended:
            raise TransactionEndedError()
        if not stmt:
            return self.session.connect().cursor()
        stmt, params = render(stmt, params)

        rv = _signal_rv(self.before_execute.send(self, stmt=stmt, params=params, many=False))
        if isinstance(rv, tuple):
            stmt, params = rv
        elif rv:
            return rv

        cur = self.session.connect().cursor()

        if self.session and self.session.logger:
            getattr(self.session.logger, self.session.logger_level)(
                "%s %s" % (stmt, params) if params else stmt
            )
        try:
            # because the default value of params may depend on some engine
            if params:
                cur.execute(stmt, params)
            else:
                cur.execute(stmt)
        except Exception as e:
            rv = _signal_rv(self.handle_error.send(self, cursor=cur, stmt=stmt, params=params, exc=e, many=False))
            if rv:
                return rv
            raise

        self.after_execute.send(self, cursor=cur, stmt=stmt, params=params, many=False)
        return cur

    def execute(self, stmt, params=None):
        if isinstance(stmt, (list, tuple)) and not isinstance(stmt, ParametrizedStmt):
            for s in stmt:
                self.execute(s)
            return
        self.cursor(stmt, params).close()

    def executemany(self, stmt, seq_of_parameters):
        rv = _signal_rv(
            self.before_execute.send(self, stmt=stmt, params=seq_of_parameters, many=True)
        )
        if isinstance(rv, tuple):
            stmt, seq_of_parameters = rv
        elif rv is False:
            return

        cur = self.cursor()

        if self.session and self.session.logger:
            getattr(self.session.logger, self.session.logger_level)(
                "%s (x%s)" % (stmt, len(seq_of_parameters)) if seq_of_parameters else stmt
            )

        try:
            cur.executemany(str(stmt), seq_of_parameters)
        except Exception as e:
            if not _signal_rv(
                self.handle_error.send(self, cursor=cur, stmt=stmt, params=seq_of_parameters, exc=e, many=True)
            ):
                raise
        
        self.after_execute.send(self, cursor=cur, stmt=stmt, params=seq_of_parameters, many=True)
        cur.close()

    def fetch(self, stmt, params=None, model=None, obj=None, loader=None):
        cursor = self.cursor(stmt, params)

        if obj and not model:
            model = obj.__class__
        if model:
            rs = HydratedResultSet(cursor, model)
            if obj:
                rs.mapper.hydrate(obj, rs.first(with_loader=False))
                return obj
            return rs

        return ResultSet(cursor, loader)

    def fetchall(self, stmt, params=None, **fetch_kwargs):
        return self.fetch(stmt, params, **fetch_kwargs).all()

    def fetchone(self, stmt, params=None, **fetch_kwargs):
        return self.fetch(stmt, params, **fetch_kwargs).first()

    def fetchscalar(self, stmt, params=None, **fetch_kwargs):
        return self.fetch(stmt, params, **fetch_kwargs).scalar()

    def fetchscalars(self, stmt, params=None, **fetch_kwargs):
        return self.fetch(stmt, params, **fetch_kwargs).scalars()

    def fetchcomposite(
        self,
        stmt,
        params=None,
        loader=None,
        model=None,
        nested=None,
        obj=None,
        map=None,
        separator=None,
    ):
        if obj and not model:
            model = obj.__class__

        if model:
            map = HydrationMap.create([model, nested])
        elif nested:
            map = CompositionMap(
                lambda r: r.values(), {k: HydrationMap.create(v) for k, v in nested.items()}
            )
        elif not map:
            map = CompositionMap.create([loader, nested])

        rs = CompositeResultSet(
            self.cursor(stmt, params), map, separator or self.default_composite_separator
        )
        if obj:
            map.mapper.hydrate(obj, rs.first(with_loader=False))
            return obj
        return rs

    def fetchhydrated(self, model, stmt, params=None, **fetchcomposite_kwargs):
        if not inspect.isclass(model) and not isinstance(model, Mapper):
            if isinstance(model, dict):
                fetchcomposite_kwargs["nested"] = model
                model = None
            elif isinstance(model, list):
                fetchcomposite_kwargs["nested"] = dict({c.__name__: c for c in model})
                model = None
            else:
                fetchcomposite_kwargs["obj"] = model
                model = model.__class__
        return self.fetchcomposite(stmt, params, model=model, **fetchcomposite_kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if not exc_type:
            self.commit()
        else:
            self.rollback()


class TransactionEndedError(Exception):
    pass


def parse_uri(uri):
    module, uri = uri.split("://", 1)
    kwargs = {}
    if "?" in uri:
        uri, qs = uri.split("?", 1)
        for k, v in urllib.parse.parse_qs(qs).items():
            for i in range(len(v)):
                if v[i].isdecimal():
                    v[i] = int(v[i])
                elif v[i] in ("True", "False"):
                    v[i] = True if v[i] == "True" else False
            v = v if len(v) > 1 else v[0]
            if "[" in k:
                k, sub = k.rstrip("]").split("[")
                kwargs.setdefault(k, {})[sub] = v
            else:
                kwargs[k] = v
    args = [uri] if uri else []
    return module, args, kwargs


def _signal_rv(signal_rv):
    final_rv = None
    for func, rv in signal_rv:
        if rv is False:
            return False
        if rv:
            final_rv = rv
    return final_rv


def connect_via_engine(engine, signal, func=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(sender, **kw):
            matches = False
            if isinstance(sender, Engine):
                matches = sender is engine
            elif isinstance(sender, Session):
                matches = sender.engine is engine
            elif isinstance(sender, Transaction):
                matches = sender.session.engine is engine
            if matches:
                return func(sender, **kw)
        signal.connect(wrapper, weak=False)
        return wrapper
    return decorator(func) if func else decorator