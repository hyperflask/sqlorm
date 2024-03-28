from importlib import import_module
from contextlib import contextmanager
import threading
import logging
import inspect
import urllib.parse
from .sql import render, ParametrizedStmt
from .resultset import ResultSet, CompositeResultSet, CompositionMap
from .mapper import Mapper, HydrationMap, HydratedResultSet


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
            except:
                dbapi = import_module(dbapi)

        def connect(dbapi):
            return dbapi.connect(*connect.args, **connect.kwargs)
        connect.args = connect_args
        connect.kwargs = kwargs

        return Engine(dbapi, connect, logger, logger_level, pool, max_pool_conns)

    def __init__(self, dbapi, connection_factory, logger=None, logger_level="debug",
                 pool=True, max_pool_conns=10):
        self.dbapi = dbapi
        self.connection_factory = connection_factory
        self.logger = logging.getLogger("sqlorm") if logger is True else logger
        self.logger_level = logger_level
        self.pool = [] if pool else False
        self.active_conns = []
        self.max_pool_conns = max_pool_conns

    def connect(self, from_pool=True):
        if not from_pool or self.pool is False:
            return self.connection_factory(self.dbapi)
        
        if self.pool:
            conn = self.pool.pop(0)
        elif not self.max_pool_conns or len(self.active_conns) < self.max_pool_conns:
            conn = self.connection_factory(self.dbapi)
        else:
            raise EngineError("Max number of connections reached")
        
        self.active_conns.append(conn)
        return conn
    
    def disconnect(self, conn, force=False):
        if conn in self.active_conns:
            self.active_conns.remove(conn)
            if force:
                conn.close()
            else:
                self.pool.append(conn)
        elif self.pool is False or force:
            conn.close()
        else:
            raise EngineError("Cannot close connection which is not part of pool")
        
    def disconnect_all(self):
        if self.pool is False:
            return
        for conn in self.pool + self.active_conns:
            conn.close()
        self.pool = []
        self.active_conns = []

    def make_session(self, **kwargs):
        kwargs["engine"] = self
        if self.pool is not False:
            kwargs["dbapi_conn"] = self.connect()
            kwargs["auto_close_conn"] = True
        return Session(**kwargs)

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
    """A per-thread stack of Session to keep track of the current session
    """
    
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
    """A context to ensure that a session is available
    """
    if session_context.top:
        yield session_context.top
    elif engine:
        with engine.session(virtual_tx=True) as sess:
            yield sess
    else:
        raise MissingSessionError()


@contextmanager
def ensure_transaction(engine=None, virtual=True):
    """A context to ensure that a session is available and provide a transaction
    """
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
    def __init__(self, dbapi_conn=None, auto_close_conn=None, virtual_tx=False, engine=None):
        if not dbapi_conn and not engine:
            raise SessionError("Session(): dbapi_conn or engine must be provided")
        self.engine = engine
        self.conn = dbapi_conn
        self.auto_close_conn = not bool(dbapi_conn) if auto_close_conn is None else auto_close_conn
        self.virtual_tx = virtual_tx
        self.transactions = []
        self.ended = False

    def connect(self):
        if not self.conn:
            self.conn = self.engine.connect()

    def commit(self):
        if self.ended:
            raise SessionEndedError()
        if self.conn:
            if self.engine and self.engine.logger:
                getattr(self.engine.logger, self.engine.logger_level)("COMMIT")
            self.conn.commit()

    def rollback(self):
        if self.ended:
            raise SessionEndedError()
        if self.conn:
            if self.engine and self.engine.logger:
                getattr(self.engine.logger, self.engine.logger_level)("ROLLBACK")
            self.conn.rollback()

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
            self.connect()
            session_context.push(self)
        virtual = virtual or self.virtual_tx or bool(self.transactions)
        self.transactions.append(Transaction(self.conn, virtual, self.engine))
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
    def __init__(self, dbapi_conn, virtual=False, engine=None):
        self.conn = dbapi_conn
        self.virtual = virtual
        self.engine = engine
        self.ended = False

    def commit(self):
        if self.ended:
            raise TransactionEndedError()
        if not self.virtual:
            if self.engine and self.engine.logger:
                getattr(self.engine.logger, self.engine.logger_level)("COMMIT")
            self.conn.commit()
        self.ended = True

    def rollback(self):
        if self.ended:
            raise TransactionEndedError()
        if not self.virtual:
            if self.engine and self.engine.logger:
                getattr(self.engine.logger, self.engine.logger_level)("ROLLBACK")
            self.conn.rollback()
        self.ended = True

    def cursor(self, stmt=None, params=None):
        if self.ended:
            raise TransactionEndedError()
        cur = self.conn.cursor()
        if not stmt:
            return cur
        stmt, params = render(stmt, params)
        if self.engine and self.engine.logger:
            getattr(self.engine.logger, self.engine.logger_level)("%s %s" % (stmt, params) if params else stmt)

        if params:
            cur.execute(stmt, params)
        else:
            cur.execute(stmt)
        return cur

    def execute(self, stmt, params=None):
        if isinstance(stmt, (list, tuple)) and not isinstance(stmt, ParametrizedStmt):
            for s in stmt:
                self.execute(s)
            return
        self.cursor(stmt, params).close()
    
    def executemany(self, operation, seq_of_parameters):
        cur = self.cursor()
        cur.executemany(str(operation), seq_of_parameters)
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
    
    def fetchcomposite(self, stmt, params=None, loader=None, model=None, nested=None, obj=None, map=None, separator="__"):
        if obj and not model:
            model = obj.__class__

        if model:
            map = HydrationMap.create([model, nested])
        elif nested:
            map = CompositionMap(lambda r: r.values(), {k: HydrationMap.create(v) for k, v in nested.items()})
        elif not map:
            map = CompositionMap.create([loader, nested])

        rs = CompositeResultSet(self.cursor(stmt, params), map, separator)
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