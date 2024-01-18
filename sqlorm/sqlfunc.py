import functools
import inspect
from .engine import ensure_transaction
from .sql_template import SQLTemplate


def sqlfunc(func=None, template_locals=None, **kwargs):
    """Execute the python doc of the function using a query decorator
    """
    def decorator(func):
        doc = inspect.getdoc(func)
        sig = inspect.signature(func)
        @functools.wraps(func)
        def query_builder(*args, **kwargs):
            return SQLTemplate(doc, dict(func.template_locals, **sig.bind(*args, **kwargs).arguments))
        
        if getattr(func, "query_decorator", False):
            # the function can already be decorated with a query decorator
            # in this case we replace the sql generation function of the query decorator
            func.sql = query_builder
            if "is_method" in kwargs:
                func.is_method = kwargs["is_method"]
        elif doc.upper().startswith("SELECT"):
            func = fetchall(query_builder, **kwargs)
        elif (doc.upper().startswith("UPDATE") or doc.upper().startswith("INSERT")) and "RETURNING" in doc.upper():
            func = update(query_builder, **kwargs)
        else:
            func = execute(query_builder, **kwargs)
        func.sqlfunc = True
        func.template_locals = template_locals if template_locals is not None else {}
        return func
    
    if func:
        return decorator(func)
    return decorator


def is_sqlfunc(func):
    """Checks if func is an empty function with a python doc
    """
    doc = inspect.getdoc(func)
    src = inspect.getsource(func).strip(" \"\n\r")
    return doc and src.endswith(func.__doc__) and not getattr(func, "sqlfunc", False)


def _query_executor_builder(executor):
    def query_decorator(func=None, model=None, engine=None, is_method=None, engine_in_kwargs=True, **executor_kwargs):
        def decorator(func):
            _is_method = is_method
            if _is_method is None:
                # there is no reliable way to detect if a function is a method when the decorator
                # is applied to one because the decorator gets executed before binding the function to the class
                argspec = inspect.getfullargspec(func)
                _is_method = bool(argspec.args and argspec.args[0] in ("self", "cls"))

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                engine = None
                if wrapper.is_method and hasattr(args[0], "__engine__"):
                    engine = args[0].__engine__
                elif wrapper.engine:
                    engine = wrapper.engine
                elif engine_in_kwargs and "engine" in kwargs:
                    engine = kwargs["engine"]
                stmt = wrapper.sql(*args, **kwargs)
                if not stmt:
                    return
                with ensure_transaction(engine) as tx:
                    return executor(tx, stmt, args[0] if wrapper.is_method else wrapper.model, **executor_kwargs)
            
            wrapper.query_decorator = executor.__name__
            wrapper.sql = func
            wrapper.is_method = _is_method
            wrapper.model = model
            wrapper.engine = engine

            return wrapper
        
        return decorator(func) if func else decorator
    
    def sqlfunc_executor_decorator(func=None, template_locals=None, **kwargs):
        def decorator(func):
            return sqlfunc(query_decorator(func, **kwargs), template_locals)
        return decorator(func) if func else decorator
    
    setattr(sqlfunc, executor.__name__, sqlfunc_executor_decorator)

    return query_decorator


@_query_executor_builder
def fetchall(tx, stmt, model, **fetch_kwargs):
    """Executes the decorated function a fetch all the rows
    """
    if model:
        return tx.fetchhydrated(model, stmt, **fetch_kwargs)
    return tx.fetchall(stmt, **fetch_kwargs)


@_query_executor_builder
def fetchone(tx, stmt, model, **fetch_kwargs):
    """Executes the decorated function and fetch the row
    """
    if model:
        return tx.fetchhydrated(model, stmt, **fetch_kwargs).first()
    return tx.fetchone(stmt, **fetch_kwargs)


@_query_executor_builder
def fetchscalar(tx, stmt, model, **fetch_kwargs):
    """Executes the decorated function and fetch the first value from the first row
    """
    return tx.fetchscalar(stmt, **fetch_kwargs)


@_query_executor_builder
def fetchscalars(tx, stmt, model, **fetch_kwargs):
    """Executes the decorated function and fetch the first value of each row
    """
    return tx.fetchscalars(stmt, **fetch_kwargs)


@_query_executor_builder
def fetchcomposite(tx, stmt, model, **fetch_kwargs):
    """Executes the decorated function and fetch the first value of each row
    """
    return tx.fetchcomposite(stmt, model=model, **fetch_kwargs)


@_query_executor_builder
def execute(tx, stmt, model):
    """Executes the decorated function
    """
    tx.execute(stmt)


@_query_executor_builder
def update(tx, stmt, obj, **fetch_kwargs):
    """Executes the decorated function and update an object
    When no model or object have been provided via the "model" argument,
    returns a function that take the object to update as parameter
    """
    if obj:
        return tx.fetchhydrated(obj, stmt, **fetch_kwargs)
    row = tx.fetchone(stmt, **fetch_kwargs)
    return lambda o: o.__dict__.update(dict(row))