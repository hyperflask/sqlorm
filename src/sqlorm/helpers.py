from .engine import Engine, Session, Transaction, ensure_session
import functools


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


def after_commit(func, *args, **kwargs):
    with ensure_session() as session:
        def rollback_listener(session):
            Session.after_commit.disconnect(commit_listener)
            Session.after_rollback.disconnect(rollback_listener)
        def commit_listener(session):
            func(*args, **kwargs)
            rollback_listener(session)
        Session.after_commit.connect(commit_listener, session, weak=False)
        Session.after_rollback.connect(rollback_listener, session, weak=False)