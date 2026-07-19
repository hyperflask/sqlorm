from .engine import Engine, Session, Transaction, ensure_session, get_current_session
import functools


def connect_via_engine(engine, signal, func=None):
    """
    Connect a function to a signal, ensuring the engine matches.
    """
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


def after_commit(func):
    if not get_current_session() or not get_current_session().in_transaction:
        func()
        return
    def rollback_listener(session):
        Session.after_commit.disconnect(commit_listener)
        Session.after_rollback.disconnect(rollback_listener)
    def commit_listener(session):
        func()
        rollback_listener(session)
    Session.after_commit.connect(commit_listener, get_current_session(), weak=False)
    Session.after_rollback.connect(rollback_listener, get_current_session(), weak=False)