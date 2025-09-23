from .model import BaseModel
from .sql import SQL, split_sql_statements
from .sqlfunc import execute
from .engine import ensure_transaction, get_current_session
from .mapper import Mapper
import os
import re
import runpy
import logging


def init_db(path="migrations", model_registry=None, logger=None, engine=None):
    if os.path.exists(path):
        migrate(path, logger=logger, engine=engine)
    else:
        create_all(model_registry, engine=engine, check_missing=True, logger=logger)


def create_all(model_registry=None, engine=None, check_missing=False, logger=None, **kwargs):
    logger = logging.getLogger("sqlorm") if logger is True else logger
    if not model_registry:
        model_registry = BaseModel.__model_registry__
    for model in model_registry.values():
        missing = True
        if check_missing:
            missing = False
            with ensure_transaction(engine) as tx:
                try:
                    model.find_one()
                except Exception:
                    missing = True
        if missing:
            if logger:
                logger.info(f"Creating table {model.__mapper__.table}")
            create_table(model.__mapper__, **kwargs)


@execute
def create_table(mapper, default_type="varchar"):
    if not isinstance(mapper, Mapper):
        mapper = Mapper.from_class(mapper)
    dbapi = get_current_session().engine.dbapi
    return SQL(
        "CREATE TABLE",
        mapper.table,
        SQL.List(
            [
                SQL(c.schema_def) if c.schema_def else SQL(
                    c.name,
                    c.type.sql_type if c.type else default_type,
                    "UNIQUE" if c.unique else "",
                    "NOT NULL" if not c.nullable else "",
                    getattr(dbapi, "PRIMARY_KEY_SCHEMA_DEFINITION", "PRIMARY KEY") if c.primary_key else "",
                    SQL(
                        "REFERENCES",
                        f"{c.references.table} ({c.references.name})"
                        if isinstance(c.references, SQL.Col)
                        else c.references,
                    )
                    if c.references
                    else "",
                )
                for c in mapper.columns
            ],
            startstr="(\n   ",
            joinstr=",\n   ",
            endstr="\n)"
        ),
    )


def migrate(
    path="migrations",
    from_version=None,
    to_version=None,
    use_schema_version=True,
    save_version_after_step=False,
    logger=None,
    dryrun=False,
    engine=None,
):
    logger = logging.getLogger("sqlorm") if logger is True else logger
    with ensure_transaction(engine):
        if from_version is None and use_schema_version:
            from_version = get_schema_version()
        if from_version is not None and logger:
            logger.info(f"Resume migrations from {from_version}")
        migrations = create_migrations_from_dir(path, from_version, to_version)
        version = None
        for version, name, filename in migrations:
            if logger:
                logger.info(f"Executing migration {version}: {name}")
            if dryrun:
                continue
            try:
                execute_migration(filename, engine)
                if use_schema_version and save_version_after_step:
                    set_schema_version(version, engine)
            except Exception:
                if logger:
                    logger.error("Last migration failed, ending")
                raise
        if not dryrun and version is not None and use_schema_version and not save_version_after_step:
            set_schema_version(version, engine)
        return version


def create_migrations_from_dir(path, from_version=None, to_version=None):
    migrations = []
    for filename in sorted(os.listdir(path)):
        m = re.match(r"([0-9]+)_([^.]+)\.(sql|py)", filename, re.I)
        if not m:
            continue
        version = int(m.group(1))
        name = m.group(2)
        if from_version is not None and version <= int(from_version):
            continue
        if to_version is not None and version > int(to_version):
            break
        migrations.append((version, name, os.path.join(path, filename)))
    return migrations


def execute_migration(filename, engine=None):
    if filename.endswith(".py"):
        runpy.run_path(filename)
    else:
        with open(filename) as f:
            statements = split_sql_statements(f.read())
        with ensure_transaction(engine) as tx:
            tx.execute(statements)


def get_schema_version(engine=None):
    with ensure_transaction(engine) as tx:
        try:
            return int(tx.fetchscalar("SELECT version FROM schema_version LIMIT 1"))
        except Exception:
            pass


def set_schema_version(version, engine=None):
    with ensure_transaction(engine) as tx:
        try:
            tx.execute(SQL.update("schema_version", {"version": version}))
        except Exception:
            tx.execute(
                (
                    "CREATE TABLE schema_version (version text)",
                    SQL.insert("schema_version", {"version": version}),
                )
            )


def create_initial_migration(model_registry=None, models=None, path="migrations", version=None, **kwargs):
    if not model_registry:
        model_registry = BaseModel.__model_registry__

    if version is None:
        migrations = create_migrations_from_dir(path)
        version = '%03d' % (migrations[-1][0] + 1) if migrations else "000"

    if not models:
        models = model_registry.values()
        header = "Initial creation of the database"
        name = "initial"
    else:
        models = [model_registry[m] if isinstance(m, str) else m for m in models]
        header = "Creation of tables: " + ", ".join(m.__mapper__.table for m in models)
        name = "create_" + "_".join(m.__mapper__.table for m in models)

    stmts = []
    for model in models:
        stmts.append(str(create_table.sql(model.__mapper__, **kwargs)))

    sql = f"-- {header} (auto-generated by sqlorm)\n\n" + ";\n\n".join(stmts) + ";\n"
    with open(os.path.join(path, f"{version}_{name}.sql"), "w") as f:
        f.write(sql)

    return (header, version, name)