import abc
import typing as t
import inspect
from .sql import SQL, Column as SQLColumn, ColumnExpr as SQLColumnExpr
from .engine import Engine, ensure_transaction
from .sqlfunc import is_sqlfunc, sqlfunc, fetchall, fetchone, execute, update
from .resultset import CompositeResultSet
from .types import SQLType
from .mapper import (Mapper, MappedColumnMixin, Relationship as MappedRelationship,
                     MapperError, PrimaryKeyColumn)


class ModelMetaclass(abc.ABCMeta):
    def __new__(cls, name, bases, dct):
        if not bases or abc.ABC in bases:
            return super().__new__(cls, name, bases, dct)
        dct = cls.pre_process_model_class_dict(name, bases, dct)
        model_class = super().__new__(cls, name, bases, dct)
        cls.process_meta_inheritance(model_class)
        return cls.post_process_model_class(model_class)
    
    @staticmethod
    def pre_process_model_class_dict(name, bases, dct):
        model_registry = {}
        for base in bases:
            if issubclass(base, BaseModel):
                model_registry = base.__model_registry__
                break
        
        dct["table"] = SQL.Id(dct.get("__table__", dct.get("table", name.lower())))

        mapped_attrs = {}
        for name, annotation in dct.get("__annotations__", {}).items():
            primary_key = False
            if annotation.__class__ is t._AnnotatedAlias:
                primary_key = PrimaryKeyColumn in getattr(annotation, "__metadata__", ())
                annotation = annotation.__args__[0]
            if name not in dct:
                # create column object for annotations with no default values
                dct[name] = mapped_attrs[name] = Column(name, annotation, primary_key=primary_key)
            elif isinstance(dct[name], Column):
                mapped_attrs[name] = dct[name]
                dct[name].type = SQLType.from_pytype(annotation)
            elif isinstance(dct[name], Relationship):
                # add now to keep the declaration order
                mapped_attrs[name] = dct[name]
                
        for attr_name, attr in dct.items():
            if isinstance(attr, Column) and not attr.name:
                # in the case of models, we allow column object to be initialized without names
                # the default name being the attribute name
                attr.name = attr_name
            if isinstance(attr, (Column, Relationship)) and attr_name not in mapped_attrs:
                # not annotated attributes
                mapped_attrs[attr_name] = attr
                continue

            wrapper = type(attr) if isinstance(attr, (staticmethod, classmethod)) else False
            if wrapper:
                # the only way to replace the wrapped function for a class/static method is before the class initialization.
                attr = attr.__wrapped__
            if callable(attr):
                if is_sqlfunc(attr):
                    if not getattr(attr, "query_decorator", None) and ".select_from(" in inspect.getdoc(attr):
                        # because the statement does not start with SELECT, it would default to execute when using .select_from()
                        attr = fetchall(attr)
                    # the model registry is passed as template locals to sql func methods
                    # so model classes are available in the evaluation scope of SQLTemplate
                    method = sqlfunc(attr, is_method=True, template_locals=model_registry)
                    dct[attr_name] = wrapper(method) if wrapper else method

        dct["__mapper__"] = mapped_attrs
        return dct
    
    @staticmethod
    def post_process_model_class(cls):
        mapped_attrs = cls.__mapper__
        cls.__mapper__ = ModelMapper(cls, cls.table.name, allow_unknown_columns=cls.Meta.allow_unknown_columns)
        cls.__mapper__.map(mapped_attrs)
        cls.c = cls.__mapper__.columns # handy shortcut

        auto_primary_key = cls.Meta.auto_primary_key
        if auto_primary_key and not cls.__mapper__.primary_key:
            if cls.__mapper__.allow_unknown_columns and not cls.__mapper__.columns:
                # we force the usage of SELECT * as we auto add a primary key without any other mapped columns
                # without doing this, only the primary key would be selected
                cls.__mapper__.force_select_wildcard = True
            cls.__mapper__.map(auto_primary_key, Column(auto_primary_key, primary_key=True))

        cls.__model_registry__.register(cls)
        return cls
    
    @staticmethod
    def process_meta_inheritance(cls):
        if getattr(cls.Meta, "__inherit__", True):
            bases_meta = ModelMetaclass.aggregate_bases_meta_attrs(cls)
            for key, value in bases_meta.items():
                if not hasattr(cls.Meta, key):
                    setattr(cls.Meta, key, value)
            cls.Meta.__inherit__ = False # prevent further inheritance processing when subclassing models

    @staticmethod
    def aggregate_bases_meta_attrs(cls):
        meta = {}
        for base in cls.__bases__:
            if issubclass(base, BaseModel):
                if getattr(base.Meta, "__inherit__", True):
                    meta.update(ModelMetaclass.aggregate_bases_meta_attrs(base))
                meta.update({k: getattr(base.Meta, k) for k in dir(base.Meta) if not k.startswith("_")})
        return meta
            

class ModelMapper(Mapper):
    """Subclass of Mapper to handle dirtyness tracking"""

    def hydrate(self, obj, data, with_unknown=None):
        attrs = super().hydrate(obj, data, with_unknown=with_unknown)
        if hasattr(obj, "__dirty__"):
            object.__setattr__(obj, "__dirty__", obj.__dirty__ - attrs)
        return attrs

    def dehydrate(self, obj, dirty_only=None, with_primary_key=True, with_unknown=None, additional_attrs=None) -> t.Mapping[str, t.Any]:
        only = None
        if dirty_only or dirty_only is None and obj.Meta.insert_update_dirty_only:
            only = getattr(obj, "__dirty__", set())
            if with_unknown or with_unknown is None and self.allow_unknown_columns:
                additional_attrs = [a for a in only if a not in self.attr_names]

        return super().dehydrate(obj, only=only, with_primary_key=with_primary_key,
                                 with_unknown=with_unknown, additional_attrs=additional_attrs)
    

class ModelColumnMixin(MappedColumnMixin):
    """Subclass of mapper.Column to provide a Model aware behavior and act as property descriptor"""

    def __init__(self, name=None, *args, **kwargs):
        # override to make name optional, in this case will be set with attribute name in __new__
        super().__init__(name, *args, **kwargs)

    def __get__(self, obj, owner=None):
        if not obj:
            return self
        if self.attribute not in obj.__dict__ and self.lazy:
            self.fetch(obj)
        return obj.__dict__.get(self.attribute)
    
    def __set__(self, obj, value):
        obj.__dict__[self.attribute] = value
        flag_dirty_attr(obj, self.attribute)

    def fetch(self, obj):
        """Fetches the value of the column from the database and loads it in the object
        """
        cols = []
        if isinstance(self.lazy, str):
            # lazy can be a string, in which case you can define groups of columns to be lazily loaded together
            for col in obj.__mapper__.columns:
                if col.lazy == self.lazy:
                    cols.append(col)
        else:
            cols.append(self)
        rs = obj.__class__.query(obj.__class__.select_from(cols))
        # we use the hydrator as multiple values may be returned at once if lazy is a group
        obj.__mapper__.hydrate(obj, rs.first(with_loader=False))
    
    def is_loaded(self, obj):
        """Checks if this column has been loaded in the object"""
        return self.attribute in obj.__dict__
    

class Column(ModelColumnMixin, SQLColumn):
    pass


class ColumnExpr(ModelColumnMixin, SQLColumnExpr):
    def __init__(self, expr, alias=None, *args, **kwargs):
        ModelColumnMixin.__init__(self, alias, *args, **kwargs)
        self.expr = expr

    def dump(self, obj, values):
        pass


class Relationship(MappedRelationship):
    """Subclass of mapper.Relationship to provide a Model behavior and act as property descriptor"""

    def __init__(self, target, *args, **kwargs):
        self.list_class = kwargs.pop("list_class", RelatedObjectsList)
        super().__init__(None, *args, **kwargs)
        self._target = target

    @property
    def target(self):
        target = self._target
        if isinstance(target, str):
            target = self.mapper.object_class.__model_registry__.get(target)
        if not target:
            raise MapperError(f"Missing target for relationship '{self.attribute}'")
        return target
    
    @property
    def target_mapper(self):
        return self.target.__mapper__
    
    @property
    def __isabstractmethod__(self):
        # compatibility between this description __getattr__ usage and abc.ABC
        return False

    def __get__(self, obj, owner=None):
        if not obj:
            return self
        if self.attribute not in obj.__dict__:
            self.fetch(obj)
        return obj.__dict__[self.attribute]
    
    def __set__(self, obj, value):
        obj.__dict__[self.attribute] = value if self.single else self.list_class(obj, self, value)
    
    def is_loaded(self, obj):
        return self.attribute in obj.__dict__
    
    def fetch(self, obj):
        """Fetches the list of related objects from the database and loads it in the object
        """
        r = self.target.query(self.select_from_target(obj))
        self.__set__(obj, r.first() if self.single else r.all())


class RelatedObjectsList(object):
    def __init__(self, obj, relationship, items):
        self.obj = obj
        self.relationship = relationship
        self.items = items

    def __iter__(self):
        return iter(self.items)
    
    def __getitem__(self, idx):
        return self.items[idx]
    
    def __len__(self):
        return len(self.items)
    
    def __contains__(self, item):
        return item in self.items

    def append(self, item):
        target_attr = self.relationship.target_attr
        if not target_attr:
            raise MapperError(f"Missing target_attr on relationship '{self.relationship.attribute}'")
        setattr(item, target_attr, getattr(self.obj, self.relationship.source_attr))

    def remove(self, item):
        target_attr = self.relationship.target_attr
        if not target_attr:
            raise MapperError(f"Missing target_attr on relationship '{self.relationship.attribute}'")
        setattr(item, target_attr, None)


def flag_dirty_attr(obj, attr):
    if not hasattr(obj, "__dirty__"):
        object.__setattr__(obj, "__dirty__", set())
    obj.__dirty__.add(attr)


def is_dirty(obj) -> bool:
    return bool(getattr(obj, "__dirty__", None))
    

class ModelRegistry(dict):
    def register(self, model):
        self[model.__name__] = model

    def getbytable(self, table):
        for model in self.values():
            if model.__mapper__.table == table:
                return model


class BaseModel(abc.ABC, metaclass=ModelMetaclass):
    """The base class for the model system whether you want utility methods or not
    """
    __engine__: t.ClassVar[t.Optional[Engine]] = None
    __model_registry__: t.ClassVar[ModelRegistry] = ModelRegistry()
    __mapper__: t.ClassVar[Mapper]
    table: SQL.Id
    c: SQL.Cols
    
    # Meta classes provide a way to configure the mapper behavior
    # Values are auto inherited from Meta classes of parent classes
    class Meta:
        auto_primary_key: t.Optional[str] = "id" # auto generate a primary key with this name if no primary key are declared
        allow_unknown_columns: bool = True # hydrate() will set attributes for unknown columns

    @classmethod
    def bind(cls, engine: Engine):
        """Creates a new base model class bound to the provided engine"""
        return type("BoundModel", (cls, abc.ABC), {"__engine__": engine, "__model_registry__": ModelRegistry()})


class Model(BaseModel, abc.ABC):
    """Our standard model class with common ORM utility methods
    """

    class Meta:
        insert_update_dirty_only: bool = True # use BaseModel.__dirty__ to decide which data to insert() or update()

    @classmethod
    def select_from(cls, columns=None, table_alias=None, with_rels=None, with_joins=None, with_lazy=False) -> SQL:
        return cls.__mapper__.select_from(columns, table_alias, with_rels, with_joins, with_lazy)
    
    @classmethod
    def query(cls, stmt, params=None, nested=None, **resultset_kwargs) -> CompositeResultSet:
        """Executes a query and returns results where rows are hydrated to model objects"""
        with ensure_transaction(cls.__engine__) as tx:
            return tx.fetchhydrated(cls, stmt, params, nested=nested, **resultset_kwargs)

    @classmethod
    @fetchall
    def find_all(cls, _where: t.Optional[str | SQL] = None, with_rels=None, with_joins=None, with_lazy=False, **cols) -> CompositeResultSet:
        where = SQL.And([])
        if _where:
            where.append(_where)
        for col, value in cols.items():
            where.append(SQL.Col(col, cls.table.name) == SQL.Param(value))
        q = cls.select_from(with_rels=with_rels, with_joins=with_joins, with_lazy=with_lazy)
        if where:
            return q.where(where)
        return q

    @classmethod
    @fetchone
    def find_one(cls, _where: t.Optional[str | SQL] = None, with_rels=None, with_joins=None, with_lazy=False, **cols):
        return cls.find_all.sql(cls, _where, with_rels=with_rels, with_joins=with_joins, with_lazy=with_lazy, **cols).limit(1)

    @classmethod
    @fetchone
    def get(cls, pk, with_rels=False, with_lazy=False):
        return cls.select_from(with_rels=with_rels, with_lazy=with_lazy)\
            .where(cls.__mapper__.primary_key_condition(pk, cls.table.name))
    
    @classmethod
    def create(cls, **values):
        obj = cls(**values)
        obj.insert()
        return obj
    
    def __init__(self, **values):
        for k, v in dict(self.__mapper__.defaults, **values).items():
            setattr(self, k, v)
    
    def __setattr__(self, name, value):
        self.__dict__[name] = value
        flag_dirty_attr(self, name)

    @update
    def refresh(self, **select_kwargs):
        return self.__mapper__.select_by_pk(self.__mapper__.get_primary_key(self), **select_kwargs)
    
    @update
    def insert(self, **dehydrate_kwargs):
        return self.__mapper__.insert(self, **dehydrate_kwargs).returning("*")
    
    @update
    def update(self, **dehydrate_kwargs):
        stmt = self.__mapper__.update(self, **dehydrate_kwargs)
        if stmt:
            return stmt.returning("*")
    
    def save(self, **dehydrate_kwargs):
        if self.__mapper__.get_primary_key(self):
            self.update(**dehydrate_kwargs)
        else:
            self.insert(**dehydrate_kwargs)

    @execute
    def delete(self):
        return self.__mapper__.delete(self)
    
    def __repr__(self):
        return f"<{self.__class__.__name__}({self.__mapper__.get_primary_key(self) or '?'})>"
