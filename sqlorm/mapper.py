from .sql import SQL, Column as SQLColumn, ColumnExpr as SQLColumnExpr, ColumnList
from .sql_template import SQLTemplate
from .resultset import CompositionMap, ResultSet
from .types import SQLType
import typing as t


PrimaryKeyColumn = object()
T = t.TypeVar("T")
PrimaryKey = t.Annotated[T, PrimaryKeyColumn]


class UnknownValue:
    pass
unknown_value = UnknownValue()


class MapperError(Exception):
    pass


class Mapper:
    """Maps database columns to object attributes
    """

    @classmethod
    def from_class(cls, object_class, **kwargs):
        """Creates a Mapper from a class annotations
        """
        if hasattr(object_class, "__mapper__"):
            return object_class.__mapper__
        mapper = cls(object_class, kwargs.pop("table", getattr(object_class, "__table__", object_class.__name__)), **kwargs)
        for key, value in object_class.__annotations__.items():
            primary_key = False
            if value.__class__ is t._AnnotatedAlias:
                primary_key = PrimaryKeyColumn in getattr(value, "__metadata__", ())
                value = value.__args__[0]
            mapper.columns.append(Column(key, SQLType.from_pytype(value), primary_key=primary_key, attribute=key))
        return mapper

    def __init__(self, object_class, table, allow_unknown_columns=True, **mapped_attrs):
        self.object_class = object_class
        self.table = table
        self.allow_unknown_columns = allow_unknown_columns
        self.force_select_wildcard = False
        self.columns = MapperColumnList(self)
        self.relationships = []
        if mapped_attrs:
            self.map(mapped_attrs)

    def map(self, attr, col_or_rel=None):
        if isinstance(col_or_rel, MappedColumnMixin):
            col_or_rel.attribute = attr
            col_or_rel.mapper = self
            self.columns.append(col_or_rel)
        elif isinstance(col_or_rel, Relationship):
            col_or_rel.attribute = attr
            col_or_rel.mapper = self
            self.relationships.append(col_or_rel)
        elif isinstance(attr, dict):
            for k, v in attr.items():
                self.map(k, v)
        else:
            raise MapperError("Provide a Column or Relationship")
        
    def get(self, name):
        for col in self.columns:
            if col.attribute == name:
                return col
        for rel in self.relationships:
            if rel.attribute == name:
                return rel
            
    def __getitem__(self, name):
        attr = self.get(name)
        if not attr:
            raise KeyError()
        return attr
    
    @property
    def attributes(self):
        attrs = {}
        attrs.update({c.attribute: c for c in self.columns})
        attrs.update({r.attribute: r for r in self.relationships})
        return attrs
        
    @property
    def attr_names(self) -> t.Sequence[str]:
        return list(self.attributes.keys())
        
    @property
    def eager_columns(self) -> ColumnList:
        """Returns a ColumnList with columns that are not lazy loaded"""
        return ColumnList([c for c in self.columns if not c.lazy])

    @property
    def lazy_columns(self) -> ColumnList:
        """Rerturns a ColumnList with only columns that are lazy loaded"""
        return ColumnList([c for c in self.columns if c.lazy])
        
    @property
    def primary_key(self):
        """Returns the single column or a tuple of column containing columns flagged as part of the primary key"""
        pk_cols = [c for c in self.columns if c.primary_key]
        if len(pk_cols) > 1:
            return pk_cols
        if pk_cols:
            return pk_cols[0]
        
    def primary_key_condition(self, pk, table_alias=None, prefix=None) -> SQL:
        """Returns the SQL condition to query a single row from this mapped table matching the primary key"""
        cols = self.primary_key
        if isinstance(cols, list):
            if not isinstance(pk, (list, tuple)):
                raise MapperError("Primary key is a composite and requires a tuple")
            return SQL.And([col.aliased_table(table_alias).prefixed(prefix) == SQL.Param(pk[i]) for i, col in enumerate(cols)])
        if cols:
            return cols.aliased_table(table_alias).prefixed(prefix) == SQL.Param(pk)
        raise MapperError(f"Missing primary key for table {self.table}")
    
    def get_primary_key(self, obj):
        """Returns the primary key value of the model object"""
        pk = []
        for col in self.columns:
            if col.primary_key:
                pk.append(getattr(obj, col.attribute))
        if len(pk) > 1:
            return pk
        if pk:
            return pk[0]
    
    @property
    def defaults(self) -> t.Mapping[str, t.Any]:
        """Returns default column values"""
        defaults = {}
        for col in self.columns:
            if callable(col.default):
                defaults[col.attribute] = col.default()
            elif col.default is not unknown_value:
                defaults[col.attribute] = col.default
        return defaults
    
    def hydrate_new(self, data):
        """Initializes a new object, skipping __init__() and populates it using hydrate()"""
        obj = self.object_class.__new__(self.object_class) # prevents calling __init__()
        self.hydrate(obj, data)
        return obj
    
    def hydrate(self, obj, data, with_unknown=None):
        """Populates a model object with data from the db
        """
        attrs = set()
        for key in data.keys(): # avoid using .items() as some DBAPI returned objects only provide keys() (eg: sqlite3)
            if key in self.columns.names:
                col = self.columns[key]
                col.load(obj, data)
                key = col.attribute
            elif with_unknown or with_unknown is None and self.allow_unknown_columns:
                obj.__dict__[key] = data[key] # ensure that no custom setter are used
            else:
                continue
            attrs.add(key)
        if not hasattr(obj, "__hydrated_attrs__"):
            object.__setattr__(obj, "__hydrated_attrs__", attrs)
        else:
            object.__setattr__(obj, "__hydrated_attrs__", obj.__hydrated_attrs__ | attrs)
        return attrs
    
    def dehydrate(self, obj, only=None, except_=None, with_primary_key=True, with_unknown=None, additional_attrs=None) -> t.Mapping[str, t.Any]:
        """Returns the model object as data to be sent to the db
        """
        cols = [c for c in self.columns if with_primary_key or not c.primary_key]
        if with_unknown or with_unknown is None and self.allow_unknown_columns:
            for attr in getattr(obj, "__hydrated_attrs__", []):
                if attr not in self.attr_names:
                    cols.append(Column(attr, attribute=attr))
        if additional_attrs:
            for attr in additional_attrs:
                cols.append(Column(additional_attrs[attr] if isinstance(additional_attrs, dict) else attr, attribute=attr))

        values = {}
        for col in cols:
            if only is not None and col.attribute not in only:
                continue
            if except_ is not None and col.attribute in except_:
                continue
            col.dump(obj, values)
        return values
    
    def select_columns(self, with_lazy=False, table_alias=None, prefix=None, wildcard_if_empty=True):
        if self.force_select_wildcard:
            return SQL.Cols([], table=table_alias, wildcard=True)
        if isinstance(with_lazy, str):
            with_lazy = [with_lazy]
        if isinstance(with_lazy, (tuple, list)):
            columns = [c for c in self.columns if not c.lazy or c.name in with_lazy]
        elif with_lazy:
            columns = self.columns
        else:
            columns = self.eager_columns
        if not columns and wildcard_if_empty:
            if prefix:
                raise MapperError("Cannot use prefix if no columns have been mapped")
            columns = ["*"]
        return SQL.Cols(columns, table=table_alias, prefix=prefix)
    
    def select_from(self, columns=None, table_alias=None, with_rels=None, with_joins=None, with_lazy=False) -> SQL:
        """Creates a SELECT statement based on the mapping information
        """
        if columns is None:
            columns = self.select_columns(with_lazy, table_alias or self.table, wildcard_if_empty=True)
        else:
            columns = SQL.Cols(columns).aliased_table(table_alias or self.table)
        joins = []
        if with_rels is True or with_rels == "lazy" or with_rels is None:
            with_rels = [r.attribute for r in self.relationships if with_rels is True or (with_rels=="lazy" and r.lazy) or (with_rels is None and not r.lazy)]
        if with_joins is True or with_joins == "lazy" or with_joins is None:
            with_joins = [r.attribute for r in self.relationships if with_joins is True or (with_joins=="lazy" and r.lazy) or (with_joins is None and not r.lazy)]
        if with_rels or with_joins:
            for rel in self.relationships:
                if rel.attribute in with_rels or rel in with_rels:
                    columns.append(rel.columns)
                if rel.attribute in with_rels or rel in with_rels or with_joins and (rel.attribute in with_joins or rel in with_joins):
                    joins.append(rel.join_clause())
        return SQL.select(columns).from_(SQL.Id(self.table, table_alias), *joins)

    def select_by_pk(self, pk, **select_kwargs):
        return self.select_from(**select_kwargs).where(self.primary_key_condition(pk, table_alias=select_kwargs.get('table_alias')))
    
    def insert(self, obj, **dehydrate_kwargs):
        return SQL.insert(self.table, self.dehydrate(obj, **dehydrate_kwargs))
    
    def update(self, obj, **dehydrate_kwargs):
        values = self.dehydrate(obj, with_primary_key=False, **dehydrate_kwargs)
        if not values:
            return
        return SQL.update(self.table, values).where(self.primary_key_condition(self.get_primary_key(obj)))

    def delete(self, obj):
        pk = self.get_primary_key(obj)
        if not pk:
            return
        return SQL.delete_from(self.table).where(self.primary_key_condition(pk))
    
    def __repr__(self):
        return f"<Mapper({self.table} -> {self.object_class.__name__})>"
    

class MapperColumnList(ColumnList):
    def __init__(self, mapper):
        super().__init__([], wildcard=None)
        self.mapper = mapper
    
    def get(self, name):
        for col in self:
            if col.name == name:
                return col
        if self.mapper.allow_unknown_columns:
            col = Column(name)
            col.attribute = name
            col.mapper = self.mapper
            return col


class MappedColumnMixin:
    """Reprensents a mapped column.
    """
    attribute: str # will be provided by Mapper when added to it
    mapper: Mapper # will be provided by Mapper when added to it

    def __init__(self, name, type=None, primary_key=False, nullable=True, default=unknown_value,
                 references=None, unique=False, lazy=False, attribute=None):
        self.name = name
        self.table = None
        self.alias = None
        self.prefix = None
        self.type = SQLType.from_pytype(type) if type and not isinstance(type, SQLType) else type
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.references = references
        self.unique = unique
        self.lazy = lazy
        self.attribute = attribute

    def get(self, obj):
        """Returns the database value from the object, using the provided dump function if needed
        Returns unknown_value when the attribute is missing on the object
        """
        if hasattr(obj, self.attribute):
            value = getattr(obj, self.attribute)
            if self.type and self.type.dumper:
                return self.type.dumper(value)
            return value
        return unknown_value
        
    def dump(self, obj, values):
        """Dumps the value from the object in the values dict under the column name if the attribute exists on the object"""
        value = self.get(obj)
        if value is not unknown_value:
            values[self.name] = value
        
    def load(self, obj, values):
        """Sets the object attribute from the database row, using the provided load function if needed
        (used by Mapper.hydrate())
        """
        value = values[self.name]
        if self.type and self.type.loader:
            value = self.type.loader(value)
        obj.__dict__[self.attribute] = value

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.name} -> {self.attribute})>"


class Column(MappedColumnMixin, SQLColumn):
    pass


class ColumnExpr(MappedColumnMixin, SQLColumnExpr):
    def __init__(self, expr, alias, *args, **kwargs):
        MappedColumnMixin.__init__(self, alias, *args, **kwargs)
        self.expr = expr

    def dump(self, obj, values):
        pass


class Relationship:
    attribute: str # will be provided by Mapper when added to it
    mapper: Mapper # will be provided by Mapper when added to it

    def __init__(self, target_mapper, target_col=None, target_attr=None, source_col=None, source_attr=None,
                 join_type="LEFT JOIN", join_condition=None, select_cols=None, single=False, lazy=True):
        self._target_mapper = target_mapper
        self.target_col = target_col
        self._target_attr = target_attr
        self._source_col = source_col
        self._source_attr = source_attr
        self.join_type = join_type
        self._join_condition = join_condition
        self.select_cols = select_cols
        self.single = single
        self.lazy = lazy

    @property
    def target_mapper(self) -> Mapper:
        # using @property to allow override in subclasses
        return self._target_mapper
    
    @property
    def target_table(self):
        return self.target_mapper.table
    
    @property
    def target_attr(self):
        if self._target_attr:
            return self._target_attr
        if self.target_col in self.target_mapper.columns:
            return self.target_mapper.columns[self.target_col].attribute
        return self.target_col
    
    @property
    def source_table(self):
        return self.mapper.table
    
    @property
    def source_col(self):
        if self._source_col:
            return self._source_col
        if self.mapper.primary_key:
            return self.mapper.primary_key.name
        return "id"
    
    @property
    def source_attr(self):
        if self._source_attr:
            return self._source_attr
        if self.source_col in self.mapper.columns:
            return self.mapper.columns[self.source_col].attribute
        return self.source_col
    
    @property
    def columns(self):
        if self.select_cols:
            cols = SQL.Cols(self.select_cols)
        else:
            cols = self.target_mapper.eager_columns
        return cols.aliased_table(self.target_table).prefixed(f"{self.attribute}__")
    
    def __getattr__(self, name):
        return self.target_mapper.columns.aliased_table(self.target_table)[name]
    
    def join_condition(self, target_alias=None, source_alias=None) -> SQL:
        if not target_alias:
            target_alias = self.target_table
        if not source_alias:
            source_alias = self.source_table
        if isinstance(self._join_condition, str):
            return SQLTemplate(str(self._join_condition), {
                source_alias: source_alias,
                target_alias: target_alias
            })
        elif self._join_condition:
            return self._join_condition
        if not self.target_col:
            raise MapperError(f"Missing target_col on relationship '{self.attribute}'")
        return SQL.Col(self.target_col, table=target_alias) == SQL.Col(self.source_col, table=source_alias)
    
    def join_clause(self, target_alias=None, source_alias=None):
        return SQL(self.join_type, SQL.Id(self.target_table, target_alias), "ON",
                   self.join_condition(target_alias, source_alias))
    
    def select_from_target(self, source_obj):
        """Returns the select statement used to retrieve objects from the target
        """
        source_attr = self.source_attr
        if self._join_condition:
            return self.target_mapper.select_from(table_alias=self.target_table).join(self.source_table).on(self.join_condition())\
                    .where(SQL.Col(self.source_col, self.source_table) == SQL.Param(getattr(source_obj, source_attr)))
        if not self.target_col:
            raise MapperError(f"Missing target_col on relationship '{self.attribute}'")
        return self.target_mapper.select_from().where(SQL.Col(self.target_col) == SQL.Param(getattr(source_obj, source_attr)))
    
    def delete_from_target(self, source_obj):
        """Returns a delete statement to delete all related rows in the target
        """
        source_attr = self.source_attr
        if self._join_condition:
            return SQL.delete_from(self.target_table).where(self.target_mapper.primary_key.in_(self.select_from_target(source_obj)))
        if not self.target_col:
            raise MapperError(f"Missing target_col on relationship '{self.attribute}'")
        return SQL.delete_from(self.target_table).where(SQL.Col(self.target_col) == SQL.Param(getattr(source_obj, source_attr)))

    def __repr__(self):
        return f"<Relationship({self.attribute})>"


class HydratedResultSet(ResultSet):
    def __init__(self, cursor, mapper, auto_close_cursor=True):
        if not isinstance(mapper, Mapper):
            mapper = Mapper.from_class(mapper)
        super().__init__(cursor, mapper.hydrate_new, auto_close_cursor)
        self.mapper = mapper


class HydrationMap(CompositionMap):
    def __init__(self, mapper, nested=None, single=False):
        if not isinstance(mapper, Mapper):
            mapper = Mapper.from_class(mapper)
        rowid = mapper.primary_key.name if mapper.primary_key else None
        _nested = {r.attribute: HydrationMap(r.target_mapper, single=r.single) for r in mapper.relationships}
        if nested:
            _nested.update({k: HydrationMap.create(v) for k, v in nested.items()})
        super().__init__(mapper.hydrate_new, _nested, rowid, single)
        self.mapper = mapper