import typing as t


class ResultSet:
    """Wraps a cursor and fetches row that can be processed with a loader
    """
    
    def __init__(self, cursor, loader=None, auto_close_cursor=True):
        self.cursor = cursor
        self.loader = loader
        self.auto_close_cursor = auto_close_cursor

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
    
    def fetch(self, with_loader=True):
        if not self.cursor:
            return
        row = self.cursor.fetchone()
        if row is None:
            if self.auto_close_cursor:
                self.cursor.close()
            return
        if with_loader and self.loader:
            return self.loader(row)
        return row
    
    def first(self, with_loader=True):
        row = self.fetch(with_loader=with_loader)
        if self.auto_close_cursor:
            self.close()
        return row
    
    def scalar(self):
        row = self.first(with_loader=False)
        return row[row.keys()[0]]
    
    def scalars(self):
        return [row[row.keys()[0]] for row in self]
    
    def all(self):
        if self.loader:
            return list(self.__iter__())
        # if no loader we can call the cursor fetchall() for optimization
        rows = self.cursor.fetchall()
        if self.auto_close_cursor:
            self.cursor.close()
        return rows

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetch()
        if row is None:
            raise StopIteration()
        return row
    

class CompositionMap:
    """Defines how a row is composited
    """

    @classmethod
    def create(cls, spec):
        if isinstance(spec, CompositionMap):
            return spec
        if isinstance(spec, (list, tuple)):
            if len(spec) == 1:
                return cls(spec[0])
            nested = {k: cls.create(v) for k, v in spec[1].items()} if spec[1] else {}
            if len(spec) > 3:
                return cls(spec[0], nested, spec[2], spec[3])
            if len(spec) > 2:
                return cls(spec[0], nested, spec[2])
            return cls(spec[0], nested)
        return cls(spec)

    def __init__(self, loader=None, nested=None, rowid=None, single=False):
        self.loader = loader
        self.nested = nested or {}
        self.rowid = rowid
        self.single = single

    def get(self, path):
        map = self
        for segment in path.split("."):
            if segment not in map.nested:
                map.nested[segment] = CompositionMap()
            map = map.nested[segment]
        return map

    def map(self, path, loader=None, nested=None, rowid=None, single=False):
        map = self.get(path)
        map.loader = loader
        map.nested = nested or {}
        map.rowid = rowid
        map.single = single


class CompositeResultSet(ResultSet):
    """A result set that can rebuild hierarchical data based on column names convention
    Useful to eager load data in a single query.

    Example:

        stmt = "SELECT users.username, comments.id AS comments__id, comments.content AS comments__content
                FROM users LEFT JOIN comments ON comments.user_id = users.id"

        cur = tx.fetchcomposited(stmt)
        row = cur.fetchone()

        assert row["username"]
        assert row["comments"]
        assert row["comments"][0]["id"]
    """

    default_map_class = CompositionMap

    def __init__(self, cursor, map=None, separator="__", auto_close_cursor=True):
        super().__init__(cursor, auto_close_cursor=auto_close_cursor)
        self.map = map if isinstance(map, CompositionMap) else self.default_map_class.create(map)
        self.separator = separator
        self.current = None
        self.disable_composition = False

    def fetch(self, with_loader=True):
        if not self.cursor:
            return
        while True:
            row = self.cursor.fetchone()

            if row is None:
                if self.auto_close_cursor:
                    self.cursor.close()
                self.cursor = None
                if self.current:
                    return self.current.compile(with_loader=with_loader)
                return
            
            if self.disable_composition:
                # skip processing the row as no composition was detected previously
                return self.map.loader(row) if with_loader and self.map.loader else row
            
            row = CompositeRow(row, self.map, self.separator)
            if self.current is None:
                if not row.is_composite:
                    # this is an optimization to reduce processing on further fetches
                    self.disable_composition = True
                    return row.compile(with_loader=with_loader)
                self.current = row
            elif not self.current.merge(row):
                current = self.current.compile(with_loader=with_loader)
                self.current = row
                return current
    
    def all(self):
        # must go through our custom fetching logic
        return list(self.__iter__())


class CompositeRowError(Exception):
    pass
        

class CompositeRow:
    def __init__(self, row, map: CompositionMap, separator="__", nested=None):
        self.map = map
        self.separator = separator
        if nested is None:
            row, nested = split_composite_row(row, separator)
        self.row = row
        self.is_composite = bool(nested)

        if not row and self.is_composite:
            # make sure all rows with no self columns but only composition merge together
            self.id = True
        elif self.is_composite:
            # we need to identify an id for this row so all future row with the same id
            # will be merged together. this allows us to handle nested data structures
            if callable(self.map.rowid):
                self.id = self.map.rowid(row)
            elif self.map.rowid:
                self.id = row[self.map.rowid]
            else:
                self.id = hash(row)
        else:
            # no nested row, we don't merge with any future row
            self.id = None

        self.nested = {}
        for k, v in nested.items():
            if v is None: # skip nested rows where all columns are null
                continue
            self.nested[k] = [CompositeRow(v[0], map.get(k), separator, v[1])]

    def merge(self, other):
        if self.id is None or (self.id != True and other.id != self.id):
            return False
        for k, rows in self.nested.items():
            if not rows[-1].merge(other.nested[k][-1]):
                rows.append(other.nested[k][-1])

    def compile(self, with_loader=True):
        for k, rows in self.nested.items():
            self.row[k] = [r.compile() for r in rows]
            if self.map.get(k).single and self.row[k]:
                self.row[k] = self.row[k][0]
        if with_loader and self.map.loader:
            return self.map.loader(self.row)
        return self.row


def split_composite_row(row, separator="__"):
    row = dict(row)
    nested = {}
    for col in list(row.keys()):
        if separator not in col:
            continue
        composite_key, composite_col = col.split(separator, 1)
        nested.setdefault(composite_key, {})[composite_col] = row.pop(col)
    for key in list(nested.keys()):
        if all([nested[key][k] is None for k in nested[key]]):
            nested[key] = None
        else:
            nested[key] = split_composite_row(nested[key], separator)
    return row, nested