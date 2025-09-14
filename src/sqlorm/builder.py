from .sql import SQLStr, SQL, List


class Conditions(List):
    def __init__(self, *items):
        super().__init__(*items, joinstr="AND")


class QueryBuilderError(Exception):
    pass


class QueryBuilder(SQLStr):
    components = ("SELECT", "FROM", "JOIN+", "WHERE+", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET")
    wrappers = {"SELECT": List, "FROM": List, "WHERE": Conditions, "HAVING": Conditions, "ORDER BY": List}

    def __init__(self):
        self.parts = {}

    def _render(self, params):
        stmt = []
        for component in self.components:
            component = component.rstrip("+")
            if component in self.parts and self.parts[component].parts:
                sql = self.wrappers.get(component, lambda *a: SQL(*a))(*self.parts[component].parts)
                stmt.append(component + " " + str(sql._render(params)))
        return " ".join(stmt)

    def __getattr__(self, name):
        name = name.replace("_", " ").upper().strip()
        if f"{name}+" in self.components:
            if name not in self.parts:
                self.parts[name] = QueryPartBuilder(self)
            return self.parts[name]
        
        if name not in self.components:
            raise QueryBuilderError(f"Unknown component: {name}")

        self.parts[name] = QueryPartBuilder(self)
        return self.parts[name]


class QueryPartBuilder:
    def __init__(self, builder):
        self.builder = builder
        self.parts = []

    def __call__(self, *parts):
        if len(parts) == 1 and parts[0] is None:
            self.parts = []
        else:
            self.parts.extend(parts)
        return self.builder
