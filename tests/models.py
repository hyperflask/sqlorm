from sqlorm import Model, Column, PrimaryKey, Relationship, Integer


class Task(Model):
    table = "tasks"
    id: PrimaryKey[int]
    title: str
    done: bool = Column("completed")
    user_id: Integer = Column(references="users(id)")

    @classmethod
    def find_todos(cls):
        "{cls.select_from()} WHERE not {cls.done}"

    @staticmethod
    def find_todos_static():
        "SELECT * FROM tasks WHERE not completed"

    def toggle(self):
        "UPDATE tasks SET completed = not completed WHERE id = %(self.id)s RETURNING completed"


class User(Model):
    table = "users"
    id: PrimaryKey[int]
    email: str
    tasks = Relationship(Task, "user_id")
