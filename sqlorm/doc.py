import abc
from .model import Model, Column
from .types import JSON


class DocModel(Model, abc.ABC):
    fields = Column(JSON)

    def __getattr__(self, name):
        pass

    def __setattr__(self, name, value):
        pass