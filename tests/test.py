import context  # noqa: F401

from agct.model import VariantId  # noqa: F401
from dataclasses import dataclass, field
import os


print(type(VariantId))


@dataclass
class TableDef:
    folder: str
    file_name: str
    pk_columns: list[str]
    non_pk_columns: list[str]
    columns: field(init=False) = None,
    full_file_name: field(init=False) = None

    def __post_init__(self):
        self.columns = self.pk_columns + self.non_pk_columns
        self.full_file_name = os.path.join(self.folder, self.file_name)


# TableDef("a", "b", ["a","b"], ["a","d"])      

class par:

    def __init__(self, prop):
        self._prop = prop

    @property
    def prop(self):
        return self._prop
    

class pars(par):

    def __init__(self, prop):
        super().__init__(prop)

inst = pars('a')
inst.prop