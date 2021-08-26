# automatically generated by the FlatBuffers compiler, do not modify

# namespace: ext

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# The contents of Relation.options will be Sql_FromOptions
# if Relation.operation = NonCanonicalOperation{
#   .name_space = "sql",
#   .name = "from",
# }
class Sql_FromOptions(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsSql_FromOptions(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Sql_FromOptions()
        x.Init(buf, n + offset)
        return x

    # Sql_FromOptions
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # The name of a table referenced as a source relation.
    # Sql_FromOptions
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

def Sql_FromOptionsStart(builder): builder.StartObject(1)
def Sql_FromOptionsAddName(builder, name): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def Sql_FromOptionsEnd(builder): return builder.EndObject()
