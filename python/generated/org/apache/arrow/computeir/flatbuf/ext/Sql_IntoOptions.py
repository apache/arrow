# automatically generated by the FlatBuffers compiler, do not modify

# namespace: ext

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

#//////////////////////////////////////////////////////////
# The contents of Relation.options will be Sql_IntoOptions
# if Relation.operation = NonCanonicalOperation{
#   .name_space = "sql",
#   .name = "into",
# }
class Sql_IntoOptions(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsSql_IntoOptions(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Sql_IntoOptions()
        x.Init(buf, n + offset)
        return x

    # Sql_IntoOptions
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # The name of a table into which rows will be inserted.
    # Sql_IntoOptions
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # Whether rows written into the table should be appended
    # to the table's current rows (INSERT INTO).
    # If false, the table will be overwritten (INTO).
    # Sql_IntoOptions
    def Append(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return bool(self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos))
        return True

def Sql_IntoOptionsStart(builder): builder.StartObject(2)
def Sql_IntoOptionsAddName(builder, name): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def Sql_IntoOptionsAddAppend(builder, append): builder.PrependBoolSlot(1, append, 1)
def Sql_IntoOptionsEnd(builder): return builder.EndObject()
