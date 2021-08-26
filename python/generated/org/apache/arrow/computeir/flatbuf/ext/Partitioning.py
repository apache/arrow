# automatically generated by the FlatBuffers compiler, do not modify

# namespace: ext

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Partitioning(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsPartitioning(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Partitioning()
        x.Init(buf, n + offset)
        return x

    # Partitioning
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Currently supported: "hive", "directory"
    # Partitioning
    def Flavor(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # Fields on which data is partitioned
    # Partitioning
    def Schema(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.flatbuf.Schema import Schema
            obj = Schema()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def PartitioningStart(builder): builder.StartObject(2)
def PartitioningAddFlavor(builder, flavor): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(flavor), 0)
def PartitioningAddSchema(builder, schema): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(schema), 0)
def PartitioningEnd(builder): return builder.EndObject()
