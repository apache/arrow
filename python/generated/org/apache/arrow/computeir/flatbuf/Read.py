# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# A table read
class Read(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsRead(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Read()
        x.Init(buf, n + offset)
        return x

    # Read
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Read
    def Base(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.RelBase import RelBase
            obj = RelBase()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Read
    def Resource(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # Read
    def Schema(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.flatbuf.Schema import Schema
            obj = Schema()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def ReadStart(builder): builder.StartObject(3)
def ReadAddBase(builder, base): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(base), 0)
def ReadAddResource(builder, resource): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(resource), 0)
def ReadAddSchema(builder, schema): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(schema), 0)
def ReadEnd(builder): return builder.EndObject()
