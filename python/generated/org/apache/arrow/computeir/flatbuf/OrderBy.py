# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Order by relation
class OrderBy(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsOrderBy(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = OrderBy()
        x.Init(buf, n + offset)
        return x

    # OrderBy
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # OrderBy
    def Base(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.RelBase import RelBase
            obj = RelBase()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Define sort order for rows of output.
    # Keys with higher precedence are ordered ahead of other keys.
    # OrderBy
    def Keys(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from org.apache.arrow.computeir.flatbuf.SortKey import SortKey
            obj = SortKey()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # OrderBy
    def KeysLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # OrderBy
    def KeysIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

def OrderByStart(builder): builder.StartObject(2)
def OrderByAddBase(builder, base): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(base), 0)
def OrderByAddKeys(builder, keys): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(keys), 0)
def OrderByStartKeysVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def OrderByEnd(builder): return builder.EndObject()
