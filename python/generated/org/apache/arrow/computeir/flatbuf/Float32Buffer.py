# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Float32Buffer(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsFloat32Buffer(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Float32Buffer()
        x.Init(buf, n + offset)
        return x

    # Float32Buffer
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Float32Buffer
    def Items(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Float32Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return 0

    # Float32Buffer
    def ItemsAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Float32Flags, o)
        return 0

    # Float32Buffer
    def ItemsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Float32Buffer
    def ItemsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

def Float32BufferStart(builder): builder.StartObject(1)
def Float32BufferAddItems(builder, items): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(items), 0)
def Float32BufferStartItemsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def Float32BufferEnd(builder): return builder.EndObject()
