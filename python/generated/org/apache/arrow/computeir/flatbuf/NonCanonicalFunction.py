# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class NonCanonicalFunction(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsNonCanonicalFunction(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = NonCanonicalFunction()
        x.Init(buf, n + offset)
        return x

    # NonCanonicalFunction
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # NonCanonicalFunction
    def NameSpace(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # NonCanonicalFunction
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

def NonCanonicalFunctionStart(builder): builder.StartObject(2)
def NonCanonicalFunctionAddNameSpace(builder, nameSpace): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(nameSpace), 0)
def NonCanonicalFunctionAddName(builder, name): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def NonCanonicalFunctionEnd(builder): return builder.EndObject()
