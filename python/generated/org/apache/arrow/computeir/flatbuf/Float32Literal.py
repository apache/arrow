# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Float32Literal(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsFloat32Literal(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Float32Literal()
        x.Init(buf, n + offset)
        return x

    # Float32Literal
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Float32Literal
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Float32Flags, o + self._tab.Pos)
        return 0.0

def Float32LiteralStart(builder): builder.StartObject(1)
def Float32LiteralAddValue(builder, value): builder.PrependFloat32Slot(0, value, 0.0)
def Float32LiteralEnd(builder): return builder.EndObject()
