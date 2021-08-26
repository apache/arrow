# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# A function call expression
class Call(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsCall(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Call()
        x.Init(buf, n + offset)
        return x

    # Call
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Call
    def KindType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # The kind of function call this is.
    # Call
    def Kind(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            from flatbuffers.table import Table
            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

    # The arguments passed to `function_name`.
    # Call
    def Arguments(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from org.apache.arrow.computeir.flatbuf.Expression import Expression
            obj = Expression()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Call
    def ArgumentsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Call
    def ArgumentsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # Parameters for `function_name`; content/format may be unique to each
    # value of `function_name`.
    # Call
    def Metadata(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.InlineBuffer import InlineBuffer
            obj = InlineBuffer()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def CallStart(builder): builder.StartObject(4)
def CallAddKindType(builder, kindType): builder.PrependUint8Slot(0, kindType, 0)
def CallAddKind(builder, kind): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(kind), 0)
def CallAddArguments(builder, arguments): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(arguments), 0)
def CallStartArgumentsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def CallAddMetadata(builder, metadata): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(metadata), 0)
def CallEnd(builder): return builder.EndObject()
