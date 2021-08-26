# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Expression types
#
# Expressions have a concrete `impl` value, which is a specific operation
# They also have a `type` field, which is the output type of the expression,
# regardless of operation type.
#
# The only exception so far is Cast, which has a type as input argument, which
# is equal to output type.
class Expression(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsExpression(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Expression()
        x.Init(buf, n + offset)
        return x

    # Expression
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Expression
    def ImplType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # Expression
    def Impl(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            from flatbuffers.table import Table
            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

    # The type of the expression.
    #
    # This is a field, because the Type union in Schema.fbs
    # isn't self-contained: Fields are necessary to describe complex types
    # and there's currently no reason to optimize the storage of this.
    # Expression
    def Type(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.flatbuf.Field import Field
            obj = Field()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def ExpressionStart(builder): builder.StartObject(3)
def ExpressionAddImplType(builder, implType): builder.PrependUint8Slot(0, implType, 0)
def ExpressionAddImpl(builder, impl): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(impl), 0)
def ExpressionAddType(builder, type): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(type), 0)
def ExpressionEnd(builder): return builder.EndObject()
