# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyarrow.schema cimport DataType, box_data_type

from pyarrow.compat import frombytes
import pyarrow.schema as schema

NA = None

cdef class NAType(Scalar):

    def __cinit__(self):
        global NA
        if NA is not None:
            raise Exception('Cannot create multiple NAType instances')

        self.type = schema.null()

    def __repr__(self):
        return 'NA'

    def as_py(self):
        return None

NA = NAType()

cdef class ArrayValue(Scalar):

    cdef void init(self, DataType type, const shared_ptr[CArray]& sp_array,
                   int index):
        self.type = type
        self.index = index
        self._set_array(sp_array)

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array

    def __repr__(self):
        if hasattr(self, 'as_py'):
            return repr(self.as_py())
        else:
            return Scalar.__repr__(self)


cdef class BooleanValue(ArrayValue):
    pass


cdef class Int8Value(ArrayValue):

    def as_py(self):
        cdef CInt8Array* ap = <CInt8Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt8Value(ArrayValue):

    def as_py(self):
        cdef CUInt8Array* ap = <CUInt8Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int16Value(ArrayValue):

    def as_py(self):
        cdef CInt16Array* ap = <CInt16Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt16Value(ArrayValue):

    def as_py(self):
        cdef CUInt16Array* ap = <CUInt16Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int32Value(ArrayValue):

    def as_py(self):
        cdef CInt32Array* ap = <CInt32Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt32Value(ArrayValue):

    def as_py(self):
        cdef CUInt32Array* ap = <CUInt32Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class Int64Value(ArrayValue):

    def as_py(self):
        cdef CInt64Array* ap = <CInt64Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class UInt64Value(ArrayValue):

    def as_py(self):
        cdef CUInt64Array* ap = <CUInt64Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class FloatValue(ArrayValue):

    def as_py(self):
        cdef CFloatArray* ap = <CFloatArray*> self.sp_array.get()
        return ap.Value(self.index)


cdef class DoubleValue(ArrayValue):

    def as_py(self):
        cdef CDoubleArray* ap = <CDoubleArray*> self.sp_array.get()
        return ap.Value(self.index)


cdef class StringValue(ArrayValue):

    def as_py(self):
        cdef CStringArray* ap = <CStringArray*> self.sp_array.get()
        return frombytes(ap.GetString(self.index))


cdef class ListValue(ArrayValue):

    def __len__(self):
        return self.ap.value_length(self.index)

    def __getitem__(self, i):
        return self.getitem(i)

    def __iter__(self):
        for i in range(len(self)):
            yield self.getitem(i)
        raise StopIteration

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = <CListArray*> sp_array.get()
        self.value_type = box_data_type(self.ap.value_type())

    cdef getitem(self, int i):
        cdef int j = self.ap.offset(self.index) + i
        return box_arrow_scalar(self.value_type, self.ap.values(), j)

    def as_py(self):
        cdef:
            int j
            list result = []

        for j in range(len(self)):
            result.append(self.getitem(j).as_py())

        return result


cdef dict _scalar_classes = {
    Type_UINT8: Int8Value,
    Type_UINT16: Int16Value,
    Type_UINT32: Int32Value,
    Type_UINT64: Int64Value,
    Type_INT8: Int8Value,
    Type_INT16: Int16Value,
    Type_INT32: Int32Value,
    Type_INT64: Int64Value,
    Type_FLOAT: FloatValue,
    Type_DOUBLE: DoubleValue,
    Type_LIST: ListValue,
    Type_STRING: StringValue
}

cdef object box_arrow_scalar(DataType type,
                             const shared_ptr[CArray]& sp_array,
                             int index):
    cdef ArrayValue val
    if sp_array.get().IsNull(index):
        return NA
    else:
        val = _scalar_classes[type.type.type]()
        val.init(type, sp_array, index)
        return val
