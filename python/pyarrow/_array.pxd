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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

from cpython cimport PyObject

cdef extern from "Python.h":
    int PySlice_Check(object)


cdef class DataType:
    cdef:
        shared_ptr[CDataType] sp_type
        CDataType* type

    cdef void init(self, const shared_ptr[CDataType]& type)


cdef class DictionaryType(DataType):
    cdef:
        const CDictionaryType* dict_type


cdef class TimestampType(DataType):
    cdef:
        const CTimestampType* ts_type


cdef class FixedSizeBinaryType(DataType):
    cdef:
        const CFixedSizeBinaryType* fixed_size_binary_type


cdef class DecimalType(FixedSizeBinaryType):
    cdef:
        const CDecimalType* decimal_type


cdef class Field:
    cdef:
        shared_ptr[CField] sp_field
        CField* field

    cdef readonly:
        DataType type

    cdef init(self, const shared_ptr[CField]& field)


cdef class Schema:
    cdef:
        shared_ptr[CSchema] sp_schema
        CSchema* schema

    cdef init(self, const vector[shared_ptr[CField]]& fields)
    cdef init_schema(self, const shared_ptr[CSchema]& schema)


cdef class Scalar:
    cdef readonly:
        DataType type


cdef class NAType(Scalar):
    pass


cdef class ArrayValue(Scalar):
    cdef:
        shared_ptr[CArray] sp_array
        int64_t index

    cdef void init(self, DataType type,
                   const shared_ptr[CArray]& sp_array, int64_t index)

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array)


cdef class Int8Value(ArrayValue):
    pass


cdef class Int64Value(ArrayValue):
    pass


cdef class ListValue(ArrayValue):
    cdef readonly:
        DataType value_type

    cdef:
        CListArray* ap

    cdef getitem(self, int64_t i)


cdef class StringValue(ArrayValue):
    pass


cdef class FixedSizeBinaryValue(ArrayValue):
    pass


cdef class Array:
    cdef:
        shared_ptr[CArray] sp_array
        CArray* ap

    cdef readonly:
        DataType type

    cdef init(self, const shared_ptr[CArray]& sp_array)
    cdef getitem(self, int64_t i)


cdef class Tensor:
    cdef:
        shared_ptr[CTensor] sp_tensor
        CTensor* tp

    cdef readonly:
        DataType type

    cdef init(self, const shared_ptr[CTensor]& sp_tensor)


cdef class BooleanArray(Array):
    pass


cdef class NumericArray(Array):
    pass


cdef class IntegerArray(NumericArray):
    pass


cdef class FloatingPointArray(NumericArray):
    pass


cdef class Int8Array(IntegerArray):
    pass


cdef class UInt8Array(IntegerArray):
    pass


cdef class Int16Array(IntegerArray):
    pass


cdef class UInt16Array(IntegerArray):
    pass


cdef class Int32Array(IntegerArray):
    pass


cdef class UInt32Array(IntegerArray):
    pass


cdef class Int64Array(IntegerArray):
    pass


cdef class UInt64Array(IntegerArray):
    pass


cdef class FloatArray(FloatingPointArray):
    pass


cdef class DoubleArray(FloatingPointArray):
    pass


cdef class FixedSizeBinaryArray(Array):
    pass


cdef class DecimalArray(FixedSizeBinaryArray):
    pass


cdef class ListArray(Array):
    pass


cdef class StringArray(Array):
    pass


cdef class BinaryArray(Array):
    pass


cdef class DictionaryArray(Array):
    cdef:
        object _indices, _dictionary


cdef wrap_array_output(PyObject* output)
cdef DataType box_data_type(const shared_ptr[CDataType]& type)
cdef Field box_field(const shared_ptr[CField]& field)
cdef Schema box_schema(const shared_ptr[CSchema]& schema)
cdef object box_array(const shared_ptr[CArray]& sp_array)
cdef object box_tensor(const shared_ptr[CTensor]& sp_tensor)
cdef object box_scalar(DataType type,
                       const shared_ptr[CArray]& sp_array,
                       int64_t index)
