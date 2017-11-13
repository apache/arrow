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
from pyarrow.includes.libarrow cimport CStatus
from cpython cimport PyObject
from libcpp cimport nullptr

cdef extern from "Python.h":
    int PySlice_Check(object)


cdef int check_status(const CStatus& status) nogil except -1


cdef class MemoryPool:
    cdef:
        CMemoryPool* pool

    cdef void init(self, CMemoryPool* pool)


cdef CMemoryPool* maybe_unbox_memory_pool(MemoryPool memory_pool)


cdef class DataType:
    cdef:
        shared_ptr[CDataType] sp_type
        CDataType* type

    cdef void init(self, const shared_ptr[CDataType]& type)


cdef class ListType(DataType):
    cdef:
        const CListType* list_type


cdef class DictionaryType(DataType):
    cdef:
        const CDictionaryType* dict_type


cdef class UnionType(DataType):
    cdef:
        list child_types


cdef class TimestampType(DataType):
    cdef:
        const CTimestampType* ts_type


cdef class Time32Type(DataType):
    cdef:
        const CTime32Type* time_type


cdef class Time64Type(DataType):
    cdef:
        const CTime64Type* time_type


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

    cdef void init(self, const shared_ptr[CField]& field)


cdef class Schema:
    cdef:
        shared_ptr[CSchema] sp_schema
        CSchema* schema

    cdef void init(self, const vector[shared_ptr[CField]]& fields)
    cdef void init_schema(self, const shared_ptr[CSchema]& schema)


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


cdef class UnionValue(ArrayValue):
    cdef:
        CUnionArray* ap
        list value_types

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

    cdef void init(self, const shared_ptr[CArray]& sp_array)
    cdef getitem(self, int64_t i)


cdef class Tensor:
    cdef:
        shared_ptr[CTensor] sp_tensor
        CTensor* tp

    cdef readonly:
        DataType type

    cdef void init(self, const shared_ptr[CTensor]& sp_tensor)


cdef class NullArray(Array):
    pass


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


cdef class Decimal128Array(FixedSizeBinaryArray):
    pass


cdef class ListArray(Array):
    pass


cdef class UnionArray(Array):
    pass


cdef class StringArray(Array):
    pass


cdef class BinaryArray(Array):
    pass


cdef class DictionaryArray(Array):
    cdef:
        object _indices, _dictionary


cdef wrap_array_output(PyObject* output)
cdef object box_scalar(DataType type,
                       const shared_ptr[CArray]& sp_array,
                       int64_t index)


cdef class ChunkedArray:
    cdef:
        shared_ptr[CChunkedArray] sp_chunked_array
        CChunkedArray* chunked_array

    cdef void init(self, const shared_ptr[CChunkedArray]& chunked_array)
    cdef int _check_nullptr(self) except -1


cdef class Column:
    cdef:
        shared_ptr[CColumn] sp_column
        CColumn* column

    cdef void init(self, const shared_ptr[CColumn]& column)
    cdef int _check_nullptr(self) except -1


cdef class Table:
    cdef:
        shared_ptr[CTable] sp_table
        CTable* table

    cdef void init(self, const shared_ptr[CTable]& table)
    cdef int _check_nullptr(self) except -1


cdef class RecordBatch:
    cdef:
        shared_ptr[CRecordBatch] sp_batch
        CRecordBatch* batch
        Schema _schema

    cdef void init(self, const shared_ptr[CRecordBatch]& table)
    cdef int _check_nullptr(self) except -1


cdef class Buffer:
    cdef:
        shared_ptr[CBuffer] buffer
        Py_ssize_t shape[1]
        Py_ssize_t strides[1]

    cdef void init(self, const shared_ptr[CBuffer]& buffer)


cdef class NativeFile:
    cdef:
        shared_ptr[RandomAccessFile] rd_file
        shared_ptr[OutputStream] wr_file
        bint is_readable
        bint is_writeable
        bint is_open
        bint own_file

    # By implementing these "virtual" functions (all functions in Cython
    # extension classes are technically virtual in the C++ sense) we can expose
    # the arrow::io abstract file interfaces to other components throughout the
    # suite of Arrow C++ libraries
    cdef read_handle(self, shared_ptr[RandomAccessFile]* file)
    cdef write_handle(self, shared_ptr[OutputStream]* file)

cdef get_reader(object source, shared_ptr[RandomAccessFile]* reader)
cdef get_writer(object source, shared_ptr[OutputStream]* writer)

cdef public object pyarrow_wrap_buffer(const shared_ptr[CBuffer]& buf)
cdef public object pyarrow_wrap_data_type(const shared_ptr[CDataType]& type)
cdef public object pyarrow_wrap_field(const shared_ptr[CField]& field)
cdef public object pyarrow_wrap_schema(const shared_ptr[CSchema]& type)
cdef public object pyarrow_wrap_array(const shared_ptr[CArray]& sp_array)
cdef public object pyarrow_wrap_tensor(const shared_ptr[CTensor]& sp_tensor)
cdef public object pyarrow_wrap_column(const shared_ptr[CColumn]& ccolumn)
cdef public object pyarrow_wrap_table(const shared_ptr[CTable]& ctable)
cdef public object pyarrow_wrap_batch(const shared_ptr[CRecordBatch]& cbatch)

cdef dict box_metadata(const CKeyValueMetadata* sp_metadata)
