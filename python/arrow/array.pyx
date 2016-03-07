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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from arrow.includes.arrow cimport *
cimport arrow.includes.pyarrow as pyarrow

from arrow.compat import frombytes, tobytes
from arrow.error cimport check_status


def total_allocated_bytes():
    cdef MemoryPool* pool = pyarrow.GetMemoryPool()
    return pool.bytes_allocated()


cdef class Array:

    cdef init(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.type = DataType()
        self.type.init(self.sp_array.get().type())

    property null_count:

        def __get__(self):
            return self.sp_array.get().null_count()

    def __len__(self):
        return self.sp_array.get().length()


cdef class NullArray(Array):
    pass


cdef class BooleanArray(Array):
    pass


cdef class NumericArray(Array):
    pass


cdef class Int8Array(NumericArray):
    pass


cdef class UInt8Array(NumericArray):
    pass


cdef class Int16Array(NumericArray):
    pass


cdef class UInt16Array(NumericArray):
    pass


cdef class Int32Array(NumericArray):
    pass


cdef class UInt32Array(NumericArray):
    pass


cdef class Int64Array(NumericArray):
    pass


cdef class UInt64Array(NumericArray):
    pass


cdef class ListArray(Array):
    pass


cdef class StringArray(Array):
    pass


cdef dict _array_classes = {
    LogicalType_NA: NullArray,
    LogicalType_BOOL: BooleanArray,
    LogicalType_INT64: Int64Array,
    LogicalType_LIST: ListArray,
    LogicalType_STRING: StringArray,
}

cdef object box_arrow_array(const shared_ptr[CArray]& sp_array):
    if sp_array.get() == NULL:
        raise ValueError('Array was NULL')

    cdef CDataType* data_type = sp_array.get().type().get()

    if data_type == NULL:
        raise ValueError('Array data type was NULL')

    cdef Array arr = _array_classes[data_type.type]()
    arr.init(sp_array)
    return arr


def from_list(object list_obj, type=None):
    """
    Convert Python list to Arrow array
    """
    cdef:
        shared_ptr[CArray] sp_array

    check_status(pyarrow.ConvertPySequence(list_obj, &sp_array))
    return box_arrow_array(sp_array)
