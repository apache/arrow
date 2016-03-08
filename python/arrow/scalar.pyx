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

from arrow.schema cimport DataType, box_data_type

from arrow.compat import frombytes
import arrow.schema as schema

cdef class NAType(Scalar):

    def __cinit__(self):
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


cdef class Int64Value(ArrayValue):

    def as_py(self):
        cdef CInt64Array* ap = <CInt64Array*> self.sp_array.get()
        return ap.Value(self.index)


cdef class StringValue(ArrayValue):

    def as_py(self):
        cdef CStringArray* ap = <CStringArray*> self.sp_array.get()
        return frombytes(ap.GetString(self.index))


cdef class ListValue(ArrayValue):

    def __len__(self):
        return self.ap.value_length(self.index)

    def __getitem__(self, i):
        return self._getitem(i)

    cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = <CListArray*> sp_array.get()
        self.value_type = box_data_type(self.ap.value_type())

    cdef _getitem(self, int i):
        cdef int j = self.ap.offset(self.index) + i
        return box_arrow_scalar(self.value_type, self.ap.values(), j)

    def as_py(self):
        cdef:
            int j
            list result = []

        for j in range(len(self)):
            result.append(self._getitem(j).as_py())

        return result


cdef dict _scalar_classes = {
    LogicalType_INT64: Int64Value,
    LogicalType_LIST: ListValue,
    LogicalType_STRING: StringValue
}

cdef object box_arrow_scalar(DataType type,
                             const shared_ptr[CArray]& sp_array,
                             int index):
    cdef ArrayValue val = _scalar_classes[type.type.type]()
    val.init(type, sp_array, index)
    return val
