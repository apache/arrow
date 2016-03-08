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

from arrow.includes.common cimport *
from arrow.includes.arrow cimport *

from arrow.schema cimport DataType

cdef class Scalar:
    cdef readonly:
        DataType type


cdef class NAType(Scalar):
    pass


cdef class ArrayValue(Scalar):
    cdef:
        shared_ptr[CArray] sp_array
        int index

    cdef void init(self, DataType type,
                   const shared_ptr[CArray]& sp_array, int index)

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

    cdef _getitem(self, int i)


cdef class StringValue(ArrayValue):
    pass

cdef object box_arrow_scalar(DataType type,
                             const shared_ptr[CArray]& sp_array,
                             int index)
