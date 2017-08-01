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

from libcpp cimport bool as c_bool, nullptr
from libcpp.vector cimport vector as c_vector
from cpython.ref cimport PyObject

from pyarrow.lib cimport Buffer, NativeFile, check_status

cdef extern from "arrow/python/python_to_arrow.h" nogil:

    cdef CStatus SerializeSequences(c_vector[PyObject*] sequences,
        int32_t recursion_depth, shared_ptr[CArray]* array_out,
        c_vector[PyObject*]& tensors_out)

    cdef shared_ptr[CRecordBatch] MakeBatch(shared_ptr[CArray] data)

cdef class PythonObject:

    cdef:
        shared_ptr[CRecordBatch] batch
        c_vector[shared_ptr[CTensor]] tensors

    def __cinit__(self):
        pass

# Main entry point for serialization
def serialize_sequence(object value):
    cdef int32_t recursion_depth = 0
    cdef PythonObject result = PythonObject()
    cdef c_vector[PyObject*] sequences
    cdef shared_ptr[CArray] array
    cdef c_vector[PyObject*] tensors
    cdef PyObject* tensor
    cdef shared_ptr[CTensor] out
    sequences.push_back(<PyObject*> value)
    check_status(SerializeSequences(sequences, recursion_depth, &array, tensors))
    result.batch = MakeBatch(array)
    for tensor in tensors:
        check_status(NdarrayToTensor(c_default_memory_pool(), <object> tensor, &out))
        result.tensors.push_back(out)
    return result
