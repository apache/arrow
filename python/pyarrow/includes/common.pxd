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

# distutils: language = c++

from libc.stdint cimport *
from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set

from cpython cimport PyObject
cimport cpython

cdef extern from "arrow/python/platform.h":
    pass

cdef extern from "<Python.h>":
    void Py_XDECREF(PyObject* o)
    Py_ssize_t Py_REFCNT(PyObject* o)

cdef extern from "numpy/halffloat.h":
    ctypedef uint16_t npy_half

cdef extern from "arrow/api.h" namespace "arrow" nogil:
    # We can later add more of the common status factory methods as needed
    cdef CStatus CStatus_OK "arrow::Status::OK"()
    cdef CStatus CStatus_Invalid "arrow::Status::Invalid"()
    cdef CStatus CStatus_NotImplemented \
        "arrow::Status::NotImplemented"(const c_string& msg)
    cdef CStatus CStatus_UnknownError \
        "arrow::Status::UnknownError"(const c_string& msg)

    cdef cppclass CStatus "arrow::Status":
        CStatus()

        c_string ToString()
        c_string message()

        c_bool ok()
        c_bool IsIOError()
        c_bool IsOutOfMemory()
        c_bool IsInvalid()
        c_bool IsKeyError()
        c_bool IsNotImplemented()
        c_bool IsTypeError()
        c_bool IsCapacityError()
        c_bool IsIndexError()
        c_bool IsSerializationError()
        c_bool IsPythonError()
        c_bool IsPlasmaObjectExists()
        c_bool IsPlasmaObjectNonexistent()
        c_bool IsPlasmaStoreFull()


cdef inline object PyObject_to_object(PyObject* o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result
