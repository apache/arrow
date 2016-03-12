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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport CArray, CDataType, Type, MemoryPool

cdef extern from "pyarrow/api.h" namespace "pyarrow" nogil:
    # We can later add more of the common status factory methods as needed
    cdef Status Status_OK "Status::OK"()

    cdef cppclass Status:
        Status()

        c_string ToString()

        c_bool ok()
        c_bool IsOutOfMemory()
        c_bool IsKeyError()
        c_bool IsTypeError()
        c_bool IsIOError()
        c_bool IsValueError()
        c_bool IsNotImplemented()
        c_bool IsArrowError()

    shared_ptr[CDataType] GetPrimitiveType(Type type)
    Status ConvertPySequence(object obj, shared_ptr[CArray]* out)

    MemoryPool* GetMemoryPool()
