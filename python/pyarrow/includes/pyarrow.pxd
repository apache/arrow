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
from pyarrow.includes.libarrow cimport (CArray, CBuffer, CColumn,
                                        CTable, CDataType, CStatus, Type,
                                        CMemoryPool, TimeUnit)

cimport pyarrow.includes.libarrow_io as arrow_io


cdef extern from "pyarrow/api.h" namespace "arrow::py" nogil:
    shared_ptr[CDataType] GetPrimitiveType(Type type)
    shared_ptr[CDataType] GetTimestampType(TimeUnit unit)
    CStatus ConvertPySequence(object obj, CMemoryPool* pool,
                              shared_ptr[CArray]* out)

    CStatus PandasDtypeToArrow(object dtype, shared_ptr[CDataType]* type)

    CStatus PandasToArrow(CMemoryPool* pool, object ao, object mo,
                          const shared_ptr[CDataType]& type,
                          shared_ptr[CArray]* out)

    CStatus PandasObjectsToArrow(CMemoryPool* pool, object ao, object mo,
                                 const shared_ptr[CDataType]& type,
                                 shared_ptr[CArray]* out)

    CStatus ConvertArrayToPandas(const shared_ptr[CArray]& arr,
                                 PyObject* py_ref, PyObject** out)

    CStatus ConvertColumnToPandas(const shared_ptr[CColumn]& arr,
                                  PyObject* py_ref, PyObject** out)

    CStatus ConvertTableToPandas(const shared_ptr[CTable]& table,
                                 int nthreads, PyObject** out)

    void set_default_memory_pool(CMemoryPool* pool)
    CMemoryPool* get_memory_pool()


cdef extern from "pyarrow/common.h" namespace "arrow::py" nogil:
    cdef cppclass PyBytesBuffer(CBuffer):
        PyBytesBuffer(object o)


cdef extern from "pyarrow/io.h" namespace "arrow::py" nogil:
    cdef cppclass PyReadableFile(arrow_io.RandomAccessFile):
        PyReadableFile(object fo)

    cdef cppclass PyOutputStream(arrow_io.OutputStream):
        PyOutputStream(object fo)

    cdef cppclass PyBytesReader(arrow_io.CBufferReader):
        PyBytesReader(object fo)
