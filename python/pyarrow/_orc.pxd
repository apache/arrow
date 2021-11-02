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
# cython: language_level = 3

from libc.string cimport const_char
from libcpp.vector cimport vector as std_vector
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport (CArray, CSchema, CStatus,
                                        CResult, CTable, CMemoryPool,
                                        CKeyValueMetadata,
                                        CRecordBatch,
                                        CTable,
                                        CRandomAccessFile, COutputStream,
                                        TimeUnit)


cdef extern from "arrow/adapters/orc/adapter.h" \
        namespace "arrow::adapters::orc" nogil:

    cdef cppclass ORCFileReader:
        @staticmethod
        CResult[unique_ptr[ORCFileReader]] Open(
            const shared_ptr[CRandomAccessFile]& file,
            CMemoryPool* pool)

        CResult[shared_ptr[const CKeyValueMetadata]] ReadMetadata()

        CResult[shared_ptr[CSchema]] ReadSchema()

        CResult[shared_ptr[CRecordBatch]] ReadStripe(int64_t stripe)
        CResult[shared_ptr[CRecordBatch]] ReadStripe(
            int64_t stripe, std_vector[c_string])

        CResult[shared_ptr[CTable]] Read()
        CResult[shared_ptr[CTable]] Read(std_vector[c_string])

        int64_t NumberOfStripes()

        int64_t NumberOfRows()

    cdef cppclass ORCFileWriter:
        @staticmethod
        CResult[unique_ptr[ORCFileWriter]] Open(COutputStream* output_stream)

        CStatus Write(const CTable& table)

        CStatus Close()
