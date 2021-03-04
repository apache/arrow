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
                                        CTable, CMemoryPool,
                                        CKeyValueMetadata,
                                        CRecordBatch,
                                        CTable,
                                        CRandomAccessFile, COutputStream,
                                        TimeUnit)


cdef extern from "arrow/adapters/orc/adapter.h" \
        namespace "arrow::adapters::orc" nogil:

    cdef cppclass ORCFileReader:
        @staticmethod
        CStatus Open(const shared_ptr[CRandomAccessFile]& file,
                     CMemoryPool* pool,
                     unique_ptr[ORCFileReader]* reader)

        CStatus ReadSchema(shared_ptr[CSchema]* out)

        CStatus ReadStripe(int64_t stripe, shared_ptr[CRecordBatch]* out)
        CStatus ReadStripe(int64_t stripe, std_vector[int],
                           shared_ptr[CRecordBatch]* out)

        CStatus Read(shared_ptr[CTable]* out)
        CStatus Read(std_vector[int], shared_ptr[CTable]* out)

        int64_t NumberOfStripes()

        int64_t NumberOfRows()
