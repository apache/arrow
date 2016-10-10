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
from pyarrow.includes.libarrow cimport (MemoryPool, CArray, CSchema,
                                        CRecordBatch)
from pyarrow.includes.libarrow_io cimport (OutputStream, ReadableFileInterface)

cdef extern from "arrow/ipc/file.h" namespace "arrow::ipc" nogil:

    cdef cppclass CFileWriter " arrow::ipc::FileWriter":
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CFileWriter]* out)

        CStatus WriteRecordBatch(const vector[shared_ptr[CArray]]& columns,
                                 int32_t num_rows)

        CStatus Close()

    cdef cppclass CFileReader " arrow::ipc::FileReader":

        @staticmethod
        CStatus Open(const shared_ptr[ReadableFileInterface]& file,
                     shared_ptr[CFileReader]* out)

        @staticmethod
        CStatus Open2" Open"(const shared_ptr[ReadableFileInterface]& file,
                     int64_t footer_offset, shared_ptr[CFileReader]* out)

        const shared_ptr[CSchema]& schema()

        int num_dictionaries()
        int num_record_batches()

        CStatus GetRecordBatch(int i, shared_ptr[CRecordBatch]* batch)
