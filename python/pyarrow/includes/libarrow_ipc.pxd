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
from pyarrow.includes.libarrow cimport (CArray, CColumn, CSchema, CRecordBatch)
from pyarrow.includes.libarrow_io cimport (InputStream, OutputStream,
                                           RandomAccessFile)


cdef extern from "arrow/ipc/api.h" namespace "arrow::ipc" nogil:

    cdef cppclass CStreamWriter " arrow::ipc::StreamWriter":
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CStreamWriter]* out)

        CStatus Close()
        CStatus WriteRecordBatch(const CRecordBatch& batch)

    cdef cppclass CStreamReader " arrow::ipc::StreamReader":

        @staticmethod
        CStatus Open(const shared_ptr[InputStream]& stream,
                     shared_ptr[CStreamReader]* out)

        shared_ptr[CSchema] schema()

        CStatus GetNextRecordBatch(shared_ptr[CRecordBatch]* batch)

    cdef cppclass CFileWriter " arrow::ipc::FileWriter"(CStreamWriter):
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CFileWriter]* out)

    cdef cppclass CFileReader " arrow::ipc::FileReader":

        @staticmethod
        CStatus Open(const shared_ptr[RandomAccessFile]& file,
                     shared_ptr[CFileReader]* out)

        @staticmethod
        CStatus Open2" Open"(const shared_ptr[RandomAccessFile]& file,
                     int64_t footer_offset, shared_ptr[CFileReader]* out)

        shared_ptr[CSchema] schema()

        int num_record_batches()

        CStatus GetRecordBatch(int i, shared_ptr[CRecordBatch]* batch)

cdef extern from "arrow/ipc/feather.h" namespace "arrow::ipc::feather" nogil:

    cdef cppclass CFeatherWriter" arrow::ipc::feather::TableWriter":
        @staticmethod
        CStatus Open(const shared_ptr[OutputStream]& stream,
                     unique_ptr[CFeatherWriter]* out)

        void SetDescription(const c_string& desc)
        void SetNumRows(int64_t num_rows)

        CStatus Append(const c_string& name, const CArray& values)
        CStatus Finalize()

    cdef cppclass CFeatherReader" arrow::ipc::feather::TableReader":
        @staticmethod
        CStatus Open(const shared_ptr[RandomAccessFile]& file,
                     unique_ptr[CFeatherReader]* out)

        c_string GetDescription()
        c_bool HasDescription()

        int64_t num_rows()
        int64_t num_columns()

        shared_ptr[CSchema] schema()

        CStatus GetColumn(int i, shared_ptr[CColumn]* out)
        c_string GetColumnName(int i)
