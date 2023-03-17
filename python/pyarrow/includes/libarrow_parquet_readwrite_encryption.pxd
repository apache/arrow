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

from pyarrow.includes.libarrow_dataset cimport *
from pyarrow._parquet cimport *
from pyarrow._parquet_encryption cimport *

cdef extern from "arrow/dataset/api.h" namespace "arrow::dataset" nogil:
    cdef cppclass CParquetFileWriteOptions \
            "arrow::dataset::ParquetFileWriteOptions"(CFileWriteOptions):
        shared_ptr[WriterProperties] writer_properties
        shared_ptr[ArrowWriterProperties] arrow_writer_properties
        shared_ptr[CParquetEncryptionConfig] parquet_encryption_config

    cdef cppclass CParquetFragmentScanOptions \
            "arrow::dataset::ParquetFragmentScanOptions"(CFragmentScanOptions):
        shared_ptr[CReaderProperties] reader_properties
        shared_ptr[ArrowReaderProperties] arrow_reader_properties
        shared_ptr[CParquetDecryptionConfig] parquet_decryption_config
