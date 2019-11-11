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

from libcpp.functional cimport function

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *


cdef extern from "arrow/dataset/api.h" namespace "arrow::dataset" nogil:

    # cdef enum CFilter" arrow::dataset::Filter::type":
    #     CFilter_EXPRESSION" arrow::dataset::Filter::type::EXPRESSION"
    #     CFilter_GENERIC" arrow::dataset::Filter::GENERIC"

    cdef cppclass CDataFragmentIterator "arrow::dataset::DataFragmentIterator"(
            CIterator[shared_ptr[CDataFragment]]):
        pass

    cdef cppclass CDataFragmentVector "arrow::dataset::DataFragmentVector":
        pass

    cdef cppclass CDataSourceVector "arrow::dataset::DataSourceVector":
        pass

    cdef cppclass CWriteOptions "arrow::dataset::WriteOptions":
        pass

    cdef cppclass CFilter "arrow::dataset::Filter":
        pass

    cdef cppclass CScanOptions" arrow::dataset::ScanOptions":
        # const shared_ptr[DataSelector]& selector()
        pass

    cdef cppclass CScanContext" arrow::dataset::ScanContext":
        CMemoryPool* pool

    cdef cppclass CScanTask" arrow::dataset::ScanTask":
        pass

    cdef cppclass CSimpleScanTask" arrow::dataset::SimpleScanTask":
        pass

    cdef cppclass CScanTaskIterator "arrow::dataset::ScanTaskIterator"(
            CIterator[unique_ptr[CScanTask]]):
        pass

    cdef cppclass CScannerBuilder" arrow::dataset::ScannerBuilder":
        CScannerBuilder(shared_ptr[CDataset] dataset,
                        shared_ptr[CScanContext] scan_context)
        CScannerBuilder* Project(const vector[c_string]& columns)
        CScannerBuilder* AddFilter(const shared_ptr[CFilter]& filter)
        CScannerBuilder* SetGlobalFileOptions(
            shared_ptr[CFileScanOptions] options)
        CScannerBuilder* IncludePartitionKeys(c_bool)
        unique_ptr[CScanner] Finish()

    cdef cppclass CScanTaskIterator" arrow::dataset::ScanTaskIterator":
        pass

    cdef cppclass CScanner" arrow::dataset::Scanner":
        unique_ptr[CScanTaskIterator] Scan()

    cdef cppclass CDataFragment" arrow::dataset::DataFragment":
        CStatus Scan(shared_ptr[CScanContext] scan_context,
                     CScanTaskIterator* out)
        c_bool splittable()
        shared_ptr[CScanOptions] scan_options()

    cdef cppclass CSimpleDataFragment "arrow::dataset::SimpleDataFragment"(
            CDataFragment):
        CSimpleDataFragment(vector[shared_ptr[CRecordBatch]] record_batches,
                            shared_ptr[CScanOptions] scan_options)

    cdef cppclass CDataSource" arrow::dataset::DataSource":
        CDataFragmentIterator GetFragments(shared_ptr[CScanOptions] options)
        # const shared_ptr[CExpression]& partition_expression()
        c_string type()

    cdef cppclass CSimpleDataSource "arrow::dataset::SimpleDataSource"(
            CDataSource):
        pass

    cdef cppclass CTreeDataSource "arrow::dataset::TreeDataSource"(
            CDataSource):
        pass

    cdef cppclass CDataset" arrow::dataset::Dataset":
        CStatus Make(vector[shared_ptr[CDataSource]] sources,
                     shared_ptr[CSchema] schema, shared_ptr[CDataset] *out)
        CStatus NewScan(unique_ptr[CScannerBuilder]* out)
        const vector[shared_ptr[CDataSource]]& sources()
        shared_ptr[CSchema] schema()

    ############################### File ######################################

    cdef cppclass CFileScanOptions "arrow::dataset::FileScanOptions"(
            CScanOptions):
        c_string file_type()

    cdef cppclass CFileSource" arrow::dataset::FileSource":
        # enum SourceType { PATH, BUFFER };
        CFileSource(c_string path, CFileSystem filesystem,
                    CompressionType compression)
        CFileSource(shared_ptr[CBuffer] buffer, CompressionType compression)
        c_bool operator==(const CFileSource other)
        # CSourceType type()
        CompressionType compression()
        c_string path()
        CFileSystem* filesystem()
        shared_ptr[CBuffer] buffer()
        CStatus Open(shared_ptr[CRandomAccessFile]* out)

    cdef cppclass CFileWriteOptions "arrow::dataset::WriteOptions"(
            CWriteOptions):
        c_string file_type()

    cdef cppclass CFileFormat" arrow::dataset::FileFormat":
        c_string name()
        c_bool IsKnownExtension(const c_string& ext)
        CStatus ScanFile(const CFileSource& source,
                         shared_ptr[CScanOptions] scan_options,
                         shared_ptr[CScanContext] scan_context,
                         unique_ptr[CScanTaskIterator]* out)
        CStatus MakeFragment(const CFileSource& location,
                             shared_ptr[CScanOptions] opts,
                             unique_ptr[CDataFragment]* out)

    cdef cppclass CFileBasedDataFragment \
            "arrow::dataset::FileBasedDataFragment"(CDataFragment):
        CFileBasedDataFragment(const CFileSource& source,
                               shared_ptr[CFileFormat] format,
                               shared_ptr[CScanOptions] scan_options)
        CStatus Scan(shared_ptr[CScanContext] scan_context,
                     unique_ptr[CScanTaskIterator]* out)
        const CFileSource& source()
        shared_ptr[CFileFormat] format()
        shared_ptr[CScanOptions] scan_options()

    cdef cppclass CFileSystemBasedDataSource \
            "arrow::dataset::FileSystemBasedDataSource"(CDataSource):
        @staticmethod
        CStatus Make(CFileSystem* filesystem,
                     const CSelector& selector,
                     shared_ptr[CFileFormat] format,
                     shared_ptr[CScanOptions] scan_options,
                     unique_ptr[CFileSystemBasedDataSource]* out)
        c_string type()
        unique_ptr[CDataFragmentIterator] GetFragments(
            shared_ptr[CScanOptions] options)

    ############################### File CSV ##################################

    cdef cppclass CCsvScanOptions "arrow::dataset::CsvScanOptions"(
            CFileScanOptions):
        c_string file_type()

    cdef cppclass CCsvWriteOptions "arrow::dataset::CsvWriteOptions"(
            CFileWriteOptions):
        c_string file_type()

    cdef cppclass CCsvFileFormat" arrow::dataset::CsvFileFormat"(CFileFormat):
        c_string name()
        c_bool IsKnownExtension(const c_string& ext)
        CStatus ScanFile(const CFileSource& source,
                         shared_ptr[CScanOptions] scan_options,
                         shared_ptr[CScanContext] scan_context,
                         unique_ptr[CScanTaskIterator]* out)

     ############################### File JSON ################################

    cdef cppclass CJsonScanOptions "arrow::dataset::JsonScanOptions"(
            CFileScanOptions):
        c_string file_type()

    cdef cppclass CJsonWriteOptions "arrow::dataset::JsonWriteOptions"(
            CFileWriteOptions):
        c_string file_type()

    cdef cppclass CJsonFileFormat "arrow::dataset::JsonFileFormat"(
            CFileFormat):
        c_string name()
        c_bool IsKnownExtension(const c_string& ext)
        CStatus ScanFile(const CFileSource& source,
                         shared_ptr[CScanOptions] scan_options,
                         shared_ptr[CScanContext] scan_context,
                         unique_ptr[CScanTaskIterator]* out)

    ############################### File Feather #############################

    cdef cppclass CFeatherScanOptions "arrow::dataset::FeatherScanOptions"(
            CFileScanOptions):
        c_string file_type()

    cdef cppclass CFeatherWriterOptions "arrow::dataset::FeatherWriterOptions"(
            CFileWriteOptions):
        c_string file_type()

    cdef cppclass CFeatherFileFormat "arrow::dataset::FeatherFileFormat"(
            CFileFormat):
        c_string name()
        c_bool IsKnownExtension(const c_string& ext)
        CStatus ScanFile(const CFileSource& source,
                         shared_ptr[CScanOptions] scan_options,
                         shared_ptr[CScanContext] scan_context,
                         unique_ptr[CScanTaskIterator]* out)

    ############################### File Parquet #############################

    cdef cppclass CParquetScanOptions "arrow::dataset::ParquetScanOptions"(
            CFileScanOptions):
        c_string file_type()

    cdef cppclass CParquetWriterOptions "arrow::dataset::ParquetWriterOptions"(
            CFileWriteOptions):
        c_string file_type()

    cdef cppclass CParquetFileFormat "arrow::dataset::ParquetFileFormat"(
            CFileFormat):
        c_string name()
        c_bool IsKnownExtension(const c_string& ext)
        CStatus ScanFile(const CFileSource& source,
                         shared_ptr[CScanOptions] scan_options,
                         shared_ptr[CScanContext] scan_context,
                         unique_ptr[CScanTaskIterator]* out)
        CStatus MakeFragment(const CFileSource& source,
                             shared_ptr[CScanOptions] opts,
                             unique_ptr[CDataFragment]* out)

    cdef cppclass CParquetFragment "arrow::dataset::ParquetFragment"(
            CFileBasedDataFragment):
        CParquetFragment(const CFileSource& source,
                         shared_ptr[CScanOptions] options)
        c_bool splittable()
