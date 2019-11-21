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


cdef extern from "arrow/api.h" namespace "arrow" nogil:

    cdef cppclass CRecordBatchIterator "arrow::RecordBatchIterator"(
            CIterator[shared_ptr[CRecordBatch]]):
        pass


cdef extern from "arrow/dataset/api.h" namespace "arrow::dataset" nogil:

    # cdef enum CFilter" arrow::dataset::Filter::type":
    #     CFilter_EXPRESSION" arrow::dataset::Filter::type::EXPRESSION"
    #     CFilter_GENERIC" arrow::dataset::Filter::GENERIC"


    cdef cppclass CDataSourceVector "arrow::dataset::DataSourceVector"(
            vector[shared_ptr[CDataSource]]):
        pass

    cdef cppclass CWriteOptions "arrow::dataset::WriteOptions":
        pass

    cdef cppclass CExpression "arrow::dataset::Expression":
        # CExpression(ExpressionType::type type) : type_(type) {}
        c_bool Equals(const CExpression& other) const
        c_bool Equals(const shared_ptr[CExpression]& other) const
        c_bool IsNull() const
        CStatus Validate(const CSchema& schema, shared_ptr[CDataType]* out) const

    cdef cppclass CFilter "arrow::dataset::Filter":
        pass

    cdef cppclass CScanOptions "arrow::dataset::ScanOptions":
        # const shared_ptr[DataSelector]& selector()
        pass

    cdef cppclass CScanOptionsPtr "arrow::dataset::ScanOptionsPtr"(
            shared_ptr[CScanOptions]):
        pass

    cdef cppclass CScanContext "arrow::dataset::ScanContext":
        CMemoryPool* pool

    cdef cppclass CScanContextPtr "arrow::dataset::ScanContextPtr"(
            shared_ptr[CScanContext]):
        pass

    cdef cppclass CScanTask" arrow::dataset::ScanTask":
        CResult[CRecordBatchIterator] Scan()

    cdef cppclass CScanTaskPtr "arrow::dataset::ScanTaskPtr"(
            shared_ptr[CScanTask]):
        pass

    cdef cppclass CScanTaskIterator "arrow::dataset::ScanTaskIterator"(
            CIterator[CScanTaskPtr]):
        pass

    cdef cppclass CSimpleScanTask "arrow::dataset::SimpleScanTask"(CScanTask):
        pass

    cdef cppclass CScannerBuilder "arrow::dataset::ScannerBuilder":
        CScannerBuilder(shared_ptr[CDataset] dataset,
                        shared_ptr[CScanContext] scan_context)
        # CStatus Project(const vector[c_string]& columns)
        # CStatus Filter(const CExpression& filter)
        # CStatus Filter(shared_ptr[CExpression] filter)
        # CStatus UseThreads(c_bool use_threads)
        CResult[CScannerPtr] Finish()
        shared_ptr[CSchema] schema() const

    cdef cppclass CScannerBuilderPtr "arrow::dataset::ScannerBuilderPtr"(
            shared_ptr[CScannerBuilder]):
        pass

    cdef cppclass CScanner "arrow::dataset::Scanner":
        CResult[CScanTaskIterator] Scan()
        CResult[shared_ptr[CTable]] ToTable()

    cdef cppclass CScannerPtr "arrow::dataset::ScannerPtr"(
            shared_ptr[CScanner]):
        pass

    cdef cppclass CDataFragment "arrow::dataset::DataFragment":
        CResult[CScanTaskIterator] Scan(CScanContextPtr context)
        c_bool splittable()
        CScanOptionsPtr scan_options()

    cdef cppclass CDataFragmentPtr "arrow::dataset::DataFragmentPtr"(
            shared_ptr[CDataFragment]):
        pass

    cdef cppclass CDataFragmentIterator "arrow::dataset::DataFragmentIterator"(
            CIterator[CDataFragmentPtr]):
        pass

    cdef cppclass CDataFragmentVector "arrow::dataset::DataFragmentVector"(
            vector[CDataFragmentPtr]):
        pass

    cdef cppclass CSimpleDataFragment "arrow::dataset::SimpleDataFragment"(
            CDataFragment):
        CSimpleDataFragment(vector[shared_ptr[CRecordBatch]] record_batches,
                            shared_ptr[CScanOptions] scan_options)

    cdef cppclass CDataSource "arrow::dataset::DataSource":
        CDataFragmentIterator GetFragments(shared_ptr[CScanOptions] options)
        # const shared_ptr[CExpression]& partition_expression()
        c_string type()

    cdef cppclass CDataSourcePtr "arrow::dataset::DataSourcePtr"(
            shared_ptr[CDataSource]):
        pass

    cdef cppclass CDataSourceVector "arrow::dataset::DataSourceVector"(
            vector[shared_ptr[CDataSource]]):
        pass

    cdef cppclass CSimpleDataSource "arrow::dataset::SimpleDataSource"(
            CDataSource):
        pass

    cdef cppclass CTreeDataSource "arrow::dataset::TreeDataSource"(
            CDataSource):
        pass

    cdef cppclass CDataset "arrow::dataset::Dataset":
        @staticmethod
        CResult[CDatasetPtr] Make(CDataSourceVector sources,
                                  shared_ptr[CSchema] schema)
        CResult[CScannerBuilderPtr] NewScan()
        const CDataSourceVector& sources()
        shared_ptr[CSchema] schema()

    cdef cppclass CDatasetPtr "arrow::dataset::DatasetPtr"(
            shared_ptr[CDataset]):
        pass

    ############################### File ######################################

    cdef cppclass CFileScanOptions "arrow::dataset::FileScanOptions"(
            CScanOptions):
        c_string file_type()

    cdef cppclass CFileSource "arrow::dataset::FileSource":
        CFileSource(c_string path, CFileSystem filesystem,
                    CompressionType compression)
        c_bool operator==(const CFileSource other)
        CompressionType compression()
        c_string path()
        CFileSystem* filesystem()
        shared_ptr[CBuffer] buffer()
        CStatus Open(shared_ptr[CRandomAccessFile]* out)

    cdef cppclass CFileWriteOptions "arrow::dataset::WriteOptions"(
            CWriteOptions):
        c_string file_type()

    cdef cppclass CFileFormat "arrow::dataset::FileFormat":
        c_string name()
        CStatus IsSupported(const CFileSource& source, c_bool* supported) const
        CStatus Inspect(const CFileSource& source,
                        shared_ptr[CSchema]* out) const
        CStatus ScanFile(const CFileSource& source,
                         shared_ptr[CScanOptions] scan_options,
                         shared_ptr[CScanContext] scan_context,
                         CScanTaskIterator* out) const
        CStatus MakeFragment(const CFileSource& location,
                             shared_ptr[CScanOptions] opts,
                             shared_ptr[CDataFragment]* out)

    cdef cppclass CFileBasedDataFragment \
            "arrow::dataset::FileBasedDataFragment"(CDataFragment):
        CFileBasedDataFragment(const CFileSource& source,
                               shared_ptr[CFileFormat] format,
                               shared_ptr[CScanOptions] scan_options)
        CStatus Scan(shared_ptr[CScanContext] scan_context,
                     shared_ptr[CScanTaskIterator]* out)
        const CFileSource& source()
        shared_ptr[CFileFormat] format()
        shared_ptr[CScanOptions] scan_options()

    cdef cppclass CFileSystemDataSource \
            "arrow::dataset::FileSystemDataSource"(CDataSource):
        @staticmethod
        CStatus Make(CFileSystem* filesystem,
                     const CSelector& selector,
                     shared_ptr[CFileFormat] format,
                     shared_ptr[CScanOptions] scan_options,
                     shared_ptr[CFileSystemDataSource]* out)
        c_string type()
        shared_ptr[CDataFragmentIterator] GetFragments(
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
                         CScanOptionsPtr scan_options,
                         CScanContextPtr scan_context,
                         shared_ptr[CScanTaskIterator]* out)

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
                         shared_ptr[CScanTaskIterator]* out)

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
                         shared_ptr[CScanTaskIterator]* out)

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
                         shared_ptr[CScanTaskIterator]* out)
        CStatus MakeFragment(const CFileSource& source,
                             shared_ptr[CScanOptions] opts,
                             shared_ptr[CDataFragment]* out)

    cdef cppclass CParquetFragment "arrow::dataset::ParquetFragment"(
            CFileBasedDataFragment):
        CParquetFragment(const CFileSource& source,
                         shared_ptr[CScanOptions] options)
        c_bool splittable()
