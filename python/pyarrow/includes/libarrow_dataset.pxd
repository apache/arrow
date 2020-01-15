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

from libcpp.unordered_map cimport unordered_map

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *


cdef extern from "arrow/api.h" namespace "arrow" nogil:

    cdef cppclass CRecordBatchIterator "arrow::RecordBatchIterator"(
            CIterator[shared_ptr[CRecordBatch]]):
        pass


cdef extern from "arrow/dataset/api.h" namespace "arrow::fs" nogil:

    ctypedef vector[CFileStats] CFileStatsVector "arrow::fs::FileStatsVector"


cdef extern from "arrow/dataset/api.h" namespace "arrow::dataset" nogil:

    cdef enum CExpressionType "arrow::dataset::ExpressionType::type":
        CExpressionType_FIELD "arrow::dataset::ExpressionType::type::FIELD"
        CExpressionType_SCALAR "arrow::dataset::ExpressionType::type::SCALAR"
        CExpressionType_NOT "arrow::dataset::ExpressionType::type::NOT"
        CExpressionType_CAST "arrow::dataset::ExpressionType::type::CAST"
        CExpressionType_AND "arrow::dataset::ExpressionType::type::AND"
        CExpressionType_OR "arrow::dataset::ExpressionType::type::OR"
        CExpressionType_COMPARISON \
            "arrow::dataset::ExpressionType::type::COMPARISON"
        CExpressionType_IS_VALID \
            "arrow::dataset::ExpressionType::type::IS_VALID"
        CExpressionType_IN "arrow::dataset::ExpressionType::type::IN"
        CExpressionType_CUSTOM "arrow::dataset::ExpressionType::type::CUSTOM"

    cdef cppclass CExpression "arrow::dataset::Expression":
        CExpression(CExpressionType type)
        c_bool Equals(const CExpression& other) const
        c_bool Equals(const shared_ptr[CExpression]& other) const
        c_bool IsNull() const
        CResult[shared_ptr[CDataType]] Validate(const CSchema& schema) const
        shared_ptr[CExpression] Assume(const CExpression& given) const
        shared_ptr[CExpression] Assume(
            const shared_ptr[CExpression]& given) const
        c_string ToString() const
        CExpressionType type() const
        shared_ptr[CExpression] Copy() const

    ctypedef vector[shared_ptr[CExpression]] CExpressionVector \
        "arrow::dataset::ExpressionVector"

    cdef cppclass CUnaryExpression "arrow::dataset::UnaryExpression"(
            CExpression):
        const shared_ptr[CExpression]& operand() const

    cdef cppclass CBinaryExpression "arrow::dataset::BinaryExpression"(
            CExpression):
        const shared_ptr[CExpression]& left_operand() const
        const shared_ptr[CExpression]& right_operand() const

    cdef cppclass CScalarExpression "arrow::dataset::ScalarExpression"(
            CExpression):
        CScalarExpression(const shared_ptr[CScalar]& value)
        const shared_ptr[CScalar]& value() const

    cdef cppclass CFieldExpression "arrow::dataset::FieldExpression"(
            CExpression):
        CFieldExpression(c_string name)
        c_string name() const

    cdef cppclass CComparisonExpression "arrow::dataset::ComparisonExpression"(
            CBinaryExpression):
        CComparisonExpression(CCompareOperator op,
                              shared_ptr[CExpression] left_operand,
                              shared_ptr[CExpression] right_operand)
        CCompareOperator op() const

    cdef cppclass CAndExpression "arrow::dataset::AndExpression"(
            CBinaryExpression):
        pass

    cdef cppclass COrExpression "arrow::dataset::OrExpression"(
            CBinaryExpression):
        pass

    cdef cppclass CNotExpression "arrow::dataset::NotExpression"(
            CUnaryExpression):
        pass

    cdef cppclass CIsValidExpression "arrow::dataset::IsValidExpression"(
            CUnaryExpression):
        pass

    cdef cppclass CCastExpression "arrow::dataset::CastExpression"(
            CUnaryExpression):
        CCastExpression(shared_ptr[CExpression] operand,
                        shared_ptr[CDataType] to,
                        CCastOptions options)

    cdef cppclass CInExpression "arrow::dataset::InExpression"(
            CUnaryExpression):
        CInExpression(shared_ptr[CExpression] operand, shared_ptr[CArray] set)

    cdef shared_ptr[CNotExpression] MakeNotExpression "arrow::dataset::not_"(
        shared_ptr[CExpression] operand)
    cdef shared_ptr[CExpression] MakeAndExpression "arrow::dataset::and_"(
        const CExpressionVector& subexpressions)
    cdef shared_ptr[CExpression] MakeOrExpression "arrow::dataset::or_"(
        const CExpressionVector& subexpressions)

    cdef cppclass CFilter "arrow::dataset::Filter":
        pass

    cdef cppclass CWriteOptions "arrow::dataset::WriteOptions":
        pass

    cdef cppclass CScanOptions "arrow::dataset::ScanOptions":
        shared_ptr[CExpression] filter
        shared_ptr[CSchema] schema
        c_bool use_threads
        # shared_ptr[CExpressionEvaluator] evaluator
        # shared_ptr[CRecordBatchProjector] projector

        @staticmethod
        shared_ptr[CScanOptions] Defaults()

    cdef cppclass CScanContext "arrow::dataset::ScanContext":
        CMemoryPool* pool

    cdef cppclass CScanTask" arrow::dataset::ScanTask":
        CResult[CRecordBatchIterator] Execute()

    cdef cppclass CSimpleScanTask "arrow::dataset::SimpleScanTask"(CScanTask):
        pass

    ctypedef CIterator[shared_ptr[CScanTask]] CScanTaskIterator \
        "arrow::dataset::ScanTaskIterator"

    cdef cppclass CScanner "arrow::dataset::Scanner":
        CResult[CScanTaskIterator] Scan()
        CResult[shared_ptr[CTable]] ToTable()

    cdef cppclass CScannerBuilder "arrow::dataset::ScannerBuilder":
        CScannerBuilder(shared_ptr[CDataset],
                        shared_ptr[CScanContext] scan_context)
        CStatus Project(const vector[c_string]& columns)
        CStatus Filter(const CExpression& filter)
        CStatus Filter(shared_ptr[CExpression] filter)
        CStatus UseThreads(c_bool use_threads)
        CResult[shared_ptr[CScanner]] Finish()
        shared_ptr[CSchema] schema() const

    cdef cppclass CDataFragment "arrow::dataset::DataFragment":
        CResult[CScanTaskIterator] Scan(shared_ptr[CScanContext] context)
        c_bool splittable()
        shared_ptr[CScanOptions] scan_options()

    ctypedef vector[shared_ptr[CDataFragment]] CDataFragmentVector \
        "arrow::dataset::DataFragmentVector"

    ctypedef CIterator[shared_ptr[CDataFragment]] CDataFragmentIterator \
        "arrow::dataset::DataFragmentIterator"

    cdef cppclass CSimpleDataFragment "arrow::dataset::SimpleDataFragment"(
            CDataFragment):
        CSimpleDataFragment(vector[shared_ptr[CRecordBatch]] record_batches,
                            shared_ptr[CScanOptions] scan_options)

    cdef cppclass CDataSource "arrow::dataset::DataSource":
        CDataFragmentIterator GetFragments(shared_ptr[CScanOptions] options)
        const shared_ptr[CSchema]& schema()
        const shared_ptr[CExpression]& partition_expression()
        c_string type_name()

    ctypedef vector[shared_ptr[CDataSource]] CDataSourceVector \
        "arrow::dataset::DataSourceVector"

    cdef cppclass CTreeDataSource "arrow::dataset::TreeDataSource"(
            CDataSource):
        CTreeDataSource(CDataSourceVector children)

    cdef cppclass CDataset "arrow::dataset::Dataset":
        @staticmethod
        CResult[shared_ptr[CDataset]] Make(CDataSourceVector sources,
                                           shared_ptr[CSchema] schema)
        CResult[shared_ptr[CScannerBuilder]] NewScanWithContext "NewScan"(
            shared_ptr[CScanContext] context)
        CResult[shared_ptr[CScannerBuilder]] NewScan()
        const CDataSourceVector& sources()
        shared_ptr[CSchema] schema()

    cdef cppclass CFileScanOptions "arrow::dataset::FileScanOptions"(
            CScanOptions):
        c_string file_type()

    cdef cppclass CFileSource "arrow::dataset::FileSource":
        CFileSource(c_string path, CFileSystem* filesystem,
                    CompressionType compression)
        c_bool operator==(const CFileSource& other) const
        CompressionType compression()
        c_string path()
        CFileSystem* filesystem()
        shared_ptr[CBuffer] buffer()
        CStatus Open(shared_ptr[CRandomAccessFile]* out)

    cdef cppclass CFileWriteOptions "arrow::dataset::WriteOptions"(
            CWriteOptions):
        c_string file_type()

    cdef cppclass CFileFormat "arrow::dataset::FileFormat":
        c_string type_name()
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

    cdef cppclass CFileDataFragment "arrow::dataset::FileDataFragment"(
            CDataFragment):
        CFileDataFragment(const CFileSource& source,
                          shared_ptr[CFileFormat] format,
                          shared_ptr[CScanOptions] scan_options)
        CStatus Scan(shared_ptr[CScanContext] scan_context,
                     shared_ptr[CScanTaskIterator]* out)
        const CFileSource& source()
        shared_ptr[CFileFormat] format()
        shared_ptr[CScanOptions] scan_options()

    cdef cppclass CParquetDataFragment "arrow::dataset::ParquetFragment"(
            CFileDataFragment):
        CParquetDataFragment(const CFileSource& source,
                             shared_ptr[CScanOptions] options)

    cdef cppclass CFileSystemDataSource \
            "arrow::dataset::FileSystemDataSource"(CDataSource):
        @staticmethod
        CResult[shared_ptr[CDataSource]] Make(
            shared_ptr[CSchema] schema,
            shared_ptr[CExpression] source_partition,
            shared_ptr[CFileFormat] format,
            shared_ptr[CFileSystem] filesystem,
            CFileStatsVector stats,
            CExpressionVector partitions)
        c_string type()
        shared_ptr[CDataFragmentIterator] GetFragments(
            shared_ptr[CScanOptions] options)

    cdef cppclass CParquetScanOptions "arrow::dataset::ParquetScanOptions"(
            CFileScanOptions):
        c_string file_type()

    cdef cppclass CParquetWriterOptions "arrow::dataset::ParquetWriterOptions"(
            CFileWriteOptions):
        c_string file_type()

    cdef cppclass CParquetFileFormat "arrow::dataset::ParquetFileFormat"(
            CFileFormat):
        pass

    cdef cppclass CParquetFragment "arrow::dataset::ParquetFragment"(
            CFileDataFragment):
        CParquetFragment(const CFileSource& source,
                         shared_ptr[CScanOptions] options)
        c_bool splittable()

    cdef cppclass CPartitionScheme "arrow::dataset::PartitionScheme":
        c_string type_name() const
        CResult[shared_ptr[CExpression]] Parse(const c_string& path) const
        const shared_ptr[CSchema]& schema()

    cdef cppclass CPartitionSchemeDiscovery \
            "arrow::dataset::PartitionSchemeDiscovery":
        pass

    cdef cppclass CDefaultPartitionScheme \
            "arrow::dataset::DefaultPartitionScheme"(CPartitionScheme):
        CDefaultPartitionScheme()

    cdef cppclass CSchemaPartitionScheme \
            "arrow::dataset::SchemaPartitionScheme"(CPartitionScheme):
        CSchemaPartitionScheme(shared_ptr[CSchema] schema)
        @staticmethod
        shared_ptr[CPartitionSchemeDiscovery] MakeDiscovery(
            vector[c_string] field_names)

    cdef cppclass CHivePartitionScheme \
            "arrow::dataset::HivePartitionScheme"(CPartitionScheme):
        CHivePartitionScheme(shared_ptr[CSchema] schema)
        @staticmethod
        shared_ptr[CPartitionSchemeDiscovery] MakeDiscovery()

    cdef cppclass CPartitionSchemeOrDiscovery \
            "arrow::dataset::PartitionSchemeOrDiscovery":
        CPartitionSchemeOrDiscovery(shared_ptr[CPartitionScheme])
        CPartitionSchemeOrDiscovery(shared_ptr[CPartitionSchemeDiscovery])
        CPartitionSchemeOrDiscovery& operator=(shared_ptr[CPartitionScheme])
        CPartitionSchemeOrDiscovery& operator=(
            shared_ptr[CPartitionSchemeDiscovery])
        shared_ptr[CPartitionScheme] scheme() const
        shared_ptr[CPartitionSchemeDiscovery] discovery() const

    cdef cppclass CFileSystemDiscoveryOptions \
            "arrow::dataset::FileSystemDiscoveryOptions":
        CPartitionSchemeOrDiscovery partition_scheme
        c_string partition_base_dir
        c_bool exclude_invalid_files
        vector[c_string] ignore_prefixes

    cdef cppclass CDataSourceDiscovery "arrow::dataset::DataSourceDiscovery":
        CResult[vector[shared_ptr[CSchema]]] InspectSchemas()
        CResult[shared_ptr[CSchema]] Inspect()
        CResult[shared_ptr[CDataSource]] Finish(shared_ptr[CSchema])
        CResult[shared_ptr[CDataSource]] Finish()
        shared_ptr[CExpression] root_partition()
        CStatus SetRootPartition(shared_ptr[CExpression] partition)

    cdef cppclass CFileSystemDataSourceDiscovery \
            "arrow::dataset::FileSystemDataSourceDiscovery"(
                CDataSourceDiscovery):
        @staticmethod
        CResult[shared_ptr[CDataSourceDiscovery]] MakeFromPaths "Make"(
            shared_ptr[CFileSystem] filesystem,
            vector[c_string] paths,
            shared_ptr[CFileFormat] format,
            CFileSystemDiscoveryOptions options
        )
        @staticmethod
        CResult[shared_ptr[CDataSourceDiscovery]] MakeFromSelector "Make"(
            shared_ptr[CFileSystem] filesystem,
            CFileSelector,
            shared_ptr[CFileFormat] format,
            CFileSystemDiscoveryOptions options
        )
