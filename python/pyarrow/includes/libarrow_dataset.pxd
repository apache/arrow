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


cdef extern from "arrow/api.h" namespace "arrow" nogil:

    cdef cppclass CRecordBatchIterator "arrow::RecordBatchIterator"(
            CIterator[shared_ptr[CRecordBatch]]):
        pass


cdef extern from "arrow/dataset/api.h" namespace "arrow::fs" nogil:

    ctypedef shared_ptr[CFileSystem] CFileSystemPtr "arrow::fs::FileSystemPtr"
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

    ctypedef shared_ptr[CExpression] CExpressionPtr \
        "arrow::dataset::ExpressionPtr"
    ctypedef vector[CExpressionPtr] CExpressionVector \
        "arrow::dataset::ExpressionVector"

    cdef cppclass CUnaryExpression "arrow::dataset::UnaryExpression"(
            CExpression):
        const CExpressionPtr& operand() const

    cdef cppclass CBinaryExpression "arrow::dataset::BinaryExpression"(
            CExpression):
        const CExpressionPtr& left_operand() const
        const CExpressionPtr& right_operand() const

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
        CComparisonExpression(CCompareOperator op, CExpressionPtr left_operand,
                              CExpressionPtr right_operand)
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
        CCastExpression(CExpressionPtr operand, shared_ptr[CDataType] to,
                        CCastOptions options)

    cdef cppclass CInExpression "arrow::dataset::InExpression"(
            CUnaryExpression):
        CInExpression(CExpressionPtr operand, shared_ptr[CArray] set)

    cdef shared_ptr[CNotExpression] MakeNotExpression "arrow::dataset::not_"(
        CExpressionPtr operand)
    cdef CExpressionPtr MakeAndExpression "arrow::dataset::and_"(
        const CExpressionVector& subexpressions)
    cdef CExpressionPtr MakeOrExpression "arrow::dataset::or_"(
        const CExpressionVector& subexpressions)

    cdef cppclass CFilter "arrow::dataset::Filter":
        pass

    cdef cppclass CWriteOptions "arrow::dataset::WriteOptions":
        pass

    cdef cppclass CScanOptions "arrow::dataset::ScanOptions":
        CExpressionPtr filter
        shared_ptr[CSchema] schema
        # shared_ptr[CExpressionEvaluator] evaluator
        # shared_ptr[CRecordBatchProjector] projector

        @staticmethod
        shared_ptr[CScanOptions] Defaults()

    cdef cppclass CScanContext "arrow::dataset::ScanContext":
        CMemoryPool* pool

    ctypedef shared_ptr[CScanOptions] CScanOptionsPtr \
        "arrow::dataset::ScanOptionsPtr"
    ctypedef shared_ptr[CScanContext] CScanContextPtr \
        "arrow::dataset::ScanContextPtr"

    cdef cppclass CScanTask" arrow::dataset::ScanTask":
        CResult[CRecordBatchIterator] Scan()

    cdef cppclass CSimpleScanTask "arrow::dataset::SimpleScanTask"(CScanTask):
        pass

    ctypedef shared_ptr[CScanTask] CScanTaskPtr "arrow::dataset::ScanTaskPtr"
    ctypedef CIterator[CScanTaskPtr] CScanTaskIterator \
        "arrow::dataset::ScanTaskIterator"

    cdef cppclass CScanner "arrow::dataset::Scanner":
        CResult[CScanTaskIterator] Scan()
        CResult[shared_ptr[CTable]] ToTable()

    ctypedef shared_ptr[CScanner] CScannerPtr "arrow::dataset::ScannerPtr"

    cdef cppclass CScannerBuilder "arrow::dataset::ScannerBuilder":
        CScannerBuilder(shared_ptr[CDataset], CScanContextPtr scan_context)
        CStatus Project(const vector[c_string]& columns)
        CStatus Filter(const CExpression& filter)
        CStatus Filter(CExpressionPtr filter)
        CStatus UseThreads(c_bool use_threads)
        CResult[CScannerPtr] Finish()
        shared_ptr[CSchema] schema() const

    ctypedef shared_ptr[CScannerBuilder] CScannerBuilderPtr \
        "arrow::dataset::ScannerBuilderPtr"

    cdef cppclass CDataFragment "arrow::dataset::DataFragment":
        CResult[CScanTaskIterator] Scan(CScanContextPtr context)
        c_bool splittable()
        c_string type()
        CScanOptionsPtr scan_options()

    ctypedef shared_ptr[CDataFragment] CDataFragmentPtr \
        "arrow::dataset::DataFragmentPtr"
    ctypedef vector[CDataFragmentPtr] CDataFragmentVector \
        "arrow::dataset::DataFragmentVector"
    ctypedef CIterator[CDataFragmentPtr] CDataFragmentIterator \
        "arrow::dataset::DataFragmentIterator"

    cdef cppclass CSimpleDataFragment "arrow::dataset::SimpleDataFragment"(
            CDataFragment):
        CSimpleDataFragment(vector[shared_ptr[CRecordBatch]] record_batches,
                            CScanOptionsPtr scan_options)

    cdef cppclass CDataSource "arrow::dataset::DataSource":
        CDataFragmentIterator GetFragments(CScanOptionsPtr options)
        const CExpressionPtr& partition_expression()
        c_string type()

    cdef cppclass CSimpleDataSource "arrow::dataset::SimpleDataSource"(
            CDataSource):
        pass

    cdef cppclass CTreeDataSource "arrow::dataset::TreeDataSource"(
            CDataSource):
        pass

    ctypedef shared_ptr[CDataSource] CDataSourcePtr \
        "arrow::dataset::DataSourcePtr"
    ctypedef vector[CDataSourcePtr] CDataSourceVector \
        "arrow::dataset::DataSourceVector"

    cdef cppclass CDataset "arrow::dataset::Dataset":
        @staticmethod
        CResult[shared_ptr[CDataset]] Make(CDataSourceVector sources,
                                           shared_ptr[CSchema] schema)
        CResult[CScannerBuilderPtr] NewScan(CScanContextPtr context)
        CResult[CScannerBuilderPtr] NewScan()
        const CDataSourceVector& sources()
        shared_ptr[CSchema] schema()

    ctypedef shared_ptr[CDataset] CDatasetPtr "arrow::dataset::DatasetPtr"

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
        c_string name()
        CStatus IsSupported(const CFileSource& source, c_bool* supported) const
        CStatus Inspect(const CFileSource& source,
                        shared_ptr[CSchema]* out) const
        CStatus ScanFile(const CFileSource& source,
                         CScanOptionsPtr scan_options,
                         CScanContextPtr scan_context,
                         CScanTaskIterator* out) const
        CStatus MakeFragment(const CFileSource& location,
                             CScanOptionsPtr opts,
                             CDataFragmentPtr* out)

    ctypedef shared_ptr[CFileFormat] CFileFormatPtr \
        "arrow::dataset::FileFormatPtr"

    cdef cppclass CFileDataFragment "arrow::dataset::FileDataFragment"(
            CDataFragment):
        CFileDataFragment(const CFileSource& source, CFileFormatPtr format,
                          CScanOptionsPtr scan_options)
        CStatus Scan(CScanContextPtr scan_context,
                     shared_ptr[CScanTaskIterator]* out)
        const CFileSource& source()
        CFileFormatPtr format()
        CScanOptionsPtr scan_options()

    cdef cppclass CParquetDataFragment "arrow::dataset::ParquetFragment"(
            CFileDataFragment):
        CParquetDataFragment(const CFileSource& source,
                             CScanOptionsPtr options)

    ctypedef unordered_map[c_string, CExpressionPtr] CPathPartitions \
        "arrow::dataset::PathPartitions"

    cdef cppclass CFileSystemDataSource \
            "arrow::dataset::FileSystemDataSource"(CDataSource):
        @staticmethod
        CResult[CDataSourcePtr] Make(CFileSystemPtr filesystem,
                                     CFileStatsVector stats,
                                     CExpressionPtr source_partition,
                                     CPathPartitions partitions,
                                     CFileFormatPtr format)
        c_string type()
        shared_ptr[CDataFragmentIterator] GetFragments(CScanOptionsPtr options)

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
        c_string name() const
        CResult[CExpressionPtr] Parse(const c_string& path) const

    ctypedef shared_ptr[CPartitionScheme] CPartitionSchemePtr \
        "arrow::dataset::PartitionSchemePtr"

    cdef cppclass CSchemaPartitionScheme \
            "arrow::dataset::SchemaPartitionScheme"(CPartitionScheme):
        CSchemaPartitionScheme(shared_ptr[CSchema] schema)
        const shared_ptr[CSchema]& schema()

    cdef cppclass CHivePartitionScheme \
            "arrow::dataset::HivePartitionScheme"(CPartitionScheme):
        CHivePartitionScheme(shared_ptr[CSchema] schema)
        const shared_ptr[CSchema]& schema()
        # vector[CUnconvertedKey] GetUnconvertedKeys(
        #    const c_string& path) const;

    cdef cppclass CFileSystemDiscoveryOptions \
            "arrow::dataset::FileSystemDiscoveryOptions":
        c_string partition_base_dir
        c_bool exclude_invalid_files
        vector[c_string] ignore_prefixes

    cdef cppclass CDataSourceDiscovery "arrow::dataset::DataSourceDiscovery":
        CResult[shared_ptr[CSchema]] Inspect()
        CResult[CDataSourcePtr] Finish()
        shared_ptr[CSchema] schema()
        CStatus SetSchema(shared_ptr[CSchema])
        CPartitionSchemePtr partition_scheme()
        CStatus SetPartitionScheme(CPartitionSchemePtr partition_scheme)
        CExpressionPtr root_partition()
        CStatus SetRootPartition(CExpressionPtr partition)

    ctypedef shared_ptr[CDataSourceDiscovery] CDataSourceDiscoveryPtr \
        "arrow::dataset::DataSourceDiscovery"

    cdef cppclass CFileSystemDataSourceDiscovery \
            "arrow::dataset::FileSystemDataSourceDiscovery"(
                CDataSourceDiscovery):
        @staticmethod
        CResult[CDataSourceDiscoveryPtr] Make(
            CFileSystemPtr filesytem,
            CFileStatsVector paths,
            CFileFormatPtr format,
            CFileSystemDiscoveryOptions options
        )
        @staticmethod
        CResult[CDataSourceDiscoveryPtr] Make(
            CFileSystemPtr filesytem,
            CSelector,
            CFileFormatPtr format,
            CFileSystemDiscoveryOptions options
        )
