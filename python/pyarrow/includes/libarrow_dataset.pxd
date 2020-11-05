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
from pyarrow._parquet cimport *


cdef extern from "arrow/api.h" namespace "arrow" nogil:

    cdef cppclass CRecordBatchIterator "arrow::RecordBatchIterator"(
            CIterator[shared_ptr[CRecordBatch]]):
        pass


cdef extern from "arrow/dataset/api.h" namespace "arrow::dataset" nogil:

    cdef cppclass CExpression "arrow::dataset::Expression":
        c_bool Equals(const CExpression& other) const
        c_bool Equals(const shared_ptr[CExpression]& other) const
        CResult[shared_ptr[CDataType]] Validate(const CSchema& schema) const
        shared_ptr[CExpression] Assume(const CExpression& given) const
        shared_ptr[CExpression] Assume(
            const shared_ptr[CExpression]& given) const
        c_string ToString() const
        shared_ptr[CExpression] Copy() const

        const CExpression& In(shared_ptr[CArray]) const
        const CExpression& IsValid() const
        const CExpression& CastTo(shared_ptr[CDataType], CCastOptions) const
        const CExpression& CastLike(shared_ptr[CExpression],
                                    CCastOptions) const

        @staticmethod
        CResult[shared_ptr[CExpression]] Deserialize(const CBuffer& buffer)
        CResult[shared_ptr[CBuffer]] Serialize() const

    ctypedef vector[shared_ptr[CExpression]] CExpressionVector \
        "arrow::dataset::ExpressionVector"

    cdef cppclass CScalarExpression \
            "arrow::dataset::ScalarExpression"(CExpression):
        CScalarExpression(const shared_ptr[CScalar]& value)

    cdef shared_ptr[CExpression] CMakeFieldExpression \
        "arrow::dataset::field_ref"(c_string name)
    cdef shared_ptr[CExpression] CMakeNotExpression \
        "arrow::dataset::not_"(shared_ptr[CExpression] operand)
    cdef shared_ptr[CExpression] CMakeAndExpression \
        "arrow::dataset::and_"(shared_ptr[CExpression],
                               shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeOrExpression \
        "arrow::dataset::or_"(shared_ptr[CExpression],
                              shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeEqualExpression \
        "arrow::dataset::equal"(shared_ptr[CExpression],
                                shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeNotEqualExpression \
        "arrow::dataset::not_equal"(shared_ptr[CExpression],
                                    shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeGreaterExpression \
        "arrow::dataset::greater"(shared_ptr[CExpression],
                                  shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeGreaterEqualExpression \
        "arrow::dataset::greater_equal"(shared_ptr[CExpression],
                                        shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeLessExpression \
        "arrow::dataset::less"(shared_ptr[CExpression],
                               shared_ptr[CExpression])
    cdef shared_ptr[CExpression] CMakeLessEqualExpression \
        "arrow::dataset::less_equal"(shared_ptr[CExpression],
                                     shared_ptr[CExpression])

    cdef CResult[shared_ptr[CExpression]] CInsertImplicitCasts \
        "arrow::dataset::InsertImplicitCasts"(
            const CExpression &, const CSchema&)

    cdef cppclass CRecordBatchProjector "arrow::dataset::RecordBatchProjector":
        pass

    cdef cppclass CScanOptions "arrow::dataset::ScanOptions":
        CRecordBatchProjector projector

        @staticmethod
        shared_ptr[CScanOptions] Make(shared_ptr[CSchema] schema)

    cdef cppclass CScanContext "arrow::dataset::ScanContext":
        c_bool use_threads
        CMemoryPool * pool

    ctypedef CIterator[shared_ptr[CScanTask]] CScanTaskIterator \
        "arrow::dataset::ScanTaskIterator"

    cdef cppclass CScanTask" arrow::dataset::ScanTask":
        CResult[CRecordBatchIterator] Execute()

    cdef cppclass CFragment "arrow::dataset::Fragment":
        CResult[shared_ptr[CSchema]] ReadPhysicalSchema()
        CResult[CScanTaskIterator] Scan(
            shared_ptr[CScanOptions] options, shared_ptr[CScanContext] context)
        c_bool splittable() const
        c_string type_name() const
        const shared_ptr[CExpression]& partition_expression() const

    ctypedef vector[shared_ptr[CFragment]] CFragmentVector \
        "arrow::dataset::FragmentVector"

    ctypedef CIterator[shared_ptr[CFragment]] CFragmentIterator \
        "arrow::dataset::FragmentIterator"

    cdef cppclass CInMemoryFragment "arrow::dataset::InMemoryFragment"(
            CFragment):
        CInMemoryFragment(vector[shared_ptr[CRecordBatch]] record_batches,
                          shared_ptr[CExpression] partition_expression)

    cdef cppclass CScanner "arrow::dataset::Scanner":
        CScanner(shared_ptr[CDataset], shared_ptr[CScanOptions],
                 shared_ptr[CScanContext])
        CScanner(shared_ptr[CFragment], shared_ptr[CScanOptions],
                 shared_ptr[CScanContext])
        CResult[CScanTaskIterator] Scan()
        CResult[shared_ptr[CTable]] ToTable()
        CFragmentIterator GetFragments()
        const shared_ptr[CScanOptions]& options()

    cdef cppclass CScannerBuilder "arrow::dataset::ScannerBuilder":
        CScannerBuilder(shared_ptr[CDataset],
                        shared_ptr[CScanContext] scan_context)
        CScannerBuilder(shared_ptr[CSchema], shared_ptr[CFragment],
                        shared_ptr[CScanContext] scan_context)
        CStatus Project(const vector[c_string]& columns)
        CStatus Filter(const CExpression& filter)
        CStatus Filter(shared_ptr[CExpression] filter)
        CStatus UseThreads(c_bool use_threads)
        CStatus BatchSize(int64_t batch_size)
        CResult[shared_ptr[CScanner]] Finish()
        shared_ptr[CSchema] schema() const

    ctypedef vector[shared_ptr[CDataset]] CDatasetVector \
        "arrow::dataset::DatasetVector"

    cdef cppclass CDataset "arrow::dataset::Dataset":
        const shared_ptr[CSchema] & schema()
        CFragmentIterator GetFragments()
        CFragmentIterator GetFragments(shared_ptr[CExpression] predicate)
        const shared_ptr[CExpression] & partition_expression()
        c_string type_name()

        CResult[shared_ptr[CDataset]] ReplaceSchema(shared_ptr[CSchema])

        CResult[shared_ptr[CScannerBuilder]] NewScanWithContext "NewScan"(
            shared_ptr[CScanContext] context)
        CResult[shared_ptr[CScannerBuilder]] NewScan()

    cdef cppclass CUnionDataset "arrow::dataset::UnionDataset"(
            CDataset):
        @staticmethod
        CResult[shared_ptr[CUnionDataset]] Make(shared_ptr[CSchema] schema,
                                                CDatasetVector children)

        const CDatasetVector& children() const

    cdef cppclass CInspectOptions "arrow::dataset::InspectOptions":
        int fragments

    cdef cppclass CFinishOptions "arrow::dataset::FinishOptions":
        shared_ptr[CSchema] schema
        CInspectOptions inspect_options
        c_bool validate_fragments

    cdef cppclass CDatasetFactory "arrow::dataset::DatasetFactory":
        CResult[vector[shared_ptr[CSchema]]] InspectSchemas(CInspectOptions)
        CResult[shared_ptr[CSchema]] Inspect(CInspectOptions)
        CResult[shared_ptr[CDataset]] FinishWithSchema "Finish"(
            const shared_ptr[CSchema]& schema)
        CResult[shared_ptr[CDataset]] Finish()
        const shared_ptr[CExpression]& root_partition()
        CStatus SetRootPartition(shared_ptr[CExpression] partition)

    cdef cppclass CUnionDatasetFactory "arrow::dataset::UnionDatasetFactory":
        @staticmethod
        CResult[shared_ptr[CDatasetFactory]] Make(
            vector[shared_ptr[CDatasetFactory]] factories)

    cdef cppclass CFileSource "arrow::dataset::FileSource":
        const c_string& path() const
        const shared_ptr[CFileSystem]& filesystem() const
        const shared_ptr[CBuffer]& buffer() const
        # HACK: Cython can't handle all the overloads so don't declare them.
        # This means invalid construction of CFileSource won't be caught in
        # the C++ generation phase (though it will still be caught when
        # the generated C++ is compiled).
        CFileSource(...)

    cdef cppclass CFileWriteOptions \
            "arrow::dataset::FileWriteOptions":
        const shared_ptr[CFileFormat]& format() const
        c_string type_name() const

    cdef cppclass CFileFormat "arrow::dataset::FileFormat":
        c_string type_name() const
        CResult[shared_ptr[CSchema]] Inspect(const CFileSource&) const
        CResult[shared_ptr[CFileFragment]] MakeFragment(
            CFileSource source,
            shared_ptr[CExpression] partition_expression,
            shared_ptr[CSchema] physical_schema)
        shared_ptr[CFileWriteOptions] DefaultWriteOptions()

    cdef cppclass CFileFragment "arrow::dataset::FileFragment"(
            CFragment):
        const CFileSource& source() const
        const shared_ptr[CFileFormat]& format() const

    cdef cppclass CRowGroupInfo "arrow::dataset::RowGroupInfo":
        CRowGroupInfo()
        CRowGroupInfo(int id)
        int id() const
        int64_t num_rows() const
        int64_t total_byte_size() const
        bint Equals(const CRowGroupInfo& other)
        c_bool HasStatistics() const
        shared_ptr[CStructScalar] statistics() const

        @staticmethod
        vector[CRowGroupInfo] FromIdentifiers(vector[int])

    cdef cppclass CParquetFileWriteOptions \
            "arrow::dataset::ParquetFileWriteOptions"(CFileWriteOptions):
        shared_ptr[WriterProperties] writer_properties
        shared_ptr[ArrowWriterProperties] arrow_writer_properties

    cdef cppclass CParquetFileFragment "arrow::dataset::ParquetFileFragment"(
            CFileFragment):
        const vector[CRowGroupInfo]* row_groups() const
        CResult[int] GetNumRowGroups()
        CResult[vector[shared_ptr[CFragment]]] SplitByRowGroup(
            shared_ptr[CExpression] predicate)
        CResult[shared_ptr[CFragment]] SubsetWithFilter "Subset"(
            shared_ptr[CExpression] predicate)
        CResult[shared_ptr[CFragment]] SubsetWithIds "Subset"(
            vector[int] row_group_ids)
        CStatus EnsureCompleteMetadata()

    cdef cppclass CFileSystemDatasetWriteOptions \
            "arrow::dataset::FileSystemDatasetWriteOptions":
        shared_ptr[CFileWriteOptions] file_write_options
        shared_ptr[CFileSystem] filesystem
        c_string base_dir
        shared_ptr[CPartitioning] partitioning
        c_string basename_template

    cdef cppclass CFileSystemDataset \
            "arrow::dataset::FileSystemDataset"(CDataset):
        @staticmethod
        CResult[shared_ptr[CDataset]] Make(
            shared_ptr[CSchema] schema,
            shared_ptr[CExpression] source_partition,
            shared_ptr[CFileFormat] format,
            shared_ptr[CFileSystem] filesystem,
            vector[shared_ptr[CFileFragment]] fragments)

        @staticmethod
        CStatus Write(
            const CFileSystemDatasetWriteOptions& write_options,
            shared_ptr[CScanner] scanner)

        c_string type()
        vector[c_string] files()
        const shared_ptr[CFileFormat]& format() const
        const shared_ptr[CFileSystem]& filesystem() const

    cdef cppclass CParquetFileFormatReaderOptions \
            "arrow::dataset::ParquetFileFormat::ReaderOptions":
        c_bool use_buffered_stream
        int64_t buffer_size
        unordered_set[c_string] dict_columns
        c_bool enable_parallel_column_conversion

    cdef cppclass CParquetFileFormat "arrow::dataset::ParquetFileFormat"(
            CFileFormat):
        CParquetFileFormatReaderOptions reader_options
        CResult[shared_ptr[CFileFragment]] MakeFragment(
            CFileSource source,
            shared_ptr[CExpression] partition_expression,
            vector[CRowGroupInfo] row_groups,
            shared_ptr[CSchema] physical_schema)

    cdef cppclass CIpcFileWriteOptions \
            "arrow::dataset::IpcFileWriteOptions"(CFileWriteOptions):
        pass

    cdef cppclass CIpcFileFormat "arrow::dataset::IpcFileFormat"(
            CFileFormat):
        pass

    cdef cppclass CCsvFileFormat "arrow::dataset::CsvFileFormat"(
            CFileFormat):
        CCSVParseOptions parse_options

    cdef cppclass CPartitioning "arrow::dataset::Partitioning":
        c_string type_name() const
        CResult[shared_ptr[CExpression]] Parse(const c_string & path) const
        const shared_ptr[CSchema] & schema()

    cdef cppclass CPartitioningFactoryOptions \
            "arrow::dataset::PartitioningFactoryOptions":
        c_bool infer_dictionary

    cdef cppclass CPartitioningFactory "arrow::dataset::PartitioningFactory":
        pass

    cdef cppclass CDirectoryPartitioning \
            "arrow::dataset::DirectoryPartitioning"(CPartitioning):
        CDirectoryPartitioning(shared_ptr[CSchema] schema)

        @staticmethod
        shared_ptr[CPartitioningFactory] MakeFactory(
            vector[c_string] field_names, CPartitioningFactoryOptions)

    cdef cppclass CHivePartitioning \
            "arrow::dataset::HivePartitioning"(CPartitioning):
        CHivePartitioning(shared_ptr[CSchema] schema)

        @staticmethod
        shared_ptr[CPartitioningFactory] MakeFactory(
            CPartitioningFactoryOptions)

    cdef cppclass CPartitioningOrFactory \
            "arrow::dataset::PartitioningOrFactory":
        CPartitioningOrFactory(shared_ptr[CPartitioning])
        CPartitioningOrFactory(shared_ptr[CPartitioningFactory])
        CPartitioningOrFactory & operator = (shared_ptr[CPartitioning])
        CPartitioningOrFactory & operator = (
            shared_ptr[CPartitioningFactory])
        shared_ptr[CPartitioning] partitioning() const
        shared_ptr[CPartitioningFactory] factory() const

    cdef CStatus CSetPartitionKeysInProjector \
        "arrow::dataset::KeyValuePartitioning::SetDefaultValuesFromKeys"(
            const CExpression& partition_expression,
            CRecordBatchProjector* projector)

    cdef CResult[unordered_map[c_string, shared_ptr[CScalar]]] \
        CGetPartitionKeys "arrow::dataset::KeyValuePartitioning::GetKeys"(
        const CExpression& partition_expression)

    cdef cppclass CFileSystemFactoryOptions \
            "arrow::dataset::FileSystemFactoryOptions":
        CPartitioningOrFactory partitioning
        c_string partition_base_dir
        c_bool exclude_invalid_files
        vector[c_string] selector_ignore_prefixes

    cdef cppclass CFileSystemDatasetFactory \
            "arrow::dataset::FileSystemDatasetFactory"(
                CDatasetFactory):
        @staticmethod
        CResult[shared_ptr[CDatasetFactory]] MakeFromPaths "Make"(
            shared_ptr[CFileSystem] filesystem,
            vector[c_string] paths,
            shared_ptr[CFileFormat] format,
            CFileSystemFactoryOptions options
        )

        @staticmethod
        CResult[shared_ptr[CDatasetFactory]] MakeFromSelector "Make"(
            shared_ptr[CFileSystem] filesystem,
            CFileSelector,
            shared_ptr[CFileFormat] format,
            CFileSystemFactoryOptions options
        )

    cdef cppclass CParquetFactoryOptions \
            "arrow::dataset::ParquetFactoryOptions":
        CPartitioningOrFactory partitioning
        c_string partition_base_dir

    cdef cppclass CParquetDatasetFactory \
            "arrow::dataset::ParquetDatasetFactory"(CDatasetFactory):
        @staticmethod
        CResult[shared_ptr[CDatasetFactory]] MakeFromMetaDataPath "Make"(
            const c_string& metadata_path,
            shared_ptr[CFileSystem] filesystem,
            shared_ptr[CParquetFileFormat] format,
            CParquetFactoryOptions options
        )

        @staticmethod
        CResult[shared_ptr[CDatasetFactory]] MakeFromMetaDataSource "Make"(
            const CFileSource& metadata_path,
            const c_string& base_path,
            shared_ptr[CFileSystem] filesystem,
            shared_ptr[CParquetFileFormat] format,
            CParquetFactoryOptions options
        )
