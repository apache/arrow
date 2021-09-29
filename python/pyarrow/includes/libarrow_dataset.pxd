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


cdef extern from * namespace "arrow::compute":
    # inlined from expression_internal.h to avoid
    # proliferation of #include <unordered_map>
    """
    #include <unordered_map>

    #include "arrow/type.h"
    #include "arrow/datum.h"

    namespace arrow {
    namespace compute {
    struct KnownFieldValues {
      std::unordered_map<FieldRef, Datum, FieldRef::Hash> map;
    };
    } //  namespace compute
    } //  namespace arrow
    """
    cdef struct CKnownFieldValues "arrow::compute::KnownFieldValues":
        unordered_map[CFieldRef, CDatum, CFieldRefHash] map

cdef extern from "arrow/compute/exec/expression.h" \
        namespace "arrow::compute" nogil:

    cdef cppclass CExpression "arrow::compute::Expression":
        c_bool Equals(const CExpression& other) const
        c_string ToString() const
        CResult[CExpression] Bind(const CSchema&)

    cdef CExpression CMakeScalarExpression \
        "arrow::compute::literal"(shared_ptr[CScalar] value)

    cdef CExpression CMakeFieldExpression \
        "arrow::compute::field_ref"(c_string name)

    cdef CExpression CMakeCallExpression \
        "arrow::compute::call"(c_string function,
                               vector[CExpression] arguments,
                               shared_ptr[CFunctionOptions] options)

    cdef CResult[shared_ptr[CBuffer]] CSerializeExpression \
        "arrow::compute::Serialize"(const CExpression&)

    cdef CResult[CExpression] CDeserializeExpression \
        "arrow::compute::Deserialize"(shared_ptr[CBuffer])

    cdef CResult[CKnownFieldValues] \
        CExtractKnownFieldValues "arrow::compute::ExtractKnownFieldValues"(
            const CExpression& partition_expression)

ctypedef CStatus cb_writer_finish_internal(CFileWriter*)
ctypedef void cb_writer_finish(dict, CFileWriter*)

cdef extern from "arrow/dataset/api.h" namespace "arrow::dataset" nogil:

    cdef cppclass CScanOptions "arrow::dataset::ScanOptions":
        @staticmethod
        shared_ptr[CScanOptions] Make(shared_ptr[CSchema] schema)

        shared_ptr[CSchema] dataset_schema
        shared_ptr[CSchema] projected_schema

    cdef cppclass CFragmentScanOptions "arrow::dataset::FragmentScanOptions":
        c_string type_name() const

    ctypedef CIterator[shared_ptr[CScanTask]] CScanTaskIterator \
        "arrow::dataset::ScanTaskIterator"

    cdef cppclass CScanTask" arrow::dataset::ScanTask":
        CResult[CRecordBatchIterator] Execute()

    cdef cppclass CFragment "arrow::dataset::Fragment":
        CResult[shared_ptr[CSchema]] ReadPhysicalSchema()
        CResult[CScanTaskIterator] Scan(shared_ptr[CScanOptions] options)
        c_bool splittable() const
        c_string type_name() const
        const CExpression& partition_expression() const

    ctypedef vector[shared_ptr[CFragment]] CFragmentVector \
        "arrow::dataset::FragmentVector"

    ctypedef CIterator[shared_ptr[CFragment]] CFragmentIterator \
        "arrow::dataset::FragmentIterator"

    cdef cppclass CInMemoryFragment "arrow::dataset::InMemoryFragment"(
            CFragment):
        CInMemoryFragment(vector[shared_ptr[CRecordBatch]] record_batches,
                          CExpression partition_expression)

    cdef cppclass CTaggedRecordBatch "arrow::dataset::TaggedRecordBatch":
        shared_ptr[CRecordBatch] record_batch
        shared_ptr[CFragment] fragment

    ctypedef CIterator[CTaggedRecordBatch] CTaggedRecordBatchIterator \
        "arrow::dataset::TaggedRecordBatchIterator"

    cdef cppclass CScanner "arrow::dataset::Scanner":
        CScanner(shared_ptr[CDataset], shared_ptr[CScanOptions])
        CScanner(shared_ptr[CFragment], shared_ptr[CScanOptions])
        CResult[CScanTaskIterator] Scan()
        CResult[CTaggedRecordBatchIterator] ScanBatches()
        CResult[shared_ptr[CTable]] ToTable()
        CResult[shared_ptr[CTable]] TakeRows(const CArray& indices)
        CResult[shared_ptr[CTable]] Head(int64_t num_rows)
        CResult[int64_t] CountRows()
        CResult[CFragmentIterator] GetFragments()
        CResult[shared_ptr[CRecordBatchReader]] ToRecordBatchReader()
        const shared_ptr[CScanOptions]& options()

    cdef cppclass CScannerBuilder "arrow::dataset::ScannerBuilder":
        CScannerBuilder(shared_ptr[CDataset],
                        shared_ptr[CScanOptions] scan_options)
        CScannerBuilder(shared_ptr[CSchema], shared_ptr[CFragment],
                        shared_ptr[CScanOptions] scan_options)

        @staticmethod
        shared_ptr[CScannerBuilder] FromRecordBatchReader(
            shared_ptr[CRecordBatchReader] reader)
        CStatus ProjectColumns "Project"(const vector[c_string]& columns)
        CStatus Project(vector[CExpression]& exprs, vector[c_string]& columns)
        CStatus Filter(CExpression filter)
        CStatus UseThreads(c_bool use_threads)
        CStatus UseAsync(c_bool use_async)
        CStatus Pool(CMemoryPool* pool)
        CStatus BatchSize(int64_t batch_size)
        CStatus FragmentScanOptions(
            shared_ptr[CFragmentScanOptions] fragment_scan_options)
        CResult[shared_ptr[CScanner]] Finish()
        shared_ptr[CSchema] schema() const

    ctypedef vector[shared_ptr[CDataset]] CDatasetVector \
        "arrow::dataset::DatasetVector"

    cdef cppclass CDataset "arrow::dataset::Dataset":
        const shared_ptr[CSchema] & schema()
        CResult[CFragmentIterator] GetFragments()
        CResult[CFragmentIterator] GetFragments(CExpression predicate)
        const CExpression & partition_expression()
        c_string type_name()

        CResult[shared_ptr[CDataset]] ReplaceSchema(shared_ptr[CSchema])

        CResult[shared_ptr[CScannerBuilder]] NewScan()

    cdef cppclass CInMemoryDataset "arrow::dataset::InMemoryDataset"(
            CDataset):
        CInMemoryDataset(shared_ptr[CRecordBatchReader])
        CInMemoryDataset(shared_ptr[CTable])

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
        const CExpression& root_partition()
        CStatus SetRootPartition(CExpression partition)

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

    cdef cppclass CFileWriter \
            "arrow::dataset::FileWriter":
        const shared_ptr[CFileFormat]& format() const
        const shared_ptr[CSchema]& schema() const
        const shared_ptr[CFileWriteOptions]& options() const
        const CFileLocator& destination() const

    cdef cppclass CParquetFileWriter \
            "arrow::dataset::ParquetFileWriter"(CFileWriter):
        const shared_ptr[FileWriter]& parquet_writer() const

    cdef cppclass CFileFormat "arrow::dataset::FileFormat":
        shared_ptr[CFragmentScanOptions] default_fragment_scan_options
        c_string type_name() const
        CResult[shared_ptr[CSchema]] Inspect(const CFileSource&) const
        CResult[shared_ptr[CFileFragment]] MakeFragment(
            CFileSource source,
            CExpression partition_expression,
            shared_ptr[CSchema] physical_schema)
        shared_ptr[CFileWriteOptions] DefaultWriteOptions()

    cdef cppclass CFileFragment "arrow::dataset::FileFragment"(
            CFragment):
        const CFileSource& source() const
        const shared_ptr[CFileFormat]& format() const

    cdef cppclass CParquetFileWriteOptions \
            "arrow::dataset::ParquetFileWriteOptions"(CFileWriteOptions):
        shared_ptr[WriterProperties] writer_properties
        shared_ptr[ArrowWriterProperties] arrow_writer_properties

    cdef cppclass CParquetFileFragment "arrow::dataset::ParquetFileFragment"(
            CFileFragment):
        const vector[int]& row_groups() const
        shared_ptr[CFileMetaData] metadata() const
        CResult[vector[shared_ptr[CFragment]]] SplitByRowGroup(
            CExpression predicate)
        CResult[shared_ptr[CFragment]] SubsetWithFilter "Subset"(
            CExpression predicate)
        CResult[shared_ptr[CFragment]] SubsetWithIds "Subset"(
            vector[int] row_group_ids)
        CStatus EnsureCompleteMetadata()

    cdef cppclass CFileSystemDatasetWriteOptions \
            "arrow::dataset::FileSystemDatasetWriteOptions":
        shared_ptr[CFileWriteOptions] file_write_options
        shared_ptr[CFileSystem] filesystem
        c_string base_dir
        shared_ptr[CPartitioning] partitioning
        int max_partitions
        c_string basename_template
        function[cb_writer_finish_internal] writer_pre_finish
        function[cb_writer_finish_internal] writer_post_finish

    cdef cppclass CFileSystemDataset \
            "arrow::dataset::FileSystemDataset"(CDataset):
        @staticmethod
        CResult[shared_ptr[CDataset]] Make(
            shared_ptr[CSchema] schema,
            CExpression source_partition,
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
        const shared_ptr[CPartitioning]& partitioning() const

    cdef cppclass CParquetFileFormatReaderOptions \
            "arrow::dataset::ParquetFileFormat::ReaderOptions":
        unordered_set[c_string] dict_columns
        TimeUnit coerce_int96_timestamp_unit

    cdef cppclass CParquetFileFormat "arrow::dataset::ParquetFileFormat"(
            CFileFormat):
        CParquetFileFormatReaderOptions reader_options
        CResult[shared_ptr[CFileFragment]] MakeFragment(
            CFileSource source,
            CExpression partition_expression,
            shared_ptr[CSchema] physical_schema,
            vector[int] row_groups)

    cdef cppclass CParquetFragmentScanOptions \
            "arrow::dataset::ParquetFragmentScanOptions"(CFragmentScanOptions):
        shared_ptr[CReaderProperties] reader_properties
        shared_ptr[ArrowReaderProperties] arrow_reader_properties
        c_bool enable_parallel_column_conversion

    cdef cppclass CIpcFileWriteOptions \
            "arrow::dataset::IpcFileWriteOptions"(CFileWriteOptions):
        pass

    cdef cppclass CIpcFileFormat "arrow::dataset::IpcFileFormat"(
            CFileFormat):
        pass

    cdef cppclass COrcFileFormat "arrow::dataset::OrcFileFormat"(
            CFileFormat):
        pass

    cdef cppclass CCsvFileWriteOptions \
            "arrow::dataset::CsvFileWriteOptions"(CFileWriteOptions):
        shared_ptr[CCSVWriteOptions] write_options
        CMemoryPool* pool

    cdef cppclass CCsvFileFormat "arrow::dataset::CsvFileFormat"(
            CFileFormat):
        CCSVParseOptions parse_options

    cdef cppclass CCsvFragmentScanOptions \
            "arrow::dataset::CsvFragmentScanOptions"(CFragmentScanOptions):
        CCSVConvertOptions convert_options
        CCSVReadOptions read_options

    cdef cppclass CPartitioning "arrow::dataset::Partitioning":
        c_string type_name() const
        CResult[CExpression] Parse(const c_string & path) const
        const shared_ptr[CSchema] & schema()

    cdef cppclass CSegmentEncoding" arrow::dataset::SegmentEncoding":
        pass

    CSegmentEncoding CSegmentEncodingNone\
        " arrow::dataset::SegmentEncoding::None"
    CSegmentEncoding CSegmentEncodingUri\
        " arrow::dataset::SegmentEncoding::Uri"

    cdef cppclass CKeyValuePartitioningOptions \
            "arrow::dataset::KeyValuePartitioningOptions":
        CSegmentEncoding segment_encoding

    cdef cppclass CHivePartitioningOptions \
            "arrow::dataset::HivePartitioningOptions":
        CSegmentEncoding segment_encoding
        c_string null_fallback

    cdef cppclass CPartitioningFactoryOptions \
            "arrow::dataset::PartitioningFactoryOptions":
        c_bool infer_dictionary
        shared_ptr[CSchema] schema
        CSegmentEncoding segment_encoding

    cdef cppclass CHivePartitioningFactoryOptions \
            "arrow::dataset::HivePartitioningFactoryOptions":
        c_bool infer_dictionary
        c_string null_fallback
        shared_ptr[CSchema] schema
        CSegmentEncoding segment_encoding

    cdef cppclass CPartitioningFactory "arrow::dataset::PartitioningFactory":
        c_string type_name() const

    cdef cppclass CDirectoryPartitioning \
            "arrow::dataset::DirectoryPartitioning"(CPartitioning):
        CDirectoryPartitioning(shared_ptr[CSchema] schema,
                               vector[shared_ptr[CArray]] dictionaries)

        @staticmethod
        shared_ptr[CPartitioningFactory] MakeFactory(
            vector[c_string] field_names, CPartitioningFactoryOptions)

        vector[shared_ptr[CArray]] dictionaries() const

    cdef cppclass CHivePartitioning \
            "arrow::dataset::HivePartitioning"(CPartitioning):
        CHivePartitioning(shared_ptr[CSchema] schema,
                          vector[shared_ptr[CArray]] dictionaries,
                          CHivePartitioningOptions options)

        @staticmethod
        shared_ptr[CPartitioningFactory] MakeFactory(
            CHivePartitioningFactoryOptions)

        vector[shared_ptr[CArray]] dictionaries() const

    cdef cppclass CPartitioningOrFactory \
            "arrow::dataset::PartitioningOrFactory":
        CPartitioningOrFactory(shared_ptr[CPartitioning])
        CPartitioningOrFactory(shared_ptr[CPartitioningFactory])
        CPartitioningOrFactory & operator = (shared_ptr[CPartitioning])
        CPartitioningOrFactory & operator = (
            shared_ptr[CPartitioningFactory])
        shared_ptr[CPartitioning] partitioning() const
        shared_ptr[CPartitioningFactory] factory() const

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
        c_bool validate_column_chunk_paths

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
