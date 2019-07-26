// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PARQUET_ARROW_READER_H
#define PARQUET_ARROW_READER_H

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "parquet/platform.h"

#include "arrow/io/interfaces.h"
#include "arrow/util/macros.h"

namespace arrow {

class Array;
class ChunkedArray;
class MemoryPool;
class RecordBatchReader;
class Schema;
class Status;
class Table;

}  // namespace arrow

namespace parquet {

class FileMetaData;
class ParquetFileReader;
class ReaderProperties;

namespace arrow {

class ColumnChunkReader;
class ColumnReader;
class RowGroupReader;

static constexpr bool DEFAULT_USE_THREADS = false;

// Default number of rows to read when using ::arrow::RecordBatchReader
static constexpr int64_t DEFAULT_BATCH_SIZE = 64 * 1024;

/// EXPERIMENTAL: Properties for configuring FileReader behavior.
class PARQUET_EXPORT ArrowReaderProperties {
 public:
  explicit ArrowReaderProperties(bool use_threads = DEFAULT_USE_THREADS)
      : use_threads_(use_threads),
        read_dict_indices_(),
        batch_size_(DEFAULT_BATCH_SIZE) {}

  void set_use_threads(bool use_threads) { use_threads_ = use_threads; }

  bool use_threads() const { return use_threads_; }

  void set_read_dictionary(int column_index, bool read_dict) {
    if (read_dict) {
      read_dict_indices_.insert(column_index);
    } else {
      read_dict_indices_.erase(column_index);
    }
  }
  bool read_dictionary(int column_index) const {
    if (read_dict_indices_.find(column_index) != read_dict_indices_.end()) {
      return true;
    } else {
      return false;
    }
  }

  void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }

  int64_t batch_size() const { return batch_size_; }

 private:
  bool use_threads_;
  std::unordered_set<int> read_dict_indices_;
  int64_t batch_size_;
};

/// EXPERIMENTAL: Constructs the default ArrowReaderProperties
PARQUET_EXPORT
ArrowReaderProperties default_arrow_reader_properties();

// Arrow read adapter class for deserializing Parquet files as Arrow row
// batches.
//
// This interfaces caters for different use cases and thus provides different
// interfaces. In its most simplistic form, we cater for a user that wants to
// read the whole Parquet at once with the FileReader::ReadTable method.
//
// More advanced users that also want to implement parallelism on top of each
// single Parquet files should do this on the RowGroup level. For this, they can
// call FileReader::RowGroup(i)->ReadTable to receive only the specified
// RowGroup as a table.
//
// In the most advanced situation, where a consumer wants to independently read
// RowGroups in parallel and consume each column individually, they can call
// FileReader::RowGroup(i)->Column(j)->Read and receive an arrow::Column
// instance.
//
// TODO(wesm): nested data does not always make sense with this user
// interface unless you are only reading a single leaf node from a branch of
// a table. For example:
//
// repeated group data {
//   optional group record {
//     optional int32 val1;
//     optional byte_array val2;
//     optional bool val3;
//   }
//   optional int32 val4;
// }
//
// In the Parquet file, there are 3 leaf nodes:
//
// * data.record.val1
// * data.record.val2
// * data.record.val3
// * data.val4
//
// When materializing this data in an Arrow array, we would have:
//
// data: list<struct<
//   record: struct<
//    val1: int32,
//    val2: string (= list<uint8>),
//    val3: bool,
//   >,
//   val4: int32
// >>
//
// However, in the Parquet format, each leaf node has its own repetition and
// definition levels describing the structure of the intermediate nodes in
// this array structure. Thus, we will need to scan the leaf data for a group
// of leaf nodes part of the same type tree to create a single result Arrow
// nested array structure.
//
// This is additionally complicated "chunky" repeated fields or very large byte
// arrays
class PARQUET_EXPORT FileReader {
 public:
  static ::arrow::Status Make(::arrow::MemoryPool* pool,
                              std::unique_ptr<ParquetFileReader> reader,
                              const ArrowReaderProperties& properties,
                              std::unique_ptr<FileReader>* out);

  static ::arrow::Status Make(::arrow::MemoryPool* pool,
                              std::unique_ptr<ParquetFileReader> reader,
                              std::unique_ptr<FileReader>* out);

  // Since the distribution of columns amongst a Parquet file's row groups may
  // be uneven (the number of values in each column chunk can be different), we
  // provide a column-oriented read interface. The ColumnReader hides the
  // details of paging through the file's row groups and yielding
  // fully-materialized arrow::Array instances
  //
  // Returns error status if the column of interest is not flat.
  ::arrow::Status GetColumn(int i, std::unique_ptr<ColumnReader>* out);

  /// \brief Return arrow schema for all the columns.
  ::arrow::Status GetSchema(std::shared_ptr<::arrow::Schema>* out);

  /// \brief Return arrow schema by apply selection of column indices.
  /// \returns error status if passed wrong indices.
  ::arrow::Status GetSchema(const std::vector<int>& indices,
                            std::shared_ptr<::arrow::Schema>* out);

  // Read column as a whole into an Array.
  ::arrow::Status ReadColumn(int i, std::shared_ptr<::arrow::ChunkedArray>* out);

  /// \note Deprecated since 0.12
  ARROW_DEPRECATED("Use version with ChunkedArray output")
  ::arrow::Status ReadColumn(int i, std::shared_ptr<::arrow::Array>* out);

  // NOTE: Experimental API
  // Reads a specific top level schema field into an Array
  // The index i refers the index of the top level schema field, which may
  // be nested or flat - e.g.
  //
  // 0 foo.bar
  //   foo.bar.baz
  //   foo.qux
  // 1 foo2
  // 2 foo3
  //
  // i=0 will read the entire foo struct, i=1 the foo2 primitive column etc
  ::arrow::Status ReadSchemaField(int i, std::shared_ptr<::arrow::ChunkedArray>* out);

  /// \note Deprecated since 0.12
  ARROW_DEPRECATED("Use version with ChunkedArray output")
  ::arrow::Status ReadSchemaField(int i, std::shared_ptr<::arrow::Array>* out);

  /// \brief Return a RecordBatchReader of row groups selected from row_group_indices, the
  ///    ordering in row_group_indices matters.
  /// \returns error Status if row_group_indices contains invalid index
  ::arrow::Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                       std::shared_ptr<::arrow::RecordBatchReader>* out);

  /// \brief Return a RecordBatchReader of row groups selected from row_group_indices,
  ///     whose columns are selected by column_indices. The ordering in row_group_indices
  ///     and column_indices matter.
  /// \returns error Status if either row_group_indices or column_indices contains invalid
  ///    index
  ::arrow::Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                       const std::vector<int>& column_indices,
                                       std::shared_ptr<::arrow::RecordBatchReader>* out);

  // Read a table of columns into a Table
  ::arrow::Status ReadTable(std::shared_ptr<::arrow::Table>* out);

  // Read a table of columns into a Table. Read only the indicated column
  // indices (relative to the schema)
  ::arrow::Status ReadTable(const std::vector<int>& column_indices,
                            std::shared_ptr<::arrow::Table>* out);

  ::arrow::Status ReadRowGroup(int i, const std::vector<int>& column_indices,
                               std::shared_ptr<::arrow::Table>* out);

  ::arrow::Status ReadRowGroup(int i, std::shared_ptr<::arrow::Table>* out);

  ::arrow::Status ReadRowGroups(const std::vector<int>& row_groups,
                                const std::vector<int>& column_indices,
                                std::shared_ptr<::arrow::Table>* out);

  ::arrow::Status ReadRowGroups(const std::vector<int>& row_groups,
                                std::shared_ptr<::arrow::Table>* out);

  /// \brief Scan file contents with one thread, return number of rows
  ::arrow::Status ScanContents(std::vector<int> columns, const int32_t column_batch_size,
                               int64_t* num_rows);

  /// \brief Return a reader for the RowGroup, this object must not outlive the
  ///   FileReader.
  std::shared_ptr<RowGroupReader> RowGroup(int row_group_index);

  int num_row_groups() const;

  const ParquetFileReader* parquet_reader() const;

  /// Set the number of threads to use during reads of multiple columns. By
  /// default only 1 thread is used
  /// \deprecated Use set_use_threads instead.
  ARROW_DEPRECATED("Use set_use_threads instead")
  void set_num_threads(int num_threads);

  /// Set whether to use multiple threads during reads of multiple columns.
  /// By default only one thread is used.
  void set_use_threads(bool use_threads);

  virtual ~FileReader();

 private:
  FileReader(::arrow::MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader,
             const ArrowReaderProperties& properties);

  friend ColumnChunkReader;
  friend RowGroupReader;

  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

class PARQUET_EXPORT RowGroupReader {
 public:
  std::shared_ptr<ColumnChunkReader> Column(int column_index);

  ::arrow::Status ReadTable(const std::vector<int>& column_indices,
                            std::shared_ptr<::arrow::Table>* out);
  ::arrow::Status ReadTable(std::shared_ptr<::arrow::Table>* out);

  virtual ~RowGroupReader();

 private:
  friend FileReader;
  RowGroupReader(FileReader::Impl* reader, int row_group_index);

  FileReader::Impl* impl_;
  int row_group_index_;
};

class PARQUET_EXPORT ColumnChunkReader {
 public:
  ::arrow::Status Read(std::shared_ptr<::arrow::ChunkedArray>* out);

  /// \note Deprecated since 0.12
  ARROW_DEPRECATED("Use version with ChunkedArray output")
  ::arrow::Status Read(std::shared_ptr<::arrow::Array>* out);

  virtual ~ColumnChunkReader();

 private:
  friend RowGroupReader;
  ColumnChunkReader(FileReader::Impl* impl, int row_group_index, int column_index);

  FileReader::Impl* impl_;
  int column_index_;
  int row_group_index_;
};

// At this point, the column reader is a stream iterator. It only knows how to
// read the next batch of values for a particular column from the file until it
// runs out.
//
// We also do not expose any internal Parquet details, such as row groups. This
// might change in the future.
class PARQUET_EXPORT ColumnReader {
 public:
  class PARQUET_NO_EXPORT ColumnReaderImpl;
  virtual ~ColumnReader();

  // Scan the next array of the indicated size. The actual size of the
  // returned array may be less than the passed size depending how much data is
  // available in the file.
  //
  // When all the data in the file has been exhausted, the result is set to
  // nullptr.
  //
  // Returns Status::OK on a successful read, including if you have exhausted
  // the data available in the file.
  ::arrow::Status NextBatch(int64_t batch_size,
                            std::shared_ptr<::arrow::ChunkedArray>* out);

  /// \note Deprecated since 0.12
  ARROW_DEPRECATED("Use version with ChunkedArray output")
  ::arrow::Status NextBatch(int64_t batch_size, std::shared_ptr<::arrow::Array>* out);

 private:
  std::unique_ptr<ColumnReaderImpl> impl_;
  explicit ColumnReader(std::unique_ptr<ColumnReaderImpl> impl);

  friend class FileReader;
  friend class PrimitiveImpl;
  friend class StructImpl;
};

// Helper function to create a file reader from an implementation of an Arrow
// random access file
//
// metadata : separately-computed file metadata, can be nullptr
PARQUET_EXPORT
::arrow::Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                         ::arrow::MemoryPool* allocator,
                         const ReaderProperties& properties,
                         const std::shared_ptr<FileMetaData>& metadata,
                         std::unique_ptr<FileReader>* reader);

PARQUET_EXPORT
::arrow::Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                         ::arrow::MemoryPool* allocator,
                         std::unique_ptr<FileReader>* reader);

PARQUET_EXPORT
::arrow::Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                         ::arrow::MemoryPool* allocator,
                         const ArrowReaderProperties& properties,
                         std::unique_ptr<FileReader>* reader);

}  // namespace arrow
}  // namespace parquet

#endif  // PARQUET_ARROW_READER_H
