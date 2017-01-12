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

#include <memory>

#include "parquet/api/reader.h"
#include "parquet/api/schema.h"

#include "arrow/io/interfaces.h"

namespace arrow {

class Array;
class MemoryPool;
class RowBatch;
class Status;
class Table;
}

namespace parquet {

namespace arrow {

class FlatColumnReader;

// Arrow read adapter class for deserializing Parquet files as Arrow row
// batches.
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
  FileReader(::arrow::MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader);

  // Since the distribution of columns amongst a Parquet file's row groups may
  // be uneven (the number of values in each column chunk can be different), we
  // provide a column-oriented read interface. The ColumnReader hides the
  // details of paging through the file's row groups and yielding
  // fully-materialized arrow::Array instances
  //
  // Returns error status if the column of interest is not flat.
  ::arrow::Status GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out);
  // Read column as a whole into an Array.
  ::arrow::Status ReadFlatColumn(int i, std::shared_ptr<::arrow::Array>* out);
  // Read a table of flat columns into a Table.
  ::arrow::Status ReadFlatTable(std::shared_ptr<::arrow::Table>* out);

  const ParquetFileReader* parquet_reader() const;

  virtual ~FileReader();

 private:
  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

// At this point, the column reader is a stream iterator. It only knows how to
// read the next batch of values for a particular column from the file until it
// runs out.
//
// We also do not expose any internal Parquet details, such as row groups. This
// might change in the future.
class PARQUET_EXPORT FlatColumnReader {
 public:
  virtual ~FlatColumnReader();

  // Scan the next array of the indicated size. The actual size of the
  // returned array may be less than the passed size depending how much data is
  // available in the file.
  //
  // When all the data in the file has been exhausted, the result is set to
  // nullptr.
  //
  // Returns Status::OK on a successful read, including if you have exhausted
  // the data available in the file.
  ::arrow::Status NextBatch(int batch_size, std::shared_ptr<::arrow::Array>* out);

 private:
  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
  explicit FlatColumnReader(std::unique_ptr<Impl> impl);

  friend class FileReader;
};

// Helper function to create a file reader from an implementation of an Arrow
// readable file
//
// metadata : separately-computed file metadata, can be nullptr
PARQUET_EXPORT
::arrow::Status OpenFile(const std::shared_ptr<::arrow::io::ReadableFileInterface>& file,
    ::arrow::MemoryPool* allocator, const ReaderProperties& properties,
    const std::shared_ptr<FileMetaData>& metadata, std::unique_ptr<FileReader>* reader);

PARQUET_EXPORT
::arrow::Status OpenFile(const std::shared_ptr<::arrow::io::ReadableFileInterface>& file,
    ::arrow::MemoryPool* allocator, std::unique_ptr<FileReader>* reader);

}  // namespace arrow
}  // namespace parquet

#endif  // PARQUET_ARROW_READER_H
