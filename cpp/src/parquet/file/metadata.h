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

#ifndef PARQUET_FILE_METADATA_H
#define PARQUET_FILE_METADATA_H

#include <set>
#include <string>
#include <vector>

#include "parquet/column/properties.h"
#include "parquet/compression/codec.h"
#include "parquet/schema/descriptor.h"
#include "parquet/types.h"
#include "parquet/util/output.h"
#include "parquet/util/visibility.h"

namespace parquet {

// ColumnStatistics does not own the min/max values
struct ColumnStatistics {
  int64_t null_count;
  int64_t distinct_count;
  const std::string* min;
  const std::string* max;
};

class PARQUET_EXPORT ColumnChunkMetaData {
 public:
  // API convenience to get a MetaData accessor
  static std::unique_ptr<ColumnChunkMetaData> Make(const uint8_t* metadata);

  ~ColumnChunkMetaData();

  // column chunk
  int64_t file_offset() const;
  // parameter is only used when a dataset is spread across multiple files
  const std::string& file_path() const;
  // column metadata
  Type::type type() const;
  int64_t num_values() const;
  std::shared_ptr<schema::ColumnPath> path_in_schema() const;
  bool is_stats_set() const;
  const ColumnStatistics& statistics() const;
  Compression::type compression() const;
  const std::vector<Encoding::type>& encodings() const;
  int64_t has_dictionary_page() const;
  int64_t dictionary_page_offset() const;
  int64_t data_page_offset() const;
  int64_t index_page_offset() const;
  int64_t total_compressed_size() const;
  int64_t total_uncompressed_size() const;

 private:
  explicit ColumnChunkMetaData(const uint8_t* metadata);
  // PIMPL Idiom
  class ColumnChunkMetaDataImpl;
  std::unique_ptr<ColumnChunkMetaDataImpl> impl_;
};

class PARQUET_EXPORT RowGroupMetaData {
 public:
  // API convenience to get a MetaData accessor
  static std::unique_ptr<RowGroupMetaData> Make(
      const uint8_t* metadata, const SchemaDescriptor* schema);

  ~RowGroupMetaData();

  // row-group metadata
  int num_columns() const;
  int64_t num_rows() const;
  int64_t total_byte_size() const;
  // Return const-pointer to make it clear that this object is not to be copied
  const SchemaDescriptor* schema() const;
  std::unique_ptr<ColumnChunkMetaData> ColumnChunk(int i) const;

 private:
  explicit RowGroupMetaData(const uint8_t* metadata, const SchemaDescriptor* schema);
  // PIMPL Idiom
  class RowGroupMetaDataImpl;
  std::unique_ptr<RowGroupMetaDataImpl> impl_;
};

class FileMetaDataBuilder;

class PARQUET_EXPORT FileMetaData {
 public:
  // API convenience to get a MetaData accessor
  static std::unique_ptr<FileMetaData> Make(
      const uint8_t* serialized_metadata, uint32_t* metadata_len);

  ~FileMetaData();

  // file metadata
  int num_columns() const;
  int64_t num_rows() const;
  int num_row_groups() const;
  int32_t version() const;
  const std::string& created_by() const;
  int num_schema_elements() const;
  std::unique_ptr<RowGroupMetaData> RowGroup(int i) const;

  void WriteTo(OutputStream* dst);

  // Return const-pointer to make it clear that this object is not to be copied
  const SchemaDescriptor* schema() const;

 private:
  friend FileMetaDataBuilder;
  explicit FileMetaData(const uint8_t* serialized_metadata, uint32_t* metadata_len);
  // PIMPL Idiom
  FileMetaData();
  class FileMetaDataImpl;
  std::unique_ptr<FileMetaDataImpl> impl_;
};

// Builder API
class PARQUET_EXPORT ColumnChunkMetaDataBuilder {
 public:
  // API convenience to get a MetaData reader
  static std::unique_ptr<ColumnChunkMetaDataBuilder> Make(
      const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column,
      uint8_t* contents);

  ~ColumnChunkMetaDataBuilder();

  // column chunk
  // Used when a dataset is spread across multiple files
  void set_file_path(const std::string& path);
  // column metadata
  // ownership of min/max is with ColumnChunkMetadata
  void SetStatistics(const ColumnStatistics& stats);
  // get the column descriptor
  const ColumnDescriptor* descr() const;
  // commit the metadata
  void Finish(int64_t num_values, int64_t dictonary_page_offset,
      int64_t index_page_offset, int64_t data_page_offset, int64_t compressed_size,
      int64_t uncompressed_size, bool has_dictionary, bool dictionary_fallback);

 private:
  explicit ColumnChunkMetaDataBuilder(const std::shared_ptr<WriterProperties>& props,
      const ColumnDescriptor* column, uint8_t* contents);
  // PIMPL Idiom
  class ColumnChunkMetaDataBuilderImpl;
  std::unique_ptr<ColumnChunkMetaDataBuilderImpl> impl_;
};

class PARQUET_EXPORT RowGroupMetaDataBuilder {
 public:
  // API convenience to get a MetaData reader
  static std::unique_ptr<RowGroupMetaDataBuilder> Make(int64_t num_rows,
      const std::shared_ptr<WriterProperties>& props, const SchemaDescriptor* schema_,
      uint8_t* contents);

  ~RowGroupMetaDataBuilder();

  ColumnChunkMetaDataBuilder* NextColumnChunk();
  int num_columns();

  // commit the metadata
  void Finish(int64_t total_bytes_written);

 private:
  explicit RowGroupMetaDataBuilder(int64_t num_rows,
      const std::shared_ptr<WriterProperties>& props, const SchemaDescriptor* schema_,
      uint8_t* contents);
  // PIMPL Idiom
  class RowGroupMetaDataBuilderImpl;
  std::unique_ptr<RowGroupMetaDataBuilderImpl> impl_;
};

class PARQUET_EXPORT FileMetaDataBuilder {
 public:
  // API convenience to get a MetaData reader
  static std::unique_ptr<FileMetaDataBuilder> Make(
      const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props);

  ~FileMetaDataBuilder();

  RowGroupMetaDataBuilder* AppendRowGroup(int64_t num_rows);

  // commit the metadata
  std::unique_ptr<FileMetaData> Finish();

 private:
  explicit FileMetaDataBuilder(
      const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props);
  // PIMPL Idiom
  class FileMetaDataBuilderImpl;
  std::unique_ptr<FileMetaDataBuilderImpl> impl_;
};

}  // namespace parquet

#endif  // PARQUET_FILE_METADATA_H
