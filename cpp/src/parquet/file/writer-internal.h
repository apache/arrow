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

#ifndef PARQUET_FILE_WRITER_INTERNAL_H
#define PARQUET_FILE_WRITER_INTERNAL_H

#include <memory>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/compression/codec.h"
#include "parquet/file/writer.h"
#include "parquet/thrift/parquet_types.h"

namespace parquet {

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
//
// TODO: Currently only writes DataPage pages.
class SerializedPageWriter : public PageWriter {
 public:
  SerializedPageWriter(OutputStream* sink, Compression::type codec,
      format::ColumnChunk* metadata, MemoryAllocator* allocator = default_allocator());

  virtual ~SerializedPageWriter() {}

  // TODO Refactor that this just takes a DataPage instance.
  // For this we need to be clear how to handle num_rows and num_values
  int64_t WriteDataPage(int32_t num_rows, int32_t num_values,
      const std::shared_ptr<Buffer>& definition_levels,
      Encoding::type definition_level_encoding,
      const std::shared_ptr<Buffer>& repetition_levels,
      Encoding::type repetition_level_encoding, const std::shared_ptr<Buffer>& values,
      Encoding::type encoding) override;

  void Close() override;

 private:
  OutputStream* sink_;
  format::ColumnChunk* metadata_;
  MemoryAllocator* allocator_;

  // Compression codec to use.
  std::unique_ptr<Codec> compressor_;
  OwnedMutableBuffer compression_buffer_;

  void AddEncoding(Encoding::type encoding);
};

// RowGroupWriter::Contents implementation for the Parquet file specification
class RowGroupSerializer : public RowGroupWriter::Contents {
 public:
  RowGroupSerializer(int64_t num_rows, const SchemaDescriptor* schema, OutputStream* sink,
      format::RowGroup* metadata, MemoryAllocator* allocator)
      : num_rows_(num_rows),
        schema_(schema),
        sink_(sink),
        metadata_(metadata),
        allocator_(allocator),
        total_bytes_written_(0),
        current_column_index_(-1) {
    metadata_->__set_num_rows(num_rows_);
    metadata_->columns.resize(schema->num_columns());
  }

  int num_columns() const override;
  int64_t num_rows() const override;
  const SchemaDescriptor* schema() const override;

  // TODO: PARQUET-579
  // void WriteRowGroupStatitics() override;

  ColumnWriter* NextColumn() override;
  void Close() override;

 private:
  int64_t num_rows_;
  const SchemaDescriptor* schema_;
  OutputStream* sink_;
  format::RowGroup* metadata_;
  MemoryAllocator* allocator_;
  int64_t total_bytes_written_;

  int64_t current_column_index_;
  std::shared_ptr<ColumnWriter> current_column_writer_;
};

// An implementation of ParquetFileWriter::Contents that deals with the Parquet
// file structure, Thrift serialization, and other internal matters

class FileSerializer : public ParquetFileWriter::Contents {
 public:
  static std::unique_ptr<ParquetFileWriter::Contents> Open(
      std::shared_ptr<OutputStream> sink, std::shared_ptr<schema::GroupNode>& schema,
      MemoryAllocator* allocator = default_allocator());

  void Close() override;

  RowGroupWriter* AppendRowGroup(int64_t num_rows) override;

  int num_columns() const override;
  int num_row_groups() const override;
  int64_t num_rows() const override;

  virtual ~FileSerializer();

 private:
  explicit FileSerializer(std::shared_ptr<OutputStream> sink,
      std::shared_ptr<schema::GroupNode>& schema, MemoryAllocator* allocator);

  std::shared_ptr<OutputStream> sink_;
  format::FileMetaData metadata_;
  std::vector<format::RowGroup> row_group_metadata_;
  MemoryAllocator* allocator_;
  int num_row_groups_;
  int num_rows_;
  bool is_open_;
  std::unique_ptr<RowGroupWriter> row_group_writer_;

  void StartFile();
  void WriteMetaData();
};

}  // namespace parquet

#endif  // PARQUET_FILE_WRITER_INTERNAL_H
