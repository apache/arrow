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
#include "parquet/file/metadata.h"
#include "parquet/file/writer.h"
#include "parquet/thrift/parquet_types.h"

namespace parquet {

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
class SerializedPageWriter : public PageWriter {
 public:
  SerializedPageWriter(OutputStream* sink, Compression::type codec,
      ColumnChunkMetaDataBuilder* metadata,
      MemoryAllocator* allocator = default_allocator());

  virtual ~SerializedPageWriter() {}

  int64_t WriteDataPage(const DataPage& page) override;

  int64_t WriteDictionaryPage(const DictionaryPage& page) override;

  void Close() override;

 private:
  OutputStream* sink_;
  ColumnChunkMetaDataBuilder* metadata_;
  int64_t num_values_;
  int64_t dictionary_page_offset_;
  int64_t data_page_offset_;
  int64_t total_uncompressed_size_;
  int64_t total_compressed_size_;

  // Compression codec to use.
  std::unique_ptr<Codec> compressor_;
  std::shared_ptr<OwnedMutableBuffer> compression_buffer_;

  /**
   * Compress a buffer.
   *
   * This method may return compression_buffer_ and thus the resulting memory
   * is only valid until the next call to Compress().
   */
  std::shared_ptr<Buffer> Compress(const std::shared_ptr<Buffer>& buffer);
};

// RowGroupWriter::Contents implementation for the Parquet file specification
class RowGroupSerializer : public RowGroupWriter::Contents {
 public:
  RowGroupSerializer(int64_t num_rows, OutputStream* sink,
      RowGroupMetaDataBuilder* metadata, const WriterProperties* properties)
      : num_rows_(num_rows),
        sink_(sink),
        metadata_(metadata),
        properties_(properties),
        total_bytes_written_(0),
        closed_(false) {}

  int num_columns() const override;
  int64_t num_rows() const override;

  // TODO: PARQUET-579
  // void WriteRowGroupStatitics() override;

  ColumnWriter* NextColumn() override;
  void Close() override;

 private:
  int64_t num_rows_;
  OutputStream* sink_;
  RowGroupMetaDataBuilder* metadata_;
  const WriterProperties* properties_;
  int64_t total_bytes_written_;
  bool closed_;

  std::shared_ptr<ColumnWriter> current_column_writer_;
};

// An implementation of ParquetFileWriter::Contents that deals with the Parquet
// file structure, Thrift serialization, and other internal matters

class FileSerializer : public ParquetFileWriter::Contents {
 public:
  static std::unique_ptr<ParquetFileWriter::Contents> Open(
      std::shared_ptr<OutputStream> sink,
      const std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties());

  void Close() override;

  RowGroupWriter* AppendRowGroup(int64_t num_rows) override;

  const std::shared_ptr<WriterProperties>& properties() const override;

  int num_columns() const override;
  int num_row_groups() const override;
  int64_t num_rows() const override;

  virtual ~FileSerializer();

 private:
  explicit FileSerializer(std::shared_ptr<OutputStream> sink,
      const std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties);

  std::shared_ptr<OutputStream> sink_;
  bool is_open_;
  const std::shared_ptr<WriterProperties> properties_;
  int num_row_groups_;
  int64_t num_rows_;
  std::unique_ptr<FileMetaDataBuilder> metadata_;
  std::unique_ptr<RowGroupWriter> row_group_writer_;

  void StartFile();
  void WriteMetaData();
};

}  // namespace parquet

#endif  // PARQUET_FILE_WRITER_INTERNAL_H
