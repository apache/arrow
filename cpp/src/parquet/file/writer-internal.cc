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

#include "parquet/file/writer-internal.h"

#include "parquet/column/writer.h"
#include "parquet/schema/converter.h"
#include "parquet/thrift/util.h"
#include "parquet/util/output.h"

using parquet::schema::GroupNode;
using parquet::schema::SchemaFlattener;

namespace parquet {

// FIXME: copied from reader-internal.cc
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

// ----------------------------------------------------------------------
// SerializedPageWriter

SerializedPageWriter::SerializedPageWriter(OutputStream* sink,
        Compression::type codec, format::ColumnChunk* metadata,
        MemoryAllocator* allocator) : sink_(sink), metadata_(metadata),
        allocator_(allocator) {
  compressor_ = Codec::Create(codec);
  // Currently we directly start with the data page
  metadata_->meta_data.__set_data_page_offset(sink_->Tell());
  metadata_->meta_data.__set_codec(ToThrift(codec));
}

void SerializedPageWriter::Close() {}

void SerializedPageWriter::AddEncoding(Encoding::type encoding) {
  auto it = std::find(metadata_->meta_data.encodings.begin(),
      metadata_->meta_data.encodings.end(), ToThrift(encoding));
  if (it != metadata_->meta_data.encodings.end()) {
    metadata_->meta_data.encodings.push_back(ToThrift(encoding));
  }
}

int64_t SerializedPageWriter::WriteDataPage(int32_t num_rows, int32_t num_values,
    const std::shared_ptr<Buffer>& definition_levels,
    Encoding::type definition_level_encoding,
    const std::shared_ptr<Buffer>& repetition_levels,
    Encoding::type repetition_level_encoding,
    const std::shared_ptr<Buffer>& values, Encoding::type encoding) {
  int64_t uncompressed_size = definition_levels->size() + repetition_levels->size()
    + values->size();

  // Concatenate data into a single buffer
  // TODO: In the uncompressed case, directly write this to the sink
  // TODO: Reuse the (un)compressed_data buffer instead of recreating it each time.
  std::shared_ptr<OwnedMutableBuffer> uncompressed_data =
    std::make_shared<OwnedMutableBuffer>(uncompressed_size, allocator_);
  uint8_t* uncompressed_ptr = uncompressed_data->mutable_data();
  memcpy(uncompressed_ptr, repetition_levels->data(), repetition_levels->size());
  uncompressed_ptr += repetition_levels->size();
  memcpy(uncompressed_ptr, definition_levels->data(), definition_levels->size());
  uncompressed_ptr += definition_levels->size();
  memcpy(uncompressed_ptr, values->data(), values->size());

  // Compress the data
  int64_t compressed_size = uncompressed_size;
  std::shared_ptr<OwnedMutableBuffer> compressed_data = uncompressed_data;
  if (compressor_) {
    // TODO(PARQUET-592): Add support for compression
    // int64_t max_compressed_size = compressor_->MaxCompressedLen(
    // uncompressed_data.size(), uncompressed_data.data());
    // OwnedMutableBuffer compressed_data(compressor_->MaxCompressedLen(
    // uncompressed_data.size(), uncompressed_data.data()));
  }
  // Compressed data is not needed anymore, so immediately get rid of it.
  uncompressed_data.reset();

  format::DataPageHeader data_page_header;
  data_page_header.__set_num_values(num_rows);
  data_page_header.__set_encoding(ToThrift(encoding));
  data_page_header.__set_definition_level_encoding(ToThrift(definition_level_encoding));
  data_page_header.__set_repetition_level_encoding(ToThrift(repetition_level_encoding));
  // TODO(PARQUET-593) statistics

  format::PageHeader page_header;
  page_header.__set_type(format::PageType::DATA_PAGE);
  page_header.__set_uncompressed_page_size(uncompressed_size);
  page_header.__set_compressed_page_size(compressed_size);
  page_header.__set_data_page_header(data_page_header);
  // TODO(PARQUET-594) crc checksum

  int64_t start_pos = sink_->Tell();
  SerializeThriftMsg(&page_header, sizeof(format::PageHeader), sink_);
  int64_t header_size = sink_->Tell() - start_pos;
  sink_->Write(compressed_data->data(), compressed_data->size());

  metadata_->meta_data.total_uncompressed_size += uncompressed_size + header_size;
  metadata_->meta_data.total_compressed_size += compressed_size + header_size;
  metadata_->meta_data.num_values += num_values;

  return sink_->Tell() - start_pos;
}

// ----------------------------------------------------------------------
// RowGroupSerializer

int RowGroupSerializer::num_columns() const {
  return schema_->num_columns();
}

int64_t RowGroupSerializer::num_rows() const {
  return num_rows_;
}

const SchemaDescriptor* RowGroupSerializer::schema() const  {
  return schema_;
}

ColumnWriter* RowGroupSerializer::NextColumn() {
  if (current_column_index_ == schema_->num_columns() - 1) {
    throw ParquetException("All columns have already been written.");
  }
  current_column_index_++;

  if (current_column_writer_) {
    total_bytes_written_ += current_column_writer_->Close();
  }

  const ColumnDescriptor* column_descr = schema_->Column(current_column_index_);
  format::ColumnChunk* col_meta = &metadata_->columns[current_column_index_];
  col_meta->__isset.meta_data = true;
  col_meta->meta_data.__set_type(ToThrift(column_descr->physical_type()));
  col_meta->meta_data.__set_path_in_schema(column_descr->path()->ToDotVector());
  std::unique_ptr<PageWriter> pager(new SerializedPageWriter(sink_,
        Compression::UNCOMPRESSED, col_meta,
        allocator_));
  current_column_writer_ = ColumnWriter::Make(column_descr,
      std::move(pager), num_rows_, allocator_);
  return current_column_writer_.get();
}

void RowGroupSerializer::Close() {
  if (current_column_index_ != schema_->num_columns() - 1) {
    throw ParquetException("Not all column were written in the current rowgroup.");
  }

  if (current_column_writer_) {
    total_bytes_written_ += current_column_writer_->Close();
    current_column_writer_.reset();
  }

  metadata_->__set_total_byte_size(total_bytes_written_);
}

// ----------------------------------------------------------------------
// FileSerializer

std::unique_ptr<ParquetFileWriter::Contents> FileSerializer::Open(
    std::shared_ptr<OutputStream> sink, std::shared_ptr<GroupNode>& schema,
    MemoryAllocator* allocator) {
  std::unique_ptr<ParquetFileWriter::Contents> result(
      new FileSerializer(sink, schema, allocator));

  return result;
}

void FileSerializer::Close() {
  if (row_group_writer_) {
    row_group_writer_->Close();
  }
  row_group_writer_.reset();

  // Write magic bytes and metadata
  WriteMetaData();

  sink_->Close();
}

int FileSerializer::num_columns() const {
  return schema_.num_columns();
}

int FileSerializer::num_row_groups() const {
  return num_row_groups_;
}

int64_t FileSerializer::num_rows() const {
  return num_rows_;
}

RowGroupWriter* FileSerializer::AppendRowGroup(int64_t num_rows) {
  if (row_group_writer_) {
    row_group_writer_->Close();
  }
  num_rows_ += num_rows;
  num_row_groups_++;

  auto rgm_size = row_group_metadata_.size();
  row_group_metadata_.resize(rgm_size + 1);
  format::RowGroup* rg_metadata = &row_group_metadata_.data()[rgm_size];
  std::unique_ptr<RowGroupWriter::Contents> contents(
      new RowGroupSerializer(num_rows, &schema_, sink_.get(), rg_metadata, allocator_));
  row_group_writer_.reset(new RowGroupWriter(std::move(contents), allocator_));
  return row_group_writer_.get();
}

FileSerializer::~FileSerializer() {
  Close();
}

void FileSerializer::WriteMetaData() {
  // Write MetaData
  uint32_t metadata_len = sink_->Tell();

  SchemaFlattener flattener(static_cast<GroupNode*>(schema_.schema().get()),
      &metadata_.schema);
  flattener.Flatten();

  // TODO: Currently we only write version 1 files
  metadata_.__set_version(1);
  metadata_.__set_num_rows(num_rows_);
  metadata_.__set_row_groups(row_group_metadata_);
  // TODO(PARQUET-595) Support key_value_metadata
  // TODO(PARQUET-590) Get from WriterProperties
  metadata_.__set_created_by("parquet-cpp");

  SerializeThriftMsg(&metadata_, 1024, sink_.get());
  metadata_len = sink_->Tell() - metadata_len;

  // Write Footer
  sink_->Write(reinterpret_cast<uint8_t*>(&metadata_len), 4);
  sink_->Write(PARQUET_MAGIC, 4);
}

FileSerializer::FileSerializer(
    std::shared_ptr<OutputStream> sink,
    std::shared_ptr<GroupNode>& schema,
    MemoryAllocator* allocator = default_allocator()) :
        sink_(sink), allocator_(allocator),
        num_row_groups_(0), num_rows_(0) {
  schema_.Init(schema);
  StartFile();
}

void FileSerializer::StartFile() {
  // Parquet files always start with PAR1
  sink_->Write(PARQUET_MAGIC, 4);
}

} // namespace parquet
