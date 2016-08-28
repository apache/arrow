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

SerializedPageWriter::SerializedPageWriter(OutputStream* sink, Compression::type codec,
    format::ColumnChunk* metadata, MemoryAllocator* allocator)
    : sink_(sink),
      metadata_(metadata),
      // allocator_(allocator),
      compression_buffer_(std::make_shared<OwnedMutableBuffer>(0, allocator)) {
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

std::shared_ptr<Buffer> SerializedPageWriter::Compress(
    const std::shared_ptr<Buffer>& buffer) {
  // Fast path, no compressor available.
  if (!compressor_) return buffer;

  // Compress the data
  int64_t max_compressed_size =
      compressor_->MaxCompressedLen(buffer->size(), buffer->data());
  compression_buffer_->Resize(max_compressed_size);
  int64_t compressed_size = compressor_->Compress(buffer->size(), buffer->data(),
      max_compressed_size, compression_buffer_->mutable_data());
  compression_buffer_->Resize(compressed_size);
  return compression_buffer_;
}

int64_t SerializedPageWriter::WriteDataPage(const DataPage& page) {
  int64_t uncompressed_size = page.size();
  std::shared_ptr<Buffer> compressed_data = Compress(page.buffer());

  format::DataPageHeader data_page_header;
  data_page_header.__set_num_values(page.num_values());
  data_page_header.__set_encoding(ToThrift(page.encoding()));
  data_page_header.__set_definition_level_encoding(
      ToThrift(page.definition_level_encoding()));
  data_page_header.__set_repetition_level_encoding(
      ToThrift(page.repetition_level_encoding()));
  // TODO(PARQUET-593) statistics

  format::PageHeader page_header;
  page_header.__set_type(format::PageType::DATA_PAGE);
  page_header.__set_uncompressed_page_size(uncompressed_size);
  page_header.__set_compressed_page_size(compressed_data->size());
  page_header.__set_data_page_header(data_page_header);
  // TODO(PARQUET-594) crc checksum

  int64_t start_pos = sink_->Tell();
  SerializeThriftMsg(&page_header, sizeof(format::PageHeader), sink_);
  int64_t header_size = sink_->Tell() - start_pos;
  sink_->Write(compressed_data->data(), compressed_data->size());

  metadata_->meta_data.total_uncompressed_size += uncompressed_size + header_size;
  metadata_->meta_data.total_compressed_size += compressed_data->size() + header_size;
  metadata_->meta_data.num_values += page.num_values();

  return sink_->Tell() - start_pos;
}

int64_t SerializedPageWriter::WriteDictionaryPage(const DictionaryPage& page) {
  int64_t uncompressed_size = page.size();
  std::shared_ptr<Buffer> compressed_data = Compress(page.buffer());

  format::DictionaryPageHeader dict_page_header;
  dict_page_header.__set_num_values(page.num_values());
  dict_page_header.__set_encoding(ToThrift(page.encoding()));
  dict_page_header.__set_is_sorted(page.is_sorted());

  format::PageHeader page_header;
  page_header.__set_type(format::PageType::DICTIONARY_PAGE);
  page_header.__set_uncompressed_page_size(uncompressed_size);
  page_header.__set_compressed_page_size(compressed_data->size());
  page_header.__set_dictionary_page_header(dict_page_header);
  // TODO(PARQUET-594) crc checksum

  int64_t start_pos = sink_->Tell();
  SerializeThriftMsg(&page_header, sizeof(format::PageHeader), sink_);
  int64_t header_size = sink_->Tell() - start_pos;
  sink_->Write(compressed_data->data(), compressed_data->size());

  metadata_->meta_data.total_uncompressed_size += uncompressed_size + header_size;
  metadata_->meta_data.total_compressed_size += compressed_data->size() + header_size;

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

const SchemaDescriptor* RowGroupSerializer::schema() const {
  return schema_;
}

ColumnWriter* RowGroupSerializer::NextColumn() {
  if (current_column_index_ == schema_->num_columns() - 1) {
    throw ParquetException("All columns have already been written.");
  }
  current_column_index_++;

  if (current_column_writer_) { total_bytes_written_ += current_column_writer_->Close(); }

  const ColumnDescriptor* column_descr = schema_->Column(current_column_index_);
  format::ColumnChunk* col_meta = &metadata_->columns[current_column_index_];
  col_meta->__isset.meta_data = true;
  col_meta->meta_data.__set_type(ToThrift(column_descr->physical_type()));
  col_meta->meta_data.__set_path_in_schema(column_descr->path()->ToDotVector());
  std::unique_ptr<PageWriter> pager(new SerializedPageWriter(
      sink_, properties_->compression(column_descr->path()), col_meta, allocator_));
  current_column_writer_ =
      ColumnWriter::Make(column_descr, std::move(pager), num_rows_, properties_);
  return current_column_writer_.get();
}

void RowGroupSerializer::Close() {
  if (!closed_) {
    closed_ = true;
    if (current_column_index_ != schema_->num_columns() - 1) {
      throw ParquetException("Not all column were written in the current rowgroup.");
    }

    if (current_column_writer_) {
      total_bytes_written_ += current_column_writer_->Close();
      current_column_writer_.reset();
    }

    metadata_->__set_total_byte_size(total_bytes_written_);
  }
}

// ----------------------------------------------------------------------
// FileSerializer

std::unique_ptr<ParquetFileWriter::Contents> FileSerializer::Open(
    std::shared_ptr<OutputStream> sink, const std::shared_ptr<GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties) {
  std::unique_ptr<ParquetFileWriter::Contents> result(
      new FileSerializer(sink, schema, properties));

  return result;
}

void FileSerializer::Close() {
  if (is_open_) {
    if (row_group_writer_) { row_group_writer_->Close(); }
    row_group_writer_.reset();

    // Write magic bytes and metadata
    WriteMetaData();

    sink_->Close();
    is_open_ = false;
  }
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

const std::shared_ptr<WriterProperties>& FileSerializer::properties() const {
  return properties_;
}

RowGroupWriter* FileSerializer::AppendRowGroup(int64_t num_rows) {
  if (row_group_writer_) { row_group_writer_->Close(); }
  num_rows_ += num_rows;
  num_row_groups_++;

  auto rgm_size = row_group_metadata_.size();
  row_group_metadata_.resize(rgm_size + 1);
  format::RowGroup* rg_metadata = &row_group_metadata_.data()[rgm_size];
  std::unique_ptr<RowGroupWriter::Contents> contents(new RowGroupSerializer(
      num_rows, &schema_, sink_.get(), rg_metadata, properties_.get()));
  row_group_writer_.reset(new RowGroupWriter(std::move(contents), allocator_));
  return row_group_writer_.get();
}

FileSerializer::~FileSerializer() {
  Close();
}

void FileSerializer::WriteMetaData() {
  // Write MetaData
  uint32_t metadata_len = sink_->Tell();

  SchemaFlattener flattener(
      static_cast<GroupNode*>(schema_.schema().get()), &metadata_.schema);
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

FileSerializer::FileSerializer(std::shared_ptr<OutputStream> sink,
    const std::shared_ptr<GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties)
    : sink_(sink),
      allocator_(properties->allocator()),
      num_row_groups_(0),
      num_rows_(0),
      is_open_(true),
      properties_(properties) {
  schema_.Init(schema);
  StartFile();
}

void FileSerializer::StartFile() {
  // Parquet files always start with PAR1
  sink_->Write(PARQUET_MAGIC, 4);
}

}  // namespace parquet
