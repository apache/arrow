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

#include "parquet/file/reader-internal.h"

#include <memory>
#include <vector>

#include "parquet/column/serialized-page.h"
#include "parquet/schema/converter.h"
#include "parquet/thrift/util.h"
#include "parquet/util/input.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// SerializedRowGroup

int SerializedRowGroup::num_columns() const {
  return metadata_->columns.size();
}

std::unique_ptr<PageReader> SerializedRowGroup::GetColumnPageReader(int i) {
  // Read column chunk from the file
  const parquet::ColumnChunk& col = metadata_->columns[i];

  int64_t col_start = col.meta_data.data_page_offset;
  if (col.meta_data.__isset.dictionary_page_offset &&
      col_start > col.meta_data.dictionary_page_offset) {
    col_start = col.meta_data.dictionary_page_offset;
  }

  // TODO(wesm): some input streams (e.g. memory maps) may not require
  // copying data. This should be added to the input stream API to support
  // zero-copy streaming
  std::unique_ptr<InputStream> input(
      new ScopedInMemoryInputStream(col.meta_data.total_compressed_size));

  source_->Seek(col_start);
  ScopedInMemoryInputStream* scoped_input =
    static_cast<ScopedInMemoryInputStream*>(input.get());
  size_t bytes_read = source_->Read(scoped_input->size(), scoped_input->data());

  if (bytes_read != scoped_input->size()) {
    throw ParquetException("Unable to read column chunk data");
  }

  const ColumnDescriptor* descr = schema_->Column(i);

  return std::unique_ptr<PageReader>(new SerializedPageReader(std::move(input),
          col.meta_data.codec));
}

RowGroupStatistics SerializedRowGroup::GetColumnStats(int i) {
  const parquet::ColumnMetaData& meta_data = metadata_->columns[i].meta_data;

  RowGroupStatistics result;
  result.num_values = meta_data.num_values;
  result.null_count = meta_data.statistics.null_count;
  result.distinct_count = meta_data.statistics.distinct_count;

  return result;
}

// ----------------------------------------------------------------------
// SerializedFile: Parquet on-disk layout

static constexpr uint32_t FOOTER_SIZE = 8;
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

std::unique_ptr<ParquetFileReader::Contents> SerializedFile::Open(
    std::unique_ptr<RandomAccessSource> source) {
  std::unique_ptr<ParquetFileReader::Contents> result(
      new SerializedFile(std::move(source)));

  // Access private methods here, but otherwise unavailable
  SerializedFile* file = static_cast<SerializedFile*>(result.get());

  // Validates magic bytes, parses metadata, and initializes the SchemaDescriptor
  file->ParseMetaData();

  return result;
}

void SerializedFile::Close() {
  source_->Close();
}

std::shared_ptr<RowGroupReader> SerializedFile::GetRowGroup(int i) {
  std::unique_ptr<SerializedRowGroup> contents(new SerializedRowGroup(source_.get(),
          &schema_, &metadata_.row_groups[i]));

  return std::make_shared<RowGroupReader>(&schema_, std::move(contents));
}

int64_t SerializedFile::num_rows() const {
  return metadata_.num_rows;
}

int SerializedFile::num_columns() const {
  return schema_.num_columns();
}

int SerializedFile::num_row_groups() const {
  return metadata_.row_groups.size();
}

SerializedFile::SerializedFile(std::unique_ptr<RandomAccessSource> source) :
    source_(std::move(source)) {}


void SerializedFile::ParseMetaData() {
  size_t filesize = source_->Size();

  if (filesize < FOOTER_SIZE) {
    throw ParquetException("Corrupted file, smaller than file footer");
  }

  uint8_t footer_buffer[FOOTER_SIZE];
  source_->Seek(filesize - FOOTER_SIZE);
  size_t bytes_read = source_->Read(FOOTER_SIZE, footer_buffer);

  if (bytes_read != FOOTER_SIZE) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }
  if (memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  size_t metadata_start = filesize - FOOTER_SIZE - metadata_len;
  if (FOOTER_SIZE + metadata_len > filesize) {
    throw ParquetException("Invalid parquet file. File is less than "
        "file metadata size.");
  }

  source_->Seek(metadata_start);

  std::vector<uint8_t> metadata_buffer(metadata_len);
  bytes_read = source_->Read(metadata_len, &metadata_buffer[0]);
  if (bytes_read != metadata_len) {
    throw ParquetException("Invalid parquet file. Could not read metadata bytes.");
  }

  DeserializeThriftMsg(&metadata_buffer[0], &metadata_len, &metadata_);

  schema::FlatSchemaConverter converter(&metadata_.schema[0],
      metadata_.schema.size());
  schema_.Init(converter.Convert());
}

} // namespace parquet_cpp
