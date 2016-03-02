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

#include <string.h>
#include <algorithm>
#include <exception>
#include <ostream>
#include <string>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/compression/codec.h"
#include "parquet/exception.h"
#include "parquet/schema/converter.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/thrift/util.h"
#include "parquet/types.h"
#include "parquet/util/buffer.h"
#include "parquet/util/input.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// SerializedPageReader deserializes Thrift metadata and pages that have been
// assembled in a serialized stream for storing in a Parquet files

SerializedPageReader::SerializedPageReader(std::unique_ptr<InputStream> stream,
    Compression::type codec_type) :
    stream_(std::move(stream)) {
  max_page_header_size_ = DEFAULT_MAX_PAGE_HEADER_SIZE;
  decompressor_ = Codec::Create(codec_type);
}

std::shared_ptr<Page> SerializedPageReader::NextPage() {
  // Loop here because there may be unhandled page types that we skip until
  // finding a page that we do know what to do with
  while (true) {
    int64_t bytes_read = 0;
    int64_t bytes_available = 0;
    uint32_t header_size = 0;
    const uint8_t* buffer;
    uint32_t allowed_page_size = DEFAULT_PAGE_HEADER_SIZE;
    std::stringstream ss;

    // Page headers can be very large because of page statistics
    // We try to deserialize a larger buffer progressively
    // until a maximum allowed header limit
    while (true) {
      buffer = stream_->Peek(allowed_page_size, &bytes_available);
      if (bytes_available == 0) {
        return std::shared_ptr<Page>(nullptr);
      }

      // This gets used, then set by DeserializeThriftMsg
      header_size = bytes_available;
      try {
        DeserializeThriftMsg(buffer, &header_size, &current_page_header_);
        break;
      } catch (std::exception& e) {
        // Failed to deserialize. Double the allowed page header size and try again
        ss << e.what();
        allowed_page_size *= 2;
        if (allowed_page_size > max_page_header_size_) {
          ss << "Deserializing page header failed.\n";
          throw ParquetException(ss.str());
        }
      }
    }
    // Advance the stream offset
    stream_->Advance(header_size);

    int compressed_len = current_page_header_.compressed_page_size;
    int uncompressed_len = current_page_header_.uncompressed_page_size;

    // Read the compressed data page.
    buffer = stream_->Read(compressed_len, &bytes_read);
    if (bytes_read != compressed_len) ParquetException::EofException();

    // Uncompress it if we need to
    if (decompressor_ != NULL) {
      // Grow the uncompressed buffer if we need to.
      if (uncompressed_len > static_cast<int>(decompression_buffer_.size())) {
        decompression_buffer_.resize(uncompressed_len);
      }
      decompressor_->Decompress(compressed_len, buffer, uncompressed_len,
          &decompression_buffer_[0]);
      buffer = &decompression_buffer_[0];
    }

    auto page_buffer = std::make_shared<Buffer>(buffer, uncompressed_len);

    if (current_page_header_.type == parquet::PageType::DICTIONARY_PAGE) {
      const parquet::DictionaryPageHeader& dict_header =
        current_page_header_.dictionary_page_header;

      bool is_sorted = dict_header.__isset.is_sorted? dict_header.is_sorted : false;

      return std::make_shared<DictionaryPage>(page_buffer,
          dict_header.num_values, FromThrift(dict_header.encoding),
          is_sorted);
    } else if (current_page_header_.type == parquet::PageType::DATA_PAGE) {
      const parquet::DataPageHeader& header = current_page_header_.data_page_header;

      auto page = std::make_shared<DataPage>(page_buffer,
          header.num_values,
          FromThrift(header.encoding),
          FromThrift(header.definition_level_encoding),
          FromThrift(header.repetition_level_encoding));

      if (header.__isset.statistics) {
        const parquet::Statistics stats = header.statistics;
        if (stats.__isset.max) {
          page->max_ = stats.max;
        }
        if (stats.__isset.min) {
          page->min_ = stats.min;
        }
      }
      return page;
    } else if (current_page_header_.type == parquet::PageType::DATA_PAGE_V2) {
      const parquet::DataPageHeaderV2& header = current_page_header_.data_page_header_v2;
      bool is_compressed = header.__isset.is_compressed? header.is_compressed : false;
      return std::make_shared<DataPageV2>(page_buffer,
          header.num_values, header.num_nulls, header.num_rows,
          FromThrift(header.encoding),
          header.definition_levels_byte_length,
          header.repetition_levels_byte_length, is_compressed);
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return std::shared_ptr<Page>(nullptr);
}

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

  int64_t bytes_to_read = col.meta_data.total_compressed_size;
  std::shared_ptr<Buffer> buffer = source_->ReadAt(col_start, bytes_to_read);

  if (buffer->size() < bytes_to_read) {
    throw ParquetException("Unable to read column chunk data");
  }

  std::unique_ptr<InputStream> stream(new InMemoryInputStream(buffer));
  return std::unique_ptr<PageReader>(new SerializedPageReader(std::move(stream),
          FromThrift(col.meta_data.codec)));
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

SerializedFile::~SerializedFile() {
  Close();
}

std::shared_ptr<RowGroupReader> SerializedFile::GetRowGroup(int i) {
  std::unique_ptr<SerializedRowGroup> contents(new SerializedRowGroup(source_.get(),
          &metadata_.row_groups[i]));

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
  int64_t filesize = source_->Size();

  if (filesize < FOOTER_SIZE) {
    throw ParquetException("Corrupted file, smaller than file footer");
  }

  uint8_t footer_buffer[FOOTER_SIZE];
  source_->Seek(filesize - FOOTER_SIZE);
  int64_t bytes_read = source_->Read(FOOTER_SIZE, footer_buffer);
  if (bytes_read != FOOTER_SIZE ||
      memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  int64_t metadata_start = filesize - FOOTER_SIZE - metadata_len;
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
