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

#include <algorithm>
#include <exception>
#include <ostream>
#include <string.h>
#include <string>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/compression.h"
#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/thrift/util.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {

// ----------------------------------------------------------------------
// SerializedPageReader deserializes Thrift metadata and pages that have been
// assembled in a serialized stream for storing in a Parquet files

SerializedPageReader::SerializedPageReader(std::unique_ptr<InputStream> stream,
    int64_t total_num_rows, Compression::type codec_type, MemoryAllocator* allocator)
    : stream_(std::move(stream)),
      decompression_buffer_(AllocateBuffer(allocator, 0)),
      seen_num_rows_(0),
      total_num_rows_(total_num_rows) {
  max_page_header_size_ = DEFAULT_MAX_PAGE_HEADER_SIZE;
  decompressor_ = Codec::Create(codec_type);
}

std::shared_ptr<Page> SerializedPageReader::NextPage() {
  // Loop here because there may be unhandled page types that we skip until
  // finding a page that we do know what to do with
  while (seen_num_rows_ < total_num_rows_) {
    int64_t bytes_read = 0;
    int64_t bytes_available = 0;
    uint32_t header_size = 0;
    const uint8_t* buffer;
    uint32_t allowed_page_size = DEFAULT_PAGE_HEADER_SIZE;

    // Page headers can be very large because of page statistics
    // We try to deserialize a larger buffer progressively
    // until a maximum allowed header limit
    while (true) {
      buffer = stream_->Peek(allowed_page_size, &bytes_available);
      if (bytes_available == 0) { return std::shared_ptr<Page>(nullptr); }

      // This gets used, then set by DeserializeThriftMsg
      header_size = bytes_available;
      try {
        DeserializeThriftMsg(buffer, &header_size, &current_page_header_);
        break;
      } catch (std::exception& e) {
        // Failed to deserialize. Double the allowed page header size and try again
        std::stringstream ss;
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
    if (bytes_read != compressed_len) { ParquetException::EofException(); }

    // Uncompress it if we need to
    if (decompressor_ != NULL) {
      // Grow the uncompressed buffer if we need to.
      if (uncompressed_len > static_cast<int>(decompression_buffer_->size())) {
        PARQUET_THROW_NOT_OK(decompression_buffer_->Resize(uncompressed_len));
      }
      decompressor_->Decompress(compressed_len, buffer, uncompressed_len,
          decompression_buffer_->mutable_data());
      buffer = decompression_buffer_->data();
    }

    auto page_buffer = std::make_shared<Buffer>(buffer, uncompressed_len);

    if (current_page_header_.type == format::PageType::DICTIONARY_PAGE) {
      const format::DictionaryPageHeader& dict_header =
          current_page_header_.dictionary_page_header;

      bool is_sorted = dict_header.__isset.is_sorted ? dict_header.is_sorted : false;

      return std::make_shared<DictionaryPage>(page_buffer, dict_header.num_values,
          FromThrift(dict_header.encoding), is_sorted);
    } else if (current_page_header_.type == format::PageType::DATA_PAGE) {
      const format::DataPageHeader& header = current_page_header_.data_page_header;

      EncodedStatistics page_statistics;
      if (header.__isset.statistics) {
        const format::Statistics& stats = header.statistics;
        if (stats.__isset.max) { page_statistics.set_max(stats.max); }
        if (stats.__isset.min) { page_statistics.set_min(stats.min); }
        if (stats.__isset.null_count) {
          page_statistics.set_null_count(stats.null_count);
        }
        if (stats.__isset.distinct_count) {
          page_statistics.set_distinct_count(stats.distinct_count);
        }
      }

      seen_num_rows_ += header.num_values;

      return std::make_shared<DataPage>(page_buffer, header.num_values,
          FromThrift(header.encoding), FromThrift(header.definition_level_encoding),
          FromThrift(header.repetition_level_encoding), page_statistics);
    } else if (current_page_header_.type == format::PageType::DATA_PAGE_V2) {
      const format::DataPageHeaderV2& header = current_page_header_.data_page_header_v2;
      bool is_compressed = header.__isset.is_compressed ? header.is_compressed : false;

      seen_num_rows_ += header.num_values;

      return std::make_shared<DataPageV2>(page_buffer, header.num_values,
          header.num_nulls, header.num_rows, FromThrift(header.encoding),
          header.definition_levels_byte_length, header.repetition_levels_byte_length,
          is_compressed);
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return std::shared_ptr<Page>(nullptr);
}

SerializedRowGroup::SerializedRowGroup(RandomAccessSource* source,
    FileMetaData* file_metadata, int row_group_number, const ReaderProperties& props)
    : source_(source), file_metadata_(file_metadata), properties_(props) {
  row_group_metadata_ = file_metadata->RowGroup(row_group_number);
}
const RowGroupMetaData* SerializedRowGroup::metadata() const {
  return row_group_metadata_.get();
}

const ReaderProperties* SerializedRowGroup::properties() const {
  return &properties_;
}

// For PARQUET-816
static constexpr int64_t kMaxDictHeaderSize = 100;

std::unique_ptr<PageReader> SerializedRowGroup::GetColumnPageReader(int i) {
  // Read column chunk from the file
  auto col = row_group_metadata_->ColumnChunk(i);

  int64_t col_start = col->data_page_offset();
  if (col->has_dictionary_page() && col_start > col->dictionary_page_offset()) {
    col_start = col->dictionary_page_offset();
  }

  int64_t col_length = col->total_compressed_size();
  std::unique_ptr<InputStream> stream;

  // PARQUET-816 workaround for old files created by older parquet-mr
  const FileMetaData::Version& version = file_metadata_->writer_version();
  if (version.application == "parquet-mr" && version.VersionLt(1, 2, 9)) {
    // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    // dictionary page header size in total_compressed_size and total_uncompressed_size
    // (see IMPALA-694). We add padding to compensate.
    int64_t bytes_remaining = source_->Size() - (col_start + col_length);
    int64_t padding = std::min<int64_t>(kMaxDictHeaderSize, bytes_remaining);
    col_length += padding;
  }

  stream = properties_.GetStream(source_, col_start, col_length);

  return std::unique_ptr<PageReader>(new SerializedPageReader(std::move(stream),
      row_group_metadata_->num_rows(), col->compression(), properties_.allocator()));
}

// ----------------------------------------------------------------------
// SerializedFile: Parquet on-disk layout

static constexpr uint32_t FOOTER_SIZE = 8;
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

std::unique_ptr<ParquetFileReader::Contents> SerializedFile::Open(
    std::unique_ptr<RandomAccessSource> source, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata) {
  std::unique_ptr<ParquetFileReader::Contents> result(
      new SerializedFile(std::move(source), props));

  // Access private methods here, but otherwise unavailable
  SerializedFile* file = static_cast<SerializedFile*>(result.get());

  if (metadata == nullptr) {
    // Validates magic bytes, parses metadata, and initializes the SchemaDescriptor
    file->ParseMetaData();
  } else {
    file->file_metadata_ = metadata;
  }

  return result;
}

void SerializedFile::Close() {
  source_->Close();
}

SerializedFile::~SerializedFile() {
  Close();
}

std::shared_ptr<RowGroupReader> SerializedFile::GetRowGroup(int i) {
  std::unique_ptr<SerializedRowGroup> contents(
      new SerializedRowGroup(source_.get(), file_metadata_.get(), i, properties_));
  return std::make_shared<RowGroupReader>(std::move(contents));
}

std::shared_ptr<FileMetaData> SerializedFile::metadata() const {
  return file_metadata_;
}

SerializedFile::SerializedFile(std::unique_ptr<RandomAccessSource> source,
    const ReaderProperties& props = default_reader_properties())
    : source_(std::move(source)), properties_(props) {}

void SerializedFile::ParseMetaData() {
  int64_t file_size = source_->Size();

  if (file_size < FOOTER_SIZE) {
    throw ParquetException("Corrupted file, smaller than file footer");
  }

  uint8_t footer_buffer[FOOTER_SIZE];
  int64_t bytes_read =
      source_->ReadAt(file_size - FOOTER_SIZE, FOOTER_SIZE, footer_buffer);

  if (bytes_read != FOOTER_SIZE || memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  int64_t metadata_start = file_size - FOOTER_SIZE - metadata_len;
  if (FOOTER_SIZE + metadata_len > file_size) {
    throw ParquetException(
        "Invalid parquet file. File is less than "
        "file metadata size.");
  }

  std::shared_ptr<PoolBuffer> metadata_buffer =
      AllocateBuffer(properties_.allocator(), metadata_len);
  bytes_read =
      source_->ReadAt(metadata_start, metadata_len, metadata_buffer->mutable_data());
  if (bytes_read != metadata_len) {
    throw ParquetException("Invalid parquet file. Could not read metadata bytes.");
  }

  file_metadata_ = FileMetaData::Make(metadata_buffer->data(), &metadata_len);
}

}  // namespace parquet
