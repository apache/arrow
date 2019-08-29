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

#include "parquet/file_reader.h"

#include <algorithm>
#include <limits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "arrow/io/file.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/deprecated_io.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

#include <time.h>

namespace parquet {

// PARQUET-978: Minimize footer reads by reading 64 KB from the end of the file
static constexpr int64_t kDefaultFooterReadSize = 64 * 1024;
static constexpr uint32_t kFooterSize = 8;
static constexpr uint8_t kParquetMagic[4] = {'P', 'A', 'R', '1'};

static constexpr uint32_t kColumnIndexReadSize = 16*1024;
static constexpr uint32_t kOffsetIndexReadSize = 16*1024;

// For PARQUET-816
static constexpr int64_t kMaxDictHeaderSize = 100;

// ----------------------------------------------------------------------
// RowGroupReader public API

RowGroupReader::RowGroupReader(std::unique_ptr<Contents> contents)
    : contents_(std::move(contents)) {}

std::shared_ptr<ColumnReader> RowGroupReader::Column(int i) {
  DCHECK(i < metadata()->num_columns())
      << "The RowGroup only has " << metadata()->num_columns()
      << "columns, requested column: " << i;
  const ColumnDescriptor* descr = metadata()->schema()->Column(i);

  std::unique_ptr<PageReader> page_reader = contents_->GetColumnPageReader(i);
  return ColumnReader::Make(
      descr, std::move(page_reader),
      const_cast<ReaderProperties*>(contents_->properties())->memory_pool());
}

std::unique_ptr<PageReader> RowGroupReader::GetColumnPageReader(int i) {
  DCHECK(i < metadata()->num_columns())
      << "The RowGroup only has " << metadata()->num_columns()
      << "columns, requested column: " << i;
  return contents_->GetColumnPageReader(i);
}

// Returns the rowgroup metadata
const RowGroupMetaData* RowGroupReader::metadata() const { return contents_->metadata(); }

// RowGroupReader::Contents implementation for the Parquet file specification
class SerializedRowGroup : public RowGroupReader::Contents {
 public:
  SerializedRowGroup(const std::shared_ptr<ArrowInputFile>& source,
                     FileMetaData* file_metadata, int row_group_number,
                     const ReaderProperties& props)
      : source_(source), file_metadata_(file_metadata), properties_(props) {
    row_group_metadata_ = file_metadata->RowGroup(row_group_number);
  }

  const RowGroupMetaData* metadata() const override { return row_group_metadata_.get(); }

  const ReaderProperties* properties() const override { return &properties_; }

  void GoToPage(int64_t v, parquet::format::ColumnIndex col_index, parquet::format::OffsetIndex offset_index, uint64_t& page_offset,uint64_t& num_values,uint64_t& next_page_offset) const {
      std::vector<int>::size_type itemindex = 0;
      //std::vector<int64_t> min_vec = std::vector<std::basic_string<char>>(col_index.min_values.begin(), col_index.min_values.end());
      int64_t min_diff = std::numeric_limits<int64_t>::max();//std::lower_bound(min_vec.begin(),min_vec.end(),v);
      int64_t min_index = 0;
      for (;itemindex < col_index.min_values.size();itemindex++) {
           int64_t* page_min = (int64_t*)(void *)col_index.min_values[itemindex].c_str();

           if ( *page_min <= v && min_diff >= v - *page_min ) {
               min_index = itemindex;
               min_diff = abs(*page_min - v);
           }
      }
      page_offset = offset_index.page_locations[min_index].offset;
      next_page_offset=offset_index.page_locations[min_index+1].offset;
      num_values = offset_index.page_locations[min_index+1].first_row_index - offset_index.page_locations[min_index].first_row_index;
  }

  void GoToPagewoIndex(int64_t v) const {
  }

  bool HasPageIndex(ColumnChunkMetaData* col) {

    int64_t column_index_offset = col->column_index_offset();
    int64_t offset_index_offset = col->offset_index_offset();
    int64_t column_index_length = col->column_index_length();
    int64_t offset_index_length = col->offset_index_length();

    int64_t ci_start = std::numeric_limits<int64_t>::max();
    int64_t oi_start = std::numeric_limits<int64_t>::max();
    int64_t ci_end = -1;
    int64_t oi_end = -1;

    if (column_index_offset && column_index_length){
       ci_start = std::min(ci_start, column_index_offset);
       ci_end = std::max(ci_end, column_index_offset + column_index_length);
    }
    if (offset_index_offset && offset_index_length) {
       oi_start = std::min(oi_start, offset_index_offset);
       oi_end = std::max(oi_end, offset_index_offset + offset_index_length);
    }

    return oi_end != -1 && ci_end != -1; 

  }

  void DeserializeColumnIndex(const ColumnChunkMetaData& col_chunk, parquet::format::ColumnIndex* column_index, std::shared_ptr<ArrowInputFile>& source_, ReaderProperties& properties_) {
    int64_t ci_start = std::numeric_limits<int64_t>::max(); 
    int64_t ci_end = std::numeric_limits<int64_t>::max();
    string_view page_buffer;
    ci_start = std::min(ci_start,col_chunk.column_index_offset());
    ci_end = std::max(ci_end,col_chunk.column_index_offset() + col_chunk.column_index_length());
    int8_t buffer_offset = col_chunk.column_index_offset() - ci_start;
    uint32_t length = col_chunk.column_index_length();

    std::shared_ptr<ArrowInputStream> stream_ = properties_.GetStream(source_, ci_start, length);
    PARQUET_THROW_NOT_OK(stream_->Peek(kColumnIndexReadSize,&page_buffer));
    if (page_buffer.size() == 0) {
       return;
    }

    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(page_buffer.data()), &length, column_index);
  }

  void DeserializeOffsetIndex(const ColumnChunkMetaData& col_chunk, parquet::format::OffsetIndex* offset_index, std::shared_ptr<ArrowInputFile>& source_, ReaderProperties& properties_) {
    int64_t oi_start = std::numeric_limits<int64_t>::max(); 
    int64_t oi_end = std::numeric_limits<int64_t>::max();
    string_view page_buffer;
    oi_start = std::min(oi_start,col_chunk.offset_index_offset());
    oi_end = std::min(oi_end, col_chunk.offset_index_offset() + col_chunk.offset_index_length());
    int8_t buffer_offset = col_chunk.offset_index_offset() - oi_start;
    uint32_t length = col_chunk.offset_index_length();

    std::shared_ptr<ArrowInputStream> stream_ = properties_.GetStream(source_, oi_start, length);
    PARQUET_THROW_NOT_OK(stream_->Peek(kOffsetIndexReadSize, &page_buffer));
    if (page_buffer.size() == 0) {
       return;
    }

    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(page_buffer.data()), &length, offset_index);
  }

  std::unique_ptr<PageReader> GetColumnPageReader(int i) override {
    // Read column chunk from the file
    auto col = row_group_metadata_->ColumnChunk(i);

    int64_t col_start = col->data_page_offset();
    if (col->has_dictionary_page() && col->dictionary_page_offset() > 0 &&
        col_start > col->dictionary_page_offset()) {
      col_start = col->dictionary_page_offset();
    }

    int64_t col_length = col->total_compressed_size();

    bool has_page_index = HasPageIndex((reinterpret_cast<ColumnChunkMetaData*>(col.get())));
    uint64_t page_offset,num_values,next_page_offset;
    if (has_page_index) {
        parquet::format::ColumnIndex col_index;
        parquet::format::OffsetIndex offset_index;
        DeserializeColumnIndex(*reinterpret_cast<ColumnChunkMetaData*>(col.get()),&col_index, source_, properties_);
        DeserializeOffsetIndex(*reinterpret_cast<ColumnChunkMetaData*>(col.get()),&offset_index, source_, properties_);
        GoToPage(3075287,
                  col_index,offset_index,page_offset,num_values,next_page_offset);
    }

    GoToPagewoIndex(3075287);

    // PARQUET-816 workaround for old files created by older parquet-mr
    const ApplicationVersion& version = file_metadata_->writer_version();
    if (version.VersionLt(ApplicationVersion::PARQUET_816_FIXED_VERSION())) {
      // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
      // dictionary page header size in total_compressed_size and total_uncompressed_size
      // (see IMPALA-694). We add padding to compensate.
      int64_t size = -1;
      PARQUET_THROW_NOT_OK(source_->GetSize(&size));
      int64_t bytes_remaining = size - (col_start + col_length);
      int64_t padding = std::min<int64_t>(kMaxDictHeaderSize, bytes_remaining);
      col_length += padding;
    }

    std::shared_ptr<ArrowInputStream> stream =
        properties_.GetStream(source_, page_offset, next_page_offset-page_offset);
    return PageReader::Open(stream, num_values, col->compression(),
                            properties_.memory_pool());
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  FileMetaData* file_metadata_;
  std::unique_ptr<RowGroupMetaData> row_group_metadata_;
  ReaderProperties properties_;
};

// ----------------------------------------------------------------------
// SerializedFile: An implementation of ParquetFileReader::Contents that deals
// with the Parquet file structure, Thrift deserialization, and other internal
// matters

// This class takes ownership of the provided data source
class SerializedFile : public ParquetFileReader::Contents {
 public:
  SerializedFile(const std::shared_ptr<ArrowInputFile>& source,
                 const ReaderProperties& props = default_reader_properties())
      : source_(source), properties_(props) {}

  void Close() override {}

  std::shared_ptr<RowGroupReader> GetRowGroup(int i) override {
    std::unique_ptr<SerializedRowGroup> contents(
        new SerializedRowGroup(source_, file_metadata_.get(), i, properties_));
    return std::make_shared<RowGroupReader>(std::move(contents));
  }

  std::shared_ptr<FileMetaData> metadata() const override { return file_metadata_; }

  void set_metadata(const std::shared_ptr<FileMetaData>& metadata) {
    file_metadata_ = metadata;
  }

  void ParseMetaData() {
    int64_t file_size = -1;
    PARQUET_THROW_NOT_OK(source_->GetSize(&file_size));

    if (file_size == 0) {
      throw ParquetException("Invalid Parquet file size is 0 bytes");
    } else if (file_size < kFooterSize) {
      std::stringstream ss;
      ss << "Invalid Parquet file size is " << file_size
         << " bytes, smaller than standard file footer (" << kFooterSize << " bytes)";
      throw ParquetException(ss.str());
    }

    std::shared_ptr<Buffer> footer_buffer;
    int64_t footer_read_size = std::min(file_size, kDefaultFooterReadSize);
    PARQUET_THROW_NOT_OK(
        source_->ReadAt(file_size - footer_read_size, footer_read_size, &footer_buffer));

    // Check if all bytes are read. Check if last 4 bytes read have the magic bits
    if (footer_buffer->size() != footer_read_size ||
        memcmp(footer_buffer->data() + footer_read_size - 4, kParquetMagic, 4) != 0) {
      throw ParquetException("Invalid parquet file. Corrupt footer.");
    }

    uint32_t metadata_len = arrow::util::SafeLoadAs<uint32_t>(
        reinterpret_cast<const uint8_t*>(footer_buffer->data()) + footer_read_size -
        kFooterSize);
    int64_t metadata_start = file_size - kFooterSize - metadata_len;
    if (kFooterSize + metadata_len > file_size) {
      throw ParquetException(
          "Invalid parquet file. File is less than "
          "file metadata size.");
    }

    std::shared_ptr<Buffer> metadata_buffer;
    // Check if the footer_buffer contains the entire metadata
    if (footer_read_size >= (metadata_len + kFooterSize)) {
      metadata_buffer = SliceBuffer(
          footer_buffer, footer_read_size - metadata_len - kFooterSize, metadata_len);
    } else {
      PARQUET_THROW_NOT_OK(
          source_->ReadAt(metadata_start, metadata_len, &metadata_buffer));
      if (metadata_buffer->size() != metadata_len) {
        throw ParquetException("Invalid parquet file. Could not read metadata bytes.");
      }
    }
    file_metadata_ = FileMetaData::Make(metadata_buffer->data(), &metadata_len);
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  std::shared_ptr<FileMetaData> file_metadata_;
  ReaderProperties properties_;
};

// ----------------------------------------------------------------------
// ParquetFileReader public API

ParquetFileReader::ParquetFileReader() {}

ParquetFileReader::~ParquetFileReader() {
  try {
    Close();
  } catch (...) {
  }
}

// Open the file. If no metadata is passed, it is parsed from the footer of
// the file
std::unique_ptr<ParquetFileReader::Contents> ParquetFileReader::Contents::Open(
    const std::shared_ptr<ArrowInputFile>& source, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata) {
  std::unique_ptr<ParquetFileReader::Contents> result(new SerializedFile(source, props));

  // Access private methods here, but otherwise unavailable
  SerializedFile* file = static_cast<SerializedFile*>(result.get());

  if (metadata == nullptr) {
    // Validates magic bytes, parses metadata, and initializes the SchemaDescriptor
    file->ParseMetaData();
  } else {
    file->set_metadata(metadata);
  }

  return result;
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::Open(
    const std::shared_ptr<::arrow::io::RandomAccessFile>& source,
    const ReaderProperties& props, const std::shared_ptr<FileMetaData>& metadata) {
  auto contents = SerializedFile::Open(source, props, metadata);
  std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
  result->Open(std::move(contents));
  return result;
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::Open(
    std::unique_ptr<RandomAccessSource> source, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata) {
  auto wrapper = std::make_shared<ParquetInputWrapper>(std::move(source));
  return Open(wrapper, props, metadata);
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::OpenFile(
    const std::string& path, bool memory_map, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata) {
  std::shared_ptr<::arrow::io::RandomAccessFile> source;
  if (memory_map) {
    std::shared_ptr<::arrow::io::MemoryMappedFile> handle;
    PARQUET_THROW_NOT_OK(
        ::arrow::io::MemoryMappedFile::Open(path, ::arrow::io::FileMode::READ, &handle));
    source = handle;
  } else {
    std::shared_ptr<::arrow::io::ReadableFile> handle;
    PARQUET_THROW_NOT_OK(
        ::arrow::io::ReadableFile::Open(path, props.memory_pool(), &handle));
    source = handle;
  }

  return Open(source, props, metadata);
}

void ParquetFileReader::Open(std::unique_ptr<ParquetFileReader::Contents> contents) {
  contents_ = std::move(contents);
}

void ParquetFileReader::Close() {
  if (contents_) {
    contents_->Close();
  }
}

std::shared_ptr<FileMetaData> ParquetFileReader::metadata() const {
  return contents_->metadata();
}

std::shared_ptr<RowGroupReader> ParquetFileReader::RowGroup(int i) {
  DCHECK(i < metadata()->num_row_groups())
      << "The file only has " << metadata()->num_row_groups()
      << "row groups, requested reader for: " << i;
  return contents_->GetRowGroup(i);
}

// ----------------------------------------------------------------------
// File metadata helpers

std::shared_ptr<FileMetaData> ReadMetaData(
    const std::shared_ptr<::arrow::io::RandomAccessFile>& source) {
  return ParquetFileReader::Open(source)->metadata();
}

// ----------------------------------------------------------------------
// File scanner for performance testing

int64_t ScanFileContents(std::vector<int> columns, const int32_t column_batch_size,
                         ParquetFileReader* reader) {
  std::vector<int16_t> rep_levels(column_batch_size);
  std::vector<int16_t> def_levels(column_batch_size);

  int num_columns = static_cast<int>(columns.size());

  // columns are not specified explicitly. Add all columns
  if (columns.size() == 0) {
    num_columns = reader->metadata()->num_columns();
    columns.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
      columns[i] = i;
    }
  }

  std::vector<int64_t> total_rows(num_columns, 0);

  for (int r = 0; r < reader->metadata()->num_row_groups(); ++r) {
    auto group_reader = reader->RowGroup(r);
    int col = 0;
    for (auto i : columns) {
      std::shared_ptr<ColumnReader> col_reader = group_reader->Column(i);
      size_t value_byte_size = GetTypeByteSize(col_reader->descr()->physical_type());
      std::vector<uint8_t> values(column_batch_size * value_byte_size);

      int64_t values_read = 0;
      while (col_reader->HasNext()) {
        int64_t levels_read =
            ScanAllValues(column_batch_size, def_levels.data(), rep_levels.data(),
                          values.data(), &values_read, col_reader.get());
        if (col_reader->descr()->max_repetition_level() > 0) {
          for (int64_t i = 0; i < levels_read; i++) {
            if (rep_levels[i] == 0) {
              total_rows[col]++;
            }
          }
        } else {
          total_rows[col] += levels_read;
        }
      }
      col++;
    }
  }

  for (int i = 1; i < num_columns; ++i) {
    if (total_rows[0] != total_rows[i]) {
      throw ParquetException("Parquet error: Total rows among columns do not match");
    }
  }

  return total_rows[0];
}

}  // namespace parquet
