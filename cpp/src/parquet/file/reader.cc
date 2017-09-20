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

#include "parquet/file/reader.h"

#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/io/file.h"

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/exception.h"
#include "parquet/file/reader-internal.h"
#include "parquet/types.h"
#include "parquet/util/logging.h"
#include "parquet/util/memory.h"

using std::string;

namespace parquet {

// ----------------------------------------------------------------------
// RowGroupReader public API

RowGroupReader::RowGroupReader(std::unique_ptr<Contents> contents)
    : contents_(std::move(contents)) {}

std::shared_ptr<ColumnReader> RowGroupReader::Column(int i) {
  DCHECK(i < metadata()->num_columns()) << "The RowGroup only has "
                                        << metadata()->num_columns()
                                        << "columns, requested column: " << i;
  const ColumnDescriptor* descr = metadata()->schema()->Column(i);

  std::unique_ptr<PageReader> page_reader = contents_->GetColumnPageReader(i);
  return ColumnReader::Make(
      descr, std::move(page_reader),
      const_cast<ReaderProperties*>(contents_->properties())->memory_pool());
}

std::unique_ptr<PageReader> RowGroupReader::GetColumnPageReader(int i) {
  DCHECK(i < metadata()->num_columns()) << "The RowGroup only has "
                                        << metadata()->num_columns()
                                        << "columns, requested column: " << i;
  return contents_->GetColumnPageReader(i);
}

// Returns the rowgroup metadata
const RowGroupMetaData* RowGroupReader::metadata() const { return contents_->metadata(); }

// ----------------------------------------------------------------------
// ParquetFileReader public API

ParquetFileReader::ParquetFileReader() {}
ParquetFileReader::~ParquetFileReader() {
  try {
    Close();
  } catch (...) {
  }
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::Open(
    const std::shared_ptr<::arrow::io::ReadableFileInterface>& source,
    const ReaderProperties& props, const std::shared_ptr<FileMetaData>& metadata) {
  std::unique_ptr<RandomAccessSource> io_wrapper(new ArrowInputFile(source));
  return Open(std::move(io_wrapper), props, metadata);
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::Open(
    std::unique_ptr<RandomAccessSource> source, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata) {
  auto contents = SerializedFile::Open(std::move(source), props, metadata);
  std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
  result->Open(std::move(contents));
  return result;
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::OpenFile(
    const std::string& path, bool memory_map, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata) {
  std::shared_ptr<::arrow::io::ReadableFileInterface> source;
  if (memory_map) {
    std::shared_ptr<::arrow::io::ReadableFile> handle;
    PARQUET_THROW_NOT_OK(
        ::arrow::io::ReadableFile::Open(path, props.memory_pool(), &handle));
    source = handle;
  } else {
    std::shared_ptr<::arrow::io::MemoryMappedFile> handle;
    PARQUET_THROW_NOT_OK(
        ::arrow::io::MemoryMappedFile::Open(path, ::arrow::io::FileMode::READ, &handle));
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
  DCHECK(i < metadata()->num_row_groups()) << "The file only has "
                                           << metadata()->num_row_groups()
                                           << "row groups, requested reader for: " << i;
  return contents_->GetRowGroup(i);
}

// ----------------------------------------------------------------------
// File metadata helpers

std::shared_ptr<FileMetaData> ReadMetaData(
    const std::shared_ptr<::arrow::io::ReadableFileInterface>& source) {
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
        total_rows[col] +=
            ScanAllValues(column_batch_size, def_levels.data(), rep_levels.data(),
                          values.data(), &values_read, col_reader.get());
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
