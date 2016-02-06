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

#include "parquet/reader.h"

#include <cstdio>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "parquet/column/reader.h"
#include "parquet/column/serialized-page.h"
#include "parquet/column/scanner.h"

#include "parquet/exception.h"
#include "parquet/schema/converter.h"
#include "parquet/thrift/util.h"
#include "parquet/util/input_stream.h"

using std::string;
using std::vector;

namespace parquet_cpp {

// ----------------------------------------------------------------------
// LocalFile methods

LocalFile::~LocalFile() {
  CloseFile();
}

void LocalFile::Open(const std::string& path) {
  path_ = path;
  file_ = fopen(path_.c_str(), "r");
  is_open_ = true;
}

void LocalFile::Close() {
  // Pure virtual
  CloseFile();
}

void LocalFile::CloseFile() {
  if (is_open_) {
    fclose(file_);
    is_open_ = false;
  }
}

size_t LocalFile::Size() {
  fseek(file_, 0L, SEEK_END);
  return Tell();
}

void LocalFile::Seek(size_t pos) {
  fseek(file_, pos, SEEK_SET);
}

size_t LocalFile::Tell() {
  return ftell(file_);
}

size_t LocalFile::Read(size_t nbytes, uint8_t* buffer) {
  return fread(buffer, 1, nbytes, file_);
}

// ----------------------------------------------------------------------
// RowGroupReader

std::shared_ptr<ColumnReader> RowGroupReader::Column(size_t i) {
  // TODO: boundschecking
  auto it = column_readers_.find(i);
  if (it !=  column_readers_.end()) {
    // Already have constructed the ColumnReader
    return it->second;
  }

  const parquet::ColumnChunk& col = row_group_->columns[i];

  size_t col_start = col.meta_data.data_page_offset;
  if (col.meta_data.__isset.dictionary_page_offset &&
      col_start > col.meta_data.dictionary_page_offset) {
    col_start = col.meta_data.dictionary_page_offset;
  }

  std::unique_ptr<InputStream> input(
      new ScopedInMemoryInputStream(col.meta_data.total_compressed_size));

  FileLike* source = this->parent_->buffer_;

  source->Seek(col_start);

  // TODO(wesm): Law of demeter violation
  ScopedInMemoryInputStream* scoped_input =
    static_cast<ScopedInMemoryInputStream*>(input.get());
  size_t bytes_read = source->Read(scoped_input->size(), scoped_input->data());
  if (bytes_read != scoped_input->size()) {
    std::cout << "Bytes needed: " << col.meta_data.total_compressed_size << std::endl;
    std::cout << "Bytes read: " << bytes_read << std::endl;
    throw ParquetException("Unable to read column chunk data");
  }

  const ColumnDescriptor* descr = parent_->column_descr(i);

  std::unique_ptr<PageReader> pager(
      new SerializedPageReader(std::move(input), col.meta_data.codec));

  std::shared_ptr<ColumnReader> reader = ColumnReader::Make(descr,
      std::move(pager));
  column_readers_[i] = reader;

  return reader;
}

// ----------------------------------------------------------------------
// ParquetFileReader

// 4 byte constant + 4 byte metadata len
static constexpr uint32_t FOOTER_SIZE = 8;
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

ParquetFileReader::ParquetFileReader() :
    parsed_metadata_(false),
    buffer_(nullptr) {}

ParquetFileReader::~ParquetFileReader() {}

void ParquetFileReader::Open(FileLike* buffer) {
  buffer_ = buffer;
}

void ParquetFileReader::Close() {
  buffer_->Close();
}

RowGroupReader* ParquetFileReader::RowGroup(size_t i) {
  if (!parsed_metadata_) {
    ParseMetaData();
  }

  if (i >= num_row_groups()) {
    std::stringstream ss;
    ss << "The file only has " << num_row_groups()
       << "row groups, requested reader for: "
       << i;
    throw ParquetException(ss.str());
  }

  auto it = row_group_readers_.find(i);
  if (it != row_group_readers_.end()) {
    // Constructed the RowGroupReader already
    return it->second.get();
  }
  if (!parsed_metadata_) {
    ParseMetaData();
  }

  // Construct the RowGroupReader
  row_group_readers_[i] = std::make_shared<RowGroupReader>(this,
      &metadata_.row_groups[i]);
  return row_group_readers_[i].get();
}

void ParquetFileReader::ParseMetaData() {
  size_t filesize = buffer_->Size();

  if (filesize < FOOTER_SIZE) {
    throw ParquetException("Corrupted file, smaller than file footer");
  }

  uint8_t footer_buffer[FOOTER_SIZE];

  buffer_->Seek(filesize - FOOTER_SIZE);

  size_t bytes_read = buffer_->Read(FOOTER_SIZE, footer_buffer);

  if (bytes_read != FOOTER_SIZE) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }
  if (memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  size_t metadata_start = filesize - FOOTER_SIZE - metadata_len;
  if (metadata_start < 0) {
    throw ParquetException("Invalid parquet file. File is less than file metadata size.");
  }

  buffer_->Seek(metadata_start);

  std::vector<uint8_t> metadata_buffer(metadata_len);
  bytes_read = buffer_->Read(metadata_len, &metadata_buffer[0]);
  if (bytes_read != metadata_len) {
    throw ParquetException("Invalid parquet file. Could not read metadata bytes.");
  }
  DeserializeThriftMsg(&metadata_buffer[0], &metadata_len, &metadata_);

  schema::FlatSchemaConverter converter(&metadata_.schema[0],
      metadata_.schema.size());
  schema_descr_.Init(converter.Convert());

  parsed_metadata_ = true;
}

// ----------------------------------------------------------------------
// ParquetFileReader::DebugPrint

// the fixed initial size is just for an example
#define COL_WIDTH "20"


void ParquetFileReader::DebugPrint(std::ostream& stream, bool print_values) {
  if (!parsed_metadata_) {
    ParseMetaData();
  }

  stream << "File statistics:\n";
  stream << "Total rows: " << metadata_.num_rows << "\n";
  for (int i = 0; i < num_columns(); ++i) {
    const ColumnDescriptor* descr = column_descr(i);
    stream << "Column " << i << ": "
           << descr->name()
           << " ("
           << type_to_string(descr->physical_type())
           << ")" << std::endl;
  }

  for (int i = 0; i < num_row_groups(); ++i) {
    stream << "--- Row Group " << i << " ---\n";

    RowGroupReader* group_reader = RowGroup(i);

    // Print column metadata
    size_t num_columns = group_reader->num_columns();

    for (int i = 0; i < num_columns; ++i) {
      const parquet::ColumnMetaData* meta_data = group_reader->column_metadata(i);
      stream << "Column " << i << ": "
             << meta_data->num_values << " rows, "
             << meta_data->statistics.null_count << " null values, "
             << meta_data->statistics.distinct_count << " distinct values, "
             << "min value: " << (meta_data->statistics.min.length() > 0 ?
                 meta_data->statistics.min : "N/A")
             << ", max value: " << (meta_data->statistics.max.length() > 0 ?
                 meta_data->statistics.max : "N/A") << ".\n";
    }

    if (!print_values) {
      continue;
    }

    static constexpr size_t bufsize = 25;
    char buffer[bufsize];

    // Create readers for all columns and print contents
    vector<std::shared_ptr<Scanner> > scanners(num_columns, NULL);
    for (int i = 0; i < num_columns; ++i) {
      std::shared_ptr<ColumnReader> col_reader = group_reader->Column(i);
      Type::type col_type = col_reader->type();

      std::stringstream ss;
      ss << "%-" << COL_WIDTH << "s";
      std::string fmt = ss.str();

      snprintf(buffer, bufsize, fmt.c_str(), column_descr(i)->name().c_str());
      stream << buffer;

      // This is OK in this method as long as the RowGroupReader does not get
      // deleted
      scanners[i] = Scanner::Make(col_reader);
    }
    stream << "\n";

    bool hasRow;
    do {
      hasRow = false;
      for (int i = 0; i < num_columns; ++i) {
        if (scanners[i]->HasNext()) {
          hasRow = true;
          scanners[i]->PrintNext(stream, 17);
        }
      }
      stream << "\n";
    } while (hasRow);
  }
}

} // namespace parquet_cpp
