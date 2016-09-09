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
#include <vector>

#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
#include "parquet/exception.h"
#include "parquet/file/reader-internal.h"
#include "parquet/util/input.h"
#include "parquet/util/logging.h"
#include "parquet/types.h"

using std::string;
using std::vector;

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
  return ColumnReader::Make(descr, std::move(page_reader),
      const_cast<ReaderProperties*>(contents_->properties())->allocator());
}

// Returns the rowgroup metadata
const RowGroupMetaData* RowGroupReader::metadata() const {
  return contents_->metadata();
}

// ----------------------------------------------------------------------
// ParquetFileReader public API

ParquetFileReader::ParquetFileReader() {}
ParquetFileReader::~ParquetFileReader() {
  Close();
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::Open(
    std::unique_ptr<RandomAccessSource> source, ReaderProperties props) {
  auto contents = SerializedFile::Open(std::move(source), props);

  std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
  result->Open(std::move(contents));

  return result;
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::OpenFile(
    const std::string& path, bool memory_map, ReaderProperties props) {
  std::unique_ptr<LocalFileSource> file;
  if (memory_map) {
    file.reset(new MemoryMapSource(props.allocator()));
  } else {
    file.reset(new LocalFileSource(props.allocator()));
  }
  file->Open(path);

  return Open(std::move(file), props);
}

void ParquetFileReader::Open(std::unique_ptr<ParquetFileReader::Contents> contents) {
  contents_ = std::move(contents);
}

void ParquetFileReader::Close() {
  if (contents_) { contents_->Close(); }
}

const FileMetaData* ParquetFileReader::metadata() const {
  return contents_->metadata();
}

std::shared_ptr<RowGroupReader> ParquetFileReader::RowGroup(int i) {
  DCHECK(i < metadata()->num_row_groups()) << "The file only has "
                                           << metadata()->num_row_groups()
                                           << "row groups, requested reader for: " << i;
  return contents_->GetRowGroup(i);
}

// ----------------------------------------------------------------------
// ParquetFileReader::DebugPrint

// the fixed initial size is just for an example
#define COL_WIDTH "20"

void ParquetFileReader::DebugPrint(
    std::ostream& stream, std::list<int> selected_columns, bool print_values) {
  const FileMetaData* file_metadata = metadata();

  stream << "File statistics:\n";
  stream << "Version: " << file_metadata->version() << "\n";
  stream << "Created By: " << file_metadata->created_by() << "\n";
  stream << "Total rows: " << file_metadata->num_rows() << "\n";
  stream << "Number of RowGroups: " << file_metadata->num_row_groups() << "\n";
  stream << "Number of Real Columns: "
         << file_metadata->schema()->group_node()->field_count() << "\n";

  if (selected_columns.size() == 0) {
    for (int i = 0; i < file_metadata->num_columns(); i++) {
      selected_columns.push_back(i);
    }
  } else {
    for (auto i : selected_columns) {
      if (i < 0 || i >= file_metadata->num_columns()) {
        throw ParquetException("Selected column is out of range");
      }
    }
  }

  stream << "Number of Columns: " << file_metadata->num_columns() << "\n";
  stream << "Number of Selected Columns: " << selected_columns.size() << "\n";
  for (auto i : selected_columns) {
    const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
    stream << "Column " << i << ": " << descr->name() << " ("
           << TypeToString(descr->physical_type()) << ")" << std::endl;
  }

  for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
    stream << "--- Row Group " << r << " ---\n";

    auto group_reader = RowGroup(r);
    std::unique_ptr<RowGroupMetaData> group_metadata = file_metadata->RowGroup(r);

    stream << "--- Total Bytes " << group_metadata->total_byte_size() << " ---\n";
    stream << "  rows: " << group_metadata->num_rows() << "---\n";

    // Print column metadata
    for (auto i : selected_columns) {
      auto column_chunk = group_metadata->ColumnChunk(i);
      const ColumnStatistics stats = column_chunk->statistics();

      const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
      stream << "Column " << i << std::endl << ", values: " << column_chunk->num_values();
      if (column_chunk->is_stats_set()) {
        stream << ", null values: " << stats.null_count
               << ", distinct values: " << stats.distinct_count << std::endl
               << "  max: " << FormatStatValue(descr->physical_type(), stats.max->c_str())
               << ", min: "
               << FormatStatValue(descr->physical_type(), stats.min->c_str());
      } else {
        stream << "  Statistics Not Set";
      }
      stream << std::endl
             << "  compression: " << CompressionToString(column_chunk->compression())
             << ", encodings: ";
      for (auto encoding : column_chunk->encodings()) {
        stream << EncodingToString(encoding) << " ";
      }
      stream << std::endl
             << "  uncompressed size: " << column_chunk->total_uncompressed_size()
             << ", compressed size: " << column_chunk->total_compressed_size()
             << std::endl;
    }

    if (!print_values) { continue; }

    static constexpr int bufsize = 25;
    char buffer[bufsize];

    // Create readers for selected columns and print contents
    vector<std::shared_ptr<Scanner>> scanners(selected_columns.size(), NULL);
    int j = 0;
    for (auto i : selected_columns) {
      std::shared_ptr<ColumnReader> col_reader = group_reader->Column(i);

      std::stringstream ss;
      ss << "%-" << COL_WIDTH << "s";
      std::string fmt = ss.str();

      snprintf(buffer, bufsize, fmt.c_str(),
          file_metadata->schema()->Column(i)->name().c_str());
      stream << buffer;

      // This is OK in this method as long as the RowGroupReader does not get
      // deleted
      scanners[j++] = Scanner::Make(col_reader);
    }
    stream << "\n";

    bool hasRow;
    do {
      hasRow = false;
      for (auto scanner : scanners) {
        if (scanner->HasNext()) {
          hasRow = true;
          scanner->PrintNext(stream, 17);
        }
      }
      stream << "\n";
    } while (hasRow);
  }
}

}  // namespace parquet
