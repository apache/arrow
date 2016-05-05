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
#include "parquet/types.h"

using std::string;
using std::vector;

namespace parquet {

// ----------------------------------------------------------------------
// RowGroupReader public API

RowGroupReader::RowGroupReader(const SchemaDescriptor* schema,
    std::unique_ptr<Contents> contents, MemoryAllocator* allocator)
    : schema_(schema), contents_(std::move(contents)), allocator_(allocator) {}

int RowGroupReader::num_columns() const {
  return contents_->num_columns();
}

int64_t RowGroupReader::num_rows() const {
  return contents_->num_rows();
}

std::shared_ptr<ColumnReader> RowGroupReader::Column(int i) {
  // TODO: boundschecking
  const ColumnDescriptor* descr = schema_->Column(i);

  std::unique_ptr<PageReader> page_reader = contents_->GetColumnPageReader(i);
  return ColumnReader::Make(descr, std::move(page_reader), allocator_);
}

RowGroupStatistics RowGroupReader::GetColumnStats(int i) const {
  return contents_->GetColumnStats(i);
}

// ----------------------------------------------------------------------
// ParquetFileReader public API

ParquetFileReader::ParquetFileReader() : schema_(nullptr) {}
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
  schema_ = contents_->schema();
}

void ParquetFileReader::Close() {
  if (contents_) { contents_->Close(); }
}

int ParquetFileReader::num_row_groups() const {
  return contents_->num_row_groups();
}

int64_t ParquetFileReader::num_rows() const {
  return contents_->num_rows();
}

int ParquetFileReader::num_columns() const {
  return schema_->num_columns();
}

std::shared_ptr<RowGroupReader> ParquetFileReader::RowGroup(int i) {
  if (i >= num_row_groups()) {
    std::stringstream ss;
    ss << "The file only has " << num_row_groups()
       << "row groups, requested reader for: " << i;
    throw ParquetException(ss.str());
  }

  return contents_->GetRowGroup(i);
}

// ----------------------------------------------------------------------
// ParquetFileReader::DebugPrint

// the fixed initial size is just for an example
#define COL_WIDTH "20"

void ParquetFileReader::DebugPrint(
    std::ostream& stream, std::list<int> selected_columns, bool print_values) {
  stream << "File statistics:\n";
  stream << "Total rows: " << num_rows() << "\n";

  if (selected_columns.size() == 0) {
    for (int i = 0; i < num_columns(); i++) {
      selected_columns.push_back(i);
    }
  } else {
    for (auto i : selected_columns) {
      if (i < 0 || i >= num_columns()) {
        throw ParquetException("Selected column is out of range");
      }
    }
  }

  for (auto i : selected_columns) {
    const ColumnDescriptor* descr = schema_->Column(i);
    stream << "Column " << i << ": " << descr->name() << " ("
           << type_to_string(descr->physical_type()) << ")" << std::endl;
  }

  for (int r = 0; r < num_row_groups(); ++r) {
    stream << "--- Row Group " << r << " ---\n";

    auto group_reader = RowGroup(r);

    // Print column metadata
    for (auto i : selected_columns) {
      RowGroupStatistics stats = group_reader->GetColumnStats(i);

      const ColumnDescriptor* descr = schema_->Column(i);
      stream << "Column " << i << ": " << group_reader->num_rows() << " rows, "
             << stats.num_values << " values, " << stats.null_count << " null values, "
             << stats.distinct_count << " distinct values, "
             << FormatValue(
                    descr->physical_type(), stats.max->c_str(), descr->type_length())
             << " max, " << FormatValue(descr->physical_type(), stats.min->c_str(),
                                descr->type_length())
             << " min, " << std::endl;
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

      snprintf(buffer, bufsize, fmt.c_str(), column_schema(i)->name().c_str());
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
