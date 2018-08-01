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

#include "parquet/printer.h"

#include <string>
#include <vector>

#include "parquet/column_scanner.h"

using std::string;
using std::vector;

namespace parquet {
// ----------------------------------------------------------------------
// ParquetFilePrinter::DebugPrint

// the fixed initial size is just for an example
#define COL_WIDTH "30"

void ParquetFilePrinter::DebugPrint(std::ostream& stream, std::list<int> selected_columns,
                                    bool print_values, const char* filename) {
  const FileMetaData* file_metadata = fileReader->metadata().get();

  stream << "File Name: " << filename << "\n";
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

    auto group_reader = fileReader->RowGroup(r);
    std::unique_ptr<RowGroupMetaData> group_metadata = file_metadata->RowGroup(r);

    stream << "--- Total Bytes " << group_metadata->total_byte_size() << " ---\n";
    stream << "  Rows: " << group_metadata->num_rows() << "---\n";

    // Print column metadata
    for (auto i : selected_columns) {
      auto column_chunk = group_metadata->ColumnChunk(i);
      std::shared_ptr<RowGroupStatistics> stats = column_chunk->statistics();

      const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
      stream << "Column " << i << std::endl << ", Values: " << column_chunk->num_values();
      if (column_chunk->is_stats_set()) {
        std::string min = stats->EncodeMin(), max = stats->EncodeMax();
        stream << ", Null Values: " << stats->null_count()
               << ", Distinct Values: " << stats->distinct_count() << std::endl
               << "  Max: " << FormatStatValue(descr->physical_type(), max)
               << ", Min: " << FormatStatValue(descr->physical_type(), min);
      } else {
        stream << "  Statistics Not Set";
      }
      stream << std::endl
             << "  Compression: " << CompressionToString(column_chunk->compression())
             << ", Encodings: ";
      for (auto encoding : column_chunk->encodings()) {
        stream << EncodingToString(encoding) << " ";
      }
      stream << std::endl
             << "  Uncompressed Size: " << column_chunk->total_uncompressed_size()
             << ", Compressed Size: " << column_chunk->total_compressed_size()
             << std::endl;
    }

    if (!print_values) {
      continue;
    }

    static constexpr int bufsize = 25;
    char buffer[bufsize];

    // Create readers for selected columns and print contents
    vector<std::shared_ptr<Scanner>> scanners(selected_columns.size(), nullptr);
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
          scanner->PrintNext(stream, 27);
        }
      }
      stream << "\n";
    } while (hasRow);
  }
}

void ParquetFilePrinter::JSONPrint(std::ostream& stream, std::list<int> selected_columns,
                                   const char* filename) {
  const FileMetaData* file_metadata = fileReader->metadata().get();
  stream << "{\n";
  stream << "  \"FileName\": \"" << filename << "\",\n";
  stream << "  \"Version\": \"" << file_metadata->version() << "\",\n";
  stream << "  \"CreatedBy\": \"" << file_metadata->created_by() << "\",\n";
  stream << "  \"TotalRows\": \"" << file_metadata->num_rows() << "\",\n";
  stream << "  \"NumberOfRowGroups\": \"" << file_metadata->num_row_groups() << "\",\n";
  stream << "  \"NumberOfRealColumns\": \""
         << file_metadata->schema()->group_node()->field_count() << "\",\n";
  stream << "  \"NumberOfColumns\": \"" << file_metadata->num_columns() << "\",\n";

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

  stream << "  \"Columns\": [\n";
  int c = 0;
  for (auto i : selected_columns) {
    const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
    stream << "     { \"Id\": \"" << i << "\", \"Name\": \"" << descr->name() << "\","
           << " \"PhysicalType\": \"" << TypeToString(descr->physical_type()) << "\","
           << " \"LogicalType\": \"" << LogicalTypeToString(descr->logical_type())
           << "\" }";
    c++;
    if (c != static_cast<int>(selected_columns.size())) {
      stream << ",\n";
    }
  }

  stream << "\n  ],\n  \"RowGroups\": [\n";
  for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
    stream << "     {\n       \"Id\": \"" << r << "\", ";

    auto group_reader = fileReader->RowGroup(r);
    std::unique_ptr<RowGroupMetaData> group_metadata = file_metadata->RowGroup(r);

    stream << " \"TotalBytes\": \"" << group_metadata->total_byte_size() << "\", ";
    stream << " \"Rows\": \"" << group_metadata->num_rows() << "\",\n";

    // Print column metadata
    stream << "       \"ColumnChunks\": [\n";
    int c1 = 0;
    for (auto i : selected_columns) {
      auto column_chunk = group_metadata->ColumnChunk(i);
      std::shared_ptr<RowGroupStatistics> stats = column_chunk->statistics();

      const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
      stream << "          {\"Id\": \"" << i << "\", \"Values\": \""
             << column_chunk->num_values() << "\", "
             << "\"StatsSet\": ";
      if (column_chunk->is_stats_set()) {
        stream << "\"True\", \"Stats\": {";
        std::string min = stats->EncodeMin(), max = stats->EncodeMax();
        stream << "\"NumNulls\": \"" << stats->null_count() << "\", "
               << "\"DistinctValues\": \"" << stats->distinct_count() << "\", "
               << "\"Max\": \"" << FormatStatValue(descr->physical_type(), max) << "\", "
               << "\"Min\": \"" << FormatStatValue(descr->physical_type(), min)
               << "\" },";
      } else {
        stream << "\"False\",";
      }
      stream << "\n           \"Compression\": \""
             << CompressionToString(column_chunk->compression())
             << "\", \"Encodings\": \"";
      for (auto encoding : column_chunk->encodings()) {
        stream << EncodingToString(encoding) << " ";
      }
      stream << "\", "
             << "\"UncompressedSize\": \"" << column_chunk->total_uncompressed_size()
             << "\", \"CompressedSize\": \"" << column_chunk->total_compressed_size();

      // end of a ColumnChunk
      stream << "\" }";
      c1++;
      if (c1 != static_cast<int>(selected_columns.size())) {
        stream << ",\n";
      }
    }

    stream << "\n        ]\n     }";
    if ((r + 1) != static_cast<int>(file_metadata->num_row_groups())) {
      stream << ",\n";
    }
  }
  stream << "\n  ]\n}\n";
}

}  // namespace parquet
