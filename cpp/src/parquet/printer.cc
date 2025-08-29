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

#include <cstdint>
#include <cstdio>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"

#include "parquet/column_scanner.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace parquet {

class ColumnReader;

namespace {

void PrintPageEncodingStats(std::ostream& stream,
                            const std::vector<PageEncodingStats>& encoding_stats) {
  for (size_t i = 0; i < encoding_stats.size(); ++i) {
    const auto& encoding = encoding_stats.at(i);
    stream << EncodingToString(encoding.encoding);
    if (encoding.page_type == parquet::PageType::DICTIONARY_PAGE) {
      // Explicitly tell if this encoding comes from a dictionary page
      stream << "(DICT_PAGE)";
    }
    if (i + 1 != encoding_stats.size()) {
      stream << " ";
    }
  }
}

void PutChars(std::ostream& stream, char c, int n) {
  for (int i = 0; i < n; ++i) {
    stream.put(c);
  }
}

void PrintKeyValueMetadata(std::ostream& stream,
                           const KeyValueMetadata& key_value_metadata,
                           int indent_level = 0, int indent_width = 1) {
  const int64_t size_of_key_value_metadata = key_value_metadata.size();
  PutChars(stream, ' ', indent_level * indent_width);
  stream << "Key Value Metadata: " << size_of_key_value_metadata << " entries\n";
  for (int64_t i = 0; i < size_of_key_value_metadata; i++) {
    PutChars(stream, ' ', (indent_level + 1) * indent_width);
    stream << "Key nr " << i << " " << key_value_metadata.key(i) << ": "
           << key_value_metadata.value(i) << "\n";
  }
}

// the fixed initial size is just for an example
constexpr int kColWidth = 30;

}  // namespace

// ----------------------------------------------------------------------
// ParquetFilePrinter::DebugPrint

void ParquetFilePrinter::DebugPrint(std::ostream& stream, std::list<int> selected_columns,
                                    bool print_values, bool format_dump,
                                    bool print_key_value_metadata, const char* filename) {
  const FileMetaData* file_metadata = fileReader->metadata().get();

  stream << "File Name: " << filename << "\n";
  stream << "Version: " << ParquetVersionToString(file_metadata->version()) << "\n";
  stream << "Created By: " << file_metadata->created_by() << "\n";
  stream << "Total rows: " << file_metadata->num_rows() << "\n";

  if (print_key_value_metadata && file_metadata->key_value_metadata()) {
    auto key_value_metadata = file_metadata->key_value_metadata();
    PrintKeyValueMetadata(stream, *key_value_metadata);
  }

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
    stream << "Column " << i << ": " << descr->path()->ToDotString() << " ("
           << TypeToString(descr->physical_type(), descr->type_length());
    const auto& logical_type = descr->logical_type();
    if (!logical_type->is_none()) {
      stream << " / " << logical_type->ToString();
    }
    if (descr->converted_type() != ConvertedType::NONE) {
      stream << " / " << ConvertedTypeToString(descr->converted_type());
      if (descr->converted_type() == ConvertedType::DECIMAL) {
        stream << "(" << descr->type_precision() << "," << descr->type_scale() << ")";
      }
    }
    stream << ")" << std::endl;
  }

  for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
    stream << "--- Row Group: " << r << " ---\n";

    auto group_reader = fileReader->RowGroup(r);
    std::unique_ptr<RowGroupMetaData> group_metadata = file_metadata->RowGroup(r);

    stream << "--- Total Bytes: " << group_metadata->total_byte_size() << " ---\n";
    stream << "--- Total Compressed Bytes: " << group_metadata->total_compressed_size()
           << " ---\n";
    auto sorting_columns = group_metadata->sorting_columns();
    if (!sorting_columns.empty()) {
      stream << "--- Sort Columns:\n";
      for (auto column : sorting_columns) {
        stream << "column_idx: " << column.column_idx
               << ", descending: " << column.descending
               << ", nulls_first: " << column.nulls_first << "\n";
      }
    }
    stream << "--- Rows: " << group_metadata->num_rows() << " ---\n";

    // Print column metadata
    for (auto i : selected_columns) {
      auto column_chunk = group_metadata->ColumnChunk(i);
      std::shared_ptr<EncodedStatistics> stats = column_chunk->encoded_statistics();

      const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
      stream << "Column " << i << std::endl;
      if (print_key_value_metadata && column_chunk->key_value_metadata()) {
        PrintKeyValueMetadata(stream, *column_chunk->key_value_metadata(), 1, 2);
      }
      stream << "  Values: " << column_chunk->num_values();
      if (column_chunk->is_stats_set()) {
        std::string min = stats->min(), max = stats->max();
        std::string max_exact =
            stats->is_max_value_exact.has_value()
                ? (stats->is_max_value_exact.value() ? "true" : "false")
                : "unknown";
        std::string min_exact =
            stats->is_min_value_exact.has_value()
                ? (stats->is_min_value_exact.value() ? "true" : "false")
                : "unknown";
        stream << ", Null Values: " << stats->null_count
               << ", Distinct Values: " << stats->distinct_count << std::endl
               << "  Max (exact: " << max_exact << "): "
               << FormatStatValue(descr->physical_type(), max, descr->logical_type())
               << ", Min (exact: " << min_exact << "): "
               << FormatStatValue(descr->physical_type(), min, descr->logical_type());
      } else {
        stream << "  Statistics Not Set";
      }
      stream << std::endl
             << "  Compression: "
             << ::arrow::internal::AsciiToUpper(
                    Codec::GetCodecAsString(column_chunk->compression()))
             << ", Encodings: ";
      if (column_chunk->encoding_stats().empty()) {
        for (auto encoding : column_chunk->encodings()) {
          stream << EncodingToString(encoding) << " ";
        }
      } else {
        PrintPageEncodingStats(stream, column_chunk->encoding_stats());
      }
      stream << std::endl
             << "  Uncompressed Size: " << column_chunk->total_uncompressed_size()
             << ", Compressed Size: " << column_chunk->total_compressed_size()
             << std::endl;
    }

    if (!print_values) {
      continue;
    }
    stream << "--- Values ---\n";

    static constexpr int bufsize = kColWidth + 1;
    char buffer[bufsize];

    // Create readers for selected columns and print contents
    std::vector<std::shared_ptr<Scanner>> scanners(selected_columns.size(), nullptr);
    int j = 0;
    for (auto i : selected_columns) {
      std::shared_ptr<ColumnReader> col_reader = group_reader->Column(i);
      // This is OK in this method as long as the RowGroupReader does not get
      // deleted
      auto& scanner = scanners[j++] = Scanner::Make(col_reader);

      if (format_dump) {
        stream << "Column " << i << std::endl;
        while (scanner->HasNext()) {
          scanner->PrintNext(stream, 0, true);
          stream << "\n";
        }
        continue;
      }

      snprintf(buffer, bufsize, "%-*s", kColWidth,
               file_metadata->schema()->Column(i)->name().c_str());
      stream << buffer << '|';
    }
    if (format_dump) {
      continue;
    }
    stream << "\n";

    bool hasRow;
    do {
      hasRow = false;
      for (const auto& scanner : scanners) {
        if (scanner->HasNext()) {
          hasRow = true;
          scanner->PrintNext(stream, kColWidth);
          stream << '|';
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
  stream << "  \"Version\": \"" << ParquetVersionToString(file_metadata->version())
         << "\",\n";
  stream << "  \"CreatedBy\": \"" << file_metadata->created_by() << "\",\n";
  stream << "  \"TotalRows\": \"" << file_metadata->num_rows() << "\",\n";
  stream << "  \"NumberOfRowGroups\": \"" << file_metadata->num_row_groups() << "\",\n";
  stream << "  \"NumberOfRealColumns\": \""
         << file_metadata->schema()->group_node()->field_count() << "\",\n";
  stream << "  \"NumberOfColumns\": \"" << file_metadata->num_columns() << "\",\n";

  if (selected_columns.empty()) {
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
    stream << "     { \"Id\": \"" << i << "\","
           << " \"Name\": \"" << descr->path()->ToDotString() << "\","
           << " \"PhysicalType\": \""
           << TypeToString(descr->physical_type(), descr->type_length()) << "\","
           << " \"ConvertedType\": \"" << ConvertedTypeToString(descr->converted_type())
           << "\","
           << " \"LogicalType\": " << (descr->logical_type())->ToJSON() << " }";
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
    stream << " \"TotalCompressedBytes\": \"" << group_metadata->total_compressed_size()
           << "\", ";
    auto row_group_sorting_columns = group_metadata->sorting_columns();
    if (!row_group_sorting_columns.empty()) {
      stream << " \"SortColumns\": [\n";
      for (size_t i = 0; i < row_group_sorting_columns.size(); i++) {
        stream << "         {\"column_idx\": " << row_group_sorting_columns[i].column_idx
               << ", \"descending\": " << row_group_sorting_columns[i].descending
               << ", \"nulls_first\": " << row_group_sorting_columns[i].nulls_first
               << "}";
        if (i + 1 != row_group_sorting_columns.size()) {
          stream << ",";
        }
        stream << '\n';
      }
      stream << "       ], ";
    }
    stream << " \"Rows\": \"" << group_metadata->num_rows() << "\",\n";

    // Print column metadata
    stream << "       \"ColumnChunks\": [\n";
    int c1 = 0;
    for (auto i : selected_columns) {
      auto column_chunk = group_metadata->ColumnChunk(i);
      std::shared_ptr<Statistics> stats = column_chunk->statistics();

      const ColumnDescriptor* descr = file_metadata->schema()->Column(i);
      stream << "          {\"Id\": \"" << i << "\", \"Values\": \""
             << column_chunk->num_values() << "\", "
             << "\"StatsSet\": ";
      if (column_chunk->is_stats_set()) {
        stream << R"("True", "Stats": {)";
        if (stats->HasNullCount()) {
          stream << R"("NumNulls": ")" << stats->null_count() << "\"";
        }
        if (stats->HasDistinctCount()) {
          stream << ", "
                 << R"("DistinctValues": ")" << stats->distinct_count() << "\"";
        }
        if (stats->HasMinMax()) {
          std::string min = stats->EncodeMin(), max = stats->EncodeMax();
          stream << ", "
                 << R"("Max": ")"
                 << FormatStatValue(descr->physical_type(), max, descr->logical_type())
                 << "\", "
                 << R"("Min": ")"
                 << FormatStatValue(descr->physical_type(), min, descr->logical_type())
                 << "\"";
          if (stats->is_max_value_exact().has_value()) {
            stream << ", "
                   << R"("IsMaxValueExact": ")"
                   << (stats->is_max_value_exact().value() ? "True" : "False") << "\"";
          } else {
            stream << ", "
                   << R"("IsMaxValueExact": "unknown")";
          }
          if (stats->is_min_value_exact().has_value()) {
            stream << ", "
                   << R"("IsMinValueExact": ")"
                   << (stats->is_min_value_exact().value() ? "True" : "False") << "\"";
          } else {
            stream << ", "
                   << R"("IsMinValueExact": "unknown")";
          }
        }
        stream << " },";
      } else {
        stream << "\"False\",";
      }
      stream << "\n           \"Compression\": \""
             << ::arrow::internal::AsciiToUpper(
                    Codec::GetCodecAsString(column_chunk->compression()))
             << R"(", "Encodings": )";
      stream << "\"";
      if (column_chunk->encoding_stats().empty()) {
        for (auto encoding : column_chunk->encodings()) {
          stream << EncodingToString(encoding) << " ";
        }
      } else {
        PrintPageEncodingStats(stream, column_chunk->encoding_stats());
      }
      stream << "\"";
      stream << ", "
             << R"("UncompressedSize": ")" << column_chunk->total_uncompressed_size()
             << R"(", "CompressedSize": ")" << column_chunk->total_compressed_size()
             << "\"";

      if (column_chunk->bloom_filter_offset()) {
        // Output BloomFilter {offset, length}
        stream << ", \"BloomFilter\": {"
               << R"("offset": ")" << column_chunk->bloom_filter_offset().value() << "\"";
        if (column_chunk->bloom_filter_length()) {
          stream << R"(, "length": ")" << column_chunk->bloom_filter_length().value()
                 << "\"";
        }
        stream << "}";
      }

      if (column_chunk->GetColumnIndexLocation()) {
        auto location = column_chunk->GetColumnIndexLocation().value();
        // Output ColumnIndex {offset, length}
        stream << ", \"ColumnIndex\": {"
               << R"("offset": ")" << location.offset;
        stream << R"(", "length": ")" << location.length;
        stream << "\"}";
      }

      if (column_chunk->GetOffsetIndexLocation()) {
        auto location = column_chunk->GetOffsetIndexLocation().value();
        // Output OffsetIndex {offset, length}
        stream << ", \"OffsetIndex\": {"
               << R"("offset": ")" << location.offset << "\"";
        stream << R"(, "length": ")" << location.length << "\"";
        stream << "}";
      }

      // end of a ColumnChunk
      stream << " }";
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
