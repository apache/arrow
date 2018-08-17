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

#include "arrow/dbi/hiveserver2/util.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

#include "arrow/dbi/hiveserver2/columnar-row-set.h"
#include "arrow/dbi/hiveserver2/thrift-internal.h"

#include "arrow/dbi/hiveserver2/TCLIService.h"
#include "arrow/dbi/hiveserver2/TCLIService_types.h"

#include "arrow/status.h"

namespace hs2 = apache::hive::service::cli::thrift;
using std::string;
using std::unique_ptr;

namespace arrow {
namespace hiveserver2 {

// PrintResults
namespace {

const char kNullSymbol[] = "NULL";
const char kTrueSymbol[] = "true";
const char kFalseSymbol[] = "false";

struct PrintInfo {
  // The PrintInfo takes ownership of the Column ptr.
  PrintInfo(Column* c, size_t m) : column(c), max_size(m) {}

  unique_ptr<Column> column;
  size_t max_size;
};

// Adds a horizontal line of '-'s, with '+'s at the column breaks.
static void AddTableBreak(std::ostream& out, std::vector<PrintInfo>* columns) {
  for (size_t i = 0; i < columns->size(); ++i) {
    out << "+";
    for (size_t j = 0; j < (*columns)[i].max_size + 2; ++j) {
      out << "-";
    }
  }
  out << "+\n";
}

// Returns the number of spaces needed to display n, i.e. the number of digits n has,
// plus 1 if n is negative.
static size_t NumSpaces(int64_t n) {
  if (n < 0) {
    return 1 + NumSpaces(-n);
  } else if (n < 10) {
    return 1;
  } else {
    return 1 + NumSpaces(n / 10);
  }
}

// Returns the max size needed to display a column of integer type.
template <typename T>
static size_t GetIntMaxSize(T* column, const string& column_name) {
  size_t max_size = column_name.size();
  for (int i = 0; i < column->length(); ++i) {
    if (!column->IsNull(i)) {
      max_size = std::max(max_size, NumSpaces(column->data()[i]));
    } else {
      max_size = std::max(max_size, sizeof(kNullSymbol));
    }
  }
  return max_size;
}

}  // namespace

void Util::PrintResults(const Operation* op, std::ostream& out) {
  unique_ptr<ColumnarRowSet> results;
  bool has_more_rows = true;
  while (has_more_rows) {
    Status s = op->Fetch(&results, &has_more_rows);
    if (!s.ok()) {
      out << s.ToString();
      return;
    }

    std::vector<ColumnDesc> column_descs;
    s = op->GetResultSetMetadata(&column_descs);

    if (!s.ok()) {
      out << s.ToString();
      return;
    } else if (column_descs.size() == 0) {
      out << "No result set to print.\n";
      return;
    }

    std::vector<PrintInfo> columns;
    for (int i = 0; i < static_cast<int>(column_descs.size()); i++) {
      const string column_name = column_descs[i].column_name();
      switch (column_descs[i].type()->type_id()) {
        case ColumnType::TypeId::BOOLEAN: {
          BoolColumn* bool_col = results->GetBoolCol(i).release();

          // The largest symbol is length 4 unless there is a FALSE, then is it
          // kFalseSymbol.size() = 5.
          size_t max_size = std::max(column_name.size(), sizeof(kTrueSymbol));
          for (int j = 0; j < bool_col->length(); ++j) {
            if (!bool_col->IsNull(j) && !bool_col->data()[j]) {
              max_size = std::max(max_size, sizeof(kFalseSymbol));
              break;
            }
          }

          columns.emplace_back(bool_col, max_size);
          break;
        }
        case ColumnType::TypeId::TINYINT: {
          ByteColumn* byte_col = results->GetByteCol(i).release();
          columns.emplace_back(byte_col, GetIntMaxSize(byte_col, column_name));
          break;
        }
        case ColumnType::TypeId::SMALLINT: {
          Int16Column* int16_col = results->GetInt16Col(i).release();
          columns.emplace_back(int16_col, GetIntMaxSize(int16_col, column_name));
          break;
        }
        case ColumnType::TypeId::INT: {
          Int32Column* int32_col = results->GetInt32Col(i).release();
          columns.emplace_back(int32_col, GetIntMaxSize(int32_col, column_name));
          break;
        }
        case ColumnType::TypeId::BIGINT: {
          Int64Column* int64_col = results->GetInt64Col(i).release();
          columns.emplace_back(int64_col, GetIntMaxSize(int64_col, column_name));
          break;
        }
        case ColumnType::TypeId::STRING: {
          unique_ptr<StringColumn> string_col = results->GetStringCol(i);

          size_t max_size = column_name.size();
          for (int j = 0; j < string_col->length(); ++j) {
            if (!string_col->IsNull(j)) {
              max_size = std::max(max_size, string_col->data()[j].size());
            } else {
              max_size = std::max(max_size, sizeof(kNullSymbol));
            }
          }

          columns.emplace_back(string_col.release(), max_size);
          break;
        }
        case ColumnType::TypeId::BINARY:
          columns.emplace_back(results->GetBinaryCol(i).release(), column_name.size());
          break;
        default: {
          out << "Unrecognized ColumnType = " << column_descs[i].type()->ToString();
        }
      }
    }

    AddTableBreak(out, &columns);
    for (size_t i = 0; i < columns.size(); ++i) {
      out << "| " << column_descs[i].column_name() << " ";

      int padding =
          static_cast<int>(columns[i].max_size - column_descs[i].column_name().size());
      while (padding > 0) {
        out << " ";
        --padding;
      }
    }
    out << "|\n";
    AddTableBreak(out, &columns);

    for (int i = 0; i < columns[0].column->length(); ++i) {
      for (size_t j = 0; j < columns.size(); ++j) {
        std::stringstream value;

        if (columns[j].column->IsNull(i)) {
          value << kNullSymbol;
        } else {
          switch (column_descs[j].type()->type_id()) {
            case ColumnType::TypeId::BOOLEAN:
              if (reinterpret_cast<BoolColumn*>(columns[j].column.get())->data()[i]) {
                value << kTrueSymbol;
              } else {
                value << kFalseSymbol;
              }
              break;
            case ColumnType::TypeId::TINYINT:
              // The cast prevents us from printing this as a char.
              value << static_cast<int16_t>(
                  reinterpret_cast<ByteColumn*>(columns[j].column.get())->data()[i]);
              break;
            case ColumnType::TypeId::SMALLINT:
              value << reinterpret_cast<Int16Column*>(columns[j].column.get())->data()[i];
              break;
            case ColumnType::TypeId::INT:
              value << reinterpret_cast<Int32Column*>(columns[j].column.get())->data()[i];
              break;
            case ColumnType::TypeId::BIGINT:
              value << reinterpret_cast<Int64Column*>(columns[j].column.get())->data()[i];
              break;
            case ColumnType::TypeId::STRING:
              value
                  << reinterpret_cast<StringColumn*>(columns[j].column.get())->data()[i];
              break;
            case ColumnType::TypeId::BINARY:
              value
                  << reinterpret_cast<BinaryColumn*>(columns[j].column.get())->data()[i];
              break;
            default:
              value << "unrecognized type";
              break;
          }
        }

        string value_str = value.str();
        out << "| " << value_str << " ";
        int padding = static_cast<int>(columns[j].max_size - value_str.size());
        while (padding > 0) {
          out << " ";
          --padding;
        }
      }
      out << "|\n";
    }
    AddTableBreak(out, &columns);
  }
}

}  // namespace hiveserver2
}  // namespace arrow
