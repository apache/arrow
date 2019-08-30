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

#ifndef ARROW_CSV_OPTIONS_H
#define ARROW_CSV_OPTIONS_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class DataType;

namespace csv {

struct ARROW_EXPORT ParseOptions {
  // Parsing options

  // Field delimiter
  char delimiter = ',';
  // Whether quoting is used
  bool quoting = true;
  // Quoting character (if `quoting` is true)
  char quote_char = '"';
  // Whether a quote inside a value is double-quoted
  bool double_quote = true;
  // Whether escaping is used
  bool escaping = false;
  // Escaping character (if `escaping` is true)
  char escape_char = '\\';
  // Whether values are allowed to contain CR (0x0d) and LF (0x0a) characters
  bool newlines_in_values = false;
  // Whether empty lines are ignored.  If false, an empty line represents
  // a single empty value (assuming a one-column CSV file).
  bool ignore_empty_lines = true;

  static ParseOptions Defaults();
};

struct ARROW_EXPORT ConvertOptions {
  // Conversion options

  // Whether to check UTF8 validity of string columns
  bool check_utf8 = true;
  // Optional per-column types (disabling type inference on those columns)
  std::unordered_map<std::string, std::shared_ptr<DataType>> column_types;
  // Recognized spellings for null values
  std::vector<std::string> null_values;
  // Recognized spellings for boolean values
  std::vector<std::string> true_values;
  std::vector<std::string> false_values;
  // Whether string / binary columns can have null values.
  // If true, then strings in "null_values" are considered null for string columns.
  // If false, then all strings are valid string values.
  bool strings_can_be_null = false;

  // XXX Should we have a separate FilterOptions?

  // If non-empty, indicates the names of columns from the CSV file that should
  // be actually read and converted (in the vector's order).
  // Columns not in this vector will be ignored.
  std::vector<std::string> include_columns;
  // If false, columns in `include_columns` but not in the CSV file will error out.
  // If true, columns in `include_columns` but not in the CSV file will produce
  // a column of nulls (whose type is selected using `column_types`,
  // or null by default)
  // This option is ignored if `include_columns` is empty.
  bool include_missing_columns = false;

  static ConvertOptions Defaults();
};

struct ARROW_EXPORT ReadOptions {
  // Reader options

  // Whether to use the global CPU thread pool
  bool use_threads = true;
  // Block size we request from the IO layer; also determines the size of
  // chunks when use_threads is true
  int32_t block_size = 1 << 20;  // 1 MB

  // Number of header rows to skip (not including the row of column names, if any)
  int32_t skip_rows = 0;
  // Column names for the target table.
  // If empty, fall back on autogenerate_column_names.
  std::vector<std::string> column_names;
  // Whether to autogenerate column names if `column_names` is empty.
  // If true, column names will be of the form "f0", "f1"...
  // If false, column names will be read from the first CSV row after `skip_rows`.
  bool autogenerate_column_names = false;

  static ReadOptions Defaults();
};

}  // namespace csv
}  // namespace arrow

#endif  // ARROW_CSV_OPTIONS_H
