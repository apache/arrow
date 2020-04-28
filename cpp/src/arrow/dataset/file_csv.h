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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/csv/options.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/result.h"

namespace arrow {
namespace dataset {

/// \brief A FileFormat implementation that reads from and writes to Csv files
class ARROW_DS_EXPORT CsvFileFormat : public FileFormat {
 public:
  /// Options affecting the parsing of CSV files
  csv::ParseOptions parse_options = csv::ParseOptions::Defaults();

  /// Block size we request from the IO layer while parsing
  int32_t block_size = 1 << 20;  // 1 MB

  /// \defgroup csv-file-format-column-semantics properties which control how column names
  /// are derived an values are interpreted.
  ///
  /// @{

  /// Number of header rows to skip (not including the row of column names, if any)
  int32_t skip_rows = 0;

  /// Whether to check UTF8 validity of string columns
  bool check_utf8 = true;

  /// Recognized spellings for null values
  std::vector<std::string> null_values = {
      "",     "#N/A", "#N/A N/A", "#NA",     "-1.#IND", "-1.#QNAN",
      "-NaN", "-nan", "1.#IND",   "1.#QNAN", "N/A",     "NA",
      "NULL", "NaN",  "n/a",      "nan",     "null"};

  /// Recognized spellings for boolean true values
  std::vector<std::string> true_values = {"1", "True", "TRUE", "true"};

  /// Recognized spellings for boolean false values
  std::vector<std::string> false_values = {"0", "False", "FALSE", "false"};

  /// Whether string / binary columns can have null values.
  ///
  /// If true, then strings in "null_values" are considered null for string columns.
  /// If false, then all strings are valid string values.
  bool strings_can_be_null = false;

  /// Whether to try to automatically dict-encode string / binary data.
  /// If true, then when type inference detects a string or binary column,
  /// it is dict-encoded up to `auto_dict_max_cardinality` distinct values
  /// (per chunk), after which it switches to regular encoding.
  bool auto_dict_encode = false;
  int32_t auto_dict_max_cardinality = 50;

  /// @}

  std::string type_name() const override { return "csv"; }

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override;
};

}  // namespace dataset
}  // namespace arrow
