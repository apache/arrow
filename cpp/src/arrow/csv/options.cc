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

#include "arrow/csv/options.h"

namespace arrow {
namespace csv {

ParseOptions ParseOptions::Defaults() { return ParseOptions(); }

Status ParseOptions::Validate() const {
  if (ARROW_PREDICT_FALSE(delimiter == '\n' || delimiter == '\r')) {
    return Status::Invalid("ParseOptions: delimiter cannot be \\r or \\n");
  }
  if (ARROW_PREDICT_FALSE(quoting && (quote_char == '\n' || quote_char == '\r'))) {
    return Status::Invalid("ParseOptions: quote_char cannot be \\r or \\n");
  }
  if (ARROW_PREDICT_FALSE(escaping && (escape_char == '\n' || escape_char == '\r'))) {
    return Status::Invalid("ParseOptions: escape_char cannot be \\r or \\n");
  }
  return Status::OK();
}

ConvertOptions ConvertOptions::Defaults() {
  auto options = ConvertOptions();
  // Same default null / true / false spellings as in Pandas.
  options.null_values = {"",     "#N/A", "#N/A N/A", "#NA",     "-1.#IND", "-1.#QNAN",
                         "-NaN", "-nan", "1.#IND",   "1.#QNAN", "N/A",     "NA",
                         "NULL", "NaN",  "n/a",      "nan",     "null"};
  options.true_values = {"1", "True", "TRUE", "true"};
  options.false_values = {"0", "False", "FALSE", "false"};
  return options;
}

Status ConvertOptions::Validate() const { return Status::OK(); }

ReadOptions ReadOptions::Defaults() { return ReadOptions(); }

Status ReadOptions::Validate() const {
  if (ARROW_PREDICT_FALSE(block_size < 1)) {
    // Min is 1 because some tests use really small block sizes
    return Status::Invalid("ReadOptions: block_size must be at least 1: ", block_size);
  }
  if (ARROW_PREDICT_FALSE(skip_rows < 0)) {
    return Status::Invalid("ReadOptions: skip_rows cannot be negative: ", skip_rows);
  }
  if (ARROW_PREDICT_FALSE(skip_rows_after_names < 0)) {
    return Status::Invalid("ReadOptions: skip_rows_after_names cannot be negative: ",
                           skip_rows_after_names);
  }
  if (ARROW_PREDICT_FALSE(autogenerate_column_names && !column_names.empty())) {
    return Status::Invalid(
        "ReadOptions: autogenerate_column_names cannot be true when column_names are "
        "provided");
  }
  return Status::OK();
}

WriteOptions WriteOptions::Defaults() { return WriteOptions(); }

Status WriteOptions::Validate() const {
  if (ARROW_PREDICT_FALSE(batch_size < 1)) {
    return Status::Invalid("WriteOptions: batch_size must be at least 1: ", batch_size);
  }
  return Status::OK();
}

}  // namespace csv
}  // namespace arrow
