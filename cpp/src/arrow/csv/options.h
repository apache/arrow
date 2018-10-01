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

#include "arrow/util/visibility.h"

namespace arrow {
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

  // XXX Should this be in ReadOptions?
  // Number of header rows to skip
  int32_t header_rows = 1;

  static ParseOptions Defaults();
};

struct ARROW_EXPORT ConvertOptions {
  static ConvertOptions Defaults();
};

struct ARROW_EXPORT ReadOptions {
  // Reader options

  // Whether to use the global CPU thread pool
  bool use_threads = true;
  // Block size we request from the IO layer
  int32_t block_size = 1 << 20;  // 1 MB
  // Max num rows per array chunk
  int32_t num_rows = 100000;

  static ReadOptions Defaults();
};

}  // namespace csv
}  // namespace arrow

#endif  // ARROW_CSV_OPTIONS_H
