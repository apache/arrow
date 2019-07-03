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

ReadOptions ReadOptions::Defaults() { return ReadOptions(); }

}  // namespace csv
}  // namespace arrow
