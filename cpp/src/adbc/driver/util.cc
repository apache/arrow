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

#include "adbc/driver/util.h"
#include "arrow/status.h"
#include "arrow/util/string_view.h"

namespace adbc {

using arrow::util::string_view;

arrow::Result<std::unordered_map<std::string, std::string>> ParseConnectionString(
    string_view target) {
  // TODO: this does not properly implement the ODBC connection string format.
  std::unordered_map<std::string, std::string> option_pairs;
  string_view cur(target);

  while (true) {
    auto divider = cur.find_first_of('=');
    if (divider == string_view::npos) {
      if (!cur.empty()) {
        return arrow::Status::Invalid("Trailing characters: ", cur);
      }
      break;
    }
    string_view key = cur.substr(0, divider);
    cur = cur.substr(divider + 1);
    auto end = cur.find_first_of(';');
    string_view value;
    if (end == string_view::npos) {
      value = cur;
      cur = string_view();
    } else {
      value = cur.substr(0, end);
      cur = cur.substr(end + 1);
    }

    if (!option_pairs.insert({std::string(key), std::string(value)}).second) {
      return arrow::Status::Invalid("Duplicate option: ", key);
    }
  }
  return option_pairs;
}

}  // namespace adbc
