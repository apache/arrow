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

#include <re2/re2.h>

#include "arrow/status.h"

#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for SQL 'parse_url'
class GANDIVA_EXPORT ParseUrlHolder : public FunctionHolder {
 public:
  ~ParseUrlHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ParseUrlHolder>* holder);

  static Status Make(const std::string& part_to_extract,
                     std::shared_ptr<ParseUrlHolder>* holder);

  static Status Make(const std::string& part_to_extract, const std::string& query_key,
                     std::shared_ptr<ParseUrlHolder>* holder);

  const char* Parse(const std::string& url) { return url.c_str(); }

 private:
  static RE2 url_regex_;
};
}  // namespace gandiva
