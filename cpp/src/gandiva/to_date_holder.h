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

#ifndef TO_DATE_HOLDER_H
#define TO_DATE_HOLDER_H

#include <memory>
#include <string>
#include <unordered_map>

#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/status.h"

namespace gandiva {

/// Function Holder for SQL 'to_date'
class ToDateHolder : public FunctionHolder {
 public:
  ~ToDateHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ToDateHolder>* holder);

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToDateHolder>* holder);

  /// Return true if the data matches the pattern.
  int64_t operator()(const std::string& data, bool in_valid, int64_t execution_context,
                     bool* out_valid);

 private:
  ToDateHolder(const std::string& pattern, int32_t suppress_errors)
      : pattern_(pattern), suppress_errors_(suppress_errors) {}

  void return_error(int64_t execution_context, const std::string& data);

  std::string pattern_;  // date format string

  int32_t suppress_errors_;  // should throw exception on runtime errors
};

}  // namespace gandiva
#endif  // TO_DATE_HOLDER_H
