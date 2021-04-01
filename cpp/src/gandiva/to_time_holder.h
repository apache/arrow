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

#include "gandiva/to_date_functions_holder.h"

namespace gandiva {
/// Function Holder for SQL 'to_date'
class GANDIVA_EXPORT ToTimeHolder : public ToDateFunctionsHolder {
 public:
  ~ToTimeHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ToTimeHolder>* holder);

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToTimeHolder>* holder);

  /// Return true if the data matches the pattern.
  int64_t operator()(ExecutionContext* context, const char* data, int data_len,
                     bool in_valid, bool* out_valid);

 private:
  ToTimeHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder(pattern, suppress_errors) {}
};
}  // namespace gandiva