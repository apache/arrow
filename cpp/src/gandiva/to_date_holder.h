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

#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/to_date_functions_holder.h"
#include "gandiva/visibility.h"

namespace gandiva {

using gandiva::ToDateFunctionsHolder;

/// Function Holder for SQL 'to_date'
class GANDIVA_EXPORT ToDateHolder : public gandiva::ToDateFunctionsHolder<ToDateHolder> {
 using ToDateFunctionsHolder::ToDateFunctionsHolder;

 public:
  ~ToDateHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ToDateHolder>* holder);

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToDateHolder>* holder);

 private:
  ToDateHolder(const std::string& pattern, int32_t suppress_errors)
      : gandiva::ToDateFunctionsHolder<ToDateHolder>(pattern, suppress_errors, true,
                                            ::arrow::TimeUnit::SECOND) {}
};
}  // namespace gandiva
