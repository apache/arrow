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
#include <utility>

#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for SQL 'to_date'
class GANDIVA_EXPORT ToDateFunctionsHolder : public FunctionHolder {
 public:
  ~ToDateFunctionsHolder() override = default;

 protected:
  ToDateFunctionsHolder(std::string  pattern, int32_t suppress_errors)
      : pattern_(std::move(pattern)), suppress_errors_(suppress_errors) {}

  void return_error(ExecutionContext* context, const char* data,
                                           int data_len) const;

  std::string pattern_;  // date format string

  int32_t suppress_errors_;  // should throw exception on runtime errors
};
}  // namespace gandiva
