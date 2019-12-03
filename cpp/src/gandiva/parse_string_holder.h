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

#ifndef GANDIVA_PARSE_STRING_HOLDER_H
#define GANDIVA_PARSE_STRING_HOLDER_H

#include <memory>
#include <string>

#include "arrow/status.h"
#include "arrow/util/parsing.h"

#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for string parsing
template <typename ARROW_TYPE>
class GANDIVA_EXPORT ParseStringHolder : public FunctionHolder {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  ~ParseStringHolder() override = default;

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<ParseStringHolder<ARROW_TYPE>>* holder) {
    ARROW_RETURN_IF(node.children().size() != 1,
                    Status::Invalid("function requires one parameter"));

    return Make(holder);
  }

  static Status Make(std::shared_ptr<ParseStringHolder<ARROW_TYPE>>* holder) {
    auto ps_holder =
        std::shared_ptr<ParseStringHolder>(new ParseStringHolder<ARROW_TYPE>());

    *holder = ps_holder;
    return Status::OK();
  }

  /// Return the value parsed from string
  value_type operator()(ExecutionContext* context, const char* data, int data_len) {
    value_type val;
    if (!converter_(data, data_len, &val)) {
      context->set_error_msg("Failed parsing the string to required format");
    }
    return val;
  }

 private:
  ParseStringHolder() {}

  arrow::internal::StringConverter<ARROW_TYPE> converter_;
};

}  // namespace gandiva

#endif  // GANDIVA_PARSE_STRING_HOLDER_H
