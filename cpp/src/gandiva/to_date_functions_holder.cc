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

#include "gandiva/to_date_functions_holder.h"

#include "arrow/util/value_parsing.h"
#include "arrow/vendored/datetime.h"
#include "gandiva/execution_context.h"
#include "gandiva/node.h"
#include "gandiva/date_utils.h"

namespace gandiva {

template <typename HOLDER_TYPE>
void ToDateFunctionsHolder<HOLDER_TYPE>::return_error(ExecutionContext* context,
                                                      const char* data, int data_len) {
  if (suppress_errors_ == 1) {
    return;
  }

  std::string err_msg =
      "Error parsing value " + std::string(data, data_len) + " for given format.";
  context->set_error_msg(err_msg.c_str());
}
template <typename HOLDER_TYPE>
Status ToDateFunctionsHolder<HOLDER_TYPE>::Make(const FunctionNode& node,
                                                std::shared_ptr<HOLDER_TYPE>* holder,
                                                std::string& function_name) {
  if (node.children().size() != 2 && node.children().size() != 3) {
    return Status::Invalid(function_name + " function requires two or three parameters");
  }

  auto literal_pattern = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  if (literal_pattern == nullptr) {
    return Status::Invalid(function_name +
                           " function requires a literal as the second parameter");
  }

  auto literal_type = literal_pattern->return_type()->id();
  if (literal_type != arrow::Type::STRING && literal_type != arrow::Type::BINARY) {
    return Status::Invalid(function_name +
                           " function requires a string literal as the second parameter");
  }
  auto pattern = arrow::util::get<std::string>(literal_pattern->holder());

  int suppress_errors = 0;
  if (node.children().size() == 3) {
    auto literal_suppress_errors =
        dynamic_cast<LiteralNode*>(node.children().at(2).get());
    if (literal_pattern == nullptr) {
      return Status::Invalid("The (optional) third parameter to " + function_name +
                             " function needs to be an integer "
                             "literal to indicate whether to suppress the error");
    }

    literal_type = literal_suppress_errors->return_type()->id();
    if (literal_type != arrow::Type::INT32) {
      return Status::Invalid("The (optional) third parameter to " + function_name +
                             " function needs to be an integer "
                             "literal to indicate whether to suppress the error");
    }
    suppress_errors = arrow::util::get<int>(literal_suppress_errors->holder());
  }

  return Make(pattern, suppress_errors, holder);
}

template <typename HOLDER_TYPE>
Status ToDateFunctionsHolder<HOLDER_TYPE>::Make(const std::string& sql_pattern,
                                                int32_t suppress_errors,
                                                std::shared_ptr<HOLDER_TYPE>* holder) {
  std::shared_ptr<std::string> transformed_pattern;
  ARROW_RETURN_NOT_OK(DateUtils::ToInternalFormat(sql_pattern, &transformed_pattern));
  auto lholder = std::shared_ptr<HOLDER_TYPE>(
      new HOLDER_TYPE(*(transformed_pattern.get()), suppress_errors));
  *holder = lholder;
  return Status::OK();
}

template <typename HOLDER_TYPE>
int64_t ToDateFunctionsHolder<HOLDER_TYPE>::operator()(ExecutionContext* context,
                                                       const char* data, int data_len,
                                                       bool in_valid, bool* out_valid) {
  *out_valid = false;
  if (!in_valid) {
    return 0;
  }
  // Issues
  // 1. processes date that do not match the format.
  // 2. does not process time in format +08:00 (or) id.
  int64_t unit_time_since_epoch = 0;
  if (!::arrow::internal::ParseTimestampStrptime(
      data, data_len, pattern_.c_str(),
      /*ignore_time_in_day=*/ignore_time_, /*allow_trailing_chars=*/true,
                                                 time_unit_, &unit_time_since_epoch)) {
    return_error(context, data, data_len);
    return 0;
  }

  *out_valid = true;
  return unit_time_since_epoch;
}
}  // namespace gandiva