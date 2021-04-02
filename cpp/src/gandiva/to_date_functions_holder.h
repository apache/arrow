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
#include "arrow/util/value_parsing.h"
#include "gandiva/date_utils.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for SQL 'to_date'
template <typename HOLDER_TYPE>
class GANDIVA_EXPORT ToDateFunctionsHolder : public FunctionHolder {
 public:
  ~ToDateFunctionsHolder() override = default;

  /// Return true if the data matches the pattern.
  int64_t operator()(ExecutionContext* context, const char* data, int data_len,
                     bool in_valid, bool* out_valid) {
    *out_valid = false;
    if (!in_valid) {
      return 0;
    }
    // Issues
    // 1. processes date that do not match the format.
    // 2. does not process time in format +08:00 (or) id.
    int64_t unit_time_since_epoch = 0;
    if (!::arrow::internal::ParseTimestampStrptime(data, data_len, pattern_.c_str(),
                                                   /*ignore_time_in_day=*/ignore_time_,
                                                   /*allow_trailing_chars=*/true,
                                                   time_unit_, &unit_time_since_epoch)) {
      return_error(context, data, data_len);
      return 0;
    }

    *out_valid = true;

    if (time_unit_ == ::arrow::TimeUnit::SECOND) {
      return unit_time_since_epoch * 1000;
    }

    return unit_time_since_epoch;
  }

 protected:
  ToDateFunctionsHolder(std::string pattern, int32_t suppress_errors, bool ignore_time,
                        ::arrow::TimeUnit::type time_unit)
      : pattern_(std::move(pattern)),
        suppress_errors_(suppress_errors),
        ignore_time_(ignore_time),
        time_unit_(time_unit) {}

  std::string pattern_;  // date format string

  int32_t suppress_errors_;  // should throw exception on runtime errors

  bool ignore_time_;  // if the hour, minutes and secs must be ignored during formatting

  ::arrow::TimeUnit::type time_unit_;

  void return_error(ExecutionContext* context, const char* data, int data_len) {
    if (suppress_errors_ == 1) {
      return;
    }

    std::string err_msg =
        "Error parsing value " + std::string(data, data_len) + " for given format.";
    context->set_error_msg(err_msg.c_str());
  }

  static Status Make(const FunctionNode& node, std::shared_ptr<HOLDER_TYPE>* holder,
                     const std::string& function_name) {
    if (node.children().size() != 2 && node.children().size() != 3) {
      return Status::Invalid(function_name +
                             " function requires two or three parameters");
    }

    auto literal_pattern = dynamic_cast<LiteralNode*>(node.children().at(1).get());
    if (literal_pattern == NULLPTR) {
      return Status::Invalid(function_name +
                             " function requires a literal as the second parameter");
    }

    auto literal_type = literal_pattern->return_type()->id();
    if (literal_type != arrow::Type::STRING && literal_type != arrow::Type::BINARY) {
      return Status::Invalid(
          function_name + " function requires a string literal as the second parameter");
    }
    auto pattern = arrow::util::get<std::string>(literal_pattern->holder());

    int suppress_errors = 0;
    if (node.children().size() == 3) {
      auto literal_suppress_errors =
          dynamic_cast<LiteralNode*>(node.children().at(2).get());
      if (literal_suppress_errors == NULLPTR) {
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

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<HOLDER_TYPE>* holder) {
    std::shared_ptr<std::string> transformed_pattern;
    ARROW_RETURN_NOT_OK(DateUtils::ToInternalFormat(sql_pattern, &transformed_pattern));
    auto lholder = std::shared_ptr<HOLDER_TYPE>(
        new HOLDER_TYPE(*(transformed_pattern.get()), suppress_errors));
    *holder = lholder;
    return Status::OK();
  }
};

/// Function Holder for SQL 'to_date'
class GANDIVA_EXPORT ToDateHolder : public gandiva::ToDateFunctionsHolder<ToDateHolder> {
 public:
  ~ToDateHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ToDateHolder>* holder);

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToDateHolder>* holder);

  ToDateHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<ToDateHolder>(pattern, suppress_errors, true,
                                            ::arrow::TimeUnit::SECOND) {}
};

/// Function Holder for SQL 'to_time'
class GANDIVA_EXPORT ToTimeHolder : public ToDateFunctionsHolder<ToTimeHolder> {
 public:
  ~ToTimeHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ToTimeHolder>* holder) {
    const std::string function_name("to_date");
    return ToDateFunctionsHolder<ToTimeHolder>::Make(node, holder, function_name);
  }

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToTimeHolder>* holder) {
    return ToDateFunctionsHolder<ToTimeHolder>::Make(sql_pattern, suppress_errors,
                                                     holder);
  }

  ToTimeHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<ToTimeHolder>(pattern, suppress_errors, false,
                                            ::arrow::TimeUnit::SECOND) {}
};
}  // namespace gandiva
