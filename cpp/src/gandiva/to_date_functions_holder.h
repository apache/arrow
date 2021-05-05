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

/// Function Holder that is father for the class that executes `
/// 'to_time', 'to_date', 'to_timestamp' and 'unix_timestamp' functions
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
    if (!arrow::internal::ParseTimestampArrowVendored(
            data, data_len, pattern_.c_str(),
            /*allow_trailing_chars=*/true, /*ignore_time_in_day=*/ignore_time_,
            time_unit_, &unit_time_since_epoch)) {
      return_error(context, data, data_len);
      return 0;
    }

    *out_valid = true;

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

  ToDateHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<ToDateHolder>(pattern, suppress_errors, true,
                                            ::arrow::TimeUnit::MILLI) {}

  static Status Make(const FunctionNode& node, std::shared_ptr<ToDateHolder>* holder) {
    const std::string function_name("to_date");
    return ToDateFunctionsHolder<ToDateHolder>::Make(node, holder, function_name);
  }

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToDateHolder>* holder) {
    return ToDateFunctionsHolder<ToDateHolder>::Make(sql_pattern, suppress_errors,
                                                     holder);
  }
};

/// Function Holder for SQL 'to_time'
class GANDIVA_EXPORT ToTimeHolder : public ToDateFunctionsHolder<ToTimeHolder> {
 public:
  ~ToTimeHolder() override = default;

  ToTimeHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<ToTimeHolder>(pattern, suppress_errors, false,
                                            ::arrow::TimeUnit::MILLI) {}

  static Status Make(const FunctionNode& node, std::shared_ptr<ToTimeHolder>* holder) {
    const std::string function_name("to_time");
    return ToDateFunctionsHolder<ToTimeHolder>::Make(node, holder, function_name);
  }

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToTimeHolder>* holder) {
    return ToDateFunctionsHolder<ToTimeHolder>::Make(sql_pattern, suppress_errors,
                                                     holder);
  }
};

/// Function Holder for SQL 'is_date'
class GANDIVA_EXPORT IsDateHolder : public ToDateFunctionsHolder<IsDateHolder> {
 public:
  ~IsDateHolder() override = default;

  IsDateHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<IsDateHolder>(pattern, suppress_errors, false,
                                            ::arrow::TimeUnit::MILLI) {}

  static Status Make(const FunctionNode& node, std::shared_ptr<IsDateHolder>* holder) {
    if (node.children().empty()) {
      return Status::Invalid("is_date function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      // is_data(given_string, format, [supress_error]) and
      // it behaves like the is_date(string, format, [supress_error])
      // function
      const std::string function_name("is_date");
      return ToDateFunctionsHolder<IsDateHolder>::Make(node, holder, function_name);
    }

    // It means that the function called was is_date(string)
    // so the behavior will be like the function
    // is_date(string, "YYYY-MM-DD HH24:MI:SS", 0)
    const std::string pattern("YYYY-MM-DD");
    const int32_t not_suppress_errors = 0;
    return Make(pattern, not_suppress_errors, holder);
  }

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<IsDateHolder>* holder) {
    return ToDateFunctionsHolder<IsDateHolder>::Make(sql_pattern, suppress_errors,
                                                     holder);
  }
};

/// Function Holder for SQL 'to_timestamp'
class GANDIVA_EXPORT ToTimestampHolder : public ToDateFunctionsHolder<ToTimestampHolder> {
 public:
  ~ToTimestampHolder() override = default;

  ToTimestampHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<ToTimestampHolder>(pattern, suppress_errors, false,
                                                 ::arrow::TimeUnit::MILLI) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<ToTimestampHolder>* holder) {
    const std::string function_name("to_timestamp");
    return ToDateFunctionsHolder<ToTimestampHolder>::Make(node, holder, function_name);
  }

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<ToTimestampHolder>* holder) {
    return ToDateFunctionsHolder<ToTimestampHolder>::Make(sql_pattern, suppress_errors,
                                                          holder);
  }
};

/// Function Holder for SQL 'unix_timestamp'
class GANDIVA_EXPORT UnixTimestampHolder
    : public ToDateFunctionsHolder<UnixTimestampHolder> {
 public:
  ~UnixTimestampHolder() override = default;

  UnixTimestampHolder(const std::string& pattern, int32_t suppress_errors)
      : ToDateFunctionsHolder<UnixTimestampHolder>(pattern, suppress_errors, false,
                                                   ::arrow::TimeUnit::SECOND) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<UnixTimestampHolder>* holder) {
    if (node.children().empty()) {
      return Status::Invalid("unix_timestamp function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      // unix_timestamp(given_string, format, [supress_error]) and
      // it behaves like the to_timestamp(string, format, [supress_error])
      // function
      const std::string function_name("unix_timestamp");
      return ToDateFunctionsHolder<UnixTimestampHolder>::Make(node, holder,
                                                              function_name);
    }

    // It means that the function called was unix_timestamp(string)
    // so the behavior will be like the function
    // unix_timestamp(string, "YYYY-MM-DD HH24:MI:SS", 0)
    const std::string pattern("YYYY-MM-DD HH24:MI:SS");
    const int32_t not_supress_errors = 0;
    return Make(pattern, not_supress_errors, holder);
  }

  static Status Make(const std::string& sql_pattern, int32_t suppress_errors,
                     std::shared_ptr<UnixTimestampHolder>* holder) {
    return ToDateFunctionsHolder<UnixTimestampHolder>::Make(sql_pattern, suppress_errors,
                                                            holder);
  }
};
}  // namespace gandiva
