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

#include <memory>
#include <regex>
#include <string>

#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder that is the base class for the holder of functions that execute
/// cast to interval types
template <typename INTERVAL_TYPE>
class GANDIVA_EXPORT IntervalHolder : public FunctionHolder {
 public:
  ~IntervalHolder() override = default;

 protected:
  static Result<std::shared_ptr<INTERVAL_TYPE>> Make(const FunctionNode& node,
                                                     const std::string& function_name) {
    ARROW_RETURN_IF(node.children().size() != 1 && node.children().size() != 2,
                    Status::Invalid(function_name + " requires one or two parameters"));

    int32_t suppress_errors = 0;
    if (node.children().size() == 2) {
      auto literal_suppress_errors =
          dynamic_cast<LiteralNode*>(node.children().at(1).get());
      if (literal_suppress_errors == NULLPTR) {
        return Status::Invalid("The (optional) second parameter to " + function_name +
                               " function needs to be an integer literal to indicate "
                               "whether to suppress the error");
      }

      auto literal_type = literal_suppress_errors->return_type()->id();
      if (literal_type != arrow::Type::INT32) {
        return Status::Invalid("The (optional) second parameter to " + function_name +
                               " function needs to be an integer literal to indicate "
                               "whether to suppress the error");
      }
      suppress_errors = std::get<int>(literal_suppress_errors->holder());
    }

    return Make(suppress_errors);
  }

  static Result<std::shared_ptr<INTERVAL_TYPE>> Make(int32_t suppress_errors) {
    return std::make_shared<INTERVAL_TYPE>(suppress_errors);
  }

  explicit IntervalHolder(int32_t suppress_errors) : suppress_errors_(suppress_errors) {}

  // If the flag is equals to 0, the errors will not be suppressed, any other value
  // will made the errors being suppressed
  int32_t suppress_errors_;

  void return_error(ExecutionContext* context, std::string& data) const {
    if (suppress_errors_ != 0) {
      return;
    }

    std::string err_msg = "Error parsing the period: " + data + ".";
    context->set_error_msg(err_msg.c_str());
  }
};

/// Function Holder for castINTERVALDAY function
class GANDIVA_EXPORT IntervalDaysHolder : public IntervalHolder<IntervalDaysHolder> {
 public:
  ~IntervalDaysHolder() override = default;

  static Result<std::shared_ptr<IntervalDaysHolder>> Make(const FunctionNode& node);

  static Result<std::shared_ptr<IntervalDaysHolder>> Make(int32_t suppress_errors);

  /// Cast a generic string to an interval
  int64_t operator()(ExecutionContext* ctx, const char* data, int32_t data_len,
                     bool in_valid, bool* out_valid);

  explicit IntervalDaysHolder(int32_t suppress_errors)
      : IntervalHolder<IntervalDaysHolder>(suppress_errors) {}

 private:
  /// Retrieves the day interval from the number of milliseconds encoded as
  /// a string
  static int64_t GetIntervalDayFromMillis(ExecutionContext* context,
                                          std::string& number_as_string,
                                          int32_t suppress_errors, bool* out_valid);

  /// Retrieves the day interval from the number of weeks encoded as
  /// a string.
  static int64_t GetIntervalDayFromWeeks(ExecutionContext* context,
                                         std::string& number_as_string,
                                         int32_t suppress_errors, bool* out_valid);

  static int64_t GetIntervalDayFromCompletePeriod(
      ExecutionContext* context, std::string& days_in_period,
      std::string& hours_in_period, std::string& minutes_in_period,
      std::string& seconds_in_period, int32_t suppress_errors, bool* out_valid);
};

/// Function Holder for the castINTERVALYEAR function
class GANDIVA_EXPORT IntervalYearsHolder : public IntervalHolder<IntervalYearsHolder> {
 public:
  ~IntervalYearsHolder() override = default;

  static Result<std::shared_ptr<IntervalYearsHolder>> Make(const FunctionNode& node);

  static Result<std::shared_ptr<IntervalYearsHolder>> Make(int32_t suppress_errors);

  /// Cast a generic string to an interval
  int32_t operator()(ExecutionContext* ctx, const char* data, int32_t data_len,
                     bool in_valid, bool* out_valid);

  explicit IntervalYearsHolder(int32_t suppress_errors)
      : IntervalHolder<IntervalYearsHolder>(suppress_errors) {}

 private:
  static int32_t GetIntervalYearFromNumber(ExecutionContext* context,
                                           std::string& number_as_string,
                                           int32_t suppress_errors, bool* out_valid);

  static int32_t GetIntervalYearFromCompletePeriod(ExecutionContext* context,
                                                   std::string& yrs_in_period,
                                                   std::string& months_in_period,
                                                   int32_t suppress_errors,
                                                   bool* out_valid);
};
}  // namespace gandiva
