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

#include <iomanip>
#include <map>
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
#include "gandiva/precompiled/epoch_time_point.h"
#include "gandiva/visibility.h"

namespace gandiva {

template <typename HOLDER_TYPE>
class GANDIVA_EXPORT FromDateFunctionsHolder : public FunctionHolder {
 public:
  ~FromDateFunctionsHolder() override = default;

  virtual const char* operator()(ExecutionContext* context, int64_t in_data,
                                 bool in_valid, bool* out_valid) {
    *out_valid = false;
    if (!in_valid) {
      return 0;
    }

    if (pattern_.empty()) {
      pattern_ = "%Y-%m-%d %H:%M:%S";
    }

    // It receives milliseconds and time_t will handle up to seconds only
    time_t rawtime = in_data / 1000;

    struct tm* ptm;
    ptm = gmtime(&rawtime);

    std::stringstream iss;
    iss << std::put_time(ptm, pattern_.c_str());
    std::string ret_str = iss.str();
    size_t length = strlen(ret_str.c_str());

    char* ret = new char[length];

    memcpy(ret, ret_str.c_str(), length);

    return ret;
  }

 protected:
  FromDateFunctionsHolder(std::string pattern, int32_t suppress_errors)
      : pattern_(std::move(pattern)), suppress_errors_(suppress_errors){};

  std::string pattern_;  // date format string

  int32_t suppress_errors_;  // should throw exception on runtime errors

  // utility function to explode a string
  std::vector<std::string> explode(std::string str, std::string delimiter) {
    std::vector<std::string> vector;
    size_t last = 0;
    size_t next = 0;
    while ((next = str.find(delimiter, last)) != std::string::npos) {
      vector.push_back(str.substr(last, next - last));
      last = next + 1;
    }
    // get last item
    vector.push_back(str.substr(last));

    return vector;
  }

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

/// Function Holder for Hive 'from_unixtime'
class GANDIVA_EXPORT FromUnixtimeHolder
    : public gandiva::FromDateFunctionsHolder<FromUnixtimeHolder> {
 public:
  ~FromUnixtimeHolder() override = default;

  FromUnixtimeHolder(const std::string& pattern, int32_t suppress_errors)
      : FromDateFunctionsHolder<FromUnixtimeHolder>(pattern, suppress_errors) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<FromUnixtimeHolder>* holder) {
    const std::string function_name("from_unixtime");

    if (node.children().empty()) {
      return Status::Invalid("from_unixtime function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      // from_unixtime(given milliseconds, format, [supress_error]) and
      return FromDateFunctionsHolder<FromUnixtimeHolder>::Make(node, holder,
                                                               function_name);
    }

    // it will use the default pattern for output, eg.: 1970-01-01 08:00:00
    const std::string pattern("YYYY-MM-DD HH24:MI:SS");
    const int32_t not_supress_errors = 0;
    return Make(pattern, not_supress_errors, holder);
  }

  static Status Make(const std::string& ts_pattern, int32_t suppress_errors,
                     std::shared_ptr<FromUnixtimeHolder>* holder) {
    return FromDateFunctionsHolder<FromUnixtimeHolder>::Make(ts_pattern, suppress_errors,
                                                             holder);
  }
};
}  // namespace gandiva
