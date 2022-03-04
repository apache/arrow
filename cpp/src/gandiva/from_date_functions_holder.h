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

#include <arrow/vendored/datetime/tz.h>

#include <chrono>
#include <iomanip>
#include <iostream>
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
#include "gdv_function_stubs.h"

namespace gandiva {
/// Super class for function holder for FromDate
template <typename HOLDER_TYPE>
class GANDIVA_EXPORT FromDateFunctionsHolder : public FunctionHolder {
 public:
  ~FromDateFunctionsHolder() override = default;

  virtual const char* operator()(ExecutionContext* context, int64_t in_data,
                                 bool in_valid, bool* out_valid) {
    *out_valid = false;

    bool pattern_begins_empty = pattern_.empty();
    if (!in_valid) {
      return 0;
    }

    if (pattern_begins_empty) {
      pattern_ = "%Y-%m-%d %H:%M:%S";
    }

    // It receives milliseconds and time_t will handle up to seconds only
    time_t rawtime = in_data / 1000;

    struct tm* ptm;
    ptm = gmtime(&rawtime);

    std::stringstream iss;
    iss << std::put_time(ptm, pattern_.c_str());
    std::string ret_str = iss.str();
    size_t length = strlen(ret_str.c_str()) + 1;

    char* ret = reinterpret_cast<char*>(
        gdv_fn_context_arena_malloc(reinterpret_cast<int64_t>(context), (20)));

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
        new HOLDER_TYPE(*(transformed_pattern), suppress_errors));
    *holder = lholder;
    return Status::OK();
  }
};

/// Function Holder for 'from_unixtime'
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

/// Super class for function holder for FromUtcDate
template <typename HOLDER_TYPE>
class GANDIVA_EXPORT FromUtcFunctionsHolder : public FunctionHolder {
 public:
  ~FromUtcFunctionsHolder() override = default;

  virtual const char* operator()(ExecutionContext* context, int64_t in_data,
                                 const char* tz, bool in_valid, bool* out_valid) {
    *out_valid = false;

    if (!in_valid) {
      return "";
    }

    std::chrono::system_clock::time_point tp{std::chrono::milliseconds{in_data}};

    auto tz_converted = arrow_vendored::date::make_zoned(tz, tp);

    std::stringstream iss;
    iss << format(pattern_, tz_converted);
    std::string ret_str = iss.str();
    ret_str = ret_str.substr(0, ret_str.find('.'));

    size_t length = 20;

    char* ret = reinterpret_cast<char*>(
        gdv_fn_context_arena_malloc(reinterpret_cast<int64_t>(context), (20)));

    memcpy(ret, ret_str.c_str(), length);

    return ret;
  }

 protected:
  FromUtcFunctionsHolder(int32_t suppress_errors) : suppress_errors_(suppress_errors){};

  std::string pattern_ = "%Y-%m-%d %H:%M:%S";  // date format string

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

    return Make(suppress_errors, holder);
  }

  static Status Make(int32_t suppress_errors, std::shared_ptr<HOLDER_TYPE>* holder) {
    auto lholder = std::shared_ptr<HOLDER_TYPE>(new HOLDER_TYPE(suppress_errors));
    *holder = lholder;
    return Status::OK();
  }
};

/// Function Holder for 'from_utc_timestamp' from utf8
class GANDIVA_EXPORT FromUtcTimestampUtf8Holder
    : public gandiva::FromUtcFunctionsHolder<FromUtcTimestampUtf8Holder> {
 public:
  virtual const char* operator()(ExecutionContext* context, const char* timestamp,
                                 int32_t timestamp_len, const char* tz, bool in_valid,
                                 bool* out_valid) {
    *out_valid = false;

    if (!in_valid) {
      return "";
    }

    int64_t millis_timestamp;
    if (!arrow::internal::ParseTimestampArrowVendored(
            timestamp, timestamp_len, pattern_.c_str(),
            /*allow_trailing_chars=*/true, /*ignore_time_in_day=*/false,
            ::arrow::TimeUnit::MILLI, &millis_timestamp)) {
      return_error(context, timestamp, timestamp_len);
      return "";
    }

    std::chrono::system_clock::time_point tp{std::chrono::milliseconds{millis_timestamp}};

    auto tz_converted = arrow_vendored::date::make_zoned(tz, tp);

    std::stringstream iss;
    iss << format(pattern_, tz_converted);
    std::string ret_str = iss.str();
    ret_str = ret_str.substr(0, ret_str.find('.'));

    size_t length = 20;

    char* ret = reinterpret_cast<char*>(
        gdv_fn_context_arena_malloc(reinterpret_cast<int64_t>(context), (20)));

    memcpy(ret, ret_str.c_str(), length);

    return ret;
  };
  ~FromUtcTimestampUtf8Holder() override = default;

  FromUtcTimestampUtf8Holder(int32_t suppress_errors)
      : FromUtcFunctionsHolder<FromUtcTimestampUtf8Holder>(suppress_errors) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<FromUtcTimestampUtf8Holder>* holder) {
    const std::string function_name("from_utc_timestamp");

    if (node.children().empty()) {
      return Status::Invalid(
          "from_utc_timestamp function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      return FromUtcFunctionsHolder<FromUtcTimestampUtf8Holder>::Make(node, holder,
                                                                      function_name);
    }

    const int32_t not_supress_errors = 0;
    return Make(not_supress_errors, holder);
  }

  static Status Make(int32_t suppress_errors,
                     std::shared_ptr<FromUtcTimestampUtf8Holder>* holder) {
    return FromUtcFunctionsHolder<FromUtcTimestampUtf8Holder>::Make(suppress_errors,
                                                                    holder);
  }
};

/// Function Holder for 'from_utc_timestamp' from int32
class GANDIVA_EXPORT FromUtcTimestampInt32Holder
    : public gandiva::FromUtcFunctionsHolder<FromUtcTimestampInt32Holder> {
 public:
  virtual const char* operator()(ExecutionContext* context, int32_t in_data,
                                 const char* tz, bool in_valid, bool* out_valid) {
    *out_valid = false;

    if (!in_valid) {
      return "";
    }

    // int32 needs to be treated as seconds or it will overflow
    std::chrono::system_clock::time_point tp{std::chrono::seconds{in_data}};

    auto tz_converted = arrow_vendored::date::make_zoned(tz, tp);

    std::stringstream iss;
    iss << format(pattern_, tz_converted);
    std::string ret_str = iss.str();
    ret_str = ret_str.substr(0, ret_str.find('.'));

    size_t length = 20;

    char* ret = reinterpret_cast<char*>(
        gdv_fn_context_arena_malloc(reinterpret_cast<int64_t>(context), (20)));

    memcpy(ret, ret_str.c_str(), length);

    return ret;
  };
  ~FromUtcTimestampInt32Holder() override = default;

  FromUtcTimestampInt32Holder(int32_t suppress_errors)
      : FromUtcFunctionsHolder<FromUtcTimestampInt32Holder>(suppress_errors) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<FromUtcTimestampInt32Holder>* holder) {
    const std::string function_name("from_utc_timestamp");

    if (node.children().empty()) {
      return Status::Invalid(
          "from_utc_timestamp function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      return FromUtcFunctionsHolder<FromUtcTimestampInt32Holder>::Make(node, holder,
                                                                       function_name);
    }

    const int32_t not_supress_errors = 0;
    return Make(not_supress_errors, holder);
  }

  static Status Make(int32_t suppress_errors,
                     std::shared_ptr<FromUtcTimestampInt32Holder>* holder) {
    return FromUtcFunctionsHolder<FromUtcTimestampInt32Holder>::Make(suppress_errors,
                                                                     holder);
  }
};

/// Function Holder for 'from_utc_timestamp' from int64
class GANDIVA_EXPORT FromUtcTimestampInt64Holder
    : public gandiva::FromUtcFunctionsHolder<FromUtcTimestampInt64Holder> {
 public:
  ~FromUtcTimestampInt64Holder() override = default;

  FromUtcTimestampInt64Holder(int32_t suppress_errors)
      : FromUtcFunctionsHolder<FromUtcTimestampInt64Holder>(suppress_errors) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<FromUtcTimestampInt64Holder>* holder) {
    const std::string function_name("from_utc_timestamp");

    if (node.children().empty()) {
      return Status::Invalid(
          "from_utc_timestamp function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      return FromUtcFunctionsHolder<FromUtcTimestampInt64Holder>::Make(node, holder,
                                                                       function_name);
    }

    const int32_t not_supress_errors = 0;
    return Make(not_supress_errors, holder);
  }

  static Status Make(int32_t suppress_errors,
                     std::shared_ptr<FromUtcTimestampInt64Holder>* holder) {
    return FromUtcFunctionsHolder<FromUtcTimestampInt64Holder>::Make(suppress_errors,
                                                                     holder);
  }
};

/// Function Holder for 'from_utc_timestamp' from float32
class GANDIVA_EXPORT FromUtcTimestampFloat32Holder
    : public gandiva::FromUtcFunctionsHolder<FromUtcTimestampFloat32Holder> {
 public:
  virtual const char* operator()(ExecutionContext* context, float in_data, const char* tz,
                                 bool in_valid, bool* out_valid) {
    *out_valid = false;

    if (!in_valid) {
      return "";
    }

    std::chrono::seconds seconds = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::duration<float>(in_data));

    std::chrono::system_clock::time_point tp{seconds};

    auto tz_converted = arrow_vendored::date::make_zoned(tz, tp);

    std::stringstream iss;
    iss << format(pattern_, tz_converted);
    std::string ret_str = iss.str();
    ret_str = ret_str.substr(0, ret_str.find('.'));

    int length = 20;
    // Allocate length = 20;
    char* ret = reinterpret_cast<char*>(
        gdv_fn_context_arena_malloc(reinterpret_cast<int64_t>(context), (20)));

    memcpy(ret, ret_str.c_str(), length);

    return ret;
  };
  ~FromUtcTimestampFloat32Holder() override = default;

  FromUtcTimestampFloat32Holder(int32_t suppress_errors)
      : FromUtcFunctionsHolder<FromUtcTimestampFloat32Holder>(suppress_errors) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<FromUtcTimestampFloat32Holder>* holder) {
    const std::string function_name("from_utc_timestamp");

    if (node.children().empty()) {
      return Status::Invalid(
          "from_utc_timestamp function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      return FromUtcFunctionsHolder<FromUtcTimestampFloat32Holder>::Make(node, holder,
                                                                         function_name);
    }

    const int32_t not_supress_errors = 0;
    return Make(not_supress_errors, holder);
  }

  static Status Make(int32_t suppress_errors,
                     std::shared_ptr<FromUtcTimestampFloat32Holder>* holder) {
    return FromUtcFunctionsHolder<FromUtcTimestampFloat32Holder>::Make(suppress_errors,
                                                                       holder);
  }
};

/// Function Holder for 'from_utc_timestamp' from float64
class GANDIVA_EXPORT FromUtcTimestampFloat64Holder
    : public gandiva::FromUtcFunctionsHolder<FromUtcTimestampFloat64Holder> {
 public:
  virtual const char* operator()(ExecutionContext* context, double in_data,
                                 const char* tz, bool in_valid, bool* out_valid) {
    *out_valid = false;

    if (!in_valid) {
      return "";
    }

    std::chrono::seconds seconds = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::duration<double>(in_data));

    std::chrono::system_clock::time_point tp{seconds};

    auto tz_converted = arrow_vendored::date::make_zoned(tz, tp);

    std::stringstream iss;
    iss << format(pattern_, tz_converted);
    std::string ret_str = iss.str();
    ret_str = ret_str.substr(0, ret_str.find('.'));

    size_t length = 20;

    char* ret = reinterpret_cast<char*>(
        gdv_fn_context_arena_malloc(reinterpret_cast<int64_t>(context), (20)));

    memcpy(ret, ret_str.c_str(), length);

    return ret;
  };
  ~FromUtcTimestampFloat64Holder() override = default;

  FromUtcTimestampFloat64Holder(int32_t suppress_errors)
      : FromUtcFunctionsHolder<FromUtcTimestampFloat64Holder>(suppress_errors) {}

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<FromUtcTimestampFloat64Holder>* holder) {
    const std::string function_name("from_utc_timestamp");

    if (node.children().empty()) {
      return Status::Invalid(
          "from_utc_timestamp function requires at least one parameter");
    }

    if (node.children().size() > 1) {
      // It means that the function called was
      return FromUtcFunctionsHolder<FromUtcTimestampFloat64Holder>::Make(node, holder,
                                                                         function_name);
    }

    const int32_t not_supress_errors = 0;
    return Make(not_supress_errors, holder);
  }

  static Status Make(int32_t suppress_errors,
                     std::shared_ptr<FromUtcTimestampFloat64Holder>* holder) {
    return FromUtcFunctionsHolder<FromUtcTimestampFloat64Holder>::Make(suppress_errors,
                                                                       holder);
  }
};
}  // namespace gandiva
