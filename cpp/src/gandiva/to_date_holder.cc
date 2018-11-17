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

#include <algorithm>
#include <string>

#include "gandiva/date_utils.h"
#include "gandiva/execution_context.h"
#include "gandiva/node.h"
#include "gandiva/precompiled/date.h"
#include "gandiva/to_date_holder.h"

namespace gandiva {

Status ToDateHolder::Make(const FunctionNode& node,
                          std::shared_ptr<ToDateHolder>* holder) {
  if (node.children().size() != 3) {
    return Status::Invalid("'to_date' function requires three parameters");
  }

  auto literal_pattern = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  if (literal_pattern == nullptr) {
    return Status::Invalid(
        "'to_date' function requires a literal as the second parameter");
  }

  auto literal_type = literal_pattern->return_type()->id();
  if (literal_type != arrow::Type::STRING && literal_type != arrow::Type::BINARY) {
    return Status::Invalid(
        "'to_date' function requires a string literal as the second parameter");
  }
  auto pattern = boost::get<std::string>(literal_pattern->holder());

  auto literal_suppress_errors = dynamic_cast<LiteralNode*>(node.children().at(2).get());
  if (literal_pattern == nullptr) {
    return Status::Invalid(
        "'to_date' function requires a int literal as the third parameter");
  }

  literal_type = literal_suppress_errors->return_type()->id();
  if (literal_type != arrow::Type::INT32) {
    return Status::Invalid(
        "'to_date' function requires a int literal as the third parameter");
  }
  auto suppress_errors = boost::get<int>(literal_suppress_errors->holder());
  return Make(pattern, suppress_errors, holder);
}

Status ToDateHolder::Make(const std::string& sql_pattern, int32_t suppress_errors,
                          std::shared_ptr<ToDateHolder>* holder) {
  std::shared_ptr<std::string> transformed_pattern;
  Status status = DateUtils::ToInternalFormat(sql_pattern, &transformed_pattern);
  ARROW_RETURN_NOT_OK(status);
  auto lholder = std::shared_ptr<ToDateHolder>(
      new ToDateHolder(*(transformed_pattern.get()), suppress_errors));
  *holder = lholder;
  return Status::OK();
}

int64_t ToDateHolder::operator()(ExecutionContext* context, const std::string& data,
                                 bool in_valid, bool* out_valid) {
  *out_valid = false;
  if (!in_valid) {
    return 0;
  }

  // Issues
  // 1. processes date that do not match the format.
  // 2. does not process time in format +08:00 (or) id.
  struct tm result = {};
  char* ret = strptime(data.c_str(), pattern_.c_str(), &result);
  if (ret == nullptr) {
    return_error(context, data);
    return 0;
  }
  *out_valid = true;
  // ignore the time part
  date::sys_seconds secs = date::sys_days(date::year(result.tm_year + 1900) /
                                          (result.tm_mon + 1) / result.tm_mday);
  int64_t seconds_since_epoch = secs.time_since_epoch().count();
  if (seconds_since_epoch == 0) {
    return_error(context, data);
    return 0;
  }
  return seconds_since_epoch * 1000;
}

void ToDateHolder::return_error(ExecutionContext* context, const std::string& data) {
  if (suppress_errors_ == 1) {
    return;
  }

  std::string err_msg = "Error parsing value " + data + " for given format.";
  context->set_error_msg(err_msg.c_str());
}

}  // namespace gandiva
