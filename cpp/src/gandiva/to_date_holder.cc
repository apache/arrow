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

#include "gandiva/to_date_holder.h"

#include <algorithm>
#include <string>

#include "arrow/util/value_parsing.h"
#include "arrow/vendored/datetime.h"
#include "gandiva/date_utils.h"
#include "gandiva/execution_context.h"
#include "gandiva/node.h"
#include "gandiva/precompiled/epoch_time_point.h"

#include <boost/date_time.hpp>
#include <iostream>
namespace bt = boost::posix_time;

namespace gandiva {

Status ToDateHolder::Make(const FunctionNode& node,
                          std::shared_ptr<ToDateHolder>* holder) {
  if (node.children().size() != 2 && node.children().size() != 3) {
    return Status::Invalid("'to_date' function requires two or three parameters");
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
  auto pattern = arrow::util::get<std::string>(literal_pattern->holder());

  int suppress_errors = 0;
  if (node.children().size() == 3) {
    auto literal_suppress_errors =
        dynamic_cast<LiteralNode*>(node.children().at(2).get());
    if (literal_pattern == nullptr) {
      return Status::Invalid(
          "The (optional) third parameter to 'to_date' function needs to an integer "
          "literal to indicate whether to suppress the error");
    }

    literal_type = literal_suppress_errors->return_type()->id();
    if (literal_type != arrow::Type::INT32) {
      return Status::Invalid(
          "The (optional) third parameter to 'to_date' function needs to an integer "
          "literal to indicate whether to suppress the error");
    }
    suppress_errors = arrow::util::get<int>(literal_suppress_errors->holder());
  }

  return Make(pattern, suppress_errors, holder);
}

Status ToDateHolder::Make(const std::string& sql_pattern, int32_t suppress_errors,
                          std::shared_ptr<ToDateHolder>* holder) {
  std::shared_ptr<std::string> transformed_pattern;
  ARROW_RETURN_NOT_OK(DateUtils::ToInternalFormat(sql_pattern, &transformed_pattern));
  auto lholder = std::shared_ptr<ToDateHolder>(
      new ToDateHolder(*(transformed_pattern.get()), suppress_errors));
  *holder = lholder;
  return Status::OK();
}

std::time_t pt_to_time_t(const bt::ptime& pt)
{
  bt::ptime timet_start(boost::gregorian::date(1970,1,1));
  bt::time_duration diff = pt - timet_start;
  return diff.ticks();
}

int64_t ToDateHolder::operator()(ExecutionContext* context, const char* data,
                                 int data_len, bool in_valid, bool* out_valid) {
  *out_valid = false;
  if (!in_valid) {
    return 0;
  }

  std::string str;
  str.assign(data, data_len);
//  std::size_t found1 = str.find("T");
//  std::size_t found2 = str.find("'T'");
//  if (found1 != std::string::npos && found2 == std::string::npos) {
//    str = str.replace(found1, 1, "'T'");
//  }

  // Issues
  // 1. processes date that do not match the format.
  // 2. does not process time in format +08:00 (or) id.
  int64_t seconds_since_epoch = 0;

  std::istringstream is(str);
  auto pattern = std::locale(std::locale::classic(),new bt::time_input_facet(pattern_.c_str()));
  is.imbue(pattern);
  bt::ptime pt;
  is >> pt;
  if(pt != bt::ptime()) {
//    EpochTimePoint tp(pt_to_time_t(pt));
//    const auto& dayWithoutHoursAndSec = tp.ClearTimeOfDay();
//    return dayWithoutHoursAndSec.MillisSinceEpoch();
    
    return pt_to_time_t(pt);
  }


//  if (!::arrow::internal::ParseTimestampStrptime(
//        str.c_str(), str.length(), pattern_.c_str(),
//        /*ignore_time_in_day=*/true, /*allow_trailing_chars=*/false,
//        ::arrow::TimeUnit::SECOND, &seconds_since_epoch)) {
//    return_error(context, data, data_len);
//    return 0;
//  }

  *out_valid = true;
  return seconds_since_epoch * 1000;
}

void ToDateHolder::return_error(ExecutionContext* context, const char* data,
                                int data_len) {
  if (suppress_errors_ == 1) {
    return;
  }

  std::string err_msg =
      "Error parsing value " + std::string(data, data_len) + " for given format.";
  context->set_error_msg(err_msg.c_str());
}

}  // namespace gandiva
