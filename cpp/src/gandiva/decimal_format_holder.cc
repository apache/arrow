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

#include "decimal_format_holder.h"

namespace gandiva {
static bool IsArrowStringLiteral(arrow::Type::type type) {
  return type == arrow::Type::STRING || type == arrow::Type::BINARY;
}

Status DecimalFormatHolder::Make(const FunctionNode& node,
                                 std::shared_ptr<DecimalFormatHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 2,
                  Status::Invalid("'to_number' function requires two parameters"));
  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());

  ARROW_RETURN_IF(literal == nullptr,
                  Status::Invalid("'to_number' function requires a"
                                  " literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid(
          "'to_number' function requires a string literal as the second parameter"));
  return DecimalFormatHolder::Make(arrow::util::get<std::string>(literal->holder()),
                                   holder);
}

Status DecimalFormatHolder::Make(const std::string& decimal_format,
                                 std::shared_ptr<DecimalFormatHolder>* holder) {
  auto lholder = std::shared_ptr<DecimalFormatHolder>(
      new DecimalFormatHolder(decimal_format.c_str(), decimal_format.size()));
  *holder = lholder;
  return Status::OK();
}

double DecimalFormatHolder::Parse(const char* number, int32_t number_size) {
  using arrow_vendored::fast_float::chars_format;
  using arrow_vendored::fast_float::from_chars;

  double answer;

  if (has_dolar_sign_) {
    number++;
    number_size--;
  }

  std::string res;

  for (int i = 0; i < number_size; ++i) {
    if (number[i] != ',') {
      res.push_back(number[i]);
    }
  }

  const char* res_ptr = res.c_str();
  from_chars(res_ptr, res_ptr + res.size(), answer, chars_format::fixed);
  return round_float64_int32(answer, maximumFractionDigits_);
}
}  // namespace gandiva
