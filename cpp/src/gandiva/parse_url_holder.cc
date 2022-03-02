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

#include "gandiva/parse_url_holder.h"
namespace gandiva {
RE2 ParseUrlHolder::url_regex_(
    R"(^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?$)");

Status ParseUrlHolder::Make(const gandiva::FunctionNode& node,
                            std::shared_ptr<ParseUrlHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 2 && node.children().size() != 3,
                  Status::Invalid("'like' function requires two or three parameters"));
  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'parse_url' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid(
          "'parse_url' function requires a string literal as the second parameter"));

  if (node.children().size() == 2) {
    return Make(arrow::util::get<std::string>(literal->holder()), holder);
  } else {
    auto key_literal = dynamic_cast<LiteralNode*>(node.children().at(2).get());
    ARROW_RETURN_IF(
        !IsArrowStringLiteral(key_literal->return_type()->id()) && key_literal == nullptr,
        Status::Invalid("'parse_url' function requires a string literal as the third "
                        "parameter for the query key"));
    return Make(arrow::util::get<std::string>(literal->holder()),
                arrow::util::get<std::string>(key_literal->holder()), holder);
  }
}
}  // namespace gandiva
