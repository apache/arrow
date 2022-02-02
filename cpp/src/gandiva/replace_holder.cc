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

#include "gandiva/replace_holder.h"

#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

Status ReplaceHolder::Make(const FunctionNode& node,
                           std::shared_ptr<ReplaceHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 3,
                  Status::Invalid("'replace' function requires three parameters"));

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'replace' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !(literal_type == arrow::Type::STRING || literal_type == arrow::Type::BINARY),
      Status::Invalid(
          "'replace' function requires a string literal as the second parameter"));

  return Make(arrow::util::get<std::string>(literal->holder()), holder);
}

Status ReplaceHolder::Make(const std::string& sql_pattern,
                           std::shared_ptr<ReplaceHolder>* holder) {
  auto lholder = std::shared_ptr<ReplaceHolder>(new ReplaceHolder(sql_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", sql_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

void ReplaceHolder::return_error(ExecutionContext* context, std::string& data,
                                 std::string& replace_string) {
  std::string err_msg = "Error replacing '" + replace_string + "' on the given string '" +
                        data + "' for the given pattern: " + pattern_;
  context->set_error_msg(err_msg.c_str());
}

}  // namespace gandiva
