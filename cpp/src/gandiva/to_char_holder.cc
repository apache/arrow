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

#include "gandiva/to_char_holder.h"

namespace gandiva {
Status ToCharHolder::Make(const FunctionNode& node,
                          std::shared_ptr<ToCharHolder>* holder) {
  if (node.children().size() != 2) {
    return Status::Invalid("'to_char' function requires two parameters");
  }

  auto literal_pattern = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  if (literal_pattern == nullptr) {
    return Status::Invalid(
        "'to_char' function requires a literal as the second parameter");
  }

  auto literal_type = literal_pattern->return_type()->id();
  if (literal_type != arrow::Type::STRING && literal_type != arrow::Type::BINARY) {
    return Status::Invalid(
        "'to_char' function requires a string literal as the second parameter");
  }
  return Make(arrow::util::get<std::string>(literal_pattern->holder()), holder);
}

Status ToCharHolder::Make(const std::string& java_pattern,
                          std::shared_ptr<ToCharHolder>* holder) {
  auto lholder = std::shared_ptr<ToCharHolder>(
      new ToCharHolder(java_pattern.c_str(), java_pattern.size()));
  *holder = lholder;
  return Status::OK();
}
}  // namespace gandiva
