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

#include "gandiva/random_generator_holder.h"
#include "gandiva/node.h"

namespace gandiva {
Status RandomGeneratorHolder::Make(const FunctionNode& node,
                                   std::shared_ptr<RandomGeneratorHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() > 1,
                  Status::Invalid("'random' function requires at most one parameter"));

  if (node.children().size() == 0) {
    *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder());
    return Status::OK();
  }

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(0).get());
  ARROW_RETURN_IF(literal == nullptr,
                  Status::Invalid("'random' function requires a literal as parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      literal_type != arrow::Type::INT32,
      Status::Invalid("'random' function requires an int32 literal as parameter"));

  *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder(
      literal->is_null() ? 0 : arrow::util::get<int32_t>(literal->holder())));
  return Status::OK();
}
}  // namespace gandiva
