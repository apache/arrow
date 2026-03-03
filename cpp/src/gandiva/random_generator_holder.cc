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

#include <limits>

#include "gandiva/node.h"

namespace gandiva {
Result<std::shared_ptr<RandomGeneratorHolder>> RandomGeneratorHolder::Make(
    const FunctionNode& node) {
  ARROW_RETURN_IF(node.children().size() > 1,
                  Status::Invalid("'random' function requires at most one parameter"));

  if (node.children().size() == 0) {
    return std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder());
  }

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(0).get());
  ARROW_RETURN_IF(literal == nullptr,
                  Status::Invalid("'random' function requires a literal as parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      literal_type != arrow::Type::INT32,
      Status::Invalid("'random' function requires an int32 literal as parameter"));

  return std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder(
      literal->is_null() ? 0 : std::get<int32_t>(literal->holder())));
}

Result<std::shared_ptr<RandomIntegerGeneratorHolder>> RandomIntegerGeneratorHolder::Make(
    const FunctionNode& node) {
  ARROW_RETURN_IF(
      node.children().size() > 2,
      Status::Invalid("'rand_integer' function requires at most two parameters"));

  // No params: full int32 range [INT32_MIN, INT32_MAX]
  if (node.children().empty()) {
    return std::shared_ptr<RandomIntegerGeneratorHolder>(
        new RandomIntegerGeneratorHolder());
  }

  // One param: range [0, range - 1]
  if (node.children().size() == 1) {
    auto literal = dynamic_cast<LiteralNode*>(node.children().at(0).get());
    ARROW_RETURN_IF(
        literal == nullptr,
        Status::Invalid("'rand_integer' function requires a literal as parameter"));
    ARROW_RETURN_IF(
        literal->return_type()->id() != arrow::Type::INT32,
        Status::Invalid(
            "'rand_integer' function requires an int32 literal as parameter"));

    // NULL range defaults to INT32_MAX (full positive range)
    int32_t range = literal->is_null() ? std::numeric_limits<int32_t>::max()
                                       : std::get<int32_t>(literal->holder());
    ARROW_RETURN_IF(range <= 0,
                    Status::Invalid("'rand_integer' function range must be positive"));

    return std::shared_ptr<RandomIntegerGeneratorHolder>(
        new RandomIntegerGeneratorHolder(range));
  }

  // Two params: min, max [min, max] inclusive
  auto min_literal = dynamic_cast<LiteralNode*>(node.children().at(0).get());
  auto max_literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());

  ARROW_RETURN_IF(
      min_literal == nullptr || max_literal == nullptr,
      Status::Invalid("'rand_integer' function requires literals as parameters"));
  ARROW_RETURN_IF(
      min_literal->return_type()->id() != arrow::Type::INT32 ||
          max_literal->return_type()->id() != arrow::Type::INT32,
      Status::Invalid("'rand_integer' function requires int32 literals as parameters"));

  // NULL min defaults to 0, NULL max defaults to INT32_MAX
  int32_t min_val = min_literal->is_null() ? 0 : std::get<int32_t>(min_literal->holder());
  int32_t max_val = max_literal->is_null() ? std::numeric_limits<int32_t>::max()
                                           : std::get<int32_t>(max_literal->holder());

  ARROW_RETURN_IF(min_val > max_val,
                  Status::Invalid("'rand_integer' function min must be <= max"));

  return std::shared_ptr<RandomIntegerGeneratorHolder>(
      new RandomIntegerGeneratorHolder(min_val, max_val));
}

}  // namespace gandiva
