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

#ifndef GANDIVA_EXPR_VALIDATOR_H
#define GANDIVA_EXPR_VALIDATOR_H

#include <string>
#include <unordered_map>

#include "boost/functional/hash.hpp"
#include "gandiva/arrow.h"
#include "gandiva/expression.h"
#include "gandiva/function_registry.h"
#include "gandiva/llvm_types.h"
#include "gandiva/node.h"
#include "gandiva/node_visitor.h"
#include "gandiva/status.h"

namespace gandiva {

class FunctionRegistry;

/// \brief Validates the entire expression tree including
/// data types, signatures and return types
class ExprValidator : public NodeVisitor {
 public:
  explicit ExprValidator(LLVMTypes* types, SchemaPtr schema)
      : types_(types), schema_(schema) {
    for (auto& field : schema_->fields()) {
      field_map_[field->name()] = field;
    }
  }

  /// \brief Validates the root node
  /// of an expression.
  /// 1. Data type of fields and literals.
  /// 2. Function signature is supported.
  /// 3. For if nodes that return types match
  ///    for if, then and else nodes.
  Status Validate(const ExpressionPtr& expr);

 private:
  Status Visit(const FieldNode& node) override;
  Status Visit(const FunctionNode& node) override;
  Status Visit(const IfNode& node) override;
  Status Visit(const LiteralNode& node) override;
  Status Visit(const BooleanNode& node) override;

  FunctionRegistry registry_;

  LLVMTypes* types_;

  SchemaPtr schema_;

  using FieldMap = std::unordered_map<std::string, FieldPtr, boost::hash<std::string>>;
  FieldMap field_map_;
};

}  // namespace gandiva

#endif  // GANDIVA_EXPR_VALIDATOR_H
