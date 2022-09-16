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
#include "gandiva/arrow.h"
#include "gandiva/function_registry.h"
#include "gandiva/node.h"
#include "gandiva/node_visitor.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Infers the types of untyped nodes
class GANDIVA_EXPORT TypeInferenceVisitor : public NodeVisitor {
 public:
  explicit TypeInferenceVisitor(SchemaPtr schema) : schema_(schema) {
    for (auto& field : schema_->fields()) {
      field_map_[field->name()] = field;
    }
  }

  /// \brief Infers the types of untyped nodes and returns a fully typed AST
  Status Infer(NodePtr input, NodePtr* result);

 private:
  Status Visit(const FieldNode& node) override;
  Status Visit(const FunctionNode& node) override;
  Status Visit(const IfNode& node) override;
  Status Visit(const LiteralNode& node) override;
  Status Visit(const BooleanNode& node) override;
  Status Visit(const InExpressionNode<int32_t>& node) override;
  Status Visit(const InExpressionNode<int64_t>& node) override;
  Status Visit(const InExpressionNode<float>& node) override;
  Status Visit(const InExpressionNode<double>& node) override;
  Status Visit(const InExpressionNode<gandiva::DecimalScalar128>& node) override;
  Status Visit(const InExpressionNode<std::string>& node) override;

  FunctionRegistry registry_;

  SchemaPtr schema_;

  std::unordered_map<std::string, FieldPtr> field_map_;

  /// Holds the result node for each visit
  NodePtr result_;

  /// Adds default types for untyped literals, used in the second pass
  bool tag_default_type_ = false;

  /// If all nodes are typed already, used for early return
  bool all_typed_ = false;
};
}  // namespace gandiva
