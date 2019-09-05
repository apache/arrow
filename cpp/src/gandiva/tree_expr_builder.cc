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

#include "gandiva/tree_expr_builder.h"

#include <iostream>
#include <utility>

#include "gandiva/decimal_type_util.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/node.h"

namespace gandiva {

#define MAKE_LITERAL(atype, ctype)                                            \
  NodePtr TreeExprBuilder::MakeLiteral(ctype value) {                         \
    return std::make_shared<LiteralNode>(atype, LiteralHolder(value), false); \
  }

MAKE_LITERAL(arrow::boolean(), bool)
MAKE_LITERAL(arrow::int8(), int8_t)
MAKE_LITERAL(arrow::int16(), int16_t)
MAKE_LITERAL(arrow::int32(), int32_t)
MAKE_LITERAL(arrow::int64(), int64_t)
MAKE_LITERAL(arrow::uint8(), uint8_t)
MAKE_LITERAL(arrow::uint16(), uint16_t)
MAKE_LITERAL(arrow::uint32(), uint32_t)
MAKE_LITERAL(arrow::uint64(), uint64_t)
MAKE_LITERAL(arrow::float32(), float)
MAKE_LITERAL(arrow::float64(), double)

NodePtr TreeExprBuilder::MakeStringLiteral(const std::string& value) {
  return std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(value), false);
}

NodePtr TreeExprBuilder::MakeBinaryLiteral(const std::string& value) {
  return std::make_shared<LiteralNode>(arrow::binary(), LiteralHolder(value), false);
}

NodePtr TreeExprBuilder::MakeDecimalLiteral(const DecimalScalar128& value) {
  return std::make_shared<LiteralNode>(arrow::decimal(value.precision(), value.scale()),
                                       LiteralHolder(value), false);
}

NodePtr TreeExprBuilder::MakeNull(DataTypePtr data_type) {
  static const std::string empty;

  if (data_type == nullptr) {
    return nullptr;
  }

  switch (data_type->id()) {
    case arrow::Type::BOOL:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder(false), true);
    case arrow::Type::INT8:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int8_t)0), true);
    case arrow::Type::INT16:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int16_t)0), true);
    case arrow::Type::INT32:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int32_t)0), true);
    case arrow::Type::INT64:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int64_t)0), true);
    case arrow::Type::UINT8:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((uint8_t)0), true);
    case arrow::Type::UINT16:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((uint16_t)0), true);
    case arrow::Type::UINT32:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((uint32_t)0), true);
    case arrow::Type::UINT64:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((uint64_t)0), true);
    case arrow::Type::FLOAT:
      return std::make_shared<LiteralNode>(data_type,
                                           LiteralHolder(static_cast<float>(0)), true);
    case arrow::Type::DOUBLE:
      return std::make_shared<LiteralNode>(data_type,
                                           LiteralHolder(static_cast<double>(0)), true);
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder(empty), true);
    case arrow::Type::DATE64:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int64_t)0), true);
    case arrow::Type::TIME32:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int32_t)0), true);
    case arrow::Type::TIME64:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int64_t)0), true);
    case arrow::Type::TIMESTAMP:
      return std::make_shared<LiteralNode>(data_type, LiteralHolder((int64_t)0), true);
    case arrow::Type::DECIMAL: {
      std::shared_ptr<arrow::DecimalType> decimal_type =
          arrow::internal::checked_pointer_cast<arrow::DecimalType>(data_type);
      DecimalScalar128 literal(decimal_type->precision(), decimal_type->scale());
      return std::make_shared<LiteralNode>(data_type, LiteralHolder(literal), true);
    }
    default:
      return nullptr;
  }
}

NodePtr TreeExprBuilder::MakeField(FieldPtr field) {
  return NodePtr(new FieldNode(field));
}

NodePtr TreeExprBuilder::MakeFunction(const std::string& name, const NodeVector& params,
                                      DataTypePtr result_type) {
  if (result_type == nullptr) {
    return nullptr;
  }
  return std::make_shared<FunctionNode>(name, params, result_type);
}

NodePtr TreeExprBuilder::MakeIf(NodePtr condition, NodePtr then_node, NodePtr else_node,
                                DataTypePtr result_type) {
  if (condition == nullptr || then_node == nullptr || else_node == nullptr ||
      result_type == nullptr) {
    return nullptr;
  }
  return std::make_shared<IfNode>(condition, then_node, else_node, result_type);
}

NodePtr TreeExprBuilder::MakeAnd(const NodeVector& children) {
  return std::make_shared<BooleanNode>(BooleanNode::AND, children);
}

NodePtr TreeExprBuilder::MakeOr(const NodeVector& children) {
  return std::make_shared<BooleanNode>(BooleanNode::OR, children);
}

// set this to true to print expressions for debugging purposes
static bool print_expr = false;

ExpressionPtr TreeExprBuilder::MakeExpression(NodePtr root_node, FieldPtr result_field) {
  if (result_field == nullptr) {
    return nullptr;
  }
  if (print_expr) {
    std::cout << "Expression: " << root_node->ToString() << "\n";
  }
  return ExpressionPtr(new Expression(root_node, result_field));
}

ExpressionPtr TreeExprBuilder::MakeExpression(const std::string& function,
                                              const FieldVector& in_fields,
                                              FieldPtr out_field) {
  if (out_field == nullptr) {
    return nullptr;
  }
  std::vector<NodePtr> field_nodes;
  for (auto& field : in_fields) {
    auto node = MakeField(field);
    field_nodes.push_back(node);
  }
  auto func_node = MakeFunction(function, field_nodes, out_field->type());
  return MakeExpression(func_node, out_field);
}

ConditionPtr TreeExprBuilder::MakeCondition(NodePtr root_node) {
  if (root_node == nullptr) {
    return nullptr;
  }
  if (print_expr) {
    std::cout << "Condition: " << root_node->ToString() << "\n";
  }

  return ConditionPtr(new Condition(root_node));
}

ConditionPtr TreeExprBuilder::MakeCondition(const std::string& function,
                                            const FieldVector& in_fields) {
  std::vector<NodePtr> field_nodes;
  for (auto& field : in_fields) {
    auto node = MakeField(field);
    field_nodes.push_back(node);
  }

  auto func_node = MakeFunction(function, field_nodes, arrow::boolean());
  return ConditionPtr(new Condition(func_node));
}

#define MAKE_IN(NAME, ctype)                                        \
  NodePtr TreeExprBuilder::MakeInExpression##NAME(                  \
      NodePtr node, const std::unordered_set<ctype>& values) {      \
    return std::make_shared<InExpressionNode<ctype>>(node, values); \
  }

MAKE_IN(Int32, int32_t);
MAKE_IN(Int64, int64_t);
MAKE_IN(Date32, int32_t);
MAKE_IN(Date64, int64_t);
MAKE_IN(TimeStamp, int64_t);
MAKE_IN(Time32, int32_t);
MAKE_IN(Time64, int64_t);
MAKE_IN(String, std::string);
MAKE_IN(Binary, std::string);

}  // namespace gandiva
