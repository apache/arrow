/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_EXPR_TREE_BUILDER_H
#define GANDIVA_EXPR_TREE_BUILDER_H

#include <string>
#include <vector>

#include "gandiva/condition.h"
#include "gandiva/expression.h"

namespace gandiva {

/// \brief Tree Builder for a nested expression.
class TreeExprBuilder {
 public:
  /// \brief create a node on a literal.
  static NodePtr MakeLiteral(bool value);
  static NodePtr MakeLiteral(uint8_t value);
  static NodePtr MakeLiteral(uint16_t value);
  static NodePtr MakeLiteral(uint32_t value);
  static NodePtr MakeLiteral(uint64_t value);
  static NodePtr MakeLiteral(int8_t value);
  static NodePtr MakeLiteral(int16_t value);
  static NodePtr MakeLiteral(int32_t value);
  static NodePtr MakeLiteral(int64_t value);
  static NodePtr MakeLiteral(float value);
  static NodePtr MakeLiteral(double value);
  static NodePtr MakeStringLiteral(const std::string &value);
  static NodePtr MakeBinaryLiteral(const std::string &value);

  /// \brief create a node on a null literal.
  /// returns null if data_type is null or if it's not a supported datatype.
  static NodePtr MakeNull(DataTypePtr data_type);

  /// \brief create a node on arrow field.
  /// returns null if input is null.
  static NodePtr MakeField(FieldPtr field);

  /// \brief create a node with a function.
  /// returns null if return_type is null
  static NodePtr MakeFunction(const std::string &name, const NodeVector &params,
                              DataTypePtr return_type);

  /// \brief create a node with an if-else expression.
  /// returns null if any of the inputs is null.
  static NodePtr MakeIf(NodePtr condition, NodePtr then_node, NodePtr else_node,
                        DataTypePtr result_type);

  /// \brief create a node with a boolean AND expression.
  static NodePtr MakeAnd(const NodeVector &children);

  /// \brief create a node with a boolean OR expression.
  static NodePtr MakeOr(const NodeVector &children);

  /// \brief create an expression with the specified root_node, and the
  /// result written to result_field.
  /// returns null if the result_field is null.
  static ExpressionPtr MakeExpression(NodePtr root_node, FieldPtr result_field);

  /// \brief convenience function for simple function expressions.
  /// returns null if the out_field is null.
  static ExpressionPtr MakeExpression(const std::string &function,
                                      const FieldVector &in_fields, FieldPtr out_field);

  /// \brief create a condition with the specified root_node
  static ConditionPtr MakeCondition(NodePtr root_node);

  /// \brief convenience function for simple function conditions.
  static ConditionPtr MakeCondition(const std::string &function,
                                    const FieldVector &in_fields);
};

}  // namespace gandiva

#endif  // GANDIVA_EXPR_TREE_BUILDER_H
