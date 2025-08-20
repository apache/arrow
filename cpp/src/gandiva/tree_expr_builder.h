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

#include <cmath>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "arrow/type.h"
#include "gandiva/condition.h"
#include "gandiva/decimal_scalar.h"
#include "gandiva/expression.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Tree Builder for a nested expression.
class GANDIVA_EXPORT TreeExprBuilder {
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
  static NodePtr MakeStringLiteral(const std::string& value);
  static NodePtr MakeBinaryLiteral(const std::string& value);
  static NodePtr MakeDecimalLiteral(const DecimalScalar128& value);

  /// \brief create a node on a null literal.
  /// returns null if data_type is null or if it's not a supported datatype.
  static NodePtr MakeNull(DataTypePtr data_type);

  /// \brief create a node on arrow field.
  /// returns null if input is null.
  static NodePtr MakeField(FieldPtr field);

  /// \brief create a node with a function.
  /// returns null if return_type is null
  static NodePtr MakeFunction(const std::string& name, const NodeVector& params,
                              DataTypePtr return_type);

  /// \brief create a node with an if-else expression.
  /// returns null if any of the inputs is null.
  static NodePtr MakeIf(NodePtr condition, NodePtr then_node, NodePtr else_node,
                        DataTypePtr result_type);

  /// \brief create a node with a boolean AND expression.
  static NodePtr MakeAnd(const NodeVector& children);

  /// \brief create a node with a boolean OR expression.
  static NodePtr MakeOr(const NodeVector& children);

  /// \brief create an expression with the specified root_node, and the
  /// result written to result_field.
  /// returns null if the result_field is null.
  static ExpressionPtr MakeExpression(NodePtr root_node, FieldPtr result_field);

  /// \brief convenience function for simple function expressions.
  /// returns null if the out_field is null.
  static ExpressionPtr MakeExpression(const std::string& function,
                                      const FieldVector& in_fields, FieldPtr out_field);

  /// \brief create a condition with the specified root_node
  static ConditionPtr MakeCondition(NodePtr root_node);

  /// \brief convenience function for simple function conditions.
  static ConditionPtr MakeCondition(const std::string& function,
                                    const FieldVector& in_fields);

  /// \brief creates an in expression
  static NodePtr MakeInExpressionInt32(NodePtr node,
                                       const std::unordered_set<int32_t>& constants);

  static NodePtr MakeInExpressionInt64(NodePtr node,
                                       const std::unordered_set<int64_t>& constants);

  static NodePtr MakeInExpressionDecimal(
      NodePtr node, std::unordered_set<gandiva::DecimalScalar128>& constants);

  static NodePtr MakeInExpressionString(NodePtr node,
                                        const std::unordered_set<std::string>& constants);

  static NodePtr MakeInExpressionBinary(NodePtr node,
                                        const std::unordered_set<std::string>& constants);

  /// \brief creates an in expression for float
  static NodePtr MakeInExpressionFloat(NodePtr node,
                                       const std::unordered_set<float>& constants);

  /// \brief creates an in expression for double
  static NodePtr MakeInExpressionDouble(NodePtr node,
                                        const std::unordered_set<double>& constants);

  /// \brief Date as s/millis since epoch.
  static NodePtr MakeInExpressionDate32(NodePtr node,
                                        const std::unordered_set<int32_t>& constants);

  /// \brief Date as millis/us/ns since epoch.
  static NodePtr MakeInExpressionDate64(NodePtr node,
                                        const std::unordered_set<int64_t>& constants);

  /// \brief Time as s/millis of day
  static NodePtr MakeInExpressionTime32(NodePtr node,
                                        const std::unordered_set<int32_t>& constants);

  /// \brief Time as millis/us/ns of day
  static NodePtr MakeInExpressionTime64(NodePtr node,
                                        const std::unordered_set<int64_t>& constants);

  /// \brief Timestamp as millis since epoch.
  static NodePtr MakeInExpressionTimeStamp(NodePtr node,
                                           const std::unordered_set<int64_t>& constants);

  static NodePtr MakePreEvalInExpression(NodePtr eval_expr,
                                         NodeVector condition_eval_exprs);
};

}  // namespace gandiva
