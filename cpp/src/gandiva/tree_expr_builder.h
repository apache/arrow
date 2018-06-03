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
#include "gandiva/node.h"
#include "gandiva/expression.h"

namespace gandiva {

/// \brief Tree Builder for a nested expression.
class TreeExprBuilder {
 public:
  /// \brief create a node on arrow field.
  static NodePtr MakeField(FieldPtr field);

  /// \brief create a node with a function.
  static NodePtr MakeFunction(const std::string &name,
                              const NodeVector &children,
                              DataTypePtr return_type);

  /// \brief create an expression with the specified root_node, and the
  /// result written to result_field.
  static ExpressionPtr MakeExpression(NodePtr root_node,
                                      FieldPtr result_field);

  /// \brief convenience function for simple function expressions.
  static ExpressionPtr MakeExpression(const std::string &function,
                                      const FieldVector &in_fields,
                                      FieldPtr out_field);
};

} // namespace gandiva

#endif //GANDIVA_EXPR_TREE_BUILDER_H
