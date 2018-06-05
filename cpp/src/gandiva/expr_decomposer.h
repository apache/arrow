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
#ifndef GANDIVA_EXPR_DECOMPOSER_H
#define GANDIVA_EXPR_DECOMPOSER_H

#include <utility>

#include "gandiva/expression.h"
#include "codegen/node_visitor.h"
#include "codegen/node.h"

namespace gandiva {

class FunctionRegistry;
class Annotator;

/// \brief Decomposes an expression tree to seperate out the validity and
/// value expressions.
class ExprDecomposer : public NodeVisitor {
 public:
  explicit ExprDecomposer(const FunctionRegistry &registry,
                          Annotator &annotator)
    : registry_(registry),
      annotator_(annotator) {}

  ValueValidityPairPtr Decompose(const Node &root) {
    root.Accept(*this);
    return result();
  }

 private:
  void Visit(const FieldNode &node) override;
  void Visit(const FunctionNode &node) override;
  void Visit(const IfNode &node) override;
  void Visit(const LiteralNode &node) override;

  ValueValidityPairPtr result() { return std::move(result_); }

  const FunctionRegistry &registry_;
  Annotator &annotator_;
  ValueValidityPairPtr result_;
};

} // namespace gandiva

#endif //GANDIVA_EXPR_DECOMPOSER_H
