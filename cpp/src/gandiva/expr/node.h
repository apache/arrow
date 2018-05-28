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
#ifndef GANDIVA_EXPR_NODE_H
#define GANDIVA_EXPR_NODE_H

#include <string>
#include <vector>
#include "common/arrow.h"
#include "common/gandiva_aliases.h"
#include "codegen/func_descriptor.h"
#include "codegen/dex.h"
#include "codegen/value_validity_pair.h"

namespace gandiva {

class FunctionRegistry;
class Annotator;

/*
 * Represents a node in the expression tree. Validity and value are
 * in a joined state.
 */
class Node {
 public:
  explicit Node(DataTypePtr return_type)
    : return_type_(return_type) { }

  const DataTypePtr &return_type() { return return_type_; }

  /*
   * Called during code generation to separate out validity and value.
   */
  virtual ValueValidityPairPtr Decompose(const FunctionRegistry &registry,
                                         Annotator &annotator) = 0;

 protected:
  DataTypePtr return_type_;
};

/*
 * Used to represent an arrow field.
 */
class FieldNode : public Node {
 public:
  explicit FieldNode(FieldPtr field)
    : Node(field->type()), field_(field) {}

  ValueValidityPairPtr Decompose(const FunctionRegistry &registry,
                                 Annotator &annotator) override;

 private:
  FieldPtr field_;
};

/*
 * Used to represent functions
 */
class FunctionNode : public Node {
 public:
  FunctionNode(FuncDescriptorPtr desc,
               const NodeVector &children,
               DataTypePtr retType)
    : Node(retType), desc_(desc), children_(children) { }

  ValueValidityPairPtr Decompose(const FunctionRegistry &registry,
                                 Annotator &annotator) override;

  const FuncDescriptorPtr &func_descriptor() { return desc_; }

  static NodePtr CreateFunction(const std::string &name,
                                const NodeVector &children,
                                DataTypePtr retType);

 private:
  FuncDescriptorPtr desc_;
  NodeVector children_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_NODE_H
