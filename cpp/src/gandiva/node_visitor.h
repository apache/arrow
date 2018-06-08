// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GANDIVA_NODE_VISITOR_H
#define GANDIVA_NODE_VISITOR_H

#include "gandiva/logging.h"

namespace gandiva {

class FieldNode;
class FunctionNode;
class IfNode;
class LiteralNode;

/// \brief Visitor for nodes in the expression tree.
class NodeVisitor {
 public:
  virtual void Visit(const FieldNode &node) = 0;
  virtual void Visit(const FunctionNode &node) = 0;
  virtual void Visit(const IfNode &node) = 0;
  virtual void Visit(const LiteralNode &node) = 0;
};

} // namespace gandiva

#endif //GANDIVA_NODE_VISITOR_H
