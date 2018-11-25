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

#ifndef GANDIVA_EXPR_EXPRESSION_H
#define GANDIVA_EXPR_EXPRESSION_H

#include <string>

#include "gandiva/arrow.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief An expression tree with a root node, and a result field.
class Expression {
 public:
  Expression(const NodePtr root, const FieldPtr result) : root_(root), result_(result) {}

  virtual ~Expression() = default;

  const NodePtr& root() const { return root_; }

  const FieldPtr& result() const { return result_; }

  std::string ToString();

 private:
  const NodePtr root_;
  const FieldPtr result_;
};

}  // namespace gandiva

#endif  // GANDIVA_EXPR_EXPRESSION_H
