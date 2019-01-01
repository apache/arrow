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

#ifndef GANDIVA_VALUEVALIDITYPAIR_H
#define GANDIVA_VALUEVALIDITYPAIR_H

#include <vector>

#include "gandiva/gandiva_aliases.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Pair of vector/validities generated after decomposing an expression tree/subtree.
class GANDIVA_EXPORT ValueValidityPair {
 public:
  ValueValidityPair(const DexVector& validity_exprs, DexPtr value_expr)
      : validity_exprs_(validity_exprs), value_expr_(value_expr) {}

  ValueValidityPair(DexPtr validity_expr, DexPtr value_expr) : value_expr_(value_expr) {
    validity_exprs_.push_back(validity_expr);
  }

  explicit ValueValidityPair(DexPtr value_expr) : value_expr_(value_expr) {}

  const DexVector& validity_exprs() const { return validity_exprs_; }

  const DexPtr& value_expr() const { return value_expr_; }

 private:
  DexVector validity_exprs_;
  DexPtr value_expr_;
};

}  // namespace gandiva

#endif  // GANDIVA_VALUEVALIDITYPAIR_H
