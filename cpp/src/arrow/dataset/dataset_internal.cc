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
#include "arrow/dataset/dataset_internal.h"

namespace arrow {
namespace dataset {

Result<compute::Expression> NormalizeDatasetExpression(const compute::Expression& expr,
                                                       const Schema& dataset_schema) {
  if (expr.IsBound()) {
    // TODO(GH-34345) We are just blindly assuming that the expression was bound to the
    // dataset schema and the correct function registry.
    return compute::RemoveNamedRefs(expr);
  } else {
    // TODO(GH-34344): We should not be using the default function registry here
    ARROW_ASSIGN_OR_RAISE(compute::Expression bound_expr, expr.Bind(dataset_schema));
    return compute::RemoveNamedRefs(std::move(bound_expr));
  }
}

}  // namespace dataset
}  // namespace arrow
