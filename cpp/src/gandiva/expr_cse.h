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

#include "gandiva/expression.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/native_function.h"

namespace gandiva {

class Condition;
class FunctionRegistry;

inline bool CanReuseNativeFunction(const NativeFunction& native_function) {
  return native_function.result_nullable_type() != kResultNullInternal &&
         !native_function.NeedsContext() && !native_function.NeedsFunctionHolder() &&
         !native_function.CanReturnErrors();
}

ExpressionVector FoldCommonSubexpressions(const FunctionRegistry& registry,
                                          const ExpressionVector& expressions);

ConditionPtr FoldCommonSubexpressions(const FunctionRegistry& registry,
                                      const ConditionPtr& condition);

}  // namespace gandiva
