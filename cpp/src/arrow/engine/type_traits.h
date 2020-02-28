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

#include "arrow/engine/expression.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace engine {

template <typename E>
using is_compare_expr = std::is_base_of<CmpOpExpr<E>, E>;

template <typename E, typename Ret = void>
using enable_if_compare_expr = enable_if_t<is_compare_expr<E>::value, Ret>;

template <typename E>
using is_relational_expr = std::is_base_of<RelExpr<E>, E>;

template <typename E, typename Ret = void>
using enable_if_relational_expr = enable_if_t<is_relational_expr<E>::value, Ret>;

}  // namespace engine
}  // namespace arrow
