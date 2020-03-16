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

#include <type_traits>

#include "arrow/engine/type_fwd.h"

namespace arrow {
namespace engine {

template <bool B, typename T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

template <typename T>
struct expr_traits;

template <>
struct expr_traits<ScalarExpr> {
  static constexpr auto kind_id = ExprKind::SCALAR_LITERAL;
};

template <>
struct expr_traits<FieldRefExpr> {
  static constexpr auto kind_id = ExprKind::FIELD_REFERENCE;
};

template <>
struct expr_traits<EqualExpr> {
  static constexpr auto kind_id = ExprKind::COMPARE_OP;
  static constexpr auto compare_kind_id = CompareKind::EQUAL;
};

template <>
struct expr_traits<NotEqualExpr> {
  static constexpr auto kind_id = ExprKind::COMPARE_OP;
  static constexpr auto compare_kind_id = CompareKind::NOT_EQUAL;
};

template <>
struct expr_traits<GreaterThanExpr> {
  static constexpr auto kind_id = ExprKind::COMPARE_OP;
  static constexpr auto compare_kind_id = CompareKind::GREATER_THAN;
};

template <>
struct expr_traits<GreaterThanEqualExpr> {
  static constexpr auto kind_id = ExprKind::COMPARE_OP;
  static constexpr auto compare_kind_id = CompareKind::GREATER_THAN_EQUAL;
};

template <>
struct expr_traits<LessThanExpr> {
  static constexpr auto kind_id = ExprKind::COMPARE_OP;
  static constexpr auto compare_kind_id = CompareKind::LESS_THAN;
};

template <>
struct expr_traits<LessThanEqualExpr> {
  static constexpr auto kind_id = ExprKind::COMPARE_OP;
  static constexpr auto compare_kind_id = CompareKind::LESS_THAN_EQUAL;
};

template <>
struct expr_traits<CountExpr> {
  static constexpr auto kind_id = ExprKind::AGGREGATE_FN_OP;
  static constexpr auto aggregate_kind_id = AggregateFnKind::COUNT;
};

template <>
struct expr_traits<EmptyRelExpr> {
  static constexpr auto kind_id = ExprKind::EMPTY_REL;
};

template <>
struct expr_traits<ScanRelExpr> {
  static constexpr auto kind_id = ExprKind::SCAN_REL;
};

template <>
struct expr_traits<ProjectionRelExpr> {
  static constexpr auto kind_id = ExprKind::PROJECTION_REL;
};

template <>
struct expr_traits<FilterRelExpr> {
  static constexpr auto kind_id = ExprKind::FILTER_REL;
};

template <typename E>
using is_compare_expr = std::is_base_of<CompareOpExpr, E>;

template <typename E, typename Ret = void>
using enable_if_compare_expr = enable_if_t<is_compare_expr<E>::value, Ret>;

template <typename E>
using is_aggregate_fn_expr = std::is_base_of<AggregateFnExpr, E>;

template <typename E, typename Ret = void>
using enable_if_aggregate_fn_expr = enable_if_t<is_aggregate_fn_expr<E>::value, Ret>;

template <typename E>
using is_relational_expr = std::is_base_of<RelExpr, E>;

template <typename E, typename Ret = void>
using enable_if_relational_expr = enable_if_t<is_relational_expr<E>::value, Ret>;

}  // namespace engine
}  // namespace arrow
