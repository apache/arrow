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

#include <cstdint>

#include "arrow/engine/visibility.h"

namespace arrow {
namespace engine {

class ExprType;

/// Tag identifier for the expression type.
enum ExprKind : uint8_t {
  /// A Scalar literal, i.e. a constant.
  SCALAR_LITERAL,
  /// A Field reference in a schema.
  FIELD_REFERENCE,

  // Comparison operators,
  COMPARE_OP,

  /// Empty relation with a known schema.
  EMPTY_REL,
  /// Scan relational operator
  SCAN_REL,
  /// Projection relational operator
  PROJECTION_REL,
  /// Filter relational operator
  FILTER_REL,
};

class Expr;
class ScalarExpr;
class FieldRefExpr;

/// Tag identifier for comparison operators
enum CompareKind : uint8_t {
  EQUAL,
  NOT_EQUAL,
  GREATER_THAN,
  GREATER_THAN_EQUAL,
  LESS_THAN,
  LESS_THAN_EQUAL,
};

class CompareOpExpr;
class EqualExpr;
class NotEqualExpr;
class GreaterThanExpr;
class GreaterThanEqualExpr;
class LessThanExpr;
class LessThanEqualExpr;

class RelExpr;

class EmptyRelExpr;
class ScanRelExpr;
class ProjectionRelExpr;
class FilterRelExpr;

class Catalog;

}  // namespace engine
}  // namespace arrow
