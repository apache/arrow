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

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle.

#pragma once

#include "arrow/compute/expression.h"
#include "arrow/util/visibility.h"

#include <vector>

namespace arrow {
namespace compute {

/// The concept "special form" is borrowed from Lisp
/// (https://courses.cs.northwestern.edu/325/readings/special-forms.html). Velox also uses
/// the same term. A special form behaves like a function call except that it has special
/// evaluation rules, mostly for arguments.
/// For example, the `if_else(cond, expr1, expr2)` special form first evaluates the
/// argument `cond` and obtains a boolean array:
///   [true, false, true, false]
/// then the argument `expr1` should ONLY be evaluated for row:
///   [0, 2]
/// and the argument `expr2` should ONLY be evaluated for row:
///   [1, 3]
/// Consider, if `expr1`/`expr2` has some observable side-effects (e.g., division by zero
/// error) on row [1, 3]/[0, 2], these side-effects would be undesirably observed if
/// evaluated using a regular function call, which always evaluates all its arguments
/// eagerly.
/// Other special forms include `case_when`, `and`, and `or`, etc.
/// In a vectorized execution engine, a special form normally takes advantage of
/// "selection vector" to mask rows of arguments to be evaluated.
class ARROW_EXPORT SpecialForm {
 public:
  /// A poor man's factory method to create a special form by name.
  /// TODO: More formal factory, a registry maybe?
  static Result<std::unique_ptr<SpecialForm>> Make(const std::string& name);

  virtual ~SpecialForm() = default;

  virtual Result<Datum> Execute(const Expression::Call& call, const ExecBatch& input,
                                ExecContext* exec_context) = 0;
};

}  // namespace compute
}  // namespace arrow
