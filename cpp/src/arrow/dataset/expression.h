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

// This API is EXPERIMENTAL.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/chunked_array.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace dataset {

/// An unbound expression which maps a single Datum to another Datum.
/// An expression is one of
/// - A literal Datum.
/// - A reference to a single (potentially nested) field of the input Datum.
/// - A call to a compute function, with arguments specified by other Expressions.
///   - Optionally, a explicitly selected Kernel of the function. If provided,
///     execution will skip function lookup and kernel dispatch and use that Kernel
///     directly.
class ARROW_DS_EXPORT Expression2 {
 public:
  struct Call {
    std::string function;
    std::vector<Expression2> arguments;
    std::shared_ptr<compute::FunctionOptions> options;

    // post-Bind properties:
    const compute::Kernel* kernel = NULLPTR;
    compute::Function::Kind function_kind;
  };

  std::string ToString() const;
  bool Equals(const Expression2& other) const;
  size_t hash() const;
  struct Hash {
    size_t operator()(const Expression2& expr) const { return expr.hash(); }
  };

  /// Bind this expression to the given input type, looking up Kernels and field types.
  /// Some expression simplification may be performed and implicit casts will be inserted.
  /// Any state necessary for execution will be initialized and returned.
  using StateAndBound = std::pair<Expression2, std::shared_ptr<ExpressionState>>;
  Result<StateAndBound> Bind(ValueDescr in, compute::ExecContext* = NULLPTR) const;
  Result<StateAndBound> Bind(const Schema& in_schema,
                             compute::ExecContext* = NULLPTR) const;

  /// Return true if all an expression's field references have explicit ValueDescr and all
  /// of its functions' kernels are looked up.
  bool IsBound() const;

  /// Return true if this expression is composed only of Scalar literals, field
  /// references, and calls to ScalarFunctions.
  bool IsScalarExpression() const;

  /// Return true if this expression is literal and entirely null.
  bool IsNullLiteral() const;

  /// Return true if this expression could evaluate to true.
  bool IsSatisfiable() const;

  // XXX someday
  // Result<PipelineGraph> GetPipelines();

  const Call* call() const;
  const Datum* literal() const;
  const FieldRef* field_ref() const;

  const ValueDescr& descr() const { return descr_; }

  using Impl = util::variant<Datum, FieldRef, Call>;

  explicit Expression2(std::shared_ptr<Impl> impl, ValueDescr descr = {})
      : impl_(std::move(impl)), descr_(std::move(descr)) {}

 private:
  std::shared_ptr<Impl> impl_;
  ValueDescr descr_;
  // XXX someday
  // NullGeneralization::type evaluates_to_null_;

  friend bool Identical(const Expression2& l, const Expression2& r);

  ARROW_EXPORT friend void PrintTo(const Expression2&, std::ostream*);
};

struct ExpressionState {
  virtual ~ExpressionState() = default;

  // Produce another instance of ExpressionState which may be safely used from a
  // different thread
  static Result<std::shared_ptr<ExpressionState>> Clone(
      const std::shared_ptr<ExpressionState>&, const Expression2&, compute::ExecContext*);
};

inline bool operator==(const Expression2& l, const Expression2& r) { return l.Equals(r); }
inline bool operator!=(const Expression2& l, const Expression2& r) {
  return !l.Equals(r);
}

// Factories

inline Expression2 call(std::string function, std::vector<Expression2> arguments,
                        std::shared_ptr<compute::FunctionOptions> options = NULLPTR) {
  Expression2::Call call;
  call.function = std::move(function);
  call.arguments = std::move(arguments);
  call.options = std::move(options);
  return Expression2(std::make_shared<Expression2::Impl>(std::move(call)));
}

template <typename Options, typename = typename std::enable_if<std::is_base_of<
                                compute::FunctionOptions, Options>::value>::type>
Expression2 call(std::string function, std::vector<Expression2> arguments,
                 Options options) {
  return call(std::move(function), std::move(arguments),
              std::make_shared<Options>(std::move(options)));
}

inline Expression2 project(std::vector<std::string> names,
                           std::vector<Expression2> values) {
  return call("struct", std::move(values), compute::StructOptions{std::move(names)});
}

template <typename... Args>
Expression2 field_ref(Args&&... args) {
  return Expression2(
      std::make_shared<Expression2::Impl>(FieldRef(std::forward<Args>(args)...)));
}

template <typename Arg>
Expression2 literal(Arg&& arg) {
  Datum lit(std::forward<Arg>(arg));
  ValueDescr descr = lit.descr();
  return Expression2(std::make_shared<Expression2::Impl>(std::move(lit)),
                     std::move(descr));
}

// Simplification passes

/// Weak canonicalization which establishes guarantees for subsequent passes. Even
/// equivalent Expressions may result in different canonicalized expressions.
/// TODO this could be a strong canonicalization
ARROW_DS_EXPORT
Result<Expression2> Canonicalize(Expression2 expr);

/// Simplify Expressions based on literal arguments (for example, add(null, x) will always
/// be null so replace the call with a null literal). Includes early evaluation of all
/// calls whose arguments are entirely literal.
ARROW_DS_EXPORT
Result<Expression2> FoldConstants(Expression2 expr, ExpressionState*);

ARROW_DS_EXPORT
Result<std::unordered_map<FieldRef, Datum, FieldRef::Hash>> ExtractKnownFieldValues(
    const Expression2& guaranteed_true_predicate);

ARROW_DS_EXPORT
Result<Expression2> ReplaceFieldsWithKnownValues(
    const std::unordered_map<FieldRef, Datum, FieldRef::Hash>& known_values,
    Expression2 expr);

/// Simplify an expression by replacing subexpressions based on a guarantee:
/// a boolean expression which is guaranteed to evaluate to `true`. For example, this is
/// used to remove redundant function calls from a filter expression or to replace a
/// reference to a constant-value field with a literal.
ARROW_DS_EXPORT
Result<Expression2> SimplifyWithGuarantee(Expression2 expr, ExpressionState*,
                                          const Expression2& guaranteed_true_predicate);

// Execution

/// Execute a scalar expression against the provided state and input Datum. This
/// expression must be bound.
Result<Datum> ExecuteScalarExpression(const Expression2&, ExpressionState*,
                                      const Datum& input,
                                      compute::ExecContext* exec_context = NULLPTR);

// Serialization

ARROW_DS_EXPORT
Result<std::shared_ptr<Buffer>> Serialize(const Expression2&);

ARROW_DS_EXPORT
Result<Expression2> Deserialize(const Buffer&);

}  // namespace dataset
}  // namespace arrow
