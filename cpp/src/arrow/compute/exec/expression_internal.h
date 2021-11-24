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

#include "arrow/compute/exec/expression.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/registry.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

struct KnownFieldValues {
  std::unordered_map<FieldRef, Datum, FieldRef::Hash> map;
};

inline const Expression::Call* CallNotNull(const Expression& expr) {
  auto call = expr.call();
  DCHECK_NE(call, nullptr);
  return call;
}

inline std::vector<ValueDescr> GetDescriptors(const std::vector<Expression>& exprs) {
  std::vector<ValueDescr> descrs(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    DCHECK(exprs[i].IsBound());
    descrs[i] = exprs[i].descr();
  }
  return descrs;
}

inline std::vector<ValueDescr> GetDescriptors(const std::vector<Datum>& values) {
  std::vector<ValueDescr> descrs(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    descrs[i] = values[i].descr();
  }
  return descrs;
}

struct Comparison {
  enum type {
    NA = 0,
    EQUAL = 1,
    LESS = 2,
    GREATER = 4,
    NOT_EQUAL = LESS | GREATER,
    LESS_EQUAL = LESS | EQUAL,
    GREATER_EQUAL = GREATER | EQUAL,
  };

  static const type* Get(const std::string& function) {
    static std::unordered_map<std::string, type> map{
        {"equal", EQUAL},     {"not_equal", NOT_EQUAL},
        {"less", LESS},       {"less_equal", LESS_EQUAL},
        {"greater", GREATER}, {"greater_equal", GREATER_EQUAL},
    };

    auto it = map.find(function);
    return it != map.end() ? &it->second : nullptr;
  }

  static const type* Get(const Expression& expr) {
    if (auto call = expr.call()) {
      return Comparison::Get(call->function_name);
    }
    return nullptr;
  }

  // Execute a simple Comparison between scalars
  static Result<type> Execute(Datum l, Datum r) {
    if (!l.is_scalar() || !r.is_scalar()) {
      return Status::Invalid("Cannot Execute Comparison on non-scalars");
    }

    std::vector<Datum> arguments{std::move(l), std::move(r)};

    ARROW_ASSIGN_OR_RAISE(auto equal, compute::CallFunction("equal", arguments));

    if (!equal.scalar()->is_valid) return NA;
    if (equal.scalar_as<BooleanScalar>().value) return EQUAL;

    ARROW_ASSIGN_OR_RAISE(auto less, compute::CallFunction("less", arguments));

    if (!less.scalar()->is_valid) return NA;
    return less.scalar_as<BooleanScalar>().value ? LESS : GREATER;
  }

  // Given an Expression wrapped in casts which preserve ordering
  // (for example, cast(field_ref("i16"), to_type=int32())), unwrap the inner Expression.
  // This is used to destructure implicitly cast field_refs during Expression
  // simplification.
  static const Expression& StripOrderPreservingCasts(const Expression& expr) {
    auto call = expr.call();
    if (!call) return expr;
    if (call->function_name != "cast") return expr;

    const Expression& from = call->arguments[0];

    auto from_id = from.type()->id();
    auto to_id = expr.type()->id();

    if (is_floating(to_id)) {
      if (is_integer(from_id) || is_floating(from_id)) {
        return StripOrderPreservingCasts(from);
      }
      return expr;
    }

    if (is_unsigned_integer(to_id)) {
      if (is_unsigned_integer(from_id) && bit_width(to_id) >= bit_width(from_id)) {
        return StripOrderPreservingCasts(from);
      }
      return expr;
    }

    if (is_signed_integer(to_id)) {
      if (is_integer(from_id) && bit_width(to_id) >= bit_width(from_id)) {
        return StripOrderPreservingCasts(from);
      }
      return expr;
    }

    return expr;
  }

  static type GetFlipped(type op) {
    switch (op) {
      case NA:
        return NA;
      case EQUAL:
        return EQUAL;
      case LESS:
        return GREATER;
      case GREATER:
        return LESS;
      case NOT_EQUAL:
        return NOT_EQUAL;
      case LESS_EQUAL:
        return GREATER_EQUAL;
      case GREATER_EQUAL:
        return LESS_EQUAL;
    }
    DCHECK(false);
    return NA;
  }

  static std::string GetName(type op) {
    switch (op) {
      case NA:
        break;
      case EQUAL:
        return "equal";
      case LESS:
        return "less";
      case GREATER:
        return "greater";
      case NOT_EQUAL:
        return "not_equal";
      case LESS_EQUAL:
        return "less_equal";
      case GREATER_EQUAL:
        return "greater_equal";
    }
    return "na";
  }

  static std::string GetOp(type op) {
    switch (op) {
      case NA:
        DCHECK(false) << "unreachable";
        break;
      case EQUAL:
        return "==";
      case LESS:
        return "<";
      case GREATER:
        return ">";
      case NOT_EQUAL:
        return "!=";
      case LESS_EQUAL:
        return "<=";
      case GREATER_EQUAL:
        return ">=";
    }
    DCHECK(false);
    return "";
  }
};

inline const compute::CastOptions* GetCastOptions(const Expression::Call& call) {
  if (call.function_name != "cast") return nullptr;
  return checked_cast<const compute::CastOptions*>(call.options.get());
}

inline bool IsSetLookup(const std::string& function) {
  return function == "is_in" || function == "index_in";
}

inline const compute::MakeStructOptions* GetMakeStructOptions(
    const Expression::Call& call) {
  if (call.function_name != "make_struct") return nullptr;
  return checked_cast<const compute::MakeStructOptions*>(call.options.get());
}

/// A helper for unboxing an Expression composed of associative function calls.
/// Such expressions can frequently be rearranged to a semantically equivalent
/// expression for more optimal execution or more straightforward manipulation.
/// For example, (a + ((b + 3) + 4)) is equivalent to (((4 + 3) + a) + b) and the latter
/// can be trivially constant-folded to ((7 + a) + b).
struct FlattenedAssociativeChain {
  /// True if a chain was already a left fold.
  bool was_left_folded = true;

  /// All "branch" expressions in a flattened chain. For example given (a + ((b + 3) + 4))
  /// exprs would be [(a + ((b + 3) + 4)), ((b + 3) + 4), (b + 3)]
  std::vector<Expression> exprs;

  /// All "leaf" expressions in a flattened chain. For example given (a + ((b + 3) + 4))
  /// the fringe would be [a, b, 3, 4]
  std::vector<Expression> fringe;

  explicit FlattenedAssociativeChain(Expression expr) : exprs{std::move(expr)} {
    auto call = CallNotNull(exprs.back());
    fringe = call->arguments;

    auto it = fringe.begin();

    while (it != fringe.end()) {
      auto sub_call = it->call();
      if (!sub_call || sub_call->function_name != call->function_name) {
        ++it;
        continue;
      }

      if (it != fringe.begin()) {
        was_left_folded = false;
      }

      exprs.push_back(std::move(*it));
      it = fringe.erase(it);

      auto index = it - fringe.begin();
      fringe.insert(it, sub_call->arguments.begin(), sub_call->arguments.end());
      it = fringe.begin() + index;
      // NB: no increment so we hit sub_call's first argument next iteration
    }

    DCHECK(std::all_of(exprs.begin(), exprs.end(), [](const Expression& expr) {
      return CallNotNull(expr)->options == nullptr;
    }));
  }
};

inline Result<std::shared_ptr<compute::Function>> GetFunction(
    const Expression::Call& call, compute::ExecContext* exec_context) {
  if (call.function_name != "cast") {
    return exec_context->func_registry()->GetFunction(call.function_name);
  }
  // XXX this special case is strange; why not make "cast" a ScalarFunction?
  const auto& to_type = checked_cast<const compute::CastOptions&>(*call.options).to_type;
  return compute::GetCastFunction(to_type);
}

/// Modify an Expression with pre-order and post-order visitation.
/// `pre` will be invoked on each Expression. `pre` will visit Calls before their
/// arguments, `post_call` will visit Calls (and no other Expressions) after their
/// arguments. Visitors should return the Identical expression to indicate no change; this
/// will prevent unnecessary construction in the common case where a modification is not
/// possible/necessary/...
///
/// If an argument was modified, `post_call` visits a reconstructed Call with the modified
/// arguments but also receives a pointer to the unmodified Expression as a second
/// argument. If no arguments were modified the unmodified Expression* will be nullptr.
template <typename PreVisit, typename PostVisitCall>
Result<Expression> Modify(Expression expr, const PreVisit& pre,
                          const PostVisitCall& post_call) {
  ARROW_ASSIGN_OR_RAISE(expr, Result<Expression>(pre(std::move(expr))));

  auto call = expr.call();
  if (!call) return expr;

  bool at_least_one_modified = false;
  std::vector<Expression> modified_arguments;

  for (size_t i = 0; i < call->arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto modified_argument,
                          Modify(call->arguments[i], pre, post_call));

    if (Identical(modified_argument, call->arguments[i])) {
      continue;
    }

    if (!at_least_one_modified) {
      modified_arguments = call->arguments;
      at_least_one_modified = true;
    }

    modified_arguments[i] = std::move(modified_argument);
  }

  if (at_least_one_modified) {
    // reconstruct the call expression with the modified arguments
    auto modified_call = *call;
    modified_call.arguments = std::move(modified_arguments);
    return post_call(Expression(std::move(modified_call)), &expr);
  }

  return post_call(std::move(expr), nullptr);
}

}  // namespace compute
}  // namespace arrow
