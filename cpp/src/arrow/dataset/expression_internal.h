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

#include "arrow/dataset/expression.h"

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

namespace dataset {

const Expression::Call* CallNotNull(const Expression& expr) {
  auto call = expr.call();
  DCHECK_NE(call, nullptr);
  return call;
}

inline void GetAllFieldRefs(const Expression& expr,
                            std::unordered_set<FieldRef, FieldRef::Hash>* refs) {
  if (auto lit = expr.literal()) return;

  if (auto ref = expr.field_ref()) {
    refs->emplace(*ref);
    return;
  }

  for (const Expression& arg : CallNotNull(expr)->arguments) {
    GetAllFieldRefs(arg, refs);
  }
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

struct FieldPathGetDatumImpl {
  template <typename T, typename = decltype(FieldPath{}.Get(std::declval<const T&>()))>
  Result<Datum> operator()(const std::shared_ptr<T>& ptr) {
    return path_.Get(*ptr).template As<Datum>();
  }

  template <typename T>
  Result<Datum> operator()(const T&) {
    return Status::NotImplemented("FieldPath::Get() into Datum ", datum_.ToString());
  }

  const Datum& datum_;
  const FieldPath& path_;
};

inline Result<Datum> GetDatumField(const FieldRef& ref, const Datum& input) {
  Datum field;

  FieldPath path;
  if (auto type = input.type()) {
    ARROW_ASSIGN_OR_RAISE(path, ref.FindOneOrNone(*type));
  } else if (auto schema = input.schema()) {
    ARROW_ASSIGN_OR_RAISE(path, ref.FindOneOrNone(*schema));
  } else {
    return Status::NotImplemented("retrieving fields from datum ", input.ToString());
  }

  if (path) {
    ARROW_ASSIGN_OR_RAISE(field,
                          util::visit(FieldPathGetDatumImpl{input, path}, input.value));
  }

  if (field == Datum{}) {
    field = Datum(std::make_shared<NullScalar>());
  }

  return field;
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

  // Execute a simple Comparison between scalars, casting the RHS if types disagree
  static Result<type> Execute(Datum l, Datum r) {
    if (!l.is_scalar() || !r.is_scalar()) {
      return Status::Invalid("Cannot Execute Comparison on non-scalars");
    }

    if (!l.type()->Equals(r.type())) {
      ARROW_ASSIGN_OR_RAISE(r, compute::Cast(r, l.type()));
    }

    std::vector<Datum> arguments{std::move(l), std::move(r)};

    ARROW_ASSIGN_OR_RAISE(auto equal, compute::CallFunction("equal", arguments));

    if (!equal.scalar()->is_valid) return NA;
    if (equal.scalar_as<BooleanScalar>().value) return EQUAL;

    ARROW_ASSIGN_OR_RAISE(auto less, compute::CallFunction("less", arguments));

    if (!less.scalar()->is_valid) return NA;
    return less.scalar_as<BooleanScalar>().value ? LESS : GREATER;
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

inline bool IsSameTypesBinary(const std::string& function) {
  if (Comparison::Get(function)) return true;

  static std::unordered_set<std::string> set{"add", "subtract", "multiply", "divide"};

  return set.find(function) != set.end();
}

inline const compute::SetLookupOptions* GetSetLookupOptions(
    const Expression::Call& call) {
  if (!IsSetLookup(call.function_name)) return nullptr;
  return checked_cast<const compute::SetLookupOptions*>(call.options.get());
}

inline const compute::ProjectOptions* GetProjectOptions(const Expression::Call& call) {
  if (call.function_name != "project") return nullptr;
  return checked_cast<const compute::ProjectOptions*>(call.options.get());
}

inline const compute::StrptimeOptions* GetStrptimeOptions(const Expression::Call& call) {
  if (call.function_name != "strptime") return nullptr;
  return checked_cast<const compute::StrptimeOptions*>(call.options.get());
}

inline std::shared_ptr<DataType> GetDictionaryValueType(
    const std::shared_ptr<DataType>& type) {
  if (type && type->id() == Type::DICTIONARY) {
    return checked_cast<const DictionaryType&>(*type).value_type();
  }
  return nullptr;
}

inline Status EnsureNotDictionary(ValueDescr* descr) {
  if (auto value_type = GetDictionaryValueType(descr->type)) {
    descr->type = std::move(value_type);
  }
  return Status::OK();
}

inline Status EnsureNotDictionary(Datum* datum) {
  if (datum->type()->id() == Type::DICTIONARY) {
    const auto& type = checked_cast<const DictionaryType&>(*datum->type()).value_type();
    ARROW_ASSIGN_OR_RAISE(*datum, compute::Cast(*datum, type));
  }
  return Status::OK();
}

inline Status EnsureNotDictionary(Expression::Call* call) {
  if (auto options = GetSetLookupOptions(*call)) {
    auto new_options = *options;
    RETURN_NOT_OK(EnsureNotDictionary(&new_options.value_set));
    call->options.reset(new compute::SetLookupOptions(std::move(new_options)));
  }
  return Status::OK();
}

inline Result<std::shared_ptr<StructScalar>> FunctionOptionsToStructScalar(
    const Expression::Call& call) {
  if (call.options == nullptr) {
    return nullptr;
  }

  if (auto options = GetSetLookupOptions(call)) {
    if (!options->value_set.is_array()) {
      return Status::NotImplemented("chunked value_set");
    }
    return StructScalar::Make(
        {
            std::make_shared<ListScalar>(options->value_set.make_array()),
            MakeScalar(options->skip_nulls),
        },
        {"value_set", "skip_nulls"});
  }

  if (auto options = GetCastOptions(call)) {
    return StructScalar::Make(
        {
            MakeNullScalar(options->to_type),
            MakeScalar(options->allow_int_overflow),
            MakeScalar(options->allow_time_truncate),
            MakeScalar(options->allow_time_overflow),
            MakeScalar(options->allow_decimal_truncate),
            MakeScalar(options->allow_float_truncate),
            MakeScalar(options->allow_invalid_utf8),
        },
        {
            "to_type_holder",
            "allow_int_overflow",
            "allow_time_truncate",
            "allow_time_overflow",
            "allow_decimal_truncate",
            "allow_float_truncate",
            "allow_invalid_utf8",
        });
  }

  return Status::NotImplemented("conversion of options for ", call.function_name);
}

inline Status FunctionOptionsFromStructScalar(const StructScalar* repr,
                                              Expression::Call* call) {
  if (repr == nullptr) {
    call->options = nullptr;
    return Status::OK();
  }

  if (IsSetLookup(call->function_name)) {
    ARROW_ASSIGN_OR_RAISE(auto value_set, repr->field("value_set"));
    ARROW_ASSIGN_OR_RAISE(auto skip_nulls, repr->field("skip_nulls"));
    call->options = std::make_shared<compute::SetLookupOptions>(
        checked_cast<const ListScalar&>(*value_set).value,
        checked_cast<const BooleanScalar&>(*skip_nulls).value);
    return Status::OK();
  }

  if (call->function_name == "cast") {
    auto options = std::make_shared<compute::CastOptions>();
    ARROW_ASSIGN_OR_RAISE(auto to_type_holder, repr->field("to_type_holder"));
    options->to_type = to_type_holder->type;

    int i = 1;
    for (bool* opt : {
             &options->allow_int_overflow,
             &options->allow_time_truncate,
             &options->allow_time_overflow,
             &options->allow_decimal_truncate,
             &options->allow_float_truncate,
             &options->allow_invalid_utf8,
         }) {
      *opt = checked_cast<const BooleanScalar&>(*repr->value[i++]).value;
    }

    call->options = std::move(options);
    return Status::OK();
  }

  return Status::NotImplemented("conversion of options for ", call->function_name);
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

template <typename PreVisit, typename PostVisitCall>
Result<Expression> Modify(Expression expr, const PreVisit& pre,
                          const PostVisitCall& post_call) {
  ARROW_ASSIGN_OR_RAISE(expr, Result<Expression>(pre(std::move(expr))));

  auto call = expr.call();
  if (!call) return expr;

  bool at_least_one_modified = false;
  auto modified_call = *call;
  auto modified_argument = modified_call.arguments.begin();

  for (const auto& argument : call->arguments) {
    ARROW_ASSIGN_OR_RAISE(*modified_argument, Modify(argument, pre, post_call));

    if (!Identical(*modified_argument, argument)) {
      at_least_one_modified = true;
    }
    ++modified_argument;
  }

  if (at_least_one_modified) {
    // reconstruct the call expression with the modified arguments
    auto modified_expr = Expression(std::move(modified_call));

    return post_call(std::move(modified_expr), &expr);
  }

  return post_call(std::move(expr), nullptr);
}

}  // namespace dataset
}  // namespace arrow
