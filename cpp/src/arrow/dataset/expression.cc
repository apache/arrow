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

#include "arrow/chunked_array.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/dataset/expression_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/atomic_shared_ptr.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace dataset {

Expression::Expression(Call call) : impl_(std::make_shared<Impl>(std::move(call))) {}

Expression::Expression(Datum literal)
    : impl_(std::make_shared<Impl>(std::move(literal))) {}

Expression::Expression(Parameter parameter)
    : impl_(std::make_shared<Impl>(std::move(parameter))) {}

Expression literal(Datum lit) { return Expression(std::move(lit)); }

Expression field_ref(FieldRef ref) {
  return Expression(Expression::Parameter{std::move(ref), {}});
}

Expression call(std::string function, std::vector<Expression> arguments,
                std::shared_ptr<compute::FunctionOptions> options) {
  Expression::Call call;
  call.function_name = std::move(function);
  call.arguments = std::move(arguments);
  call.options = std::move(options);
  return Expression(std::move(call));
}

const Datum* Expression::literal() const { return util::get_if<Datum>(impl_.get()); }

const FieldRef* Expression::field_ref() const {
  if (auto parameter = util::get_if<Parameter>(impl_.get())) {
    return &parameter->ref;
  }
  return nullptr;
}

const Expression::Call* Expression::call() const {
  return util::get_if<Call>(impl_.get());
}

ValueDescr Expression::descr() const {
  if (impl_ == nullptr) return {};

  if (auto lit = literal()) {
    return lit->descr();
  }

  if (auto parameter = util::get_if<Parameter>(impl_.get())) {
    return parameter->descr;
  }

  return CallNotNull(*this)->descr;
}

std::string Expression::ToString() const {
  if (auto lit = literal()) {
    if (lit->is_scalar()) {
      switch (lit->type()->id()) {
        case Type::STRING:
        case Type::LARGE_STRING:
          return '"' +
                 Escape(util::string_view(*lit->scalar_as<BaseBinaryScalar>().value)) +
                 '"';

        case Type::BINARY:
        case Type::FIXED_SIZE_BINARY:
        case Type::LARGE_BINARY:
          return '"' + lit->scalar_as<BaseBinaryScalar>().value->ToHexString() + '"';

        default:
          break;
      }
      return lit->scalar()->ToString();
    }
    return lit->ToString();
  }

  if (auto ref = field_ref()) {
    if (auto name = ref->name()) {
      return *name;
    }
    if (auto path = ref->field_path()) {
      return path->ToString();
    }
    return ref->ToString();
  }

  auto call = CallNotNull(*this);
  auto binary = [&](std::string op) {
    return "(" + call->arguments[0].ToString() + " " + op + " " +
           call->arguments[1].ToString() + ")";
  };

  if (auto cmp = Comparison::Get(call->function_name)) {
    return binary(Comparison::GetOp(*cmp));
  }

  constexpr util::string_view kleene = "_kleene";
  if (util::string_view{call->function_name}.ends_with(kleene)) {
    auto op = call->function_name.substr(0, call->function_name.size() - kleene.size());
    return binary(std::move(op));
  }

  if (auto options = GetStructOptions(*call)) {
    std::string out = "{";
    auto argument = call->arguments.begin();
    for (const auto& field_name : options->field_names) {
      out += field_name + "=" + argument++->ToString() + ", ";
    }
    out.resize(out.size() - 1);
    out.back() = '}';
    return out;
  }

  std::string out = call->function_name + "(";
  for (const auto& arg : call->arguments) {
    out += arg.ToString() + ", ";
  }

  if (call->options == nullptr) {
    out.resize(out.size() - 1);
    out.back() = ')';
    return out;
  }

  if (auto options = GetSetLookupOptions(*call)) {
    DCHECK_EQ(options->value_set.kind(), Datum::ARRAY);
    out += "value_set=" + options->value_set.make_array()->ToString();
    if (options->skip_nulls) {
      out += ", skip_nulls";
    }
    return out + ")";
  }

  if (auto options = GetCastOptions(*call)) {
    if (options->to_type == nullptr) {
      return out + "to_type=<INVALID NOT PROVIDED>)";
    }
    out += "to_type=" + options->to_type->ToString();
    if (options->allow_int_overflow) out += ", allow_int_overflow";
    if (options->allow_time_truncate) out += ", allow_time_truncate";
    if (options->allow_time_overflow) out += ", allow_time_overflow";
    if (options->allow_decimal_truncate) out += ", allow_decimal_truncate";
    if (options->allow_float_truncate) out += ", allow_float_truncate";
    if (options->allow_invalid_utf8) out += ", allow_invalid_utf8";
    return out + ")";
  }

  if (auto options = GetStrptimeOptions(*call)) {
    return out + "format=" + options->format +
           ", unit=" + internal::ToString(options->unit) + ")";
  }

  return out + "{NON-REPRESENTABLE OPTIONS})";
}

void PrintTo(const Expression& expr, std::ostream* os) {
  *os << expr.ToString();
  if (expr.IsBound()) {
    *os << "[bound]";
  }
}

bool Expression::Equals(const Expression& other) const {
  if (Identical(*this, other)) return true;

  if (impl_->index() != other.impl_->index()) {
    return false;
  }

  if (auto lit = literal()) {
    return lit->Equals(*other.literal());
  }

  if (auto ref = field_ref()) {
    return ref->Equals(*other.field_ref());
  }

  auto call = CallNotNull(*this);
  auto other_call = CallNotNull(other);

  if (call->function_name != other_call->function_name ||
      call->kernel != other_call->kernel) {
    return false;
  }

  for (size_t i = 0; i < call->arguments.size(); ++i) {
    if (!call->arguments[i].Equals(other_call->arguments[i])) {
      return false;
    }
  }

  if (call->options == other_call->options) return true;

  if (auto options = GetSetLookupOptions(*call)) {
    auto other_options = GetSetLookupOptions(*other_call);
    return options->value_set == other_options->value_set &&
           options->skip_nulls == other_options->skip_nulls;
  }

  if (auto options = GetCastOptions(*call)) {
    auto other_options = GetCastOptions(*other_call);
    for (auto safety_opt : {
             &compute::CastOptions::allow_int_overflow,
             &compute::CastOptions::allow_time_truncate,
             &compute::CastOptions::allow_time_overflow,
             &compute::CastOptions::allow_decimal_truncate,
             &compute::CastOptions::allow_float_truncate,
             &compute::CastOptions::allow_invalid_utf8,
         }) {
      if (options->*safety_opt != other_options->*safety_opt) return false;
    }
    return options->to_type->Equals(other_options->to_type);
  }

  if (auto options = GetStructOptions(*call)) {
    auto other_options = GetStructOptions(*other_call);
    return options->field_names == other_options->field_names;
  }

  if (auto options = GetStrptimeOptions(*call)) {
    auto other_options = GetStrptimeOptions(*other_call);
    return options->format == other_options->format &&
           options->unit == other_options->unit;
  }

  ARROW_LOG(WARNING) << "comparing unknown FunctionOptions for function "
                     << call->function_name;
  return false;
}

size_t Expression::hash() const {
  if (auto lit = literal()) {
    if (lit->is_scalar()) {
      return Scalar::Hash::hash(*lit->scalar());
    }
    return 0;
  }

  if (auto ref = field_ref()) {
    return ref->hash();
  }

  auto call = CallNotNull(*this);
  if (call->hash != nullptr) {
    return call->hash->load();
  }

  size_t out = std::hash<std::string>{}(call->function_name);
  for (const auto& arg : call->arguments) {
    out ^= arg.hash();
  }

  std::shared_ptr<std::atomic<size_t>> expected = nullptr;
  internal::atomic_compare_exchange_strong(&const_cast<Call*>(call)->hash, &expected,
                                           std::make_shared<std::atomic<size_t>>(out));
  return out;
}

bool Expression::IsBound() const {
  if (descr().type == nullptr) return false;

  if (auto lit = literal()) return true;

  if (auto ref = field_ref()) return true;

  auto call = CallNotNull(*this);

  for (const Expression& arg : call->arguments) {
    if (!arg.IsBound()) return false;
  }

  return call->kernel != nullptr;
}

bool Expression::IsScalarExpression() const {
  if (auto lit = literal()) {
    return lit->is_scalar();
  }

  // FIXME handle case where a list's item field is referenced
  if (auto ref = field_ref()) return true;

  auto call = CallNotNull(*this);

  for (const Expression& arg : call->arguments) {
    if (!arg.IsScalarExpression()) return false;
  }

  if (call->function) {
    return call->function->kind() == compute::Function::SCALAR;
  }

  // this expression is not bound; make a best guess based on
  // the default function registry
  if (auto function = compute::GetFunctionRegistry()
                          ->GetFunction(call->function_name)
                          .ValueOr(nullptr)) {
    return function->kind() == compute::Function::SCALAR;
  }

  // unknown function or other error; conservatively return false
  return false;
}

bool Expression::IsNullLiteral() const {
  if (auto lit = literal()) {
    if (lit->null_count() == lit->length()) {
      return true;
    }
  }

  return false;
}

bool Expression::IsSatisfiable() const {
  if (descr().type && descr().type->id() == Type::NA) {
    return false;
  }

  if (auto lit = literal()) {
    if (lit->null_count() == lit->length()) {
      return false;
    }

    if (lit->is_scalar() && lit->type()->id() == Type::BOOL) {
      return lit->scalar_as<BooleanScalar>().value;
    }
  }

  if (auto ref = field_ref()) {
    return true;
  }

  return true;
}

inline bool KernelStateIsImmutable(const std::string& function) {
  // XXX maybe just add Kernel::state_is_immutable or so?

  // known functions with non-null but nevertheless immutable KernelState
  static std::unordered_set<std::string> names = {
      "is_in", "index_in", "cast", "struct", "strptime",
  };

  return names.find(function) != names.end();
}

Result<std::unique_ptr<compute::KernelState>> InitKernelState(
    const Expression::Call& call, compute::ExecContext* exec_context) {
  if (!call.kernel->init) return nullptr;

  compute::KernelContext kernel_context(exec_context);
  compute::KernelInitArgs kernel_init_args{call.kernel, GetDescriptors(call.arguments),
                                           call.options.get()};

  auto kernel_state = call.kernel->init(&kernel_context, kernel_init_args);
  RETURN_NOT_OK(kernel_context.status());
  return std::move(kernel_state);
}

Status MaybeInsertCast(std::shared_ptr<DataType> to_type, Expression* expr) {
  if (expr->descr().type->Equals(to_type)) {
    return Status::OK();
  }

  if (auto lit = expr->literal()) {
    ARROW_ASSIGN_OR_RAISE(Datum new_lit, compute::Cast(*lit, to_type));
    *expr = literal(std::move(new_lit));
    return Status::OK();
  }

  // FIXME the resulting cast Call must be bound but this is a hack
  auto with_cast = call("cast", {literal(MakeNullScalar(expr->descr().type))},
                        compute::CastOptions::Safe(to_type));

  static ValueDescr ignored_descr;
  ARROW_ASSIGN_OR_RAISE(with_cast, with_cast.Bind(ignored_descr));

  auto call_with_cast = *CallNotNull(with_cast);
  call_with_cast.arguments[0] = std::move(*expr);
  call_with_cast.descr = ValueDescr{std::move(to_type), expr->descr().shape};

  *expr = Expression(std::move(call_with_cast));
  return Status::OK();
}

Status InsertImplicitCasts(Expression::Call* call) {
  DCHECK(std::all_of(call->arguments.begin(), call->arguments.end(),
                     [](const Expression& argument) { return argument.IsBound(); }));

  if (IsSameTypesBinary(call->function_name)) {
    for (auto&& argument : call->arguments) {
      if (auto value_type = GetDictionaryValueType(argument.descr().type)) {
        RETURN_NOT_OK(MaybeInsertCast(std::move(value_type), &argument));
      }
    }

    if (call->arguments[0].descr().shape == ValueDescr::SCALAR) {
      // argument 0 is scalar so casting is cheap
      return MaybeInsertCast(call->arguments[1].descr().type, &call->arguments[0]);
    }

    // cast argument 1 unconditionally
    return MaybeInsertCast(call->arguments[0].descr().type, &call->arguments[1]);
  }

  if (auto options = GetSetLookupOptions(*call)) {
    if (auto value_type = GetDictionaryValueType(call->arguments[0].descr().type)) {
      // DICTIONARY input is not supported; decode it.
      RETURN_NOT_OK(MaybeInsertCast(std::move(value_type), &call->arguments[0]));
    }

    if (options->value_set.type()->id() == Type::DICTIONARY) {
      // DICTIONARY value_set is not supported; decode it.
      auto new_options = std::make_shared<compute::SetLookupOptions>(*options);
      RETURN_NOT_OK(EnsureNotDictionary(&new_options->value_set));
      options = new_options.get();
      call->options = std::move(new_options);
    }

    if (!options->value_set.type()->Equals(call->arguments[0].descr().type)) {
      // The value_set is assumed smaller than inputs, casting it should be cheaper.
      auto new_options = std::make_shared<compute::SetLookupOptions>(*options);
      ARROW_ASSIGN_OR_RAISE(new_options->value_set,
                            compute::Cast(std::move(new_options->value_set),
                                          call->arguments[0].descr().type));
      options = new_options.get();
      call->options = std::move(new_options);
    }

    return Status::OK();
  }

  return Status::OK();
}

Result<Expression> Expression::Bind(ValueDescr in,
                                    compute::ExecContext* exec_context) const {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return Bind(std::move(in), &exec_context);
  }

  if (literal()) return *this;

  if (auto ref = field_ref()) {
    ARROW_ASSIGN_OR_RAISE(auto field, ref->GetOneOrNone(*in.type));
    auto descr = field ? ValueDescr{field->type(), in.shape} : ValueDescr::Scalar(null());
    return Expression{Parameter{*ref, std::move(descr)}};
  }

  auto bound_call = *CallNotNull(*this);

  ARROW_ASSIGN_OR_RAISE(bound_call.function, GetFunction(bound_call, exec_context));

  for (auto&& argument : bound_call.arguments) {
    ARROW_ASSIGN_OR_RAISE(argument, argument.Bind(in, exec_context));
  }
  RETURN_NOT_OK(InsertImplicitCasts(&bound_call));

  auto descrs = GetDescriptors(bound_call.arguments);
  ARROW_ASSIGN_OR_RAISE(bound_call.kernel, bound_call.function->DispatchExact(descrs));

  compute::KernelContext kernel_context(exec_context);
  ARROW_ASSIGN_OR_RAISE(bound_call.kernel_state,
                        InitKernelState(bound_call, exec_context));
  kernel_context.SetState(bound_call.kernel_state.get());

  ARROW_ASSIGN_OR_RAISE(
      bound_call.descr,
      bound_call.kernel->signature->out_type().Resolve(&kernel_context, descrs));

  return Expression(std::move(bound_call));
}

Result<Expression> Expression::Bind(const Schema& in_schema,
                                    compute::ExecContext* exec_context) const {
  return Bind(ValueDescr::Array(struct_(in_schema.fields())), exec_context);
}

Result<Datum> ExecuteScalarExpression(const Expression& expr, const Datum& input,
                                      compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return ExecuteScalarExpression(expr, input, &exec_context);
  }

  if (!expr.IsBound()) {
    return Status::Invalid("Cannot Execute unbound expression.");
  }

  if (!expr.IsScalarExpression()) {
    return Status::Invalid(
        "ExecuteScalarExpression cannot Execute non-scalar expression ", expr.ToString());
  }

  if (auto lit = expr.literal()) return *lit;

  if (auto ref = expr.field_ref()) {
    ARROW_ASSIGN_OR_RAISE(Datum field, GetDatumField(*ref, input));

    if (field.descr() != expr.descr()) {
      // Refernced field was present but didn't have the expected type.
      // Should we just error here? For now, pay dispatch cost and just cast.
      ARROW_ASSIGN_OR_RAISE(
          field, compute::Cast(field, expr.descr().type, compute::CastOptions::Safe(),
                               exec_context));
    }

    return field;
  }

  auto call = CallNotNull(expr);

  std::vector<Datum> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(
        arguments[i], ExecuteScalarExpression(call->arguments[i], input, exec_context));
  }

  auto executor = compute::detail::KernelExecutor::MakeScalar();

  compute::KernelContext kernel_context(exec_context);
  kernel_context.SetState(call->kernel_state.get());

  auto kernel = call->kernel;
  auto descrs = GetDescriptors(arguments);
  auto options = call->options.get();
  RETURN_NOT_OK(executor->Init(&kernel_context, {kernel, descrs, options}));

  auto listener = std::make_shared<compute::detail::DatumAccumulator>();
  RETURN_NOT_OK(executor->Execute(arguments, listener.get()));
  return executor->WrapResults(arguments, listener->values());
}

std::array<std::pair<const Expression&, const Expression&>, 2>
ArgumentsAndFlippedArguments(const Expression::Call& call) {
  DCHECK_EQ(call.arguments.size(), 2);
  return {std::pair<const Expression&, const Expression&>{call.arguments[0],
                                                          call.arguments[1]},
          std::pair<const Expression&, const Expression&>{call.arguments[1],
                                                          call.arguments[0]}};
}

template <typename BinOp, typename It,
          typename Out = typename std::iterator_traits<It>::value_type>
util::optional<Out> FoldLeft(It begin, It end, const BinOp& bin_op) {
  if (begin == end) return util::nullopt;

  Out folded = std::move(*begin++);
  while (begin != end) {
    folded = bin_op(std::move(folded), std::move(*begin++));
  }
  return folded;
}

util::optional<compute::NullHandling::type> GetNullHandling(
    const Expression::Call& call) {
  if (call.function && call.function->kind() == compute::Function::SCALAR) {
    return static_cast<const compute::ScalarKernel*>(call.kernel)->null_handling;
  }
  return util::nullopt;
}

bool DefinitelyNotNull(const Expression& expr) {
  DCHECK(expr.IsBound());

  if (expr.literal()) {
    return !expr.IsNullLiteral();
  }

  if (expr.field_ref()) return false;

  auto call = CallNotNull(expr);
  if (auto null_handling = GetNullHandling(*call)) {
    if (null_handling == compute::NullHandling::OUTPUT_NOT_NULL) {
      return true;
    }
    if (null_handling == compute::NullHandling::INTERSECTION) {
      return std::all_of(call->arguments.begin(), call->arguments.end(),
                         DefinitelyNotNull);
    }
  }

  return false;
}

std::vector<FieldRef> FieldsInExpression(const Expression& expr) {
  if (auto lit = expr.literal()) return {};

  if (auto ref = expr.field_ref()) {
    return {*ref};
  }

  std::vector<FieldRef> fields;
  for (const Expression& arg : CallNotNull(expr)->arguments) {
    auto argument_fields = FieldsInExpression(arg);
    std::move(argument_fields.begin(), argument_fields.end(), std::back_inserter(fields));
  }
  return fields;
}

Result<Expression> FoldConstants(Expression expr) {
  return Modify(
      std::move(expr), [](Expression expr) { return expr; },
      [](Expression expr, ...) -> Result<Expression> {
        auto call = CallNotNull(expr);
        if (std::all_of(call->arguments.begin(), call->arguments.end(),
                        [](const Expression& argument) { return argument.literal(); })) {
          // all arguments are literal; we can evaluate this subexpression *now*
          static const Datum ignored_input;
          ARROW_ASSIGN_OR_RAISE(Datum constant,
                                ExecuteScalarExpression(expr, ignored_input));

          return literal(std::move(constant));
        }

        // XXX the following should probably be in a registry of passes instead
        // of inline

        if (GetNullHandling(*call) == compute::NullHandling::INTERSECTION) {
          // kernels which always produce intersected validity can be resolved
          // to null *now* if any of their inputs is a null literal
          for (const auto& argument : call->arguments) {
            if (argument.IsNullLiteral()) {
              return argument;
            }
          }
        }

        if (call->function_name == "and_kleene") {
          for (auto args : ArgumentsAndFlippedArguments(*call)) {
            // true and x == x
            if (args.first == literal(true)) return args.second;

            // false and x == false
            if (args.first == literal(false)) return args.first;

            // x and x == x
            if (args.first == args.second) return args.first;
          }
          return expr;
        }

        if (call->function_name == "or_kleene") {
          for (auto args : ArgumentsAndFlippedArguments(*call)) {
            // false or x == x
            if (args.first == literal(false)) return args.second;

            // true or x == true
            if (args.first == literal(true)) return args.first;

            // x or x == x
            if (args.first == args.second) return args.first;
          }
          return expr;
        }

        return expr;
      });
}

inline std::vector<Expression> GuaranteeConjunctionMembers(
    const Expression& guaranteed_true_predicate) {
  auto guarantee = guaranteed_true_predicate.call();
  if (!guarantee || guarantee->function_name != "and_kleene") {
    return {guaranteed_true_predicate};
  }
  return FlattenedAssociativeChain(guaranteed_true_predicate).fringe;
}

// Conjunction members which are represented in known_values are erased from
// conjunction_members
Status ExtractKnownFieldValuesImpl(
    std::vector<Expression>* conjunction_members,
    std::unordered_map<FieldRef, Datum, FieldRef::Hash>* known_values) {
  auto unconsumed_end =
      std::partition(conjunction_members->begin(), conjunction_members->end(),
                     [](const Expression& expr) {
                       // search for an equality conditions between a field and a literal
                       auto call = expr.call();
                       if (!call) return true;

                       if (call->function_name == "equal") {
                         auto ref = call->arguments[0].field_ref();
                         auto lit = call->arguments[1].literal();
                         return !(ref && lit);
                       }

                       return true;
                     });

  for (auto it = unconsumed_end; it != conjunction_members->end(); ++it) {
    auto call = CallNotNull(*it);

    auto ref = call->arguments[0].field_ref();
    auto lit = call->arguments[1].literal();

    auto it_success = known_values->emplace(*ref, *lit);
    if (it_success.second) continue;

    // A value was already known for ref; check it
    auto ref_lit = it_success.first;
    if (*lit != ref_lit->second) {
      return Status::Invalid("Conflicting guarantees: (", ref->ToString(),
                             " == ", lit->ToString(), ") vs (", ref->ToString(),
                             " == ", ref_lit->second.ToString());
    }
  }

  conjunction_members->erase(unconsumed_end, conjunction_members->end());

  return Status::OK();
}

Result<std::unordered_map<FieldRef, Datum, FieldRef::Hash>> ExtractKnownFieldValues(
    const Expression& guaranteed_true_predicate) {
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);
  std::unordered_map<FieldRef, Datum, FieldRef::Hash> known_values;
  RETURN_NOT_OK(ExtractKnownFieldValuesImpl(&conjunction_members, &known_values));
  return known_values;
}

Result<Expression> ReplaceFieldsWithKnownValues(
    const std::unordered_map<FieldRef, Datum, FieldRef::Hash>& known_values,
    Expression expr) {
  if (!expr.IsBound()) {
    return Status::Invalid(
        "ReplaceFieldsWithKnownValues called on an unbound Expression");
  }

  return Modify(
      std::move(expr),
      [&known_values](Expression expr) -> Result<Expression> {
        if (auto ref = expr.field_ref()) {
          auto it = known_values.find(*ref);
          if (it != known_values.end()) {
            ARROW_ASSIGN_OR_RAISE(Datum lit,
                                  compute::Cast(it->second, expr.descr().type));
            return literal(std::move(lit));
          }
        }
        return expr;
      },
      [](Expression expr, ...) { return expr; });
}

inline bool IsBinaryAssociativeCommutative(const Expression::Call& call) {
  static std::unordered_set<std::string> binary_associative_commutative{
      "and",      "or",  "and_kleene",       "or_kleene",  "xor",
      "multiply", "add", "multiply_checked", "add_checked"};

  auto it = binary_associative_commutative.find(call.function_name);
  return it != binary_associative_commutative.end();
}

Result<Expression> Canonicalize(Expression expr, compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return Canonicalize(std::move(expr), &exec_context);
  }

  // If potentially reconstructing more deeply than a call's immediate arguments
  // (for example, when reorganizing an associative chain), add expressions to this set to
  // avoid unnecessary work
  struct {
    std::unordered_set<Expression, Expression::Hash> set_;

    bool operator()(const Expression& expr) const {
      return set_.find(expr) != set_.end();
    }

    void Add(std::vector<Expression> exprs) {
      std::move(exprs.begin(), exprs.end(), std::inserter(set_, set_.end()));
    }
  } AlreadyCanonicalized;

  return Modify(
      std::move(expr),
      [&AlreadyCanonicalized, exec_context](Expression expr) -> Result<Expression> {
        auto call = expr.call();
        if (!call) return expr;

        if (AlreadyCanonicalized(expr)) return expr;

        if (IsBinaryAssociativeCommutative(*call)) {
          struct {
            int Priority(const Expression& operand) const {
              // order literals first, starting with nulls
              if (operand.IsNullLiteral()) return 0;
              if (operand.literal()) return 1;
              return 2;
            }
            bool operator()(const Expression& l, const Expression& r) const {
              return Priority(l) < Priority(r);
            }
          } CanonicalOrdering;

          FlattenedAssociativeChain chain(expr);
          if (chain.was_left_folded &&
              std::is_sorted(chain.fringe.begin(), chain.fringe.end(),
                             CanonicalOrdering)) {
            AlreadyCanonicalized.Add(std::move(chain.exprs));
            return expr;
          }

          std::stable_sort(chain.fringe.begin(), chain.fringe.end(), CanonicalOrdering);

          // fold the chain back up
          auto folded =
              FoldLeft(chain.fringe.begin(), chain.fringe.end(),
                       [call, &AlreadyCanonicalized](Expression l, Expression r) {
                         auto canonicalized_call = *call;
                         canonicalized_call.arguments = {std::move(l), std::move(r)};
                         Expression expr(std::move(canonicalized_call));
                         AlreadyCanonicalized.Add({expr});
                         return expr;
                       });
          return std::move(*folded);
        }

        if (auto cmp = Comparison::Get(call->function_name)) {
          if (call->arguments[0].literal() && !call->arguments[1].literal()) {
            // ensure that literals are on comparisons' RHS
            auto flipped_call = *call;
            flipped_call.function_name =
                Comparison::GetName(Comparison::GetFlipped(*cmp));
            // look up the flipped kernel
            // TODO extract a helper for use here and in Bind
            ARROW_ASSIGN_OR_RAISE(
                auto function,
                exec_context->func_registry()->GetFunction(flipped_call.function_name));

            auto descrs = GetDescriptors(flipped_call.arguments);
            ARROW_ASSIGN_OR_RAISE(flipped_call.kernel, function->DispatchExact(descrs));

            std::swap(flipped_call.arguments[0], flipped_call.arguments[1]);
            return Expression(std::move(flipped_call));
          }
        }

        return expr;
      },
      [](Expression expr, ...) { return expr; });
}

Result<Expression> DirectComparisonSimplification(Expression expr,
                                                  const Expression::Call& guarantee) {
  return Modify(
      std::move(expr), [](Expression expr) { return expr; },
      [&guarantee](Expression expr, ...) -> Result<Expression> {
        auto call = expr.call();
        if (!call) return expr;

        // Ensure both calls are comparisons with equal LHS and scalar RHS
        auto cmp = Comparison::Get(expr);
        auto cmp_guarantee = Comparison::Get(guarantee.function_name);
        if (!cmp || !cmp_guarantee) return expr;

        if (call->arguments[0] != guarantee.arguments[0]) return expr;

        auto rhs = call->arguments[1].literal();
        auto guarantee_rhs = guarantee.arguments[1].literal();
        if (!rhs || !guarantee_rhs) return expr;

        if (!rhs->is_scalar() || !guarantee_rhs->is_scalar()) {
          return expr;
        }

        ARROW_ASSIGN_OR_RAISE(auto cmp_rhs_guarantee_rhs,
                              Comparison::Execute(*rhs, *guarantee_rhs));
        DCHECK_NE(cmp_rhs_guarantee_rhs, Comparison::NA);

        if (cmp_rhs_guarantee_rhs == Comparison::EQUAL) {
          // RHS of filter is equal to RHS of guarantee

          if ((*cmp_guarantee & *cmp) == *cmp_guarantee) {
            // guarantee is a subset of filter, so all data will be included
            return literal(true);
          }

          if ((*cmp_guarantee & *cmp) == 0) {
            // guarantee disjoint with filter, so all data will be excluded
            return literal(false);
          }

          return expr;
        }

        if (*cmp_guarantee & cmp_rhs_guarantee_rhs) {
          // unusable guarantee
          return expr;
        }

        if (*cmp & Comparison::GetFlipped(cmp_rhs_guarantee_rhs)) {
          // x > 1, x >= 1, x != 1 guaranteed by x >= 3
          return literal(true);
        } else {
          // x < 1, x <= 1, x == 1 unsatisfiable if x >= 3
          return literal(false);
        }
      });
}

Result<Expression> SimplifyWithGuarantee(Expression expr,
                                         const Expression& guaranteed_true_predicate) {
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);

  std::unordered_map<FieldRef, Datum, FieldRef::Hash> known_values;
  RETURN_NOT_OK(ExtractKnownFieldValuesImpl(&conjunction_members, &known_values));

  ARROW_ASSIGN_OR_RAISE(expr,
                        ReplaceFieldsWithKnownValues(known_values, std::move(expr)));

  auto CanonicalizeAndFoldConstants = [&expr] {
    ARROW_ASSIGN_OR_RAISE(expr, Canonicalize(std::move(expr)));
    ARROW_ASSIGN_OR_RAISE(expr, FoldConstants(std::move(expr)));
    return Status::OK();
  };
  RETURN_NOT_OK(CanonicalizeAndFoldConstants());

  for (const auto& guarantee : conjunction_members) {
    if (Comparison::Get(guarantee) && guarantee.call()->arguments[1].literal()) {
      ARROW_ASSIGN_OR_RAISE(
          auto simplified, DirectComparisonSimplification(expr, *CallNotNull(guarantee)));

      if (Identical(simplified, expr)) continue;

      expr = std::move(simplified);
      RETURN_NOT_OK(CanonicalizeAndFoldConstants());
    }
  }

  return expr;
}

// Serialization is accomplished by converting expressions to KeyValueMetadata and storing
// this in the schema of a RecordBatch. Embedded arrays and scalars are stored in its
// columns. Finally, the RecordBatch is written to an IPC file.
Result<std::shared_ptr<Buffer>> Serialize(const Expression& expr) {
  struct {
    std::shared_ptr<KeyValueMetadata> metadata_ = std::make_shared<KeyValueMetadata>();
    ArrayVector columns_;

    Result<std::string> AddScalar(const Scalar& scalar) {
      auto ret = columns_.size();
      ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(scalar, 1));
      columns_.push_back(std::move(array));
      return std::to_string(ret);
    }

    Status Visit(const Expression& expr) {
      if (auto lit = expr.literal()) {
        if (!lit->is_scalar()) {
          return Status::NotImplemented("Serialization of non-scalar literals");
        }
        ARROW_ASSIGN_OR_RAISE(auto value, AddScalar(*lit->scalar()));
        metadata_->Append("literal", std::move(value));
        return Status::OK();
      }

      if (auto ref = expr.field_ref()) {
        if (!ref->name()) {
          return Status::NotImplemented("Serialization of non-name field_refs");
        }
        metadata_->Append("field_ref", *ref->name());
        return Status::OK();
      }

      auto call = CallNotNull(expr);
      metadata_->Append("call", call->function_name);

      for (const auto& argument : call->arguments) {
        RETURN_NOT_OK(Visit(argument));
      }

      if (call->options) {
        ARROW_ASSIGN_OR_RAISE(auto options_scalar, FunctionOptionsToStructScalar(*call));
        ARROW_ASSIGN_OR_RAISE(auto value, AddScalar(*options_scalar));
        metadata_->Append("options", std::move(value));
      }

      metadata_->Append("end", call->function_name);
      return Status::OK();
    }

    Result<std::shared_ptr<RecordBatch>> operator()(const Expression& expr) {
      RETURN_NOT_OK(Visit(expr));
      FieldVector fields(columns_.size());
      for (size_t i = 0; i < fields.size(); ++i) {
        fields[i] = field("", columns_[i]->type());
      }
      return RecordBatch::Make(schema(std::move(fields), std::move(metadata_)), 1,
                               std::move(columns_));
    }
  } ToRecordBatch;

  ARROW_ASSIGN_OR_RAISE(auto batch, ToRecordBatch(expr));
  ARROW_ASSIGN_OR_RAISE(auto stream, io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, ipc::MakeFileWriter(stream, batch->schema()));
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  RETURN_NOT_OK(writer->Close());
  return stream->Finish();
}

Result<Expression> Deserialize(const Buffer& buffer) {
  io::BufferReader stream(buffer);
  ARROW_ASSIGN_OR_RAISE(auto reader, ipc::RecordBatchFileReader::Open(&stream));
  ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(0));
  if (batch->schema()->metadata() == nullptr) {
    return Status::Invalid("serialized Expression's batch repr had null metadata");
  }
  if (batch->num_rows() != 1) {
    return Status::Invalid(
        "serialized Expression's batch repr was not a single row - had ",
        batch->num_rows());
  }

  struct FromRecordBatch {
    const RecordBatch& batch_;
    int index_;

    const KeyValueMetadata& metadata() { return *batch_.schema()->metadata(); }

    Result<std::shared_ptr<Scalar>> GetScalar(const std::string& i) {
      int32_t column_index;
      if (!internal::ParseValue<Int32Type>(i.data(), i.length(), &column_index)) {
        return Status::Invalid("Couldn't parse column_index");
      }
      if (column_index >= batch_.num_columns()) {
        return Status::Invalid("column_index out of bounds");
      }
      return batch_.column(column_index)->GetScalar(0);
    }

    Result<Expression> GetOne() {
      if (index_ >= metadata().size()) {
        return Status::Invalid("unterminated serialized Expression");
      }

      const std::string& key = metadata().key(index_);
      const std::string& value = metadata().value(index_);
      ++index_;

      if (key == "literal") {
        ARROW_ASSIGN_OR_RAISE(auto scalar, GetScalar(value));
        return literal(std::move(scalar));
      }

      if (key == "field_ref") {
        return field_ref(value);
      }

      if (key != "call") {
        return Status::Invalid("Unrecognized serialized Expression key ", key);
      }

      std::vector<Expression> arguments;
      while (metadata().key(index_) != "end") {
        if (metadata().key(index_) == "options") {
          ARROW_ASSIGN_OR_RAISE(auto options_scalar, GetScalar(metadata().value(index_)));
          auto expr = call(value, std::move(arguments));
          RETURN_NOT_OK(FunctionOptionsFromStructScalar(
              checked_cast<const StructScalar*>(options_scalar.get()),
              const_cast<Expression::Call*>(expr.call())));
          index_ += 2;
          return expr;
        }

        ARROW_ASSIGN_OR_RAISE(auto argument, GetOne());
        arguments.push_back(std::move(argument));
      }

      ++index_;
      return call(value, std::move(arguments));
    }
  };

  return FromRecordBatch{*batch, 0}.GetOne();
}

Expression project(std::vector<Expression> values, std::vector<std::string> names) {
  return call("struct", std::move(values), compute::StructOptions{std::move(names)});
}

Expression equal(Expression lhs, Expression rhs) {
  return call("equal", {std::move(lhs), std::move(rhs)});
}

Expression not_equal(Expression lhs, Expression rhs) {
  return call("not_equal", {std::move(lhs), std::move(rhs)});
}

Expression less(Expression lhs, Expression rhs) {
  return call("less", {std::move(lhs), std::move(rhs)});
}

Expression less_equal(Expression lhs, Expression rhs) {
  return call("less_equal", {std::move(lhs), std::move(rhs)});
}

Expression greater(Expression lhs, Expression rhs) {
  return call("greater", {std::move(lhs), std::move(rhs)});
}

Expression greater_equal(Expression lhs, Expression rhs) {
  return call("greater_equal", {std::move(lhs), std::move(rhs)});
}

Expression and_(Expression lhs, Expression rhs) {
  return call("and_kleene", {std::move(lhs), std::move(rhs)});
}

Expression and_(const std::vector<Expression>& operands) {
  auto folded = FoldLeft<Expression(Expression, Expression)>(operands.begin(),
                                                             operands.end(), and_);
  if (folded) {
    return std::move(*folded);
  }
  return literal(true);
}

Expression or_(Expression lhs, Expression rhs) {
  return call("or_kleene", {std::move(lhs), std::move(rhs)});
}

Expression or_(const std::vector<Expression>& operands) {
  auto folded =
      FoldLeft<Expression(Expression, Expression)>(operands.begin(), operands.end(), or_);
  if (folded) {
    return std::move(*folded);
  }
  return literal(false);
}

Expression not_(Expression operand) { return call("invert", {std::move(operand)}); }

Expression operator&&(Expression lhs, Expression rhs) {
  return and_(std::move(lhs), std::move(rhs));
}

Expression operator||(Expression lhs, Expression rhs) {
  return or_(std::move(lhs), std::move(rhs));
}

}  // namespace dataset
}  // namespace arrow
