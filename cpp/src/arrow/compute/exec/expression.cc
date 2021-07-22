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

#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec/expression_internal.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/function_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

void Expression::Call::ComputeHash() {
  hash = std::hash<std::string>{}(function_name);
  for (const auto& arg : arguments) {
    arrow::internal::hash_combine(hash, arg.hash());
  }
}

Expression::Expression(Call call) {
  call.ComputeHash();
  impl_ = std::make_shared<Impl>(std::move(call));
}

Expression::Expression(Datum literal)
    : impl_(std::make_shared<Impl>(std::move(literal))) {}

Expression::Expression(Parameter parameter)
    : impl_(std::make_shared<Impl>(std::move(parameter))) {}

Expression literal(Datum lit) { return Expression(std::move(lit)); }

Expression field_ref(FieldRef ref) {
  return Expression(Expression::Parameter{std::move(ref), ValueDescr{}, -1});
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

const Expression::Parameter* Expression::parameter() const {
  return util::get_if<Parameter>(impl_.get());
}

const FieldRef* Expression::field_ref() const {
  if (auto parameter = this->parameter()) {
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

  if (auto parameter = this->parameter()) {
    return parameter->descr;
  }

  return CallNotNull(*this)->descr;
}

namespace {

std::string PrintDatum(const Datum& datum) {
  if (datum.is_scalar()) {
    if (!datum.scalar()->is_valid) return "null";

    switch (datum.type()->id()) {
      case Type::STRING:
      case Type::LARGE_STRING:
        return '"' +
               Escape(util::string_view(*datum.scalar_as<BaseBinaryScalar>().value)) +
               '"';

      case Type::BINARY:
      case Type::FIXED_SIZE_BINARY:
      case Type::LARGE_BINARY:
        return '"' + datum.scalar_as<BaseBinaryScalar>().value->ToHexString() + '"';

      default:
        break;
    }

    return datum.scalar()->ToString();
  }
  return datum.ToString();
}

}  // namespace

std::string Expression::ToString() const {
  if (auto lit = literal()) {
    return PrintDatum(*lit);
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

  if (auto options = GetMakeStructOptions(*call)) {
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

  if (call->options) {
    out += call->options->ToString();
    out.resize(out.size() + 1);
  } else {
    out.resize(out.size() - 1);
  }
  out.back() = ')';
  return out;
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
  if (call->options && other_call->options) {
    return call->options->Equals(other_call->options);
  }
  return false;
}

bool Identical(const Expression& l, const Expression& r) { return l.impl_ == r.impl_; }

size_t Expression::hash() const {
  if (auto lit = literal()) {
    if (lit->is_scalar()) {
      return lit->scalar()->hash();
    }
    return 0;
  }

  if (auto ref = field_ref()) {
    return ref->hash();
  }

  return CallNotNull(*this)->hash;
}

bool Expression::IsBound() const {
  if (type() == nullptr) return false;

  if (auto call = this->call()) {
    if (call->kernel == nullptr) return false;

    for (const Expression& arg : call->arguments) {
      if (!arg.IsBound()) return false;
    }
  }

  return true;
}

bool Expression::IsScalarExpression() const {
  if (auto lit = literal()) {
    return lit->is_scalar();
  }

  if (field_ref()) return true;

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
  if (type() && type()->id() == Type::NA) {
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

  return true;
}

namespace {

// Produce a bound Expression from unbound Call and bound arguments.
Result<Expression> BindNonRecursive(Expression::Call call, bool insert_implicit_casts,
                                    compute::ExecContext* exec_context) {
  DCHECK(std::all_of(call.arguments.begin(), call.arguments.end(),
                     [](const Expression& argument) { return argument.IsBound(); }));

  auto descrs = GetDescriptors(call.arguments);
  ARROW_ASSIGN_OR_RAISE(call.function, GetFunction(call, exec_context));

  if (!insert_implicit_casts) {
    ARROW_ASSIGN_OR_RAISE(call.kernel, call.function->DispatchExact(descrs));
  } else {
    ARROW_ASSIGN_OR_RAISE(call.kernel, call.function->DispatchBest(&descrs));

    for (size_t i = 0; i < descrs.size(); ++i) {
      if (descrs[i] == call.arguments[i].descr()) continue;

      if (descrs[i].shape != call.arguments[i].descr().shape) {
        return Status::NotImplemented(
            "Automatic broadcasting of scalars arguments to arrays in ",
            Expression(std::move(call)).ToString());
      }

      if (auto lit = call.arguments[i].literal()) {
        ARROW_ASSIGN_OR_RAISE(Datum new_lit, compute::Cast(*lit, descrs[i].type));
        call.arguments[i] = literal(std::move(new_lit));
        continue;
      }

      // construct an implicit cast Expression with which to replace this argument
      Expression::Call implicit_cast;
      implicit_cast.function_name = "cast";
      implicit_cast.arguments = {std::move(call.arguments[i])};
      implicit_cast.options = std::make_shared<compute::CastOptions>(
          compute::CastOptions::Safe(descrs[i].type));

      ARROW_ASSIGN_OR_RAISE(
          call.arguments[i],
          BindNonRecursive(std::move(implicit_cast),
                           /*insert_implicit_casts=*/false, exec_context));
    }
  }

  compute::KernelContext kernel_context(exec_context);
  if (call.kernel->init) {
    ARROW_ASSIGN_OR_RAISE(
        call.kernel_state,
        call.kernel->init(&kernel_context, {call.kernel, descrs, call.options.get()}));

    kernel_context.SetState(call.kernel_state.get());
  }

  ARROW_ASSIGN_OR_RAISE(
      call.descr, call.kernel->signature->out_type().Resolve(&kernel_context, descrs));

  return Expression(std::move(call));
}

template <typename TypeOrSchema>
Result<Expression> BindImpl(Expression expr, const TypeOrSchema& in,
                            ValueDescr::Shape shape, compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return BindImpl(std::move(expr), in, shape, &exec_context);
  }

  if (expr.literal()) return expr;

  if (auto ref = expr.field_ref()) {
    if (ref->IsNested()) {
      return Status::NotImplemented("nested field references");
    }

    ARROW_ASSIGN_OR_RAISE(auto path, ref->FindOne(in));

    auto bound = *expr.parameter();
    bound.index = path[0];
    ARROW_ASSIGN_OR_RAISE(auto field, path.Get(in));
    bound.descr.type = field->type();
    bound.descr.shape = shape;
    return Expression{std::move(bound)};
  }

  auto call = *CallNotNull(expr);
  for (auto& argument : call.arguments) {
    ARROW_ASSIGN_OR_RAISE(argument,
                          BindImpl(std::move(argument), in, shape, exec_context));
  }
  return BindNonRecursive(std::move(call),
                          /*insert_implicit_casts=*/true, exec_context);
}

}  // namespace

Result<Expression> Expression::Bind(const ValueDescr& in,
                                    compute::ExecContext* exec_context) const {
  return BindImpl(*this, *in.type, in.shape, exec_context);
}

Result<Expression> Expression::Bind(const Schema& in_schema,
                                    compute::ExecContext* exec_context) const {
  return BindImpl(*this, in_schema, ValueDescr::ARRAY, exec_context);
}

Result<ExecBatch> MakeExecBatch(const Schema& full_schema, const Datum& partial) {
  ExecBatch out;

  if (partial.kind() == Datum::RECORD_BATCH) {
    const auto& partial_batch = *partial.record_batch();
    out.length = partial_batch.num_rows();

    for (const auto& field : full_schema.fields()) {
      ARROW_ASSIGN_OR_RAISE(auto column,
                            FieldRef(field->name()).GetOneOrNone(partial_batch));

      if (column) {
        if (!column->type()->Equals(field->type())) {
          // Referenced field was present but didn't have the expected type.
          // This *should* be handled by readers, and will just be an error in the future.
          ARROW_ASSIGN_OR_RAISE(
              auto converted,
              compute::Cast(column, field->type(), compute::CastOptions::Safe()));
          column = converted.make_array();
        }
        out.values.emplace_back(std::move(column));
      } else {
        out.values.emplace_back(MakeNullScalar(field->type()));
      }
    }
    return out;
  }

  // wasteful but useful for testing:
  if (partial.type()->id() == Type::STRUCT) {
    if (partial.is_array()) {
      ARROW_ASSIGN_OR_RAISE(auto partial_batch,
                            RecordBatch::FromStructArray(partial.make_array()));

      return MakeExecBatch(full_schema, partial_batch);
    }

    if (partial.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(auto partial_array,
                            MakeArrayFromScalar(*partial.scalar(), 1));
      ARROW_ASSIGN_OR_RAISE(auto out, MakeExecBatch(full_schema, partial_array));

      for (Datum& value : out.values) {
        if (value.is_scalar()) continue;
        ARROW_ASSIGN_OR_RAISE(value, value.make_array()->GetScalar(0));
      }
      return out;
    }
  }

  return Status::NotImplemented("MakeExecBatch from ", PrintDatum(partial));
}

Result<Datum> ExecuteScalarExpression(const Expression& expr, const Schema& full_schema,
                                      const Datum& partial_input,
                                      compute::ExecContext* exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto input, MakeExecBatch(full_schema, partial_input));
  return ExecuteScalarExpression(expr, input, exec_context);
}

Result<Datum> ExecuteScalarExpression(const Expression& expr, const ExecBatch& input,
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

  if (auto param = expr.parameter()) {
    if (param->descr.type->id() == Type::NA) {
      return MakeNullScalar(null());
    }

    const Datum& field = input[param->index];
    if (!field.type()->Equals(param->descr.type)) {
      return Status::Invalid("Referenced field ", expr.ToString(), " was ",
                             field.type()->ToString(), " but should have been ",
                             param->descr.type->ToString());
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

namespace {

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

}  // namespace

std::vector<FieldRef> FieldsInExpression(const Expression& expr) {
  if (expr.literal()) return {};

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

bool ExpressionHasFieldRefs(const Expression& expr) {
  if (expr.literal()) return false;

  if (expr.field_ref()) return true;

  for (const Expression& arg : CallNotNull(expr)->arguments) {
    if (ExpressionHasFieldRefs(arg)) return true;
  }
  return false;
}

Result<Expression> FoldConstants(Expression expr) {
  return Modify(
      std::move(expr), [](Expression expr) { return expr; },
      [](Expression expr, ...) -> Result<Expression> {
        auto call = CallNotNull(expr);
        if (std::all_of(call->arguments.begin(), call->arguments.end(),
                        [](const Expression& argument) { return argument.literal(); })) {
          // all arguments are literal; we can evaluate this subexpression *now*
          static const ExecBatch ignored_input = ExecBatch{};
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

namespace {

std::vector<Expression> GuaranteeConjunctionMembers(
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

                       if (call->function_name == "is_null") {
                         auto ref = call->arguments[0].field_ref();
                         return !ref;
                       }

                       return true;
                     });

  for (auto it = unconsumed_end; it != conjunction_members->end(); ++it) {
    auto call = CallNotNull(*it);

    if (call->function_name == "equal") {
      auto ref = call->arguments[0].field_ref();
      auto lit = call->arguments[1].literal();
      known_values->emplace(*ref, *lit);
    } else if (call->function_name == "is_null") {
      auto ref = call->arguments[0].field_ref();
      known_values->emplace(*ref, Datum(std::make_shared<NullScalar>()));
    }
  }

  conjunction_members->erase(unconsumed_end, conjunction_members->end());

  return Status::OK();
}

}  // namespace

Result<KnownFieldValues> ExtractKnownFieldValues(
    const Expression& guaranteed_true_predicate) {
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);
  KnownFieldValues known_values;
  RETURN_NOT_OK(ExtractKnownFieldValuesImpl(&conjunction_members, &known_values.map));
  return known_values;
}

Result<Expression> ReplaceFieldsWithKnownValues(const KnownFieldValues& known_values,
                                                Expression expr) {
  if (!expr.IsBound()) {
    return Status::Invalid(
        "ReplaceFieldsWithKnownValues called on an unbound Expression");
  }

  return Modify(
      std::move(expr),
      [&known_values](Expression expr) -> Result<Expression> {
        if (auto ref = expr.field_ref()) {
          auto it = known_values.map.find(*ref);
          if (it != known_values.map.end()) {
            Datum lit = it->second;
            if (lit.descr() == expr.descr()) return literal(std::move(lit));
            // type mismatch, try casting the known value to the correct type

            if (expr.type()->id() == Type::DICTIONARY &&
                lit.type()->id() != Type::DICTIONARY) {
              // the known value must be dictionary encoded

              const auto& dict_type = checked_cast<const DictionaryType&>(*expr.type());
              if (!lit.type()->Equals(dict_type.value_type())) {
                ARROW_ASSIGN_OR_RAISE(lit, compute::Cast(lit, dict_type.value_type()));
              }

              if (lit.is_scalar()) {
                ARROW_ASSIGN_OR_RAISE(auto dictionary,
                                      MakeArrayFromScalar(*lit.scalar(), 1));

                lit = Datum{DictionaryScalar::Make(MakeScalar<int32_t>(0),
                                                   std::move(dictionary))};
              }
            }

            ARROW_ASSIGN_OR_RAISE(lit, compute::Cast(lit, expr.type()));
            return literal(std::move(lit));
          }
        }
        return expr;
      },
      [](Expression expr, ...) { return expr; });
}

namespace {

bool IsBinaryAssociativeCommutative(const Expression::Call& call) {
  static std::unordered_set<std::string> binary_associative_commutative{
      "and",      "or",  "and_kleene",       "or_kleene",  "xor",
      "multiply", "add", "multiply_checked", "add_checked"};

  auto it = binary_associative_commutative.find(call.function_name);
  return it != binary_associative_commutative.end();
}

}  // namespace

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

            std::swap(flipped_call.arguments[0], flipped_call.arguments[1]);
            flipped_call.function_name =
                Comparison::GetName(Comparison::GetFlipped(*cmp));

            return BindNonRecursive(flipped_call,
                                    /*insert_implicit_casts=*/false, exec_context);
          }
        }

        return expr;
      },
      [](Expression expr, ...) { return expr; });
}

namespace {

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

        if (!cmp) return expr;
        if (!cmp_guarantee) return expr;

        const auto& lhs = Comparison::StripOrderPreservingCasts(call->arguments[0]);
        const auto& guarantee_lhs = guarantee.arguments[0];
        if (lhs != guarantee_lhs) return expr;

        auto rhs = call->arguments[1].literal();
        auto guarantee_rhs = guarantee.arguments[1].literal();

        if (!rhs) return expr;
        if (!rhs->is_scalar()) return expr;

        if (!guarantee_rhs) return expr;
        if (!guarantee_rhs->is_scalar()) return expr;

        ARROW_ASSIGN_OR_RAISE(auto cmp_rhs_guarantee_rhs,
                              Comparison::Execute(*rhs, *guarantee_rhs));
        DCHECK_NE(cmp_rhs_guarantee_rhs, Comparison::NA);

        if (cmp_rhs_guarantee_rhs == Comparison::EQUAL) {
          // RHS of filter is equal to RHS of guarantee

          if ((*cmp & *cmp_guarantee) == *cmp_guarantee) {
            // guarantee is a subset of filter, so all data will be included
            // x > 1, x >= 1, x != 1 guaranteed by x > 1
            return literal(true);
          }

          if ((*cmp & *cmp_guarantee) == 0) {
            // guarantee disjoint with filter, so all data will be excluded
            // x > 1, x >= 1, x != 1 unsatisfiable if x == 1
            return literal(false);
          }

          return expr;
        }

        if (*cmp_guarantee & cmp_rhs_guarantee_rhs) {
          // x > 1, x >= 1, x != 1 cannot use guarantee x >= 3
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

}  // namespace

Result<Expression> SimplifyWithGuarantee(Expression expr,
                                         const Expression& guaranteed_true_predicate) {
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);

  KnownFieldValues known_values;
  RETURN_NOT_OK(ExtractKnownFieldValuesImpl(&conjunction_members, &known_values.map));

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
        ARROW_ASSIGN_OR_RAISE(auto options_scalar,
                              internal::FunctionOptionsToStructScalar(*call->options));
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

Result<Expression> Deserialize(std::shared_ptr<Buffer> buffer) {
  io::BufferReader stream(std::move(buffer));
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
      if (!::arrow::internal::ParseValue<Int32Type>(i.data(), i.length(),
                                                    &column_index)) {
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
          std::shared_ptr<compute::FunctionOptions> options;
          if (options_scalar) {
            ARROW_ASSIGN_OR_RAISE(
                options, internal::FunctionOptionsFromStructScalar(
                             checked_cast<const StructScalar&>(*options_scalar)));
          }
          auto expr = call(value, std::move(arguments), std::move(options));
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
  return call("make_struct", std::move(values),
              compute::MakeStructOptions{std::move(names)});
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

Expression is_null(Expression lhs) { return call("is_null", {std::move(lhs)}); }

Expression is_valid(Expression lhs) { return call("is_valid", {std::move(lhs)}); }

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

}  // namespace compute
}  // namespace arrow
