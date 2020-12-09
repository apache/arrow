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
#include "arrow/dataset/filter.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace dataset {

const Expression2::Call* Expression2::call() const {
  return util::get_if<Call>(impl_.get());
}

const Datum* Expression2::literal() const { return util::get_if<Datum>(impl_.get()); }

const FieldRef* Expression2::field_ref() const {
  return util::get_if<FieldRef>(impl_.get());
}

Expression2::operator std::shared_ptr<Expression>() const {
  if (auto lit = literal()) {
    DCHECK(lit->is_scalar());
    return std::make_shared<ScalarExpression>(lit->scalar());
  }

  if (auto ref = field_ref()) {
    DCHECK(ref->name());
    return std::make_shared<FieldExpression>(*ref->name());
  }

  auto call = CallNotNull(*this);
  if (call->function == "invert") {
    return std::make_shared<NotExpression>(call->arguments[0]);
  }

  if (call->function == "cast") {
    const auto& options = checked_cast<const compute::CastOptions&>(*call->options);
    return std::make_shared<CastExpression>(call->arguments[0], options.to_type, options);
  }

  if (call->function == "and_kleene") {
    return std::make_shared<AndExpression>(call->arguments[0], call->arguments[1]);
  }

  if (call->function == "or_kleene") {
    return std::make_shared<OrExpression>(call->arguments[0], call->arguments[1]);
  }

  if (auto cmp = Comparison::Get(call->function)) {
    compute::CompareOperator op = [&] {
      switch (*cmp) {
        case Comparison::EQUAL:
          return compute::EQUAL;
        case Comparison::LESS:
          return compute::LESS;
        case Comparison::GREATER:
          return compute::GREATER;
        case Comparison::NOT_EQUAL:
          return compute::NOT_EQUAL;
        case Comparison::LESS_EQUAL:
          return compute::LESS_EQUAL;
        case Comparison::GREATER_EQUAL:
          return compute::GREATER_EQUAL;
        default:
          break;
      }
      return static_cast<compute::CompareOperator>(-1);
    }();

    return std::make_shared<ComparisonExpression>(op, call->arguments[0],
                                                  call->arguments[1]);
  }

  if (call->function == "is_valid") {
    return std::make_shared<IsValidExpression>(call->arguments[0]);
  }

  if (call->function == "is_in") {
    auto set = checked_cast<const compute::SetLookupOptions&>(*call->options)
                   .value_set.make_array();
    return std::make_shared<InExpression>(call->arguments[0], std::move(set));
  }

  DCHECK(false) << "untranslatable Expression2: " << ToString();
  return nullptr;
}

Expression2::Expression2(const Expression& expr) {
  switch (expr.type()) {
    case ExpressionType::FIELD:
      *this =
          ::arrow::dataset::field_ref(checked_cast<const FieldExpression&>(expr).name());
      return;

    case ExpressionType::SCALAR:
      *this =
          ::arrow::dataset::literal(checked_cast<const ScalarExpression&>(expr).value());
      return;

    case ExpressionType::NOT:
      *this = ::arrow::dataset::call(
          "invert", {checked_cast<const NotExpression&>(expr).operand()});
      return;

    case ExpressionType::CAST: {
      const auto& cast_expr = checked_cast<const CastExpression&>(expr);
      auto options = cast_expr.options();
      options.to_type = cast_expr.to_type();
      *this = ::arrow::dataset::call("cast", {cast_expr.operand()}, std::move(options));
      return;
    }

    case ExpressionType::AND: {
      const auto& and_expr = checked_cast<const AndExpression&>(expr);
      *this = ::arrow::dataset::call("and_kleene",
                                     {and_expr.left_operand(), and_expr.right_operand()});
      return;
    }

    case ExpressionType::OR: {
      const auto& or_expr = checked_cast<const OrExpression&>(expr);
      *this = ::arrow::dataset::call("or_kleene",
                                     {or_expr.left_operand(), or_expr.right_operand()});
      return;
    }

    case ExpressionType::COMPARISON: {
      const auto& cmp_expr = checked_cast<const ComparisonExpression&>(expr);
      static std::array<std::string, 6> ops = {
          "equal", "not_equal", "greater", "greater_equal", "less", "less_equal",
      };
      *this = ::arrow::dataset::call(ops[cmp_expr.op()],
                                     {cmp_expr.left_operand(), cmp_expr.right_operand()});
      return;
    }

    case ExpressionType::IS_VALID: {
      const auto& is_valid_expr = checked_cast<const IsValidExpression&>(expr);
      *this = ::arrow::dataset::call("is_valid", {is_valid_expr.operand()});
      return;
    }

    case ExpressionType::IN: {
      const auto& in_expr = checked_cast<const InExpression&>(expr);
      *this = ::arrow::dataset::call(
          "is_in", {in_expr.operand()},
          compute::SetLookupOptions{in_expr.set(), /*skip_nulls=*/true});
      return;
    }

    default:
      break;
  }

  DCHECK(false) << "untranslatable Expression: " << expr.ToString();
}

std::string Expression2::ToString() const {
  if (auto lit = literal()) {
    if (lit->is_scalar()) {
      return lit->scalar()->ToString();
    }
    return lit->ToString();
  }

  if (auto ref = field_ref()) {
    if (auto name = ref->name()) {
      return "FieldRef(" + *name + ")";
    }
    if (auto path = ref->field_path()) {
      return path->ToString();
    }
    return ref->ToString();
  }

  auto call = CallNotNull(*this);

  // FIXME represent FunctionOptions
  std::string out = call->function + "(";
  for (const auto& arg : call->arguments) {
    out += arg.ToString() + ",";
  }
  out.back() = ')';
  return out;
}

void PrintTo(const Expression2& expr, std::ostream* os) {
  *os << expr.ToString();
  if (expr.IsBound()) {
    *os << "[bound]";
  }
}

bool Expression2::Equals(const Expression2& other) const {
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

  if (call->function != other_call->function || call->kernel != other_call->kernel) {
    return false;
  }

  // FIXME compare FunctionOptions for equality
  for (size_t i = 0; i < call->arguments.size(); ++i) {
    if (!call->arguments[i].Equals(other_call->arguments[i])) {
      return false;
    }
  }

  return true;
}

size_t Expression2::hash() const {
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

  size_t out = std::hash<std::string>{}(call->function);
  for (const auto& arg : call->arguments) {
    out ^= arg.hash();
  }
  return out;
}

bool Expression2::IsBound() const {
  if (descr_.type == nullptr) return false;

  if (auto lit = literal()) return true;

  if (auto ref = field_ref()) return true;

  auto call = CallNotNull(*this);

  for (const Expression2& arg : call->arguments) {
    if (!arg.IsBound()) return false;
  }

  return call->kernel != nullptr;
}

bool Expression2::IsScalarExpression() const {
  if (auto lit = literal()) {
    return lit->is_scalar();
  }

  // FIXME handle case where a list's item field is referenced
  if (auto ref = field_ref()) return true;

  auto call = CallNotNull(*this);

  for (const Expression2& arg : call->arguments) {
    if (!arg.IsScalarExpression()) return false;
  }

  if (call->kernel == nullptr) {
    // this expression is not bound; make a best guess based on
    // the default function registry
    if (auto function = compute::GetFunctionRegistry()
                            ->GetFunction(call->function)
                            .ValueOr(nullptr)) {
      return function->kind() == compute::Function::SCALAR;
    }

    // unknown function or other error; conservatively return false
    return false;
  }

  return call->function_kind == compute::Function::SCALAR;
}

bool Expression2::IsNullLiteral() const {
  if (auto lit = literal()) {
    if (lit->null_count() == lit->length()) {
      return true;
    }
  }

  return false;
}

bool Expression2::IsSatisfiable() const {
  if (descr_.type && descr_.type->id() == Type::NA) {
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
    const Expression2::Call& call, compute::ExecContext* exec_context) {
  if (!call.kernel->init) return nullptr;

  compute::KernelContext kernel_context(exec_context);
  auto kernel_state = call.kernel->init(
      &kernel_context, {call.kernel, GetDescriptors(call.arguments), call.options.get()});

  RETURN_NOT_OK(kernel_context.status());
  return std::move(kernel_state);
}

Result<Expression2> Expression2::Bind(ValueDescr in,
                                      compute::ExecContext* exec_context) const {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return Bind(std::move(in), &exec_context);
  }

  if (literal()) return *this;

  if (auto ref = field_ref()) {
    ARROW_ASSIGN_OR_RAISE(auto field, ref->GetOneOrNone(*in.type));
    auto out = *this;
    out.descr_ = field ? ValueDescr{field->type(), in.shape} : ValueDescr::Scalar(null());
    return out;
  }

  auto bound_call = *CallNotNull(*this);

  ARROW_ASSIGN_OR_RAISE(auto function, GetFunction(bound_call, exec_context));
  bound_call.function_kind = function->kind();

  for (auto&& argument : bound_call.arguments) {
    ARROW_ASSIGN_OR_RAISE(argument, argument.Bind(in, exec_context));
  }

  if (RequriesDictionaryTransparency(bound_call)) {
    RETURN_NOT_OK(EnsureNotDictionary(&bound_call));
  }

  auto descrs = GetDescriptors(bound_call.arguments);
  for (auto&& descr : descrs) {
    if (RequriesDictionaryTransparency(bound_call)) {
      RETURN_NOT_OK(EnsureNotDictionary(&descr));
    }
  }

  ARROW_ASSIGN_OR_RAISE(bound_call.kernel, function->DispatchExact(descrs));

  compute::KernelContext kernel_context(exec_context);
  ARROW_ASSIGN_OR_RAISE(bound_call.kernel_state,
                        InitKernelState(bound_call, exec_context));
  kernel_context.SetState(bound_call.kernel_state.get());

  ARROW_ASSIGN_OR_RAISE(auto descr, bound_call.kernel->signature->out_type().Resolve(
                                        &kernel_context, descrs));

  return Expression2(std::make_shared<Impl>(std::move(bound_call)), std::move(descr));
}

Result<Expression2> Expression2::Bind(const Schema& in_schema,
                                      compute::ExecContext* exec_context) const {
  return Bind(ValueDescr::Array(struct_(in_schema.fields())), exec_context);
}

Result<Datum> ExecuteScalarExpression(const Expression2& expr, const Datum& input,
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

    if (RequriesDictionaryTransparency(*call)) {
      RETURN_NOT_OK(EnsureNotDictionary(&arguments[i]));
    }
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

std::array<std::pair<const Expression2&, const Expression2&>, 2>
ArgumentsAndFlippedArguments(const Expression2::Call& call) {
  DCHECK_EQ(call.arguments.size(), 2);
  return {std::pair<const Expression2&, const Expression2&>{call.arguments[0],
                                                            call.arguments[1]},
          std::pair<const Expression2&, const Expression2&>{call.arguments[1],
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
    const Expression2::Call& call) {
  if (call.function_kind == compute::Function::SCALAR) {
    return static_cast<const compute::ScalarKernel*>(call.kernel)->null_handling;
  }
  return util::nullopt;
}

bool DefinitelyNotNull(const Expression2& expr) {
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

std::vector<FieldRef> FieldsInExpression(const Expression2& expr) {
  if (auto lit = expr.literal()) return {};

  if (auto ref = expr.field_ref()) {
    return {*ref};
  }

  std::vector<FieldRef> fields;
  for (const Expression2& arg : CallNotNull(expr)->arguments) {
    auto argument_fields = FieldsInExpression(arg);
    std::move(argument_fields.begin(), argument_fields.end(), std::back_inserter(fields));
  }
  return fields;
}

template <typename PreVisit, typename PostVisitCall>
Result<Expression2> Modify(Expression2 expr, const PreVisit& pre,
                           const PostVisitCall& post_call) {
  ARROW_ASSIGN_OR_RAISE(expr, Result<Expression2>(pre(std::move(expr))));

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
    auto modified_expr = Expression2(
        std::make_shared<Expression2::Impl>(std::move(modified_call)), expr.descr());

    return post_call(std::move(modified_expr), &expr);
  }

  return post_call(std::move(expr), nullptr);
}

Result<Expression2> FoldConstants(Expression2 expr) {
  return Modify(
      std::move(expr), [](Expression2 expr) { return expr; },
      [](Expression2 expr, ...) -> Result<Expression2> {
        auto call = CallNotNull(expr);
        if (std::all_of(call->arguments.begin(), call->arguments.end(),
                        [](const Expression2& argument) { return argument.literal(); })) {
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

        if (call->function == "and_kleene") {
          for (auto args : ArgumentsAndFlippedArguments(*call)) {
            // true and x == x
            if (args.first == literal(true)) return args.second;

            // false and x == false
            if (args.first == literal(false)) return args.first;
          }
          return expr;
        }

        if (call->function == "or_kleene") {
          for (auto args : ArgumentsAndFlippedArguments(*call)) {
            // false or x == x
            if (args.first == literal(false)) return args.second;

            // true or x == true
            if (args.first == literal(true)) return args.first;
          }
          return expr;
        }

        return expr;
      });
}

inline std::vector<Expression2> GuaranteeConjunctionMembers(
    const Expression2& guaranteed_true_predicate) {
  auto guarantee = guaranteed_true_predicate.call();
  if (!guarantee || guarantee->function != "and_kleene") {
    return {guaranteed_true_predicate};
  }
  return FlattenedAssociativeChain(guaranteed_true_predicate).fringe;
}

// Conjunction members which are represented in known_values are erased from
// conjunction_members
Status ExtractKnownFieldValuesImpl(
    std::vector<Expression2>* conjunction_members,
    std::unordered_map<FieldRef, Datum, FieldRef::Hash>* known_values) {
  for (auto&& member : *conjunction_members) {
    ARROW_ASSIGN_OR_RAISE(member, Canonicalize({std::move(member)}));
  }

  auto unconsumed_end =
      std::partition(conjunction_members->begin(), conjunction_members->end(),
                     [](const Expression2& expr) {
                       // search for an equality conditions between a field and a literal
                       auto call = expr.call();
                       if (!call) return true;

                       if (call->function == "equal") {
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
    const Expression2& guaranteed_true_predicate) {
  if (!guaranteed_true_predicate.IsBound()) {
    return Status::Invalid("guaranteed_true_predicate was not bound");
  }
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);
  std::unordered_map<FieldRef, Datum, FieldRef::Hash> known_values;
  RETURN_NOT_OK(ExtractKnownFieldValuesImpl(&conjunction_members, &known_values));
  return known_values;
}

Result<Expression2> ReplaceFieldsWithKnownValues(
    const std::unordered_map<FieldRef, Datum, FieldRef::Hash>& known_values,
    Expression2 expr) {
  return Modify(
      std::move(expr),
      [&known_values](Expression2 expr) {
        if (auto ref = expr.field_ref()) {
          auto it = known_values.find(*ref);
          if (it != known_values.end()) {
            return literal(it->second);
          }
        }
        return expr;
      },
      [](Expression2 expr, ...) { return expr; });
}

inline bool IsBinaryAssociativeCommutative(const Expression2::Call& call) {
  static std::unordered_set<std::string> binary_associative_commutative{
      "and",      "or",  "and_kleene",       "or_kleene",  "xor",
      "multiply", "add", "multiply_checked", "add_checked"};

  auto it = binary_associative_commutative.find(call.function);
  return it != binary_associative_commutative.end();
}

Result<Expression2> Canonicalize(Expression2 expr, compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return Canonicalize(std::move(expr), &exec_context);
  }

  // If potentially reconstructing more deeply than a call's immediate arguments
  // (for example, when reorganizing an associative chain), add expressions to this set to
  // avoid unnecessary work
  struct {
    std::unordered_set<Expression2, Expression2::Hash> set_;

    bool operator()(const Expression2& expr) const {
      return set_.find(expr) != set_.end();
    }

    void Add(std::vector<Expression2> exprs) {
      std::move(exprs.begin(), exprs.end(), std::inserter(set_, set_.end()));
    }
  } AlreadyCanonicalized;

  return Modify(
      std::move(expr),
      [&AlreadyCanonicalized, exec_context](Expression2 expr) -> Result<Expression2> {
        auto call = expr.call();
        if (!call) return expr;

        if (AlreadyCanonicalized(expr)) return expr;

        if (IsBinaryAssociativeCommutative(*call)) {
          struct {
            int Priority(const Expression2& operand) const {
              // order literals first, starting with nulls
              if (operand.IsNullLiteral()) return 0;
              if (operand.literal()) return 1;
              return 2;
            }
            bool operator()(const Expression2& l, const Expression2& r) const {
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
          const auto& descr = expr.descr();
          auto folded = FoldLeft(
              chain.fringe.begin(), chain.fringe.end(),
              [call, &descr, &AlreadyCanonicalized](Expression2 l, Expression2 r) {
                auto ret = *call;
                ret.arguments = {std::move(l), std::move(r)};
                Expression2 expr(std::make_shared<Expression2::Impl>(std::move(ret)),
                                 descr);
                AlreadyCanonicalized.Add({expr});
                return expr;
              });
          return std::move(*folded);
        }

        if (auto cmp = Comparison::Get(call->function)) {
          if (call->arguments[0].literal() && !call->arguments[1].literal()) {
            // ensure that literals are on comparisons' RHS
            auto flipped_call = *call;
            flipped_call.function = Comparison::GetName(Comparison::GetFlipped(*cmp));
            // look up the flipped kernel
            // TODO extract a helper for use here and in Bind
            ARROW_ASSIGN_OR_RAISE(
                auto function,
                exec_context->func_registry()->GetFunction(flipped_call.function));

            auto descrs = GetDescriptors(flipped_call.arguments);
            for (auto& descr : descrs) {
              if (RequriesDictionaryTransparency(flipped_call)) {
                RETURN_NOT_OK(EnsureNotDictionary(&descr));
              }
            }
            ARROW_ASSIGN_OR_RAISE(flipped_call.kernel, function->DispatchExact(descrs));

            std::swap(flipped_call.arguments[0], flipped_call.arguments[1]);
            return Expression2(
                std::make_shared<Expression2::Impl>(std::move(flipped_call)),
                expr.descr());
          }
        }

        return expr;
      },
      [](Expression2 expr, ...) { return expr; });
}

Result<Expression2> DirectComparisonSimplification(Expression2 expr,
                                                   const Expression2::Call& guarantee) {
  return Modify(
      std::move(expr), [](Expression2 expr) { return expr; },
      [&guarantee](Expression2 expr, ...) -> Result<Expression2> {
        auto call = expr.call();
        if (!call) return expr;

        // Ensure both calls are comparisons with equal LHS and scalar RHS
        auto cmp = Comparison::Get(expr);
        auto cmp_guarantee = Comparison::Get(guarantee.function);
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

Result<Expression2> SimplifyWithGuarantee(Expression2 expr,
                                          const Expression2& guaranteed_true_predicate) {
  if (!guaranteed_true_predicate.IsBound()) {
    return Status::Invalid("guaranteed_true_predicate was not bound");
  }

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

Result<std::shared_ptr<Buffer>> Serialize(const Expression2& expr) {
  struct {
    std::shared_ptr<KeyValueMetadata> metadata_ = std::make_shared<KeyValueMetadata>();
    ArrayVector columns_;

    Result<std::string> AddScalar(const Scalar& scalar) {
      auto ret = columns_.size();
      ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(scalar, 1));
      columns_.push_back(std::move(array));
      return std::to_string(ret);
    }

    Status Visit(const Expression2& expr) {
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
      metadata_->Append("call", call->function);

      for (const auto& argument : call->arguments) {
        RETURN_NOT_OK(Visit(argument));
      }

      if (call->options) {
        ARROW_ASSIGN_OR_RAISE(auto options_scalar, FunctionOptionsToStructScalar(*call));
        ARROW_ASSIGN_OR_RAISE(auto value, AddScalar(*options_scalar));
        metadata_->Append("options", std::move(value));
      }

      metadata_->Append("end", call->function);
      return Status::OK();
    }

    Result<std::shared_ptr<RecordBatch>> operator()(const Expression2& expr) {
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

Result<Expression2> Deserialize(const Buffer& buffer) {
  io::BufferReader stream(buffer);
  ARROW_ASSIGN_OR_RAISE(auto reader, ipc::RecordBatchFileReader::Open(&stream));
  ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(0));
  if (batch->schema()->metadata() == nullptr) {
    return Status::Invalid("serialized Expression2's batch repr had null metadata");
  }
  if (batch->num_rows() != 1) {
    return Status::Invalid(
        "serialized Expression2's batch repr was not a single row - had ",
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

    Result<Expression2> GetOne() {
      if (index_ >= metadata().size()) {
        return Status::Invalid("unterminated serialized Expression2");
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
        return Status::Invalid("Unrecognized serialized Expression2 key ", key);
      }

      std::vector<Expression2> arguments;
      while (metadata().key(index_) != "end") {
        if (metadata().key(index_) == "options") {
          ARROW_ASSIGN_OR_RAISE(auto options_scalar, GetScalar(metadata().value(index_)));
          auto expr = call(value, std::move(arguments));
          RETURN_NOT_OK(FunctionOptionsFromStructScalar(
              checked_cast<const StructScalar*>(options_scalar.get()),
              const_cast<Expression2::Call*>(expr.call())));
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

Expression2 project(std::vector<Expression2> values, std::vector<std::string> names) {
  return call("struct", std::move(values), compute::StructOptions{std::move(names)});
}

Expression2 equal(Expression2 lhs, Expression2 rhs) {
  return call("equal", {std::move(lhs), std::move(rhs)});
}

Expression2 not_equal(Expression2 lhs, Expression2 rhs) {
  return call("not_equal", {std::move(lhs), std::move(rhs)});
}

Expression2 less(Expression2 lhs, Expression2 rhs) {
  return call("less", {std::move(lhs), std::move(rhs)});
}

Expression2 less_equal(Expression2 lhs, Expression2 rhs) {
  return call("less_equal", {std::move(lhs), std::move(rhs)});
}

Expression2 greater(Expression2 lhs, Expression2 rhs) {
  return call("greater", {std::move(lhs), std::move(rhs)});
}

Expression2 greater_equal(Expression2 lhs, Expression2 rhs) {
  return call("greater_equal", {std::move(lhs), std::move(rhs)});
}

Expression2 and_(Expression2 lhs, Expression2 rhs) {
  return call("and_kleene", {std::move(lhs), std::move(rhs)});
}

Expression2 and_(const std::vector<Expression2>& operands) {
  auto folded = FoldLeft<Expression2(Expression2, Expression2)>(operands.begin(),
                                                                operands.end(), and_);
  if (folded) {
    return std::move(*folded);
  }
  return literal(true);
}

Expression2 or_(Expression2 lhs, Expression2 rhs) {
  return call("or_kleene", {std::move(lhs), std::move(rhs)});
}

Expression2 or_(const std::vector<Expression2>& operands) {
  auto folded = FoldLeft<Expression2(Expression2, Expression2)>(operands.begin(),
                                                                operands.end(), or_);
  if (folded) {
    return std::move(*folded);
  }
  return literal(false);
}

Expression2 not_(Expression2 operand) { return call("invert", {std::move(operand)}); }

Expression2 operator&&(Expression2 lhs, Expression2 rhs) {
  return and_(std::move(lhs), std::move(rhs));
}

Expression2 operator||(Expression2 lhs, Expression2 rhs) {
  return or_(std::move(lhs), std::move(rhs));
}

Result<Expression2> InsertImplicitCasts(Expression2 expr, const Schema& s) {
  std::shared_ptr<Expression> e(expr);
  return InsertImplicitCasts(*e, s);
}

}  // namespace dataset
}  // namespace arrow
