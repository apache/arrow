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
#include "arrow/compute/registry.h"
#include "arrow/dataset/expression_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string.h"

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

void PrintTo(const Expression2& expr, std::ostream* os) { *os << expr.ToString(); }

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

bool KernelStateIsImmutable(const std::string& function) {
  // XXX maybe just add Kernel::state_is_immutable or so?

  // known functions with non-null but nevertheless immutable KernelState
  static std::unordered_set<std::string> immutable_state = {
      "is_in", "index_in", "cast", "struct", "strptime",
  };

  return immutable_state.find(function) != immutable_state.end();
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

Result<std::shared_ptr<ExpressionState>> ExpressionState::Clone(
    const std::shared_ptr<ExpressionState>& state, const Expression2& expr,
    compute::ExecContext* exec_context) {
  if (!expr.IsBound()) {
    return Status::Invalid("Cannot clone State against an unbound expression.");
  }

  if (state == nullptr) return nullptr;

  auto call = CallNotNull(expr);
  auto call_state = checked_cast<const CallState*>(state.get());

  CallState clone;
  clone.argument_states.resize(call_state->argument_states.size());

  bool recursively_share = true;
  for (size_t i = 0; i < call->arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(
        clone.argument_states[i],
        Clone(call_state->argument_states[i], call->arguments[i], exec_context));

    if (clone.argument_states[i] != call_state->argument_states[i]) {
      recursively_share = false;
    }
  }

  if (call_state->kernel_state == nullptr || KernelStateIsImmutable(call->function)) {
    // The kernel's state is immutable so it's safe to just
    // share a pointer between threads
    if (recursively_share) {
      return state;
    }
    clone.kernel_state = call_state->kernel_state;
  } else {
    // The kernel's state must be re-initialized.
    ARROW_ASSIGN_OR_RAISE(clone.kernel_state, InitKernelState(*call, exec_context));
  }

  return std::make_shared<CallState>(std::move(clone));
}

Result<Expression2::StateAndBound> Expression2::Bind(
    ValueDescr in, compute::ExecContext* exec_context) const {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return Bind(std::move(in), &exec_context);
  }

  if (literal()) return StateAndBound{*this, nullptr};

  if (auto ref = field_ref()) {
    ARROW_ASSIGN_OR_RAISE(auto field, ref->GetOneOrNone(*in.type));
    auto bound = *this;
    bound.descr_ =
        field ? ValueDescr{field->type(), in.shape} : ValueDescr::Scalar(null());
    return StateAndBound{std::move(bound), nullptr};
  }

  auto bound_call = *CallNotNull(*this);

  std::shared_ptr<compute::Function> function;
  if (bound_call.function == "cast") {
    // XXX this special case is strange; why not make "cast" a ScalarFunction?
    const auto& to_type =
        checked_cast<const compute::CastOptions&>(*bound_call.options).to_type;
    ARROW_ASSIGN_OR_RAISE(function, compute::GetCastFunction(to_type));
  } else {
    ARROW_ASSIGN_OR_RAISE(
        function, exec_context->func_registry()->GetFunction(bound_call.function));
  }
  bound_call.function_kind = function->kind();

  std::vector<std::shared_ptr<ExpressionState>> argument_states(
      bound_call.arguments.size());
  for (size_t i = 0; i < argument_states.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(std::tie(bound_call.arguments[i], argument_states[i]),
                          bound_call.arguments[i].Bind(in, exec_context));
  }

  auto descrs = GetDescriptors(bound_call.arguments);
  ARROW_ASSIGN_OR_RAISE(bound_call.kernel, function->DispatchExact(descrs));

  auto call_state = std::make_shared<CallState>();
  ARROW_ASSIGN_OR_RAISE(call_state->kernel_state,
                        InitKernelState(bound_call, exec_context));
  call_state->argument_states = std::move(argument_states);

  compute::KernelContext kernel_context(exec_context);
  kernel_context.SetState(call_state->kernel_state.get());

  auto bound = Expression2(std::make_shared<Expression2::Impl>(std::move(bound_call)));
  ARROW_ASSIGN_OR_RAISE(bound.descr_, bound_call.kernel->signature->out_type().Resolve(
                                          &kernel_context, descrs));
  return StateAndBound{std::move(bound), std::move(call_state)};
}

Result<Expression2::StateAndBound> Expression2::Bind(
    const Schema& in_schema, compute::ExecContext* exec_context) const {
  return Bind(ValueDescr::Array(struct_(in_schema.fields())), exec_context);
}

Result<Datum> ExecuteScalarExpression(const Expression2& expr, ExpressionState* state,
                                      const Datum& input,
                                      compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return ExecuteScalarExpression(expr, state, input, &exec_context);
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
  auto call_state = checked_cast<CallState*>(state);

  std::vector<Datum> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    auto argument_state = call_state->argument_states[i].get();
    ARROW_ASSIGN_OR_RAISE(
        arguments[i],
        ExecuteScalarExpression(call->arguments[i], argument_state, input, exec_context));
  }

  auto executor = compute::detail::KernelExecutor::MakeScalar();

  compute::KernelContext kernel_context(exec_context);
  kernel_context.SetState(call_state->kernel_state.get());

  auto kernel = call->kernel;
  auto inputs = GetDescriptors(call->arguments);
  auto options = call->options.get();
  RETURN_NOT_OK(executor->Init(&kernel_context, {kernel, inputs, options}));

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

Result<Expression2> FoldConstants(Expression2 expr, ExpressionState* state) {
  DCHECK(expr.IsBound());

  struct StateAndIndex {
    CallState* state;
    int index;
  };
  std::vector<StateAndIndex> stack;

  auto root_state = checked_cast<CallState*>(state);
  return Modify(
      std::move(expr),
      [&stack, root_state](Expression2 expr) {
        auto call = expr.call();

        if (stack.empty()) {
          stack = {{root_state, 0}};
          return expr;
        }

        int i = stack.back().index++;

        if (!call) return expr;
        auto next_state = stack.back().state->argument_states[i].get();
        stack.push_back({checked_cast<CallState*>(next_state), 0});

        return expr;
      },
      [&stack](Expression2 expr, ...) -> Result<Expression2> {
        auto state = stack.back().state;
        stack.pop_back();

        auto call = CallNotNull(expr);
        if (std::all_of(call->arguments.begin(), call->arguments.end(),
                        [](const Expression2& argument) { return argument.literal(); })) {
          // all arguments are literal; we can evaluate this subexpression *now*
          static const Datum ignored_input;
          ARROW_ASSIGN_OR_RAISE(Datum constant,
                                ExecuteScalarExpression(expr, state, ignored_input));
          return literal(std::move(constant));
        }

        // XXX the following should probably be in a registry of passes instead of inline

        if (GetNullHandling(*call) == compute::NullHandling::INTERSECTION) {
          // kernels which always produce intersected validity can be resolved to null
          // *now* if any of their inputs is a null literal
          for (const auto& argument : call->arguments) {
            if (argument.IsNullLiteral()) return argument;
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

  return expr;
}

struct FlattenedAssociativeChain {
  bool was_left_folded = true;
  std::vector<Expression2> exprs, fringe;

  explicit FlattenedAssociativeChain(Expression2 expr) : exprs{std::move(expr)} {
    auto call = CallNotNull(exprs.back());
    fringe = call->arguments;

    auto it = fringe.begin();

    while (it != fringe.end()) {
      auto sub_call = it->call();
      if (!sub_call || sub_call->function != call->function) {
        ++it;
        continue;
      }

      if (it != fringe.begin()) {
        was_left_folded = false;
      }

      exprs.push_back(std::move(*it));
      it = fringe.erase(it);
      it = fringe.insert(it, sub_call->arguments.begin(), sub_call->arguments.end());
      // NB: no increment so we hit sub_call's first argument next iteration
    }

    DCHECK(std::all_of(exprs.begin(), exprs.end(), [](const Expression2& expr) {
      return CallNotNull(expr)->options == nullptr;
    }));
  }
};

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
    ARROW_ASSIGN_OR_RAISE(member, Canonicalize(std::move(member)));
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

template <typename It, typename BinOp,
          typename Out = typename std::iterator_traits<It>::value_type>
util::optional<Out> FoldLeft(It begin, It end, const BinOp& bin_op) {
  if (begin == end) return util::nullopt;

  Out folded = std::move(*begin++);
  while (begin != end) {
    folded = bin_op(std::move(folded), std::move(*begin++));
  }
  return folded;
}

inline bool IsBinaryAssociativeCommutative(const Expression2::Call& call) {
  static std::unordered_set<std::string> binary_associative_commutative{
      "and",      "or",  "and_kleene",       "or_kleene",  "xor",
      "multiply", "add", "multiply_checked", "add_checked"};

  auto it = binary_associative_commutative.find(call.function);
  return it != binary_associative_commutative.end();
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
    static std::unordered_map<std::string, type> flipped_comparisons{
        {"equal", EQUAL},     {"not_equal", NOT_EQUAL},
        {"less", LESS},       {"less_equal", LESS_EQUAL},
        {"greater", GREATER}, {"greater_equal", GREATER_EQUAL},
    };

    auto it = flipped_comparisons.find(function);
    return it != flipped_comparisons.end() ? &it->second : nullptr;
  }

  static const type* Get(const Expression2& expr) {
    if (auto call = expr.call()) {
      return Comparison::Get(call->function);
    }
    return nullptr;
  }

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
    };
    DCHECK(false);
    return NA;
  }

  static std::string GetName(type op) {
    switch (op) {
      case NA:
        DCHECK(false) << "unreachable";
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
    };
    DCHECK(false);
    return "na";
  }
};

Result<Expression2> Canonicalize(Expression2 expr) {
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
      [&AlreadyCanonicalized](Expression2 expr) -> Result<Expression2> {
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
            for (auto&& argument : flipped_call.arguments) {
              ARROW_ASSIGN_OR_RAISE(argument, Canonicalize(std::move(argument)));
            }
            flipped_call.function = Comparison::GetName(Comparison::GetFlipped(*cmp));
            std::swap(flipped_call.arguments[0], flipped_call.arguments[1]);
            return Expression2(
                std::make_shared<Expression2::Impl>(std::move(flipped_call)));
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

Result<Expression2> SimplifyWithGuarantee(Expression2 expr, ExpressionState* state,
                                          const Expression2& guaranteed_true_predicate) {
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);

  std::unordered_map<FieldRef, Datum, FieldRef::Hash> known_values;
  RETURN_NOT_OK(ExtractKnownFieldValuesImpl(&conjunction_members, &known_values));

  ARROW_ASSIGN_OR_RAISE(expr,
                        ReplaceFieldsWithKnownValues(known_values, std::move(expr)));

  auto CanonicalizeAndFoldConstants = [&expr, state] {
    ARROW_ASSIGN_OR_RAISE(expr, Canonicalize(std::move(expr)));
    ARROW_ASSIGN_OR_RAISE(expr, FoldConstants(std::move(expr), state));
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

}  // namespace dataset
}  // namespace arrow
