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

#include "arrow/util/config.h"

#include "arrow/compute/expression.h"

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/expression_internal.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/range_evaluation_internal.h"
#include "arrow/compute/util.h"
#include "arrow/io/memory.h"
#ifdef ARROW_IPC
#  include "arrow/ipc/reader.h"
#  include "arrow/ipc/writer.h"
#endif
#include "arrow/util/hash_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::ToChars;

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
  return Expression(Expression::Parameter{std::move(ref), TypeHolder{}, {-1}});
}

Expression call(std::string function, std::vector<Expression> arguments,
                std::shared_ptr<compute::FunctionOptions> options) {
  Expression::Call call;
  call.function_name = std::move(function);
  call.arguments = std::move(arguments);
  call.options = std::move(options);
  return Expression(std::move(call));
}

const Datum* Expression::literal() const {
  if (impl_ == nullptr) return nullptr;

  return std::get_if<Datum>(impl_.get());
}

const Expression::Parameter* Expression::parameter() const {
  if (impl_ == nullptr) return nullptr;

  return std::get_if<Parameter>(impl_.get());
}

const FieldRef* Expression::field_ref() const {
  if (auto parameter = this->parameter()) {
    return &parameter->ref;
  }
  return nullptr;
}

const Expression::Call* Expression::call() const {
  if (impl_ == nullptr) return nullptr;

  return std::get_if<Call>(impl_.get());
}

const DataType* Expression::type() const {
  if (impl_ == nullptr) return nullptr;

  if (const Datum* lit = literal()) {
    return lit->type().get();
  }

  if (const Parameter* parameter = this->parameter()) {
    return parameter->type.type;
  }

  return CallNotNull(*this)->type.type;
}

bool Expression::nullable() const {
  if (const Parameter* parameter = this->parameter()) {
    if (parameter->type.type != nullptr) {
      return parameter->nullable;
    }
  }
  return true;
}

namespace {

std::string PrintDatum(const Datum& datum) {
  if (datum.is_scalar()) {
    if (!datum.scalar()->is_valid) return "null[" + datum.type()->ToString() + "]";

    switch (datum.type()->id()) {
      case Type::STRING:
      case Type::LARGE_STRING:
        return '"' +
               Escape(std::string_view(*datum.scalar_as<BaseBinaryScalar>().value)) + '"';

      case Type::BINARY:
      case Type::FIXED_SIZE_BINARY:
      case Type::LARGE_BINARY:
        return '"' + datum.scalar_as<BaseBinaryScalar>().value->ToHexString() + '"';

      default:
        break;
    }

    return datum.scalar()->ToString();
  } else if (datum.is_array()) {
    return "Array[" + datum.type()->ToString() + "]";
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

  constexpr std::string_view kleene = "_kleene";
  if (call->function_name.ends_with(kleene)) {
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
  } else if (call->arguments.size()) {
    out.resize(out.size() - 2);
  }

  out += ')';
  return out;
}

void PrintTo(const Expression& expr, std::ostream* os) {
  *os << expr.ToString();
  if (expr.IsBound()) {
    *os << "[bound]";
  }
}

bool Expression::Equals(const Expression& other) const {
  if (Expression::Identical(*this, other)) return true;

  if (impl_ == nullptr || other.impl_ == nullptr) return false;

  if (impl_->index() != other.impl_->index()) {
    return false;
  }

  if (auto lit = literal()) {
    // The scalar NaN is not equal to the scalar NaN but the literal NaN
    // is equal to the literal NaN (e.g. the expressions are equal even if
    // the values are not)
    EqualOptions equal_options = EqualOptions::Defaults().nans_equal(true);
    return lit->scalar()->Equals(*other.literal()->scalar(), equal_options);
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
    return call->options->Equals(*other_call->options);
  }
  return false;
}

bool Expression::Identical(const Expression& l, const Expression& r) {
  return l.impl_ == r.impl_;
}

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

  if (const Call* call = this->call()) {
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

namespace {
std::optional<compute::NullHandling::type> GetNullHandling(const Expression::Call& call) {
  DCHECK_NE(call.function, nullptr);
  if (call.function->kind() == compute::Function::SCALAR) {
    return static_cast<const compute::ScalarKernel*>(call.kernel)->null_handling;
  }
  return std::nullopt;
}
}  // namespace

bool Expression::IsSatisfiable() const {
  if (type() == nullptr) return true;
  if (type()->id() != Type::BOOL) return true;

  if (auto lit = literal()) {
    if (lit->null_count() == lit->length()) {
      return false;
    }

    if (lit->is_scalar()) {
      return lit->scalar_as<BooleanScalar>().value;
    }

    return true;
  }

  if (field_ref()) return true;

  auto call = CallNotNull(*this);

  // invert(true_unless_null(x)) is always false or null by definition
  // true_unless_null arises in simplification of inequalities below
  if (call->function_name == "invert") {
    if (auto nested_call = call->arguments[0].call()) {
      if (nested_call->function_name == "true_unless_null") return false;
    }
  }

  if (call->function_name == "and_kleene" || call->function_name == "and") {
    return std::all_of(call->arguments.begin(), call->arguments.end(),
                       [](const Expression& arg) { return arg.IsSatisfiable(); });
  }
  if (call->function_name == "or_kleene" || call->function_name == "or") {
    return std::any_of(call->arguments.begin(), call->arguments.end(),
                       [](const Expression& arg) { return arg.IsSatisfiable(); });
  }

  return true;
}

namespace {

TypeHolder SmallestTypeFor(const arrow::Datum& value) {
  switch (value.type()->id()) {
    case Type::INT8:
      return int8();
    case Type::UINT8:
      return uint8();
    case Type::INT16: {
      int16_t i16 = value.scalar_as<Int16Scalar>().value;
      if (i16 <= std::numeric_limits<int8_t>::max() &&
          i16 >= std::numeric_limits<int8_t>::min()) {
        return int8();
      }
      return int16();
    }
    case Type::UINT16: {
      uint16_t ui16 = value.scalar_as<UInt16Scalar>().value;
      if (ui16 <= std::numeric_limits<uint8_t>::max()) {
        return uint8();
      }
      return uint16();
    }
    case Type::INT32: {
      int32_t i32 = value.scalar_as<Int32Scalar>().value;
      if (i32 <= std::numeric_limits<int8_t>::max() &&
          i32 >= std::numeric_limits<int8_t>::min()) {
        return int8();
      }
      if (i32 <= std::numeric_limits<int16_t>::max() &&
          i32 >= std::numeric_limits<int16_t>::min()) {
        return int16();
      }
      return int32();
    }
    case Type::UINT32: {
      uint32_t ui32 = value.scalar_as<UInt32Scalar>().value;
      if (ui32 <= std::numeric_limits<uint8_t>::max()) {
        return uint8();
      }
      if (ui32 <= std::numeric_limits<uint16_t>::max()) {
        return uint16();
      }
      return uint32();
    }
    case Type::INT64: {
      int64_t i64 = value.scalar_as<Int64Scalar>().value;
      if (i64 <= std::numeric_limits<int8_t>::max() &&
          i64 >= std::numeric_limits<int8_t>::min()) {
        return int8();
      }
      if (i64 <= std::numeric_limits<int16_t>::max() &&
          i64 >= std::numeric_limits<int16_t>::min()) {
        return int16();
      }
      if (i64 <= std::numeric_limits<int32_t>::max() &&
          i64 >= std::numeric_limits<int32_t>::min()) {
        return int32();
      }
      return int64();
    }
    case Type::UINT64: {
      uint64_t ui64 = value.scalar_as<UInt64Scalar>().value;
      if (ui64 <= std::numeric_limits<uint8_t>::max()) {
        return uint8();
      }
      if (ui64 <= std::numeric_limits<uint16_t>::max()) {
        return uint16();
      }
      if (ui64 <= std::numeric_limits<uint32_t>::max()) {
        return uint32();
      }
      return uint64();
    }
    case Type::DOUBLE: {
      double doub = value.scalar_as<DoubleScalar>().value;
      if (!std::isfinite(doub)) {
        // Special values can be float
        return float32();
      }
      // Test if float representation is the same
      if (static_cast<double>(static_cast<float>(doub)) == doub) {
        return float32();
      }
      return float64();
    }
    case Type::LARGE_STRING: {
      if (value.scalar_as<LargeStringScalar>().value->size() <=
          std::numeric_limits<int32_t>::max()) {
        return utf8();
      }
      return large_utf8();
    }
    case Type::LARGE_BINARY:
      if (value.scalar_as<LargeBinaryScalar>().value->size() <=
          std::numeric_limits<int32_t>::max()) {
        return binary();
      }
      return large_binary();
    case Type::TIMESTAMP: {
      const auto& ts_type = checked_pointer_cast<TimestampType>(value.type());
      uint64_t ts = value.scalar_as<TimestampScalar>().value;
      switch (ts_type->unit()) {
        case TimeUnit::SECOND:
          return value.type();
        case TimeUnit::MILLI:
          if (ts % 1000 == 0) {
            return timestamp(TimeUnit::SECOND, ts_type->timezone());
          }
          return value.type();
        case TimeUnit::MICRO:
          if (ts % 1000000 == 0) {
            return timestamp(TimeUnit::SECOND, ts_type->timezone());
          }
          if (ts % 1000 == 0) {
            return timestamp(TimeUnit::MILLI, ts_type->timezone());
          }
          return value.type();
        case TimeUnit::NANO:
          if (ts % 1000000000 == 0) {
            return timestamp(TimeUnit::SECOND, ts_type->timezone());
          }
          if (ts % 1000000 == 0) {
            return timestamp(TimeUnit::MILLI, ts_type->timezone());
          }
          if (ts % 1000 == 0) {
            return timestamp(TimeUnit::MICRO, ts_type->timezone());
          }
          return value.type();
        default:
          return value.type();
      }
    }
    default:
      return value.type();
  }
}

inline std::vector<TypeHolder> GetTypesWithSmallestLiteralRepresentation(
    const std::vector<Expression>& exprs) {
  std::vector<TypeHolder> types(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    DCHECK(exprs[i].IsBound());
    if (const Datum* literal = exprs[i].literal()) {
      if (literal->is_scalar()) {
        types[i] = SmallestTypeFor(*literal);
      }
    } else {
      types[i] = exprs[i].type();
    }
  }
  return types;
}

// Produce a bound Expression from unbound Call and bound arguments.
Result<Expression> BindNonRecursive(Expression::Call call, bool insert_implicit_casts,
                                    compute::ExecContext* exec_context) {
  DCHECK(std::all_of(call.arguments.begin(), call.arguments.end(),
                     [](const Expression& argument) { return argument.IsBound(); }));

  std::vector<TypeHolder> types = GetTypes(call.arguments);
  ARROW_ASSIGN_OR_RAISE(call.function, GetFunction(call, exec_context));

  // First try and bind exactly
  Result<const Kernel*> maybe_exact_match = call.function->DispatchExact(types);
  if (maybe_exact_match.ok()) {
    call.kernel = *maybe_exact_match;
  } else {
    if (!insert_implicit_casts) {
      return maybe_exact_match.status();
    }

    // If exact binding fails, and we are allowed to cast, then prefer casting literals
    // first.  Since DispatchBest generally prefers up-casting the best way to do this is
    // first down-cast the literals as much as possible
    types = GetTypesWithSmallestLiteralRepresentation(call.arguments);
    ARROW_ASSIGN_OR_RAISE(call.kernel, call.function->DispatchBest(&types));

    for (size_t i = 0; i < types.size(); ++i) {
      if (types[i] == call.arguments[i].type()) continue;

      if (const Datum* lit = call.arguments[i].literal()) {
        ARROW_ASSIGN_OR_RAISE(Datum new_lit,
                              compute::Cast(*lit, types[i].GetSharedPtr()));
        call.arguments[i] = literal(std::move(new_lit));
        continue;
      }

      // construct an implicit cast Expression with which to replace this argument
      Expression::Call implicit_cast;
      implicit_cast.function_name = "cast";
      implicit_cast.arguments = {std::move(call.arguments[i])};

      // TODO(wesm): Use TypeHolder in options
      implicit_cast.options = std::make_shared<compute::CastOptions>(
          compute::CastOptions::Safe(types[i].GetSharedPtr()));

      ARROW_ASSIGN_OR_RAISE(
          call.arguments[i],
          BindNonRecursive(std::move(implicit_cast),
                           /*insert_implicit_casts=*/false, exec_context));
    }
  }

  compute::KernelContext kernel_context(exec_context, call.kernel);
  if (call.kernel->init) {
    const FunctionOptions* options =
        call.options ? call.options.get() : call.function->default_options();
    ARROW_ASSIGN_OR_RAISE(
        call.kernel_state,
        call.kernel->init(&kernel_context, {call.kernel, types, options}));

    kernel_context.SetState(call.kernel_state.get());
  }

  ARROW_ASSIGN_OR_RAISE(
      call.type, call.kernel->signature->out_type().Resolve(&kernel_context, types));

  return Expression(std::move(call));
}

template <typename TypeOrSchema>
Result<Expression> BindImpl(Expression expr, const TypeOrSchema& in,
                            compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    compute::ExecContext exec_context;
    return BindImpl(std::move(expr), in, &exec_context);
  }

  if (expr.literal()) return expr;

  if (const FieldRef* ref = expr.field_ref()) {
    ARROW_ASSIGN_OR_RAISE(FieldPath path, ref->FindOne(in));

    Expression::Parameter param = *expr.parameter();
    param.indices.resize(path.indices().size());
    std::copy(path.indices().begin(), path.indices().end(), param.indices.begin());
    ARROW_ASSIGN_OR_RAISE(auto field, path.Get(in));
    param.type = field->type();
    param.nullable = path.indices().size() == 1 ? field->nullable() : true;
    return Expression{std::move(param)};
  }

  auto call = *CallNotNull(expr);
  for (auto& argument : call.arguments) {
    ARROW_ASSIGN_OR_RAISE(argument, BindImpl(std::move(argument), in, exec_context));
  }
  return BindNonRecursive(std::move(call),
                          /*insert_implicit_casts=*/true, exec_context);
}

}  // namespace

Result<Expression> Expression::Bind(const TypeHolder& in,
                                    compute::ExecContext* exec_context) const {
  return BindImpl(*this, *in.type, exec_context);
}

Result<Expression> Expression::Bind(const Schema& in_schema,
                                    compute::ExecContext* exec_context) const {
  return BindImpl(*this, in_schema, exec_context);
}

Result<ExecBatch> MakeExecBatch(const Schema& full_schema, const Datum& partial,
                                Expression guarantee) {
  ExecBatch out;

  if (partial.kind() == Datum::RECORD_BATCH) {
    const auto& partial_batch = *partial.record_batch();
    out.guarantee = std::move(guarantee);
    out.length = partial_batch.num_rows();

    ARROW_ASSIGN_OR_RAISE(auto known_field_values,
                          ExtractKnownFieldValues(out.guarantee));

    for (const auto& field : full_schema.fields()) {
      auto field_ref = FieldRef(field->name());

      // If we know what the value must be from the guarantee, prefer to use that value
      // than the data from the record batch (if it exists at all -- probably it doesn't),
      // because this way it will be a scalar.
      auto known_field_value = known_field_values.map.find(field_ref);
      if (known_field_value != known_field_values.map.end()) {
        out.values.emplace_back(known_field_value->second);
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(auto column, field_ref.GetOneOrNone(partial_batch));
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
  const auto& partial_type = partial.type();
  if (partial_type && partial_type->id() == Type::STRUCT) {
    if (partial.is_array()) {
      ARROW_ASSIGN_OR_RAISE(auto partial_batch,
                            RecordBatch::FromStructArray(partial.make_array()));

      return MakeExecBatch(full_schema, partial_batch, std::move(guarantee));
    }

    if (partial.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(auto partial_array,
                            MakeArrayFromScalar(*partial.scalar(), 1));
      ARROW_ASSIGN_OR_RAISE(
          auto out, MakeExecBatch(full_schema, partial_array, std::move(guarantee)));

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
    if (param->type.id() == Type::NA) {
      return MakeNullScalar(null());
    }

    Datum field = input[param->indices[0]];
    if (param->indices.size() > 1) {
      std::vector<int> indices(param->indices.begin() + 1, param->indices.end());
      compute::StructFieldOptions options(std::move(indices));
      ARROW_ASSIGN_OR_RAISE(
          field, compute::CallFunction("struct_field", {std::move(field)}, &options));
    }
    if (!field.type()->Equals(*param->type.type)) {
      return Status::Invalid("Referenced field ", expr.ToString(), " was ",
                             field.type()->ToString(), " but should have been ",
                             param->type.ToString());
    }

    return field;
  }

  auto call = CallNotNull(expr);

  std::vector<Datum> arguments(call->arguments.size());

  bool all_scalar = true;
  for (size_t i = 0; i < arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(
        arguments[i], ExecuteScalarExpression(call->arguments[i], input, exec_context));
    all_scalar &= arguments[i].is_scalar();
  }

  int64_t input_length;
  if (!arguments.empty() && all_scalar) {
    // all inputs are scalar, so use a 1-long batch to avoid
    // computing input.length equivalent outputs
    input_length = 1;
  } else {
    input_length = input.length;
  }

  auto executor = compute::detail::KernelExecutor::MakeScalar();

  compute::KernelContext kernel_context(exec_context, call->kernel);
  kernel_context.SetState(call->kernel_state.get());

  const Kernel* kernel = call->kernel;
  std::vector<TypeHolder> types = GetTypes(arguments);
  auto options = call->options.get();
  RETURN_NOT_OK(executor->Init(&kernel_context, {kernel, types, options}));

  compute::detail::DatumAccumulator listener;
  RETURN_NOT_OK(executor->Execute(ExecBatch(arguments, input_length), &listener));
  const auto out = executor->WrapResults(arguments, listener.values());
#ifndef NDEBUG
  DCHECK_OK(executor->CheckResultType(out, call->function_name.c_str()));
#endif
  return out;
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
  if (!expr.IsBound()) {
    return Status::Invalid("Cannot fold constants in unbound expression.");
  }

  return ModifyExpression(
      std::move(expr), [](Expression expr) { return expr; },
      [](Expression expr, ...) -> Result<Expression> {
        auto call = CallNotNull(expr);
        if (!call->function->is_pure()) return expr;

        if (std::all_of(call->arguments.begin(), call->arguments.end(),
                        [](const Expression& argument) { return argument.literal(); })) {
          // all arguments are literal; we can evaluate this subexpression *now*
          static const ExecBatch ignored_input = ExecBatch({}, 1);
          ARROW_ASSIGN_OR_RAISE(Datum constant,
                                ExecuteScalarExpression(expr, ignored_input));

          return literal(std::move(constant));
        }

        // XXX the following should probably be in a registry of passes instead
        // of inline

        if (GetNullHandling(*call) == compute::NullHandling::INTERSECTION) {
          // kernels which always produce intersected validity can be resolved
          // to null *now* if any of their inputs is a null literal
          for (const Expression& argument : call->arguments) {
            if (argument.IsNullLiteral()) {
              if (argument.type()->Equals(*call->type.type)) {
                return argument;
              } else {
                return literal(MakeNullScalar(call->type.GetSharedPtr()));
              }
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

/// \brief Extract an equality from an expression.
///
/// Recognizes expressions of the form:
/// equal(a, 2)
/// is_null(a)
std::optional<std::pair<FieldRef, Datum>> ExtractOneFieldValue(
    const Expression& guarantee) {
  auto call = guarantee.call();
  if (!call) return std::nullopt;

  // search for an equality conditions between a field and a literal
  if (call->function_name == "equal") {
    auto ref = call->arguments[0].field_ref();
    if (!ref) return std::nullopt;

    auto lit = call->arguments[1].literal();
    if (!lit) return std::nullopt;

    return std::make_pair(*ref, *lit);
  }

  // ... or a known null field
  if (call->function_name == "is_null") {
    auto ref = call->arguments[0].field_ref();
    if (!ref) return std::nullopt;

    return std::make_pair(*ref, Datum(std::make_shared<NullScalar>()));
  }

  return std::nullopt;
}

// Conjunction members which are represented in known_values are erased from
// conjunction_members
Status ExtractKnownFieldValues(std::vector<Expression>* conjunction_members,
                               KnownFieldValues* known_values) {
  // filter out consumed conjunction members, leaving only unconsumed
  *conjunction_members = arrow::internal::FilterVector(
      std::move(*conjunction_members),
      [known_values](const Expression& guarantee) -> bool {
        if (auto known_value = ExtractOneFieldValue(guarantee)) {
          known_values->map.insert(std::move(*known_value));
          return false;
        }
        return true;
      });

  return Status::OK();
}

}  // namespace

Result<KnownFieldValues> ExtractKnownFieldValues(
    const Expression& guaranteed_true_predicate) {
  KnownFieldValues known_values;
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);
  RETURN_NOT_OK(ExtractKnownFieldValues(&conjunction_members, &known_values));
  return known_values;
}

Result<Expression> ReplaceFieldsWithKnownValues(const KnownFieldValues& known_values,
                                                Expression expr) {
  if (!expr.IsBound()) {
    return Status::Invalid(
        "ReplaceFieldsWithKnownValues called on an unbound Expression");
  }

  return ModifyExpression(
      std::move(expr),
      [&known_values](Expression expr) -> Result<Expression> {
        if (auto ref = expr.field_ref()) {
          auto it = known_values.map.find(*ref);
          if (it != known_values.map.end()) {
            Datum lit = it->second;
            if (lit.type()->Equals(*expr.type())) return literal(std::move(lit));
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

            ARROW_ASSIGN_OR_RAISE(lit, compute::Cast(lit, expr.type()->GetSharedPtr()));
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

Result<Expression> HandleInconsistentTypes(Expression::Call call,
                                           compute::ExecContext* exec_context) {
  // ARROW-18334: due to reordering of arguments, the call may have
  // inconsistent argument types. For example, the call's kernel may
  // correspond to `timestamp + duration` but the arguments happen to
  // be `duration, timestamp`. The addition itself is still commutative,
  // but the mismatch in declared argument types is potentially problematic
  // if we ever start using the Expression::Call::kernel field more than
  // we do currently. Check and rebind if necessary.
  //
  // The more correct fix for this problem is to ensure that all kernels of
  // functions which are commutative be commutative as well, which would
  // obviate rebinding like this. In the context of ARROW-18334, this
  // would require rewriting KernelSignature so that a single kernel can
  // handle both `timestamp + duration` and `duration + timestamp`.
  if (call.kernel->signature->MatchesInputs(GetTypes(call.arguments))) {
    return Expression(std::move(call));
  }
  return BindNonRecursive(std::move(call), /*insert_implicit_casts=*/false, exec_context);
}

}  // namespace

Result<Expression> Canonicalize(Expression expr, compute::ExecContext* exec_context) {
  if (!expr.IsBound()) {
    return Status::Invalid("Cannot canonicalize an unbound expression.");
  }

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

  return ModifyExpression(
      std::move(expr),
      [&AlreadyCanonicalized, exec_context](Expression expr) -> Result<Expression> {
        auto call = expr.call();
        if (!call) return expr;
        if (!call->function->is_pure()) return expr;

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
            // fast path for expressions which happen to have arrived in an
            // already-canonical form
            AlreadyCanonicalized.Add(std::move(chain.exprs));
            return expr;
          }

          std::stable_sort(chain.fringe.begin(), chain.fringe.end(), CanonicalOrdering);

          // fold the chain back up
          Expression folded = std::move(chain.fringe.front());

          for (auto it = chain.fringe.begin() + 1; it != chain.fringe.end(); ++it) {
            auto canonicalized_call = *call;
            canonicalized_call.arguments = {std::move(folded), std::move(*it)};
            ARROW_ASSIGN_OR_RAISE(
                folded,
                HandleInconsistentTypes(std::move(canonicalized_call), exec_context));
            AlreadyCanonicalized.Add({expr});
          }
          return folded;
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

// A strict comparison weakened to an inclusive one. A nondecreasing conversion which
// may map distinct values to the same value only preserves inclusive comparisons:
// `x > b` implies `f(x) >= f(b)`, but not `f(x) > f(b)`.
Comparison::type WeakenStrictComparison(Comparison::type cmp) {
  if (cmp == Comparison::GREATER) return Comparison::GREATER_EQUAL;
  if (cmp == Comparison::LESS) return Comparison::LESS_EQUAL;
  return cmp;
}

// The order preserving casts wrapping an Expression, outermost first, and the
// wrapped Expression. See Comparison::StripOrderPreservingCasts.
struct OrderPreservingCasts {
  std::vector<const Expression::Call*> casts;
  const Expression* inner;
  // Whether the composed casts map distinct values to distinct values. Each cast
  // is nondecreasing, so `x > b` implies `cast(x) >= cast(b)`, but only through
  // injective casts does it imply `cast(x) > cast(b)`. Integer widenings are
  // injective, as are casts to a floating point type whose mantissa holds every
  // value of the source type; float64 to float32 or int64 to float64 are not.
  bool injective = true;

  static OrderPreservingCasts Strip(const Expression& expr) {
    OrderPreservingCasts out;
    const Expression* lhs = &expr;
    while (lhs->call() && Comparison::IsOrderPreservingCast(*lhs->call())) {
      const Expression& from = lhs->call()->arguments[0];
      out.injective = out.injective && IsInjective(from.type()->id(), lhs->type()->id());
      out.casts.push_back(lhs->call());
      lhs = &from;
    }
    out.inner = lhs;
    return out;
  }

  static bool IsInjective(Type::type from, Type::type to) {
    // Only integer widenings are stripped between integer types.
    if (!is_floating(to)) return true;
    if (is_floating(from)) return bit_width(from) <= bit_width(to);
    const int mantissa_bits = to == Type::DOUBLE ? 53 : to == Type::FLOAT ? 24 : 11;
    return bit_width(from) <= mantissa_bits;
  }

  // Put a bound on the wrapped Expression through the casts, innermost first and
  // with the same options.
  Result<std::shared_ptr<Scalar>> ApplyToBound(std::shared_ptr<Scalar> bound,
                                               ExecContext* exec_context) const {
    for (auto it = casts.rbegin(); it != casts.rend(); ++it) {
      const auto& options = checked_cast<const CastOptions&>(*(*it)->options);
      ARROW_ASSIGN_OR_RAISE(Datum cast,
                            Cast(Datum(std::move(bound)), options, exec_context));
      bound = cast.scalar();
    }
    return bound;
  }

  // The comparison between the cast Expression and a cast bound implied by `cmp`
  // between the wrapped Expression and the bound: strict bounds are weakened to
  // inclusive ones unless the casts are injective.
  Comparison::type ApplyToComparison(Comparison::type cmp) const {
    return injective ? cmp : WeakenStrictComparison(cmp);
  }
};

// Value sets up to this length are simplified against a guarantee by filtering the
// value set and rebinding the `is_in` call (see Inequality::SimplifyIsIn). This is
// too expensive for longer value sets (https://github.com/apache/arrow/issues/46777),
// for which the kernel is instead asked what the call evaluates to over the range of
// the guarantee (see internal::RangeEvaluator), which never rebinds.
constexpr int64_t kIsInSimplificationMaxValueSet = 50;

// Whether values of the two types are ordered alike, so that a guarantee bound of
// one type which casts exactly to the other bounds the same values there. This does
// not hold across families: "10" <= "9" as strings, but both cast exactly to
// integers (and back), for which 10 > 9.
bool HaveSameOrder(const DataType& left, const DataType& right) {
  if (left.id() == right.id()) return true;
  auto family = [](Type::type id) {
    if (is_integer(id) || is_floating(id)) return 1;
    if (is_base_binary_like(id)) return 2;
    if (id == Type::DATE32 || id == Type::DATE64 || id == Type::TIMESTAMP) return 3;
    return 0;
  };
  const int left_family = family(left.id());
  return left_family != 0 && left_family == family(right.id());
}

// An Expression which evaluates to `value` where `target` is valid and to null where
// it is null: true_unless_null(target) or invert(true_unless_null(target)).
//
// true_unless_null is cheap; it purely reuses the validity bitmap for the values
// buffer. Inversion is less cheap but we expect that term never to be evaluated
// since invert(true_unless_null(x)) is not satisfiable.
Result<Expression> ValueUnlessNull(const Expression& target, bool value) {
  ExecContext exec_context;

  Expression::Call call;
  call.function_name = "true_unless_null";
  call.arguments = {target};
  ARROW_ASSIGN_OR_RAISE(auto true_unless_null,
                        BindNonRecursive(std::move(call),
                                         /*insert_implicit_casts=*/false, &exec_context));
  if (value) return true_unless_null;

  Expression::Call invert;
  invert.function_name = "invert";
  invert.arguments = {std::move(true_unless_null)};
  return BindNonRecursive(std::move(invert),
                          /*insert_implicit_casts=*/false, &exec_context);
}

// An inequality comparison which a target Expression is known to satisfy. If nullable,
// the target may evaluate to null in addition to values satisfying the comparison.
//
// `target` and `bound` refer into the guarantee the inequality was extracted from,
// which must outlive it.
struct Inequality {
  // The inequality type
  Comparison::type cmp;
  // The LHS of the inequality
  const FieldRef& target;
  // The RHS of the inequality
  const Datum& bound;
  // Whether target can be null
  bool nullable;

  // Extract the inequalities implied by a guarantee, derived from "less",
  // "greater", "less_equal", "greater_equal" and "equal" expressions, possibly
  // disjuncted with an "is_null" Expression:
  //   cmp(a, 2)
  //   cmp(a, 2) or is_null(a)
  //   (cmp(a, 2) and cmp(a, 5)) or is_null(a)
  // The last is the shape of a Parquet row group's statistics when it may contain
  // nulls. In a disjunction each bound only holds for non-null values of the field.
  static std::vector<Inequality> Extract(const Expression& guarantee) {
    if (auto inequality = ExtractOneFromComparison(guarantee)) return {*inequality};

    auto call = guarantee.call();
    if (!call || call->function_name != "or_kleene") return {};

    // expect the RHS to be an is_null expression ...
    auto null_call = call->arguments[1].call();
    if (!null_call || null_call->function_name != "is_null") return {};
    auto target = null_call->arguments[0].field_ref();
    if (!target) return {};

    // ... and the LHS to be a conjunction of usable inequalities on the same field
    std::vector<Inequality> inequalities;
    for (const auto& member : GuaranteeConjunctionMembers(call->arguments[0])) {
      auto inequality = ExtractOneFromComparison(member);
      if (!inequality || inequality->target != *target) continue;
      inequality->nullable = true;
      inequalities.push_back(*inequality);
    }
    return inequalities;
  }

  static std::optional<Inequality> ExtractOneFromComparison(const Expression& guarantee) {
    auto call = guarantee.call();
    if (!call) return std::nullopt;

    if (auto cmp = Comparison::Get(call->function_name)) {
      // not_equal comparisons are not very usable as guarantees
      if (*cmp == Comparison::NOT_EQUAL) return std::nullopt;

      auto target = call->arguments[0].field_ref();
      if (!target) return std::nullopt;

      auto bound = call->arguments[1].literal();
      if (!bound) return std::nullopt;
      if (!bound->is_scalar()) return std::nullopt;

      return Inequality{*cmp, /*target=*/*target, *bound, /*nullable=*/false};
    }

    return std::nullopt;
  }

  /// This inequality restated for the target wrapped in `casts`, as a comparison of
  /// the cast target with a bound of the cast target's type. Every simplification
  /// goes through this, so that they agree on what a guarantee says:
  ///
  /// - A bound of another type than the target's is brought into the target's type
  ///   with a safe cast, provided the types are ordered alike (see HaveSameOrder).
  ///   Such a bound arises when a column is read as another type than it is stored
  ///   as: its statistics bound the stored values and the same cast is applied to
  ///   the data. A safe cast is nondecreasing but may round (between floating point
  ///   types), so unless the bound round trips, strict comparisons are weakened to
  ///   inclusive ones. This also holds if the guarantee is read as comparing the
  ///   target with the bound in their common type: no value of the target's type
  ///   lies strictly between the bound and its rounding.
  /// - The bound is then put through the casts the target goes through. Each cast is
  ///   nondecreasing, so `x <= b` implies `cast(x) <= cast(b)` even if `cast(b)`
  ///   rounds, and no exactness is needed.
  /// - Through casts which are not injective `x > b` only implies
  ///   `cast(x) >= cast(b)`, so strict comparisons are weakened to inclusive ones.
  ///
  /// Nullopt if the inequality cannot be restated; ignoring it only loses precision.
  std::optional<std::pair<Comparison::type, std::shared_ptr<Scalar>>> Restate(
      const OrderPreservingCasts& casts) const {
    std::shared_ptr<Scalar> value = bound.scalar();
    // A comparison against null cannot be guaranteed true, but be defensive.
    if (!value->is_valid) return std::nullopt;

    ExecContext exec_context;
    Comparison::type restated_cmp = cmp;
    const auto& target_type = casts.inner->type();
    if (!value->type->Equals(*target_type)) {
      if (!HaveSameOrder(*value->type, *target_type)) return std::nullopt;
      auto maybe_cast =
          Cast(Datum(value), target_type, CastOptions::Safe(), &exec_context);
      if (!maybe_cast.ok()) return std::nullopt;
      auto maybe_round_trip =
          Cast(*maybe_cast, value->type, CastOptions::Safe(), &exec_context);
      if (!maybe_round_trip.ok() || !maybe_round_trip->scalar()->Equals(*value)) {
        restated_cmp = WeakenStrictComparison(restated_cmp);
      }
      value = maybe_cast->scalar();
    }
    auto cast_value = casts.ApplyToBound(std::move(value), &exec_context);
    if (!cast_value.ok()) return std::nullopt;
    return std::make_pair(casts.ApplyToComparison(restated_cmp), *std::move(cast_value));
  }

  /// The given expression simplifies to `value` if the inequality
  /// target is not nullable. Otherwise, it simplifies to either a
  /// call to true_unless_null or !true_unless_null.
  Result<Expression> simplified_to(const Expression& bound_target, bool value) const {
    if (!nullable) return literal(value);

    // Data may be null, so comparison will yield `value` - or null IFF the data was null
    return ValueUnlessNull(bound_target, value);
  }

  /// Simplify an `is_in` call against an inequality guarantee.
  ///
  /// We avoid the complexity of fully simplifying EQUAL comparisons to true
  /// literals (e.g., 'x is_in [1, 2, 3]' given the guarantee 'x = 2') due to
  /// potential complications with null matching behavior. This is ok for the
  /// predicate pushdown use case because the overall aim is to simplify to an
  /// unsatisfiable expression.
  ///
  /// \pre `is_in_call` is a call to the `is_in` function
  /// \return a simplified expression, or nullopt if no simplification occurred
  static Result<std::optional<Expression>> SimplifyIsIn(
      const Inequality& guarantee, const Expression::Call* is_in_call) {
    DCHECK_EQ(is_in_call->function_name, "is_in");

    auto options = checked_pointer_cast<SetLookupOptions>(is_in_call->options);

    // Large value sets are handled by the kernel instead, see SimplifyWithRanges.
    if (options->value_set.length() > kIsInSimplificationMaxValueSet) {
      return std::nullopt;
    }

    auto casts = OrderPreservingCasts::Strip(is_in_call->arguments[0]);
    const Expression& lhs = *casts.inner;
    if (!lhs.field_ref()) return std::nullopt;
    if (*lhs.field_ref() != guarantee.target) return std::nullopt;

    // The guarantee bounds the field but the value set is matched against the
    // cast field.
    auto restated = guarantee.Restate(casts);
    if (!restated) return std::nullopt;
    const auto& [cmp, bound] = *restated;

    // Statistics based guarantees cannot bound NaN (a row group's minimum and
    // maximum ignore it) whereas `is_in` matches NaN with NaN, so a NaN in the
    // value set must not be filtered out. Leave such value sets alone, as the
    // kernel does for large ones.
    const auto value_set_type_id = options->value_set.type()->id();
    if (value_set_type_id == Type::FLOAT || value_set_type_id == Type::DOUBLE) {
      ARROW_ASSIGN_OR_RAISE(Datum is_nan, IsNan(options->value_set));
      ARROW_ASSIGN_OR_RAISE(Datum any_nan, Any(is_nan));
      if (any_nan.scalar_as<BooleanScalar>().value) return std::nullopt;
    }

    FilterOptions::NullSelectionBehavior null_selection{};
    switch (options->GetNullMatchingBehavior()) {
      case SetLookupOptions::MATCH:
        null_selection =
            guarantee.nullable ? FilterOptions::EMIT_NULL : FilterOptions::DROP;
        break;
      case SetLookupOptions::SKIP:
        null_selection = FilterOptions::DROP;
        break;
      case SetLookupOptions::EMIT_NULL:
        if (guarantee.nullable) return std::nullopt;
        null_selection = FilterOptions::DROP;
        break;
      case SetLookupOptions::INCONCLUSIVE:
        if (guarantee.nullable) return std::nullopt;
        ARROW_ASSIGN_OR_RAISE(Datum is_null, IsNull(options->value_set));
        ARROW_ASSIGN_OR_RAISE(Datum any_null, Any(is_null));
        if (any_null.scalar_as<BooleanScalar>().value) return std::nullopt;
        null_selection = FilterOptions::DROP;
        break;
    }

    std::string func_name = Comparison::GetName(cmp);
    DCHECK_NE(func_name, "na");
    std::vector<Datum> args{options->value_set, bound};
    ARROW_ASSIGN_OR_RAISE(Datum filter_mask, CallFunction(func_name, args));
    FilterOptions filter_options(null_selection);
    ARROW_ASSIGN_OR_RAISE(Datum simplified_value_set,
                          Filter(options->value_set, filter_mask, filter_options));

    if (simplified_value_set.length() == 0) return literal(false);
    if (simplified_value_set.length() == options->value_set.length()) return std::nullopt;

    ExecContext exec_context;
    Expression::Call simplified_call;
    simplified_call.function_name = "is_in";
    simplified_call.arguments = is_in_call->arguments;
    simplified_call.options = std::make_shared<SetLookupOptions>(
        simplified_value_set, options->GetNullMatchingBehavior());
    ARROW_ASSIGN_OR_RAISE(
        Expression simplified_expr,
        BindNonRecursive(std::move(simplified_call),
                         /*insert_implicit_casts=*/false, &exec_context));
    return simplified_expr;
  }

  /// \brief Simplify the given expression given this inequality as a guarantee.
  Result<Expression> Simplify(Expression expr) const {
    const auto& guarantee = *this;

    auto call = expr.call();
    if (!call) return expr;

    if (call->function_name == "is_valid" || call->function_name == "is_null") {
      if (guarantee.nullable) return expr;
      const auto& lhs = Comparison::StripOrderPreservingCasts(call->arguments[0]);
      if (!lhs.field_ref()) return expr;
      if (*lhs.field_ref() != guarantee.target) return expr;

      return call->function_name == "is_valid" ? literal(true) : literal(false);
    }

    if (call->function_name == "is_in") {
      ARROW_ASSIGN_OR_RAISE(std::optional<Expression> result,
                            SimplifyIsIn(guarantee, call));
      return result.value_or(expr);
    }

    auto cmp = Comparison::Get(expr);
    if (!cmp) return expr;

    auto rhs = call->arguments[1].literal();
    if (!rhs) return expr;
    if (!rhs->is_scalar()) return expr;

    auto casts = OrderPreservingCasts::Strip(call->arguments[0]);
    const Expression& lhs = *casts.inner;
    if (!lhs.field_ref()) return expr;
    if (*lhs.field_ref() != guarantee.target) return expr;

    // The guarantee bounds the field but the expression compares the cast field.
    auto restated = guarantee.Restate(casts);
    if (!restated) return expr;
    const auto& [guarantee_cmp, bound] = *restated;

    // Whether the RHS of the expression is EQUAL, LESS, or GREATER than the
    // RHS of the guarantee. N.B. Comparison::type is a bitmask
    ARROW_ASSIGN_OR_RAISE(const Comparison::type cmp_rhs_bound,
                          Comparison::Execute(*rhs, bound));
    DCHECK_NE(cmp_rhs_bound, Comparison::NA);

    if (cmp_rhs_bound == Comparison::EQUAL) {
      // RHS of filter is equal to RHS of guarantee

      if ((*cmp & guarantee_cmp) == guarantee_cmp) {
        // guarantee is a subset of filter, so all data will be included
        // x > 1, x >= 1, x != 1 guaranteed by x > 1
        return simplified_to(lhs, true);
      }

      if ((*cmp & guarantee_cmp) == 0) {
        // guarantee disjoint with filter, so all data will be excluded
        // x > 1, x >= 1 unsatisfiable if x == 1
        return simplified_to(lhs, false);
      }

      return expr;
    }

    if (guarantee_cmp & cmp_rhs_bound) {
      // We guarantee (x (?) N) and are trying to simplify (x (?) M).  We know
      // either M < N or M > N (i.e. cmp_rhs_bound is either LESS or GREATER).

      // If M > N, then if the guarantee is (x > N), (x >= N), or (x != N)
      // (i.e. guarantee_cmp & cmp_rhs_bound), we cannot do anything with the
      // guarantee, and bail out here.

      // For example, take M = 5, N = 3. Then cmp_rhs_bound = GREATER.
      // x > 3, x >= 3, x != 3 implies nothing about x < 5, x <= 5, x > 5,
      // x >= 5, x != 5 and we bail out here.
      // x < 3, x <= 3 could simplify (some of) those expressions.
      return expr;
    }

    if (*cmp & Comparison::GetFlipped(cmp_rhs_bound)) {
      // x > 1, x >= 1, x != 1 guaranteed by x >= 3
      // (where `guarantee_cmp` is GREATER_EQUAL, `cmp_rhs_bound` is LESS)
      return simplified_to(lhs, true);
    } else {
      // x < 1, x <= 1, x == 1 unsatisfiable if x >= 3
      return simplified_to(lhs, false);
    }
  }
};

/// \brief What a guarantee says about the fields it bounds: for each field, the
///   inequalities it satisfies and whether it may be null.
///
/// The inequalities refer into the guarantee, which must outlive this.
struct FieldRanges {
  std::unordered_map<FieldRef, std::vector<Inequality>, FieldRef::Hash> map;

  /// Collect the inequalities implied by the members of a guarantee. A field can
  /// only be null if every inequality on it admits nulls and it is not also
  /// guaranteed valid.
  static FieldRanges Make(const std::vector<Expression>& conjunction_members) {
    FieldRanges ranges;
    for (const auto& guarantee : conjunction_members) {
      for (const auto& inequality : Inequality::Extract(guarantee)) {
        ranges.map[inequality.target].push_back(inequality);
      }
    }
    for (const auto& guarantee : conjunction_members) {
      auto call = guarantee.call();
      if (!call || call->function_name != "is_valid") continue;
      auto target = call->arguments[0].field_ref();
      if (!target) continue;
      auto it = ranges.map.find(*target);
      if (it != ranges.map.end()) it->second.front().nullable = false;
    }
    for (auto& [target, inequalities] : ranges.map) {
      const bool nullable =
          std::all_of(inequalities.begin(), inequalities.end(),
                      [](const Inequality& inequality) { return inequality.nullable; });
      for (auto& inequality : inequalities) inequality.nullable = nullable;
    }
    return ranges;
  }

  const std::vector<Inequality>* Find(const FieldRef& target) const {
    auto it = map.find(target);
    return it != map.end() ? &it->second : nullptr;
  }
};

/// \brief The range which the inequalities on a field leave to the field wrapped in
///   `casts`: the tightest lower and upper bound, in the type of the cast field.
///
/// Considering all inequalities on a field together matters: a row group's
/// statistics yield `x >= min and x <= max`, and neither bound on its own can rule
/// out a value set whose hull spans the row group.
internal::ArgumentRange ResolveRange(const std::vector<Inequality>& inequalities,
                                     const OrderPreservingCasts& casts) {
  internal::ArgumentRange range;
  DCHECK(!inequalities.empty());
  range.nullable = inequalities.front().nullable;

  // Whether a bound is tighter than the current one on that side. Bounds which
  // cannot be compared are ignored, which only loses precision.
  auto is_tighter = [](const std::shared_ptr<Scalar>& bound, bool inclusive,
                       const std::shared_ptr<Scalar>& current, bool current_inclusive,
                       Comparison::type tighter) {
    if (!current) return true;
    auto cmp = Comparison::Execute(bound, current);
    if (!cmp.ok()) return false;
    if (*cmp == Comparison::EQUAL) return current_inclusive && !inclusive;
    return *cmp == tighter;
  };

  for (const Inequality& inequality : inequalities) {
    auto restated = inequality.Restate(casts);
    if (!restated) continue;
    const auto& [cmp, bound] = *restated;
    const bool inclusive = cmp & Comparison::EQUAL;
    // Unless the comparison admits values below the bound, it is a lower bound;
    // likewise for values above the bound. Equality is both.
    if (!(cmp & Comparison::LESS) &&
        is_tighter(bound, inclusive, range.lower, range.lower_inclusive,
                   Comparison::GREATER)) {
      range.lower = bound;
      range.lower_inclusive = inclusive;
    }
    if (!(cmp & Comparison::GREATER) &&
        is_tighter(bound, inclusive, range.upper, range.upper_inclusive,
                   Comparison::LESS)) {
      range.upper = bound;
      range.upper_inclusive = inclusive;
    }
  }
  return range;
}

/// \brief Simplify a call against what the guarantee says about the field it is
///   applied to (possibly through order preserving casts).
///
/// Kernels which can tell what they evaluate to over a range (currently `is_in`,
/// which searches its sorted value set) are asked; this costs O(log N) per guarantee
/// with no rebinding. Comparisons, validity checks and small `is_in` value sets are
/// simplified against each inequality in turn.
Result<Expression> SimplifyWithRanges(const FieldRanges& ranges, Expression expr) {
  auto call = expr.call();
  if (!call || call->arguments.empty()) return expr;

  auto casts = OrderPreservingCasts::Strip(call->arguments[0]);
  if (!casts.inner->field_ref()) return expr;
  const auto* inequalities = ranges.Find(*casts.inner->field_ref());
  if (!inequalities) return expr;

  const bool is_small_is_in =
      call->function_name == "is_in" &&
      checked_cast<const SetLookupOptions&>(*call->options).value_set.length() <=
          kIsInSimplificationMaxValueSet;
  const auto* evaluator =
      dynamic_cast<const internal::RangeEvaluator*>(call->kernel_state.get());
  if (evaluator && call->arguments.size() == 1 && !is_small_is_in) {
    ARROW_ASSIGN_OR_RAISE(auto evaluation, evaluator->EvaluateOverRange(
                                               ResolveRange(*inequalities, casts)));
    switch (evaluation) {
      case internal::RangeEvaluation::kUnknown:
        return expr;
      case internal::RangeEvaluation::kFalse:
        return literal(false);
      case internal::RangeEvaluation::kNull:
        return literal(MakeNullScalar(call->type.GetSharedPtr()));
      case internal::RangeEvaluation::kFalseUnlessNull:
        return ValueUnlessNull(call->arguments[0], false);
    }
    return expr;
  }

  for (const Inequality& inequality : *inequalities) {
    ARROW_ASSIGN_OR_RAISE(expr, inequality.Simplify(std::move(expr)));
  }
  return expr;
}

/// \brief Simplify an expression given a guarantee, if the guarantee
///   is is_valid().
Result<Expression> SimplifyIsValidGuarantee(Expression expr,
                                            const Expression::Call& guarantee) {
  if (guarantee.function_name != "is_valid") return expr;

  return ModifyExpression(
      std::move(expr), [](Expression expr) { return expr; },
      [&](Expression expr, ...) -> Result<Expression> {
        auto call = expr.call();
        if (!call) return expr;

        if (call->arguments[0] != guarantee.arguments[0]) return expr;

        if (call->function_name == "is_valid") return literal(true);

        if (call->function_name == "true_unless_null") return literal(true);

        if (call->function_name == "is_null") return literal(false);

        return expr;
      });
}

}  // namespace

Result<Expression> SimplifyWithGuarantee(Expression expr,
                                         const Expression& guaranteed_true_predicate) {
  KnownFieldValues known_values;
  auto conjunction_members = GuaranteeConjunctionMembers(guaranteed_true_predicate);

  RETURN_NOT_OK(ExtractKnownFieldValues(&conjunction_members, &known_values));

  ARROW_ASSIGN_OR_RAISE(expr,
                        ReplaceFieldsWithKnownValues(known_values, std::move(expr)));

  auto CanonicalizeAndFoldConstants = [&expr] {
    ARROW_ASSIGN_OR_RAISE(expr, Canonicalize(std::move(expr)));
    ARROW_ASSIGN_OR_RAISE(expr, FoldConstants(std::move(expr)));
    return Status::OK();
  };
  RETURN_NOT_OK(CanonicalizeAndFoldConstants());

  // What the guarantee says about each field it bounds. This refers into
  // conjunction_members, which must not change from here on. Every call is
  // simplified against the range of its field in a single pass.
  const FieldRanges ranges = FieldRanges::Make(conjunction_members);
  if (!ranges.map.empty()) {
    Expression before = expr;
    ARROW_ASSIGN_OR_RAISE(expr, ModifyExpression(
                                    std::move(expr), [](Expression expr) { return expr; },
                                    [&](Expression expr, ...) -> Result<Expression> {
                                      return SimplifyWithRanges(ranges, std::move(expr));
                                    }));

    if (!Expression::Identical(expr, before)) {
      RETURN_NOT_OK(CanonicalizeAndFoldConstants());
    }
  }

  for (const auto& guarantee : conjunction_members) {
    if (!guarantee.call() || guarantee.call()->function_name != "is_valid") continue;

    ARROW_ASSIGN_OR_RAISE(auto simplified, SimplifyIsValidGuarantee(
                                               std::move(expr), *CallNotNull(guarantee)));

    if (Expression::Identical(simplified, expr)) continue;

    expr = std::move(simplified);
    RETURN_NOT_OK(CanonicalizeAndFoldConstants());
  }

  return expr;
}

Result<Expression> RemoveNamedRefs(Expression src) {
  if (!src.IsBound()) {
    return Status::Invalid("RemoveNamedRefs called on unbound expression");
  }
  return ModifyExpression(
      std::move(src),
      /*pre=*/
      [](Expression expr) {
        const Expression::Parameter* param = expr.parameter();
        if (param && !param->ref.IsFieldPath()) {
          auto param_as_path = *param;
          param_as_path.ref =
              FieldPath(std::vector<int>(param->indices.begin(), param->indices.end()));
          return Expression(std::move(param_as_path));
        }

        return expr;
      },
      /*post_call=*/[](Expression expr, ...) { return expr; });
}

// Serialization is accomplished by converting expressions to KeyValueMetadata and storing
// this in the schema of a RecordBatch. Embedded arrays and scalars are stored in its
// columns. Finally, the RecordBatch is written to an IPC file.
Result<std::shared_ptr<Buffer>> Serialize(const Expression& expr) {
#ifdef ARROW_IPC
  struct {
    std::shared_ptr<KeyValueMetadata> metadata_ = std::make_shared<KeyValueMetadata>();
    ArrayVector columns_;

    Result<std::string> AddScalar(const Scalar& scalar) {
      auto ret = columns_.size();
      ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(scalar, 1));
      columns_.push_back(std::move(array));
      return ToChars(ret);
    }

    Status VisitFieldRef(const FieldRef& ref) {
      if (ref.nested_refs()) {
        metadata_->Append("nested_field_ref", ToChars(ref.nested_refs()->size()));
        for (const auto& child : *ref.nested_refs()) {
          RETURN_NOT_OK(VisitFieldRef(child));
        }
        return Status::OK();
      }
      if (!ref.name()) {
        return Status::NotImplemented("Serialization of non-name field_refs");
      }
      metadata_->Append("field_ref", *ref.name());
      return Status::OK();
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
        return VisitFieldRef(*ref);
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
#else
  return Status::NotImplemented("IPC feature isn't enabled");
#endif
}

Result<Expression> Deserialize(std::shared_ptr<Buffer> buffer) {
#ifdef ARROW_IPC
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

    bool ParseInteger(const std::string& s, int32_t* value) {
      return ::arrow::internal::ParseValue<Int32Type>(s.data(), s.length(), value);
    }

    Result<std::shared_ptr<Scalar>> GetScalar(const std::string& i) {
      int32_t column_index;
      if (!ParseInteger(i, &column_index)) {
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

      if (key == "nested_field_ref") {
        int32_t size;
        if (!ParseInteger(value, &size)) {
          return Status::Invalid("Couldn't parse nested field ref length");
        }
        if (size <= 0) {
          return Status::Invalid("nested field ref length must be > 0");
        }
        std::vector<FieldRef> nested;
        nested.reserve(size);
        while (size-- > 0) {
          ARROW_ASSIGN_OR_RAISE(auto ref, GetOne());
          if (!ref.field_ref()) {
            return Status::Invalid("invalid nested field ref");
          }
          nested.push_back(*ref.field_ref());
        }
        return field_ref(FieldRef(std::move(nested)));
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
#else
  return Status::NotImplemented("IPC feature isn't enabled");
#endif
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

Expression is_null(Expression lhs, bool nan_is_null) {
  return call("is_null", {std::move(lhs)}, compute::NullOptions(std::move(nan_is_null)));
}

Expression is_valid(Expression lhs) { return call("is_valid", {std::move(lhs)}); }

Expression and_(Expression lhs, Expression rhs) {
  return call("and_kleene", {std::move(lhs), std::move(rhs)});
}

Expression and_(const std::vector<Expression>& operands) {
  if (operands.empty()) return literal(true);

  Expression folded = operands.front();
  for (auto it = operands.begin() + 1; it != operands.end(); ++it) {
    folded = and_(std::move(folded), *it);
  }
  return folded;
}

Expression or_(Expression lhs, Expression rhs) {
  return call("or_kleene", {std::move(lhs), std::move(rhs)});
}

Expression or_(const std::vector<Expression>& operands) {
  if (operands.empty()) return literal(false);

  Expression folded = operands.front();
  for (auto it = operands.begin() + 1; it != operands.end(); ++it) {
    folded = or_(std::move(folded), *it);
  }
  return folded;
}

Expression not_(Expression operand) { return call("invert", {std::move(operand)}); }

}  // namespace compute
}  // namespace arrow
