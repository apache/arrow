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

#include "arrow/engine/substrait/expression_internal.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <google/protobuf/descriptor.h>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/expression_internal.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/engine/substrait/util_internal.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/string.h"
#include "arrow/visit_scalar_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::ToChars;

namespace engine {

namespace {

Id NormalizeFunctionName(Id id) {
  // Substrait plans encode the types into the function name so it might look like
  // add:opt_i32_i32.  We don't care about  the :opt_i32_i32 so we just trim it
  std::string_view func_name = id.name;
  std::size_t colon_index = func_name.find_first_of(':');
  if (colon_index != std::string_view::npos) {
    func_name = func_name.substr(0, colon_index);
  }
  return {id.uri, func_name};
}

}  // namespace

Status DecodeArg(const substrait::FunctionArgument& arg, int idx, SubstraitCall* call,
                 const ExtensionSet& ext_set,
                 const ConversionOptions& conversion_options) {
  if (!arg.enum_().empty()) {
    call->SetEnumArg(idx, arg.enum_());
  } else if (arg.has_value()) {
    ARROW_ASSIGN_OR_RAISE(compute::Expression expr,
                          FromProto(arg.value(), ext_set, conversion_options));
    call->SetValueArg(idx, std::move(expr));
  } else if (arg.has_type()) {
    return Status::NotImplemented("Type arguments not currently supported");
  } else {
    return Status::NotImplemented("Unrecognized function argument class");
  }
  return Status::OK();
}

Status DecodeOption(const substrait::FunctionOption& opt, SubstraitCall* call) {
  std::vector<std::string_view> prefs;
  if (opt.preference_size() == 0) {
    return Status::Invalid("Invalid Substrait plan.  The option ", opt.name(),
                           " is specified but does not list any choices");
  }
  for (const auto& preference : opt.preference()) {
    prefs.push_back(preference);
  }
  call->SetOption(opt.name(), prefs);
  return Status::OK();
}

Result<SubstraitCall> DecodeScalarFunction(
    Id id, const substrait::Expression::ScalarFunction& scalar_fn,
    const ExtensionSet& ext_set, const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto output_type_and_nullable,
                        FromProto(scalar_fn.output_type(), ext_set, conversion_options));
  SubstraitCall call(id, output_type_and_nullable.first, output_type_and_nullable.second);
  for (int i = 0; i < scalar_fn.arguments_size(); i++) {
    ARROW_RETURN_NOT_OK(
        DecodeArg(scalar_fn.arguments(i), i, &call, ext_set, conversion_options));
  }
  for (const auto& opt : scalar_fn.options()) {
    ARROW_RETURN_NOT_OK(DecodeOption(opt, &call));
  }
  return std::move(call);
}

std::string EnumToString(int value, const google::protobuf::EnumDescriptor* descriptor) {
  const google::protobuf::EnumValueDescriptor* value_desc =
      descriptor->FindValueByNumber(value);
  if (value_desc == nullptr) {
    return "unknown";
  }
  return value_desc->name();
}

Result<SubstraitCall> FromProto(const substrait::AggregateFunction& func, bool is_hash,
                                const ExtensionSet& ext_set,
                                const ConversionOptions& conversion_options) {
  if (func.phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_RESULT) {
    return Status::NotImplemented(
        "Unsupported aggregation phase '",
        EnumToString(func.phase(), *substrait::AggregationPhase_descriptor()),
        "'.  Only INITIAL_TO_RESULT is supported");
  }
  if (func.invocation() != substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL &&
      func.invocation() !=
          substrait::AggregateFunction::AGGREGATION_INVOCATION_UNSPECIFIED) {
    return Status::NotImplemented(
        "Unsupported aggregation invocation '",
        EnumToString(func.invocation(),
                     *substrait::AggregateFunction::AggregationInvocation_descriptor()),
        "'.  Only AGGREGATION_INVOCATION_ALL is "
        "supported");
  }
  if (func.sorts_size() > 0) {
    return Status::NotImplemented("Aggregation sorts are not supported");
  }
  ARROW_ASSIGN_OR_RAISE(auto output_type_and_nullable,
                        FromProto(func.output_type(), ext_set, conversion_options));
  ARROW_ASSIGN_OR_RAISE(Id id, ext_set.DecodeFunction(func.function_reference()));
  id = NormalizeFunctionName(id);
  SubstraitCall call(id, output_type_and_nullable.first, output_type_and_nullable.second,
                     is_hash);
  for (int i = 0; i < func.arguments_size(); i++) {
    ARROW_RETURN_NOT_OK(DecodeArg(func.arguments(i), static_cast<uint32_t>(i), &call,
                                  ext_set, conversion_options));
  }
  return std::move(call);
}

Result<compute::Expression> FromProto(const substrait::Expression& expr,
                                      const ExtensionSet& ext_set,
                                      const ConversionOptions& conversion_options) {
  switch (expr.rex_type_case()) {
    case substrait::Expression::kLiteral: {
      ARROW_ASSIGN_OR_RAISE(auto datum,
                            FromProto(expr.literal(), ext_set, conversion_options));
      return compute::literal(std::move(datum));
    }

    case substrait::Expression::kSelection: {
      if (!expr.selection().has_direct_reference()) break;

      std::optional<compute::Expression> out;
      if (expr.selection().has_expression()) {
        ARROW_ASSIGN_OR_RAISE(
            out, FromProto(expr.selection().expression(), ext_set, conversion_options));
      }

      const auto* ref = &expr.selection().direct_reference();
      while (ref != nullptr) {
        switch (ref->reference_type_case()) {
          case substrait::Expression::ReferenceSegment::kStructField: {
            auto index = ref->struct_field().field();
            if (!out) {
              // Root StructField (column selection)
              out = compute::field_ref(FieldRef(index));
            } else if (auto out_ref = out->field_ref()) {
              // Nested StructFields on the root (selection of struct-typed column
              // combined with selecting struct fields)
              out = compute::field_ref(FieldRef(*out_ref, index));
            } else if (out->call() && out->call()->function_name == "struct_field") {
              // Nested StructFields on top of an arbitrary expression
              auto* field_options =
                  checked_cast<compute::StructFieldOptions*>(out->call()->options.get());
              field_options->field_ref =
                  FieldRef(std::move(field_options->field_ref), index);
            } else {
              // First StructField on top of an arbitrary expression
              out = compute::call("struct_field", {std::move(*out)},
                                  arrow::compute::StructFieldOptions({index}));
            }

            // Segment handled, continue with child segment (if any)
            if (ref->struct_field().has_child()) {
              ref = &ref->struct_field().child();
            } else {
              ref = nullptr;
            }
            break;
          }
          case substrait::Expression::ReferenceSegment::kListElement: {
            if (!out) {
              // Root ListField (illegal)
              return Status::Invalid(
                  "substrait::ListElement cannot take a Relation as an argument");
            }

            // ListField on top of an arbitrary expression
            out = compute::call(
                "list_element",
                {std::move(*out), compute::literal(ref->list_element().offset())});

            // Segment handled, continue with child segment (if any)
            if (ref->list_element().has_child()) {
              ref = &ref->list_element().child();
            } else {
              ref = nullptr;
            }
            break;
          }
          default:
            // Unimplemented construct, break out of loop
            out.reset();
            ref = nullptr;
        }
      }
      if (out) {
        return *std::move(out);
      }
      break;
    }

    case substrait::Expression::kIfThen: {
      const auto& if_then = expr.if_then();
      if (!if_then.has_else_()) break;
      if (if_then.ifs_size() == 0) break;

      if (if_then.ifs_size() == 1) {
        ARROW_ASSIGN_OR_RAISE(
            auto if_, FromProto(if_then.ifs(0).if_(), ext_set, conversion_options));
        ARROW_ASSIGN_OR_RAISE(
            auto then, FromProto(if_then.ifs(0).then(), ext_set, conversion_options));
        ARROW_ASSIGN_OR_RAISE(auto else_,
                              FromProto(if_then.else_(), ext_set, conversion_options));
        return compute::call("if_else",
                             {std::move(if_), std::move(then), std::move(else_)});
      }

      std::vector<compute::Expression> conditions, args;
      std::vector<std::string> condition_names;
      conditions.reserve(if_then.ifs_size());
      condition_names.reserve(if_then.ifs_size());
      size_t name_counter = 0;
      args.reserve(if_then.ifs_size() + 2);
      args.emplace_back();
      for (const auto& if_ : if_then.ifs()) {
        ARROW_ASSIGN_OR_RAISE(auto compute_if,
                              FromProto(if_.if_(), ext_set, conversion_options));
        ARROW_ASSIGN_OR_RAISE(auto compute_then,
                              FromProto(if_.then(), ext_set, conversion_options));
        conditions.emplace_back(std::move(compute_if));
        args.emplace_back(std::move(compute_then));
        condition_names.emplace_back("cond" + ToChars(++name_counter));
      }
      ARROW_ASSIGN_OR_RAISE(auto compute_else,
                            FromProto(if_then.else_(), ext_set, conversion_options));
      args.emplace_back(std::move(compute_else));
      args[0] = compute::call("make_struct", std::move(conditions),
                              compute::MakeStructOptions(condition_names));
      return compute::call("case_when", std::move(args));
    }

    case substrait::Expression::kScalarFunction: {
      const auto& scalar_fn = expr.scalar_function();

      ARROW_ASSIGN_OR_RAISE(Id function_id,
                            ext_set.DecodeFunction(scalar_fn.function_reference()));
      function_id = NormalizeFunctionName(function_id);
      ExtensionIdRegistry::SubstraitCallToArrow function_converter;

      if (function_id.uri.empty() || function_id.uri[0] == '/') {
        // Currently the Substrait project has not aligned on a standard URI and often
        // seems to use /.  In that case we fall back to name-only matching.
        ARROW_ASSIGN_OR_RAISE(
            function_converter,
            ext_set.registry()->GetSubstraitCallToArrowFallback(function_id.name));
      } else {
        ARROW_ASSIGN_OR_RAISE(function_converter,
                              ext_set.registry()->GetSubstraitCallToArrow(function_id));
      }
      ARROW_ASSIGN_OR_RAISE(
          SubstraitCall substrait_call,
          DecodeScalarFunction(function_id, scalar_fn, ext_set, conversion_options));
      return function_converter(substrait_call);
    }

    case substrait::Expression::kCast: {
      const auto& cast_exp = expr.cast();
      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(cast_exp.input(), ext_set, conversion_options));

      ARROW_ASSIGN_OR_RAISE(auto type_nullable,
                            FromProto(cast_exp.type(), ext_set, conversion_options));

      if (!type_nullable.second &&
          conversion_options.strictness == ConversionStrictness::EXACT_ROUNDTRIP) {
        return Status::Invalid("Substrait cast type must be of nullable type");
      }

      if (cast_exp.failure_behavior() ==
          substrait::Expression::Cast::FailureBehavior::
              Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION) {
        return compute::call("cast", {std::move(input)},
                             compute::CastOptions::Safe(std::move(type_nullable.first)));
      } else if (cast_exp.failure_behavior() ==
                 substrait::Expression::Cast::FailureBehavior::
                     Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL) {
        return Status::NotImplemented(
            "Unsupported cast failure behavior: "
            "FAILURE_BEHAVIOR_RETURN_NULL");
        // i.e. if unspecified
      } else {
        return Status::Invalid(
            "substrait::Expression::Cast::FailureBehavior unspecified; must be "
            "FAILURE_BEHAVIOR_RETURN_NULL or FAILURE_BEHAVIOR_THROW_EXCEPTION");
      }
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Expression from Substrait expression ",
      expr.DebugString());
}

Result<Datum> FromProto(const substrait::Expression::Literal& lit,
                        const ExtensionSet& ext_set,
                        const ConversionOptions& conversion_options) {
  if (lit.nullable() &&
      conversion_options.strictness == ConversionStrictness::EXACT_ROUNDTRIP) {
    // FIXME not sure how this field should be interpreted and there's no way to round
    // trip it through arrow
    return Status::Invalid(
        "Nullable Literals - Literal.nullable must be left at the default");
  }

  switch (lit.literal_type_case()) {
    case substrait::Expression::Literal::kBoolean:
      return Datum(lit.boolean());

    case substrait::Expression::Literal::kI8:
      return Datum(static_cast<int8_t>(lit.i8()));
    case substrait::Expression::Literal::kI16:
      return Datum(static_cast<int16_t>(lit.i16()));
    case substrait::Expression::Literal::kI32:
      return Datum(static_cast<int32_t>(lit.i32()));
    case substrait::Expression::Literal::kI64:
      return Datum(static_cast<int64_t>(lit.i64()));

    case substrait::Expression::Literal::kFp32:
      return Datum(lit.fp32());
    case substrait::Expression::Literal::kFp64:
      return Datum(lit.fp64());

    case substrait::Expression::Literal::kString:
      return Datum(lit.string());
    case substrait::Expression::Literal::kBinary:
      return Datum(BinaryScalar(lit.binary()));

    case substrait::Expression::Literal::kTimestamp:
      return Datum(
          TimestampScalar(static_cast<int64_t>(lit.timestamp()), TimeUnit::MICRO));

    case substrait::Expression::Literal::kTimestampTz:
      return Datum(TimestampScalar(static_cast<int64_t>(lit.timestamp_tz()),
                                   TimeUnit::MICRO, TimestampTzTimezoneString()));

    case substrait::Expression::Literal::kDate:
      return Datum(Date32Scalar(lit.date()));
    case substrait::Expression::Literal::kTime:
      return Datum(Time64Scalar(lit.time(), TimeUnit::MICRO));

    case substrait::Expression::Literal::kIntervalYearToMonth:
    case substrait::Expression::Literal::kIntervalDayToSecond: {
      Int32Builder builder;
      std::shared_ptr<DataType> type;
      if (lit.has_interval_year_to_month()) {
        RETURN_NOT_OK(builder.Append(lit.interval_year_to_month().years()));
        RETURN_NOT_OK(builder.Append(lit.interval_year_to_month().months()));
        type = interval_year();
      } else {
        RETURN_NOT_OK(builder.Append(lit.interval_day_to_second().days()));
        RETURN_NOT_OK(builder.Append(lit.interval_day_to_second().seconds()));
        type = interval_day();
      }
      ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
      return Datum(
          ExtensionScalar(FixedSizeListScalar(std::move(array)), std::move(type)));
    }

    case substrait::Expression::Literal::kUuid:
      return Datum(ExtensionScalar(FixedSizeBinaryScalar(lit.uuid()), uuid()));

    case substrait::Expression::Literal::kFixedChar:
      return Datum(
          ExtensionScalar(FixedSizeBinaryScalar(lit.fixed_char()),
                          fixed_char(static_cast<int32_t>(lit.fixed_char().size()))));

    case substrait::Expression::Literal::kVarChar:
      return Datum(
          ExtensionScalar(StringScalar(lit.var_char().value()),
                          varchar(static_cast<int32_t>(lit.var_char().length()))));

    case substrait::Expression::Literal::kFixedBinary:
      return Datum(FixedSizeBinaryScalar(lit.fixed_binary()));

    case substrait::Expression::Literal::kDecimal: {
      if (lit.decimal().value().size() != sizeof(Decimal128)) {
        return Status::Invalid("Decimal literal had ", lit.decimal().value().size(),
                               " bytes (expected ", sizeof(Decimal128), ")");
      }

      Decimal128 value;
      std::memcpy(value.mutable_native_endian_bytes(), lit.decimal().value().data(),
                  sizeof(Decimal128));
#if !ARROW_LITTLE_ENDIAN
      std::reverse(value.mutable_native_endian_bytes(),
                   value.mutable_native_endian_bytes() + sizeof(Decimal128));
#endif
      auto type = decimal128(lit.decimal().precision(), lit.decimal().scale());
      return Datum(Decimal128Scalar(value, std::move(type)));
    }

    case substrait::Expression::Literal::kStruct: {
      const auto& struct_ = lit.struct_();

      ScalarVector fields(struct_.fields_size());
      for (int i = 0; i < struct_.fields_size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto field,
                              FromProto(struct_.fields(i), ext_set, conversion_options));
        DCHECK(field.is_scalar());
        fields[i] = field.scalar();
      }

      // Note that Substrait struct types don't have field names, but Arrow does, so we
      // just use empty strings for them.
      std::vector<std::string> field_names(fields.size(), "");

      ARROW_ASSIGN_OR_RAISE(
          auto scalar, StructScalar::Make(std::move(fields), std::move(field_names)));
      return Datum(std::move(scalar));
    }

    case substrait::Expression::Literal::kList: {
      const auto& list = lit.list();
      if (list.values_size() == 0) {
        return Status::Invalid(
            "substrait::Expression::Literal::List had no values; should have been an "
            "substrait::Expression::Literal::EmptyList");
      }

      std::shared_ptr<DataType> element_type;

      ScalarVector values(list.values_size());
      for (int i = 0; i < list.values_size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto value,
                              FromProto(list.values(i), ext_set, conversion_options));
        DCHECK(value.is_scalar());
        values[i] = value.scalar();
        if (element_type) {
          if (!value.type()->Equals(*element_type)) {
            return Status::Invalid(
                list.DebugString(),
                " has a value whose type doesn't match the other list values");
          }
        } else {
          element_type = value.type();
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(element_type));
      RETURN_NOT_OK(builder->AppendScalars(values));
      ARROW_ASSIGN_OR_RAISE(auto arr, builder->Finish());
      return Datum(ListScalar(std::move(arr)));
    }

    case substrait::Expression::Literal::kMap: {
      const auto& map = lit.map();
      if (map.key_values_size() == 0) {
        return Status::Invalid(
            "substrait::Expression::Literal::Map had no values; should have been an "
            "substrait::Expression::Literal::EmptyMap");
      }

      std::shared_ptr<DataType> key_type, value_type;
      ScalarVector keys(map.key_values_size()), values(map.key_values_size());
      for (int i = 0; i < map.key_values_size(); ++i) {
        const auto& kv = map.key_values(i);

        static const std::array<char const*, 4> kMissing = {"key and value", "value",
                                                            "key", nullptr};
        if (auto missing = kMissing[kv.has_key() + kv.has_value() * 2]) {
          return Status::Invalid("While converting to MapScalar encountered missing ",
                                 missing, " in ", map.DebugString());
        }
        ARROW_ASSIGN_OR_RAISE(auto key, FromProto(kv.key(), ext_set, conversion_options));
        ARROW_ASSIGN_OR_RAISE(auto value,
                              FromProto(kv.value(), ext_set, conversion_options));

        DCHECK(key.is_scalar());
        DCHECK(value.is_scalar());

        keys[i] = key.scalar();
        values[i] = value.scalar();

        if (key_type) {
          if (!key.type()->Equals(*key_type)) {
            return Status::Invalid(map.DebugString(),
                                   " has a key whose type doesn't match key_type");
          }
        } else {
          key_type = value.type();
        }

        if (value_type) {
          if (!value.type()->Equals(*value_type)) {
            return Status::Invalid(map.DebugString(),
                                   " has a value whose type doesn't match value_type");
          }
        } else {
          value_type = value.type();
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto key_builder, MakeBuilder(key_type));
      ARROW_ASSIGN_OR_RAISE(auto value_builder, MakeBuilder(value_type));
      RETURN_NOT_OK(key_builder->AppendScalars(keys));
      RETURN_NOT_OK(value_builder->AppendScalars(values));
      ARROW_ASSIGN_OR_RAISE(auto key_arr, key_builder->Finish());
      ARROW_ASSIGN_OR_RAISE(auto value_arr, value_builder->Finish());
      ARROW_ASSIGN_OR_RAISE(
          auto kv_arr,
          StructArray::Make(ArrayVector{std::move(key_arr), std::move(value_arr)},
                            std::vector<std::string>{"key", "value"}));
      return Datum(std::make_shared<MapScalar>(std::move(kv_arr)));
    }

    case substrait::Expression::Literal::kEmptyList: {
      ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(lit.empty_list().type(),
                                                          ext_set, conversion_options));
      ARROW_ASSIGN_OR_RAISE(auto values, MakeEmptyArray(type_nullable.first));
      return ListScalar{std::move(values)};
    }

    case substrait::Expression::Literal::kEmptyMap: {
      ARROW_ASSIGN_OR_RAISE(
          auto key_type_nullable,
          FromProto(lit.empty_map().key(), ext_set, conversion_options));
      ARROW_ASSIGN_OR_RAISE(auto keys,
                            MakeEmptyArray(std::move(key_type_nullable.first)));

      ARROW_ASSIGN_OR_RAISE(
          auto value_type_nullable,
          FromProto(lit.empty_map().value(), ext_set, conversion_options));
      ARROW_ASSIGN_OR_RAISE(auto values,
                            MakeEmptyArray(std::move(value_type_nullable.first)));

      auto map_type = std::make_shared<MapType>(keys->type(), values->type());
      ARROW_ASSIGN_OR_RAISE(
          auto key_values,
          StructArray::Make(
              {std::move(keys), std::move(values)},
              checked_cast<const ListType&>(*map_type).value_type()->fields()));

      return MapScalar{std::move(key_values)};
    }

    case substrait::Expression::Literal::kNull: {
      ARROW_ASSIGN_OR_RAISE(auto type_nullable,
                            FromProto(lit.null(), ext_set, conversion_options));
      if (!type_nullable.second) {
        return Status::Invalid("Substrait null literal ", lit.DebugString(),
                               " is of non-nullable type");
      }

      return Datum(MakeNullScalar(std::move(type_nullable.first)));
    }

    default:
      break;
  }

  return Status::NotImplemented("conversion to arrow::Datum from Substrait literal ",
                                lit.DebugString());
}

namespace {
struct ScalarToProtoImpl {
  Status Visit(const NullScalar& s) { return NotImplemented(s); }

  using Lit = substrait::Expression::Literal;

  template <typename Arg, typename PrimitiveScalar>
  Status Primitive(void (substrait::Expression::Literal::*set)(Arg),
                   const PrimitiveScalar& primitive_scalar) {
    (lit_->*set)(static_cast<Arg>(primitive_scalar.value));
    return Status::OK();
  }

  template <typename LiteralSetter, typename ScalarWithBufferValue>
  Status FromBuffer(LiteralSetter&& set_lit,
                    const ScalarWithBufferValue& scalar_with_buffer) {
    set_lit(lit_, scalar_with_buffer.value->ToString());
    return Status::OK();
  }

  Status Visit(const BooleanScalar& s) { return Primitive(&Lit::set_boolean, s); }

  Status Visit(const Int8Scalar& s) { return Primitive(&Lit::set_i8, s); }
  Status Visit(const Int16Scalar& s) { return Primitive(&Lit::set_i16, s); }
  Status Visit(const Int32Scalar& s) { return Primitive(&Lit::set_i32, s); }
  Status Visit(const Int64Scalar& s) { return Primitive(&Lit::set_i64, s); }

  Status Visit(const UInt8Scalar& s) { return NotImplemented(s); }
  Status Visit(const UInt16Scalar& s) { return NotImplemented(s); }
  Status Visit(const UInt32Scalar& s) { return NotImplemented(s); }
  Status Visit(const UInt64Scalar& s) { return NotImplemented(s); }

  Status Visit(const HalfFloatScalar& s) { return NotImplemented(s); }
  Status Visit(const FloatScalar& s) { return Primitive(&Lit::set_fp32, s); }
  Status Visit(const DoubleScalar& s) { return Primitive(&Lit::set_fp64, s); }

  Status Visit(const StringScalar& s) {
    return FromBuffer([](Lit* lit, std::string&& s) { lit->set_string(std::move(s)); },
                      s);
  }
  Status Visit(const BinaryScalar& s) {
    return FromBuffer([](Lit* lit, std::string&& s) { lit->set_binary(std::move(s)); },
                      s);
  }

  Status Visit(const FixedSizeBinaryScalar& s) {
    return FromBuffer(
        [](Lit* lit, std::string&& s) { lit->set_fixed_binary(std::move(s)); }, s);
  }

  Status Visit(const Date32Scalar& s) { return Primitive(&Lit::set_date, s); }
  Status Visit(const Date64Scalar& s) { return NotImplemented(s); }

  Status Visit(const TimestampScalar& s) {
    const auto& t = checked_cast<const TimestampType&>(*s.type);

    if (t.unit() != TimeUnit::MICRO) return NotImplemented(s);

    if (t.timezone() == "") return Primitive(&Lit::set_timestamp, s);

    if (t.timezone() == TimestampTzTimezoneString()) {
      return Primitive(&Lit::set_timestamp_tz, s);
    }

    return NotImplemented(s);
  }

  Status Visit(const Time32Scalar& s) { return NotImplemented(s); }
  Status Visit(const Time64Scalar& s) {
    if (checked_cast<const Time64Type&>(*s.type).unit() != TimeUnit::MICRO) {
      return NotImplemented(s);
    }
    return Primitive(&Lit::set_time, s);
  }

  Status Visit(const MonthIntervalScalar& s) { return NotImplemented(s); }
  Status Visit(const DayTimeIntervalScalar& s) { return NotImplemented(s); }

  Status Visit(const Decimal128Scalar& s) {
    auto decimal = std::make_unique<Lit::Decimal>();

    auto decimal_type = checked_cast<const Decimal128Type*>(s.type.get());
    decimal->set_precision(decimal_type->precision());
    decimal->set_scale(decimal_type->scale());

    decimal->set_value(reinterpret_cast<const char*>(s.value.native_endian_bytes()),
                       sizeof(Decimal128));
#if !ARROW_LITTLE_ENDIAN
    std::reverse(decimal->mutable_value()->begin(), decimal->mutable_value()->end());
#endif
    lit_->set_allocated_decimal(decimal.release());
    return Status::OK();
  }

  Status Visit(const Decimal256Scalar& s) { return NotImplemented(s); }

  Status Visit(const ListScalar& s) {
    if (s.value->length() == 0) {
      ARROW_ASSIGN_OR_RAISE(auto list_type, ToProto(*s.type, /*nullable=*/true, ext_set_,
                                                    conversion_options_));
      lit_->set_allocated_empty_list(list_type->release_list());
      return Status::OK();
    }

    lit_->set_allocated_list(new Lit::List());

    const auto& list_type = checked_cast<const ListType&>(*s.type);
    ARROW_ASSIGN_OR_RAISE(auto element_type, ToProto(*list_type.value_type(),
                                                     list_type.value_field()->nullable(),
                                                     ext_set_, conversion_options_));

    auto values = lit_->mutable_list()->mutable_values();
    values->Reserve(static_cast<int>(s.value->length()));

    for (int64_t i = 0; i < s.value->length(); ++i) {
      ARROW_ASSIGN_OR_RAISE(Datum list_element, s.value->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto lit,
                            ToProto(list_element, ext_set_, conversion_options_));
      values->AddAllocated(lit.release());
    }
    return Status::OK();
  }

  Status Visit(const StructScalar& s) {
    lit_->set_allocated_struct_(new Lit::Struct());

    auto fields = lit_->mutable_struct_()->mutable_fields();
    fields->Reserve(static_cast<int>(s.value.size()));

    for (Datum field : s.value) {
      ARROW_ASSIGN_OR_RAISE(auto lit, ToProto(field, ext_set_, conversion_options_));
      fields->AddAllocated(lit.release());
    }
    return Status::OK();
  }

  Status Visit(const SparseUnionScalar& s) { return NotImplemented(s); }
  Status Visit(const DenseUnionScalar& s) { return NotImplemented(s); }
  Status Visit(const DictionaryScalar& s) { return NotImplemented(s); }

  Status Visit(const MapScalar& s) {
    if (s.value->length() == 0) {
      ARROW_ASSIGN_OR_RAISE(auto map_type, ToProto(*s.type, /*nullable=*/true, ext_set_,
                                                   conversion_options_));
      lit_->set_allocated_empty_map(map_type->release_map());
      return Status::OK();
    }

    lit_->set_allocated_map(new Lit::Map());

    const auto& kv_arr = checked_cast<const StructArray&>(*s.value);

    auto key_values = lit_->mutable_map()->mutable_key_values();
    key_values->Reserve(static_cast<int>(kv_arr.length()));

    for (int64_t i = 0; i < s.value->length(); ++i) {
      auto kv = std::make_unique<Lit::Map::KeyValue>();

      ARROW_ASSIGN_OR_RAISE(Datum key_scalar, kv_arr.field(0)->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto key, ToProto(key_scalar, ext_set_, conversion_options_));
      kv->set_allocated_key(key.release());

      ARROW_ASSIGN_OR_RAISE(Datum value_scalar, kv_arr.field(1)->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto value,
                            ToProto(value_scalar, ext_set_, conversion_options_));
      kv->set_allocated_value(value.release());

      key_values->AddAllocated(kv.release());
    }
    return Status::OK();
  }

  Status Visit(const RunEndEncodedScalar& s) { return (*this)(*s.value); }

  Status Visit(const ExtensionScalar& s) {
    if (UnwrapUuid(*s.type)) {
      return FromBuffer([](Lit* lit, std::string&& s) { lit->set_uuid(std::move(s)); },
                        checked_cast<const FixedSizeBinaryScalar&>(*s.value));
    }

    if (UnwrapFixedChar(*s.type)) {
      return FromBuffer(
          [](Lit* lit, std::string&& s) { lit->set_fixed_char(std::move(s)); },
          checked_cast<const FixedSizeBinaryScalar&>(*s.value));
    }

    if (auto length = UnwrapVarChar(*s.type)) {
      auto var_char = std::make_unique<Lit::VarChar>();
      var_char->set_length(*length);
      var_char->set_value(checked_cast<const StringScalar&>(*s.value).value->ToString());

      lit_->set_allocated_var_char(var_char.release());
      return Status::OK();
    }

    auto GetPairOfInts = [&] {
      const auto& array = *checked_cast<const FixedSizeListScalar&>(*s.value).value;
      auto ints = checked_cast<const Int32Array&>(array).raw_values();
      return std::make_pair(ints[0], ints[1]);
    };

    if (UnwrapIntervalYear(*s.type)) {
      auto interval_year = std::make_unique<Lit::IntervalYearToMonth>();
      interval_year->set_years(GetPairOfInts().first);
      interval_year->set_months(GetPairOfInts().second);

      lit_->set_allocated_interval_year_to_month(interval_year.release());
      return Status::OK();
    }

    if (UnwrapIntervalDay(*s.type)) {
      auto interval_day = std::make_unique<Lit::IntervalDayToSecond>();
      interval_day->set_days(GetPairOfInts().first);
      interval_day->set_seconds(GetPairOfInts().second);

      lit_->set_allocated_interval_day_to_second(interval_day.release());
      return Status::OK();
    }

    return NotImplemented(s);
  }

  Status Visit(const FixedSizeListScalar& s) { return NotImplemented(s); }
  Status Visit(const DurationScalar& s) { return NotImplemented(s); }
  Status Visit(const LargeStringScalar& s) { return NotImplemented(s); }
  Status Visit(const LargeBinaryScalar& s) { return NotImplemented(s); }
  Status Visit(const LargeListScalar& s) { return NotImplemented(s); }
  Status Visit(const MonthDayNanoIntervalScalar& s) { return NotImplemented(s); }

  Status NotImplemented(const Scalar& s) {
    return Status::NotImplemented("conversion to substrait::Expression::Literal from ",
                                  s.ToString());
  }

  Status operator()(const Scalar& scalar) { return VisitScalarInline(scalar, this); }

  substrait::Expression::Literal* lit_;
  ExtensionSet* ext_set_;
  const ConversionOptions& conversion_options_;
};
}  // namespace

Result<std::unique_ptr<substrait::Expression::Literal>> ToProto(
    const Datum& datum, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  if (!datum.is_scalar()) {
    return Status::NotImplemented("representing ", datum.ToString(),
                                  " as a substrait::Expression::Literal");
  }

  auto out = std::make_unique<substrait::Expression::Literal>();

  if (datum.scalar()->is_valid) {
    RETURN_NOT_OK(
        (ScalarToProtoImpl{out.get(), ext_set, conversion_options})(*datum.scalar()));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*datum.type(), /*nullable=*/true, ext_set,
                                             conversion_options));
    out->set_allocated_null(type.release());
  }

  return std::move(out);
}

static Status AddChildToReferenceSegment(
    substrait::Expression::ReferenceSegment& segment,
    std::unique_ptr<substrait::Expression::ReferenceSegment>&& child) {
  auto status = Status::Invalid("Attempt to add child to incomplete reference segment");
  switch (segment.reference_type_case()) {
    case substrait::Expression::ReferenceSegment::kMapKey: {
      auto map_key = segment.mutable_map_key();
      if (map_key->has_child()) {
        status = AddChildToReferenceSegment(*map_key->mutable_child(), std::move(child));
      } else {
        map_key->set_allocated_child(child.release());
        status = Status::OK();
      }
      break;
    }
    case substrait::Expression::ReferenceSegment::kStructField: {
      auto struct_field = segment.mutable_struct_field();
      if (struct_field->has_child()) {
        status =
            AddChildToReferenceSegment(*struct_field->mutable_child(), std::move(child));
      } else {
        struct_field->set_allocated_child(child.release());
        status = Status::OK();
      }
      break;
    }
    case substrait::Expression::ReferenceSegment::kListElement: {
      auto list_element = segment.mutable_list_element();
      if (list_element->has_child()) {
        status =
            AddChildToReferenceSegment(*list_element->mutable_child(), std::move(child));
      } else {
        list_element->set_allocated_child(child.release());
        status = Status::OK();
      }
      break;
    }
    default:
      break;
  }
  return status;
}

// Indexes the given Substrait expression or root (if expr is empty) using the given
// ReferenceSegment.
static Result<std::unique_ptr<substrait::Expression>> MakeDirectReference(
    std::unique_ptr<substrait::Expression>&& expr,
    std::unique_ptr<substrait::Expression::ReferenceSegment>&& ref_segment) {
  // If expr is already a selection expression, add the index to its index stack.
  if (expr && expr->has_selection() && expr->selection().has_direct_reference()) {
    auto selection = expr->mutable_selection();
    auto root_ref_segment = selection->mutable_direct_reference();
    auto status = AddChildToReferenceSegment(*root_ref_segment, std::move(ref_segment));
    if (status.ok()) {
      return std::move(expr);
    }
  }

  auto selection = std::make_unique<substrait::Expression::FieldReference>();
  selection->set_allocated_direct_reference(ref_segment.release());

  if (expr && expr->rex_type_case() != substrait::Expression::REX_TYPE_NOT_SET) {
    selection->set_allocated_expression(expr.release());
  } else {
    selection->set_allocated_root_reference(
        new substrait::Expression::FieldReference::RootReference());
  }

  auto out = std::make_unique<substrait::Expression>();
  out->set_allocated_selection(selection.release());
  return std::move(out);
}

// Indexes the given Substrait struct-typed expression or root (if expr is empty) using
// the given field index.
static Result<std::unique_ptr<substrait::Expression>> MakeStructFieldReference(
    std::unique_ptr<substrait::Expression>&& expr, int field) {
  auto struct_field =
      std::make_unique<substrait::Expression::ReferenceSegment::StructField>();
  struct_field->set_field(field);

  auto ref_segment = std::make_unique<substrait::Expression::ReferenceSegment>();
  ref_segment->set_allocated_struct_field(struct_field.release());

  return MakeDirectReference(std::move(expr), std::move(ref_segment));
}

// Indexes the given Substrait list-typed expression using the given offset.
static Result<std::unique_ptr<substrait::Expression>> MakeListElementReference(
    std::unique_ptr<substrait::Expression>&& expr, int offset) {
  auto list_element =
      std::make_unique<substrait::Expression::ReferenceSegment::ListElement>();
  list_element->set_offset(offset);

  auto ref_segment = std::make_unique<substrait::Expression::ReferenceSegment>();
  ref_segment->set_allocated_list_element(list_element.release());

  return MakeDirectReference(std::move(expr), std::move(ref_segment));
}

Result<std::unique_ptr<substrait::Expression::ScalarFunction>> EncodeSubstraitCall(
    const SubstraitCall& call, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(uint32_t anchor, ext_set->EncodeFunction(call.id()));
  auto scalar_fn = std::make_unique<substrait::Expression::ScalarFunction>();
  scalar_fn->set_function_reference(anchor);
  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::Type> output_type,
      ToProto(*call.output_type(), call.output_nullable(), ext_set, conversion_options));
  scalar_fn->set_allocated_output_type(output_type.release());

  for (int i = 0; i < call.size(); i++) {
    substrait::FunctionArgument* arg = scalar_fn->add_arguments();
    if (call.HasEnumArg(i)) {
      ARROW_ASSIGN_OR_RAISE(std::string_view enum_val, call.GetEnumArg(i));
      arg->set_enum_(std::string(enum_val));
    } else if (call.HasValueArg(i)) {
      ARROW_ASSIGN_OR_RAISE(compute::Expression value_arg, call.GetValueArg(i));
      ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Expression> value_expr,
                            ToProto(value_arg, ext_set, conversion_options));
      arg->set_allocated_value(value_expr.release());
    } else {
      return Status::Invalid("Call reported having ", call.size(),
                             " arguments but no argument could be found at index ", i);
    }
  }
  return std::move(scalar_fn);
}

Result<std::unique_ptr<substrait::Expression>> ToProto(
    const compute::Expression& expr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  if (!expr.IsBound()) {
    return Status::Invalid("ToProto requires a bound Expression");
  }

  auto out = std::make_unique<substrait::Expression>();

  if (auto datum = expr.literal()) {
    ARROW_ASSIGN_OR_RAISE(auto literal, ToProto(*datum, ext_set, conversion_options));
    out->set_allocated_literal(literal.release());
    return std::move(out);
  }

  if (auto param = expr.parameter()) {
    // Special case of a nested StructField
    DCHECK(!param->indices.empty());

    for (int index : param->indices) {
      ARROW_ASSIGN_OR_RAISE(out, MakeStructFieldReference(std::move(out), index));
    }

    return std::move(out);
  }

  auto call = CallNotNull(expr);

  if (call->function_name == "case_when") {
    auto conditions = call->arguments[0].call();
    if (conditions && conditions->function_name == "make_struct") {
      // catch the special case of calls convertible to IfThen
      auto if_then_ = std::make_unique<substrait::Expression::IfThen>();

      // don't try to convert argument 0 of the case_when; we have to convert the elements
      // of make_struct individually
      std::vector<std::unique_ptr<substrait::Expression>> arguments(
          call->arguments.size() - 1);
      for (size_t i = 1; i < call->arguments.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(arguments[i - 1],
                              ToProto(call->arguments[i], ext_set, conversion_options));
      }

      for (size_t i = 0; i < conditions->arguments.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto cond_substrait, ToProto(conditions->arguments[i],
                                                           ext_set, conversion_options));
        auto clause = std::make_unique<substrait::Expression::IfThen::IfClause>();
        clause->set_allocated_if_(cond_substrait.release());
        clause->set_allocated_then(arguments[i].release());
        if_then_->mutable_ifs()->AddAllocated(clause.release());
      }

      if_then_->set_allocated_else_(arguments.back().release());

      out->set_allocated_if_then(if_then_.release());
      return std::move(out);
    }
  }

  // the remaining function pattern matchers only convert the function itself, so we
  // should be able to convert all its arguments first here
  std::vector<std::unique_ptr<substrait::Expression>> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(arguments[i],
                          ToProto(call->arguments[i], ext_set, conversion_options));
  }

  if (call->function_name == "struct_field") {
    // catch the special case of calls convertible to a StructField
    const auto& field_options =
        checked_cast<const compute::StructFieldOptions&>(*call->options);
    const DataType& struct_type = *call->arguments[0].type();
    DCHECK_EQ(struct_type.id(), Type::STRUCT);

    ARROW_ASSIGN_OR_RAISE(auto field_path, field_options.field_ref.FindOne(struct_type));
    out = std::move(arguments[0]);
    for (int index : field_path.indices()) {
      ARROW_ASSIGN_OR_RAISE(out, MakeStructFieldReference(std::move(out), index));
    }
    return std::move(out);
  }

  if (call->function_name == "list_element") {
    // catch the special case of calls convertible to a ListElement
    if (arguments[0]->has_selection() &&
        arguments[0]->selection().has_direct_reference()) {
      if (arguments[1]->has_literal() && arguments[1]->literal().literal_type_case() ==
                                             substrait::Expression::Literal::kI32) {
        return MakeListElementReference(std::move(arguments[0]),
                                        arguments[1]->literal().i32());
      }
    }
  }

  if (call->function_name == "if_else") {
    // catch the special case of calls convertible to IfThen
    auto if_clause = std::make_unique<substrait::Expression::IfThen::IfClause>();
    if_clause->set_allocated_if_(arguments[0].release());
    if_clause->set_allocated_then(arguments[1].release());

    auto if_then = std::make_unique<substrait::Expression::IfThen>();
    if_then->mutable_ifs()->AddAllocated(if_clause.release());
    if_then->set_allocated_else_(arguments[2].release());

    out->set_allocated_if_then(if_then.release());
    return std::move(out);
  }

  // other expression types dive into extensions immediately
  ARROW_ASSIGN_OR_RAISE(
      ExtensionIdRegistry::ArrowToSubstraitCall converter,
      ext_set->registry()->GetArrowToSubstraitCall(call->function_name));
  ARROW_ASSIGN_OR_RAISE(SubstraitCall substrait_call, converter(*call));
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Expression::ScalarFunction> scalar_fn,
                        EncodeSubstraitCall(substrait_call, ext_set, conversion_options));
  out->set_allocated_scalar_function(scalar_fn.release());
  return std::move(out);
}

}  // namespace engine
}  // namespace arrow
