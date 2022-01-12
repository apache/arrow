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

#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/expression_internal.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/engine/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/make_unique.h"
#include "arrow/visit_scalar_inline.h"
#include "generated/substrait/expression.pb.h"  // IWYU pragma: export

namespace arrow {
namespace engine {
namespace {

std::shared_ptr<FixedSizeBinaryScalar> FixedSizeBinaryScalarFromBytes(
    const std::string& bytes) {
  auto buf = Buffer::FromString(bytes);
  auto type = fixed_size_binary(static_cast<int>(buf->size()));
  return std::make_shared<FixedSizeBinaryScalar>(std::move(buf), std::move(type));
}

}  // namespace

Result<compute::Expression> FromProto(const substrait::Expression& expr) {
  switch (expr.rex_type_case()) {
    case substrait::Expression::kLiteral: {
      ARROW_ASSIGN_OR_RAISE(auto datum, FromProto(expr.literal()));
      return compute::literal(std::move(datum));
    }

    case substrait::Expression::kSelection: {
      if (!expr.selection().has_direct_reference()) break;

      util::optional<compute::Expression> out;
      if (expr.selection().has_expression()) {
        ARROW_ASSIGN_OR_RAISE(out, FromProto(expr.selection().expression()));
      }

      const auto* ref = &expr.selection().direct_reference();
      while (ref != NULLPTR) {
        switch (ref->reference_type_case()) {
          case substrait::Expression::ReferenceSegment::kStructField: {
            if (!out) {
              // Root StructField (column selection)
              out = compute::field_ref(FieldRef(ref->struct_field().field()));
            } else if (auto out_ref = out->field_ref()) {
              // StructField on top of another StructField
              out = compute::field_ref(FieldRef(*out_ref, ref->struct_field().field()));
            } else {
              // StructField on top of an arbitrary expression
              // FIXME add struct_field compute function to handle this case
              out.reset();
              ref = NULLPTR;
              break;
            }

            // Segment handled, continue with child segment (if any)
            if (ref->struct_field().has_child()) {
              ref = &ref->struct_field().child();
            } else {
              ref = NULLPTR;
            }
            break;
          }
          case substrait::Expression::ReferenceSegment::kListElement: {
            if (!out) {
              // Root ListField (illegal)
              return Status::Invalid(
                  "substrait::ListElement cannot take a Relation as an argument");
            } else {
              // ListField on top of an arbitrary expression
              out = compute::call(
                  "list_element",
                  {std::move(*out), compute::literal(ref->list_element().offset())});
            }

            // Segment handled, continue with child segment (if any)
            if (ref->list_element().has_child()) {
              ref = &ref->list_element().child();
            } else {
              ref = NULLPTR;
            }
            break;
          }
          default:
            // Unimplemented construct, break out of loop
            out.reset();
            ref = NULLPTR;
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
      if (if_then.ifs_size() != 1) break;
      ARROW_ASSIGN_OR_RAISE(auto if_, FromProto(if_then.ifs(0).if_()));
      ARROW_ASSIGN_OR_RAISE(auto then, FromProto(if_then.ifs(0).then()));
      ARROW_ASSIGN_OR_RAISE(auto else_, FromProto(if_then.else_()));
      return compute::call("if_else",
                           {std::move(if_), std::move(then), std::move(else_)});
    }

    case substrait::Expression::kScalarFunction: {
      const auto& scalar_fn = expr.scalar_function();

      ExtensionSet ext_set;
      auto id = ext_set.function_ids()[scalar_fn.function_reference()];

      std::vector<compute::Expression> arguments(scalar_fn.args_size());
      for (size_t i = 0; i < arguments.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(arguments[i], FromProto(scalar_fn.args(i)));
      }

      return compute::call(id.name.to_string(), std::move(arguments));
    }

    default:
      break;
  }

  return Status::NotImplemented("conversion to arrow::compute::Expression from ",
                                expr.DebugString());
}

Result<Datum> FromProto(const substrait::Expression::Literal& lit) {
  if (lit.nullable()) {
    // FIXME not sure how this field should be interpreted and there's no way to round
    // trip it through arrow
    return Status::Invalid(
        "Nullable Literals - Literal.nullable must be left at the default");
  }

  ExtensionSet ext_set;

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
      return Datum(BinaryScalar(Buffer::FromString(lit.binary())));

    case substrait::Expression::Literal::kTimestamp:
      return Datum(
          TimestampScalar(static_cast<int64_t>(lit.timestamp()), TimeUnit::MICRO));

    case substrait::Expression::Literal::kTimestampTz:
      return Datum(TimestampScalar(static_cast<int64_t>(lit.timestamp_tz()),
                                   TimeUnit::MICRO, TimestampTzTimezoneString()));

    case substrait::Expression::Literal::kDate:
      return Datum(Date64Scalar(static_cast<int64_t>(lit.date())));
    case substrait::Expression::Literal::kTime:
      return Datum(Time64Scalar(static_cast<int64_t>(lit.time()), TimeUnit::MICRO));

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
      return Datum(ExtensionScalar(FixedSizeBinaryScalarFromBytes(lit.uuid()), uuid()));

    case substrait::Expression::Literal::kFixedChar:
      return Datum(
          ExtensionScalar(FixedSizeBinaryScalarFromBytes(lit.fixed_char()),
                          fixed_char(static_cast<int32_t>(lit.fixed_char().size()))));

    case substrait::Expression::Literal::kVarChar:
      return Datum(
          ExtensionScalar(StringScalar(lit.var_char().value()),
                          varchar(static_cast<int32_t>(lit.var_char().length()))));

    case substrait::Expression::Literal::kFixedBinary:
      return Datum(FixedSizeBinaryScalarFromBytes(lit.fixed_binary()));

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
      std::vector<std::string> field_names(fields.size(), "");
      for (size_t i = 0; i < fields.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto field, FromProto(struct_.fields(i)));
        DCHECK(field.is_scalar());
        fields[i] = field.scalar();
      }
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
      for (size_t i = 0; i < values.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto value, FromProto(list.values(i)));
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

      ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(std::move(element_type)));
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
      for (size_t i = 0; i < values.size(); ++i) {
        const auto& kv = map.key_values(i);

        static const std::array<char const*, 4> kMissing = {"key and value", "value",
                                                            "key", nullptr};
        if (auto missing = kMissing[kv.has_key() + kv.has_value() * 2]) {
          return Status::Invalid("While converting to MapScalar encountered missing ",
                                 missing, " in ", map.DebugString());
        }
        ARROW_ASSIGN_OR_RAISE(auto key, FromProto(kv.key()));
        ARROW_ASSIGN_OR_RAISE(auto value, FromProto(kv.value()));

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
      ARROW_ASSIGN_OR_RAISE(auto type_nullable,
                            FromProto(lit.empty_list().type(), ext_set));
      ARROW_ASSIGN_OR_RAISE(auto values, MakeEmptyArray(type_nullable.first));
      return ListScalar{std::move(values)};
    }

    case substrait::Expression::Literal::kEmptyMap: {
      ARROW_ASSIGN_OR_RAISE(auto key_type_nullable,
                            FromProto(lit.empty_map().key(), ext_set));
      ARROW_ASSIGN_OR_RAISE(auto keys,
                            MakeEmptyArray(std::move(key_type_nullable.first)));

      ARROW_ASSIGN_OR_RAISE(auto value_type_nullable,
                            FromProto(lit.empty_map().value(), ext_set));
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
      ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(lit.null(), ext_set));
      if (!type_nullable.second) {
        return Status::Invalid("Null literal ", lit.DebugString(),
                               " is of non-nullable type");
      }

      return Datum(MakeNullScalar(std::move(type_nullable.first)));
    }

    default:
      break;
  }

  return Status::NotImplemented("conversion to arrow::Datum from ", lit.DebugString());
}

namespace {
struct ToProtoImpl {
  Status Visit(const NullScalar& s) { return NotImplemented(s); }

  using Lit = substrait::Expression::Literal;

  template <typename Arg, typename PrimitiveScalar>
  Status Primitive(void (substrait::Expression::Literal::*set)(Arg),
                   const PrimitiveScalar& primitive_scalar) {
    (lit_->*set)(static_cast<Arg>(primitive_scalar.value));
    return Status::OK();
  }

  template <typename ScalarWithBufferValue>
  Status FromBuffer(void (substrait::Expression::Literal::*set)(std::string&&),
                    const ScalarWithBufferValue& scalar_with_buffer) {
    (lit_->*set)(scalar_with_buffer.value->ToString());
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

  Status Visit(const StringScalar& s) { return FromBuffer(&Lit::set_string, s); }
  Status Visit(const BinaryScalar& s) { return FromBuffer(&Lit::set_binary, s); }

  Status Visit(const FixedSizeBinaryScalar& s) {
    return FromBuffer(&Lit::set_fixed_binary, s);
  }

  Status Visit(const Date32Scalar& s) { return NotImplemented(s); }
  Status Visit(const Date64Scalar& s) { return Primitive(&Lit::set_date, s); }

  Status Visit(const TimestampScalar& s) {
    const auto& t = internal::checked_cast<const TimestampType&>(*s.type);

    if (t.unit() != TimeUnit::MICRO) return NotImplemented(s);

    if (t.timezone() == "") return Primitive(&Lit::set_timestamp, s);

    if (t.timezone() == TimestampTzTimezoneString()) {
      return Primitive(&Lit::set_timestamp_tz, s);
    }

    return NotImplemented(s);
  }

  Status Visit(const Time32Scalar& s) { return NotImplemented(s); }
  Status Visit(const Time64Scalar& s) {
    if (internal::checked_cast<const Time64Type&>(*s.type).unit() != TimeUnit::MICRO) {
      return NotImplemented(s);
    }
    return Primitive(&Lit::set_time, s);
  }

  Status Visit(const MonthIntervalScalar& s) { return NotImplemented(s); }
  Status Visit(const DayTimeIntervalScalar& s) { return NotImplemented(s); }

  Status Visit(const Decimal128Scalar& s) {
    auto decimal = internal::make_unique<Lit::Decimal>();

    auto decimal_type = internal::checked_cast<const Decimal128Type*>(s.type.get());
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
    ExtensionSet ext_set;
    if (s.value->length() == 0) {
      ARROW_ASSIGN_OR_RAISE(auto list_type,
                            ToProto(*s.type, /*nullable=*/true, &ext_set));
      lit_->set_allocated_empty_list(list_type->release_list());
      return Status::OK();
    }

    lit_->set_allocated_list(new Lit::List());

    const auto& list_type = internal::checked_cast<const ListType&>(*s.type);
    ARROW_ASSIGN_OR_RAISE(
        auto element_type,
        ToProto(*list_type.value_type(), list_type.value_field()->nullable(), &ext_set));

    auto values = lit_->mutable_list()->mutable_values();
    values->Reserve(static_cast<int>(s.value->length()));

    for (int64_t i = 0; i < s.value->length(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, s.value->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto lit, ToProto(Datum(std::move(scalar))));
      values->AddAllocated(lit.release());
    }
    return Status::OK();
  }

  Status Visit(const StructScalar& s) {
    lit_->set_allocated_struct_(new Lit::Struct());

    auto fields = lit_->mutable_struct_()->mutable_fields();
    fields->Reserve(static_cast<int>(s.value.size()));

    for (Datum field : s.value) {
      ARROW_ASSIGN_OR_RAISE(auto lit, ToProto(field));
      fields->AddAllocated(lit.release());
    }
    return Status::OK();
  }

  Status Visit(const SparseUnionScalar& s) { return NotImplemented(s); }
  Status Visit(const DenseUnionScalar& s) { return NotImplemented(s); }
  Status Visit(const DictionaryScalar& s) { return NotImplemented(s); }

  Status Visit(const MapScalar& s) {
    ExtensionSet ext_set;
    if (s.value->length() == 0) {
      ARROW_ASSIGN_OR_RAISE(auto map_type, ToProto(*s.type, /*nullable=*/true, &ext_set));
      lit_->set_allocated_empty_map(map_type->release_map());
      return Status::OK();
    }

    lit_->set_allocated_map(new Lit::Map());

    const auto& kv_arr = internal::checked_cast<const StructArray&>(*s.value);

    auto key_values = lit_->mutable_map()->mutable_key_values();
    key_values->Reserve(static_cast<int>(kv_arr.length()));

    for (int64_t i = 0; i < s.value->length(); ++i) {
      auto kv = internal::make_unique<Lit::Map::KeyValue>();

      ARROW_ASSIGN_OR_RAISE(Datum key_scalar, kv_arr.field(0)->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto key, ToProto(key_scalar));
      kv->set_allocated_key(key.release());

      ARROW_ASSIGN_OR_RAISE(Datum value_scalar, kv_arr.field(1)->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto value, ToProto(value_scalar));
      kv->set_allocated_value(value.release());

      key_values->AddAllocated(kv.release());
    }
    return Status::OK();
  }

  Status Visit(const ExtensionScalar& s) {
    if (UnwrapUuid(*s.type)) {
      return FromBuffer(&Lit::set_uuid,
                        internal::checked_cast<const FixedSizeBinaryScalar&>(*s.value));
    }

    if (UnwrapFixedChar(*s.type)) {
      return FromBuffer(&Lit::set_fixed_char,
                        internal::checked_cast<const FixedSizeBinaryScalar&>(*s.value));
    }

    if (auto length = UnwrapVarChar(*s.type)) {
      auto var_char = internal::make_unique<Lit::VarChar>();
      var_char->set_length(*length);
      var_char->set_value(
          internal::checked_cast<const StringScalar&>(*s.value).value->ToString());

      lit_->set_allocated_var_char(var_char.release());
      return Status::OK();
    }

    auto GetPairOfInts = [&] {
      const auto& array =
          *internal::checked_cast<const FixedSizeListScalar&>(*s.value).value;
      auto ints = internal::checked_cast<const Int32Array&>(array).raw_values();
      return std::make_pair(ints[0], ints[1]);
    };

    if (UnwrapIntervalYear(*s.type)) {
      auto interval_year = internal::make_unique<Lit::IntervalYearToMonth>();
      interval_year->set_years(GetPairOfInts().first);
      interval_year->set_months(GetPairOfInts().second);

      lit_->set_allocated_interval_year_to_month(interval_year.release());
      return Status::OK();
    }

    if (UnwrapIntervalDay(*s.type)) {
      auto interval_day = internal::make_unique<Lit::IntervalDayToSecond>();
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
};
}  // namespace

Result<std::unique_ptr<substrait::Expression::Literal>> ToProto(const Datum& datum) {
  if (!datum.is_scalar()) {
    return Status::NotImplemented("representing ", datum.ToString(),
                                  " as a substrait::Expression::Literal");
  }

  auto out = internal::make_unique<substrait::Expression::Literal>();

  if (datum.scalar()->is_valid) {
    RETURN_NOT_OK((ToProtoImpl{out.get()})(*datum.scalar()));
  } else {
    ExtensionSet ext_set;
    ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*datum.type(), /*nullable=*/true, &ext_set));
    out->set_allocated_null(type.release());
  }

  return std::move(out);
}

Result<std::unique_ptr<substrait::Expression>> ToProto(const compute::Expression& expr) {
  if (!expr.IsBound()) {
    return Status::Invalid("ToProto requires a bound Expression");
  }

  auto out = internal::make_unique<substrait::Expression>();

  if (auto datum = expr.literal()) {
    ARROW_ASSIGN_OR_RAISE(auto literal, ToProto(*datum));
    out->set_allocated_literal(literal.release());
    return std::move(out);
  }

  if (auto param = expr.parameter()) {
    // Special case of a nested StructField
    DCHECK(!param->indices.empty());

    for (auto it = param->indices.begin(); it != param->indices.end(); ++it) {
      auto struct_field =
          internal::make_unique<substrait::Expression::ReferenceSegment::StructField>();
      struct_field->set_field(*it);

      auto ref_segment = internal::make_unique<substrait::Expression::ReferenceSegment>();
      ref_segment->set_allocated_struct_field(struct_field.release());

      auto selection = internal::make_unique<substrait::Expression::FieldReference>();
      selection->set_allocated_direct_reference(ref_segment.release());

      if (out->has_selection()) {
        selection->set_allocated_expression(out.release());
        out = internal::make_unique<substrait::Expression>();
      } else {
        selection->set_allocated_root_reference(
            new substrait::Expression::FieldReference::RootReference());
      }

      out->set_allocated_selection(selection.release());
    }

    return std::move(out);
  }

  auto call = CallNotNull(expr);

  // convert all arguments first
  std::vector<std::unique_ptr<substrait::Expression>> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(arguments[i], ToProto(call->arguments[i]));
  }

  if (call->function_name == "list_element") {
    // catch the special case of calls convertible to a ListElement
    if (arguments[0]->has_selection() &&
        arguments[0]->selection().has_direct_reference()) {
      if (arguments[1]->has_literal() && arguments[1]->literal().has_i32()) {
        auto list_element =
            internal::make_unique<substrait::Expression::ReferenceSegment::ListElement>();

        list_element->set_offset(arguments[1]->literal().i32());

        auto ref_segment =
            internal::make_unique<substrait::Expression::ReferenceSegment>();
        ref_segment->set_allocated_list_element(list_element.release());

        auto field_ref = internal::make_unique<substrait::Expression::FieldReference>();
        field_ref->set_allocated_direct_reference(ref_segment.release());
        field_ref->set_allocated_expression(arguments[0].release());

        out->set_allocated_selection(field_ref.release());
        return std::move(out);
      }
    }
  }

  if (call->function_name == "if_else") {
    // catch the special case of calls convertible to IfThen
    auto if_clause = internal::make_unique<substrait::Expression::IfThen::IfClause>();
    if_clause->set_allocated_if_(arguments[0].release());
    if_clause->set_allocated_then(arguments[1].release());

    auto if_then = internal::make_unique<substrait::Expression::IfThen>();
    if_then->mutable_ifs()->AddAllocated(if_clause.release());
    if_then->set_allocated_else_(arguments[2].release());

    out->set_allocated_if_then(if_then.release());
    return std::move(out);
  }

  /*
  if (call->function_name == "case_when") {
    auto conditions = call->arguments[0].call();
    if (conditions && conditions->function_name == "make_struct") {
      // catch the special case of calls convertible to SwitchExpression
      auto switch_ = internal::make_unique<substrait::Expression::SwitchExpression>();

      for (auto& cond : conditions->arguments) {
        auto if_value =
            internal::make_unique<substrait::Expression::SwitchExpression::IfValue>();
        if_value->set_allocated_if_(arguments[0].release());
        if_value->set_allocated_then(arguments[1].release());

        switch_->mutable_ifs()->AddAllocated(if_value.release());
      }

      switch_->set_allocated_else_(arguments[2].release());

      auto out = std::move(arguments[0]);  // reuse an emptied substrait::Expression
      out->set_allocated_switch_expression(switch_.release());
      return std::move(out);
    }
  }
  // other expression types dive into extensions immediately
  ExtensionSet* ext_set = nullptr;
  ARROW_ASSIGN_OR_RAISE(auto anchor, ext_set->EncodeFunction({"", call->function_name}));

  auto scalar_fn = internal::make_unique<substrait::Expression::ScalarFunction>();
  scalar_fn->set_function_reference(anchor);
  scalar_fn->mutable_args()->Reserve(arguments.size());
  for (auto& arg : arguments) {
    scalar_fn->mutable_args()->AddAllocated(arg.release());
  }

  out->set_allocated_scalar_function(scalar_fn.release());
  return std::move(out);
  */
  return Status::NotImplemented("");
}

}  // namespace engine
}  // namespace arrow
