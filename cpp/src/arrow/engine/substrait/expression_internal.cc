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
#include "generated/substrait/expression.pb.h"  // IWYU pragma: export

namespace st = io::substrait;

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

Result<compute::Expression> FromProto(const st::Expression& expr) {
  switch (expr.rex_type_case()) {
    case st::Expression::kLiteral: {
      ARROW_ASSIGN_OR_RAISE(auto datum, FromProto(expr.literal()));
      return compute::literal(std::move(datum));
    }

    case st::Expression::kSelection: {
      if (!expr.selection().has_direct_reference()) break;

      util::optional<compute::Expression> root_expr;
      if (expr.selection().has_expression()) {
        ARROW_ASSIGN_OR_RAISE(*root_expr, FromProto(expr.selection().expression()));
      }

      const auto& ref = expr.selection().direct_reference();
      switch (ref.reference_type_case()) {
        case st::Expression::ReferenceSegment::kStructField: {
          if (ref.struct_field().has_child()) break;

          FieldRef out(ref.struct_field().field());

          if (root_expr) {
            if (auto root_ref = root_expr->field_ref()) {
              out = FieldRef(std::move(out), *root_ref);
            } else {
              // FIXME add field_ref compute function to handle
              // field references into expressions
              break;
            }
          }
          return compute::field_ref(std::move(out));
        }

        case st::Expression::ReferenceSegment::kListElement: {
          if (ref.list_element().has_child()) break;
          if (!root_expr) {
            return Status::Invalid(
                "substrait::ListElement cannot take a Relation as an argument");
          }

          return compute::call(
              "list_element",
              {std::move(*root_expr), compute::literal(ref.list_element().offset())});
          break;
        }

        default:
          break;
      }
      break;
    }

    case st::Expression::kIfThen: {
      const auto& if_then = expr.if_then();
      if (!if_then.has_else_()) break;
      if (if_then.ifs_size() != 1) break;
      ARROW_ASSIGN_OR_RAISE(auto if_, FromProto(if_then.ifs(0).if_()));
      ARROW_ASSIGN_OR_RAISE(auto then, FromProto(if_then.ifs(0).then()));
      ARROW_ASSIGN_OR_RAISE(auto else_, FromProto(if_then.else_()));
      return compute::call("if_else",
                           {std::move(if_), std::move(then), std::move(else_)});
    }

    default:
      break;
  }

  return Status::NotImplemented("conversion to arrow::compute::Expression from ",
                                expr.DebugString());
}

Result<Datum> FromProto(const st::Expression::Literal& lit) {
  switch (lit.literal_type_case()) {
    case st::Expression::Literal::kBoolean:
      return Datum(lit.boolean());

    case st::Expression::Literal::kI8:
      return Datum(static_cast<int8_t>(lit.i8()));
    case st::Expression::Literal::kI16:
      return Datum(static_cast<int16_t>(lit.i16()));
    case st::Expression::Literal::kI32:
      return Datum(static_cast<int32_t>(lit.i32()));
    case st::Expression::Literal::kI64:
      return Datum(static_cast<int64_t>(lit.i64()));

    case st::Expression::Literal::kFp32:
      return Datum(lit.fp32());
    case st::Expression::Literal::kFp64:
      return Datum(lit.fp64());

    case st::Expression::Literal::kString:
      return Datum(lit.string());
    case st::Expression::Literal::kBinary:
      return Datum(std::make_shared<BinaryScalar>(Buffer::FromString(lit.binary())));

    case st::Expression::Literal::kTimestamp:
      return Datum(std::make_shared<TimestampScalar>(
          static_cast<int64_t>(lit.timestamp()), TimeUnit::MICRO));

    case st::Expression::Literal::kTimestampTz:
      return Datum(std::make_shared<TimestampScalar>(
          static_cast<int64_t>(lit.timestamp_tz()), TimeUnit::MICRO,
          TimestampTzTimezoneString()));

    case st::Expression::Literal::kDate:
      return Datum(std::make_shared<Date32Scalar>(static_cast<int32_t>(lit.date())));
    case st::Expression::Literal::kTime:
      return Datum(std::make_shared<Time64Scalar>(static_cast<int64_t>(lit.time()),
                                                  TimeUnit::MICRO));

    case st::Expression::Literal::kIntervalYearToMonth:
    case st::Expression::Literal::kIntervalDayToSecond:
      break;

    case st::Expression::Literal::kUuid:
      return Datum(std::make_shared<ExtensionScalar>(
          FixedSizeBinaryScalarFromBytes(lit.uuid()), uuid()));

    case st::Expression::Literal::kFixedChar:
      return Datum(std::make_shared<ExtensionScalar>(
          FixedSizeBinaryScalarFromBytes(lit.fixed_char()),
          fixed_char(static_cast<int32_t>(lit.fixed_char().size()))));

    case st::Expression::Literal::kVarChar:
      return Datum(std::make_shared<ExtensionScalar>(
          std::make_shared<StringScalar>(lit.var_char().value()),
          varchar(static_cast<int32_t>(lit.var_char().length()))));

    case st::Expression::Literal::kFixedBinary:
      return Datum(FixedSizeBinaryScalarFromBytes(lit.fixed_char()));

    case st::Expression::Literal::kDecimal: {
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
      return Datum(std::make_shared<Decimal128Scalar>(value, std::move(type)));
    }

    case st::Expression::Literal::kStruct: {
      const auto& struct_ = lit.struct_();

      ScalarVector fields(struct_.fields_size());
      std::vector<std::string> field_names(fields.size(), "");
      for (size_t i = 0; i < fields.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto field, FromProto(struct_.fields(i)));
        DCHECK(field.is_scalar());
        fields.push_back(field.scalar());
      }
      ARROW_ASSIGN_OR_RAISE(
          auto scalar, StructScalar::Make(std::move(fields), std::move(field_names)));
      return Datum(std::move(scalar));
    }

    case st::Expression::Literal::kList: {
      const auto& list = lit.list();
      if (list.values_size() == 0 && !list.has_element_type()) {
        return Status::Invalid(
            "substrait::Expression::Literal::List had no values and no element_type");
      }

      std::shared_ptr<DataType> element_type;
      if (list.has_element_type()) {
        ARROW_ASSIGN_OR_RAISE(std::tie(element_type, std::ignore),
                              FromProto(list.element_type()));
      }

      ScalarVector values(list.values_size());
      for (size_t i = 0; i < values.size(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto value, FromProto(list.values(i)));
        DCHECK(value.is_scalar());
        values.push_back(value.scalar());
        if (element_type) {
          if (!value.type()->Equals(*element_type)) {
            return Status::Invalid(list.DebugString(),
                                   " has a value whose type doesn't match element_type");
          }
        } else {
          element_type = value.type();
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(element_type));
      RETURN_NOT_OK(builder->AppendScalars(values));
      ARROW_ASSIGN_OR_RAISE(auto arr, builder->Finish());
      return Datum(std::make_shared<ListScalar>(std::move(arr)));
    }

    case st::Expression::Literal::kMap: {
      const auto& map = lit.map();
      if (map.key_values_size() == 0 && !(map.has_key_type() && map.has_value_type())) {
        return Status::Invalid(
            "substrait::Expression::Literal::Map had no key_values and no key_type or "
            "values_type");
      }

      std::shared_ptr<DataType> key_type, value_type;
      if (map.has_key_type()) {
        ARROW_ASSIGN_OR_RAISE(std::tie(key_type, std::ignore), FromProto(map.key_type()));
      }
      if (map.has_value_type()) {
        ARROW_ASSIGN_OR_RAISE(std::tie(value_type, std::ignore),
                              FromProto(map.value_type()));
      }

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

        keys.push_back(key.scalar());
        values.push_back(value.scalar());

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

    case st::Expression::Literal::kNull: {
      ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(lit.null()));
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

  using Lit = st::Expression::Literal;

  template <typename Arg, typename PrimitiveScalar>
  Status Primitive(void (st::Expression::Literal::*set)(Arg),
                   const PrimitiveScalar& primitive_scalar) {
    (type_->*set)(static_cast<Arg>(primitive_scalar.value));
    return Status::OK();
  }

  template <typename ScalarWithBufferValue>
  Status FromBuffer(void (st::Expression::Literal::*set)(std::string&&),
                    const ScalarWithBufferValue& scalar_with_buffer) {
    (type_->*set)(util::string_view{*scalar_with_buffer.value}.to_string());
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

  Status Visit(const Date32Scalar& s) { return Primitive(&Lit::set_date, s); }
  Status Visit(const Date64Scalar& s) { return NotImplemented(s); }

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

  Status Visit(const Decimal128Scalar& s) { return NotImplemented(s); }
  Status Visit(const Decimal256Scalar& s) { return NotImplemented(s); }

  Status Visit(const ListScalar& s) {
    type_->set_allocated_list(new Lit::List());

    auto values = type_->mutable_list()->mutable_values();
    values->Reserve(static_cast<int>(s.value->length()));

    for (int64_t i = 0; i < s.value->length(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, s.value->GetScalar(i));
      ARROW_ASSIGN_OR_RAISE(auto lit, ToProto(Datum(std::move(scalar))));
      values->AddAllocated(lit.release());
    }
    return Status::OK();
  }

  Status Visit(const StructScalar& s) {
    type_->set_allocated_struct_(new Lit::Struct());

    auto fields = type_->mutable_struct_()->mutable_fields();
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
    type_->set_allocated_map(new Lit::Map());

    const auto& kv_arr = internal::checked_cast<const StructArray&>(*s.value);

    auto key_values = type_->mutable_map()->mutable_key_values();
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
      return FromBuffer(&Lit::set_uuid,
                        internal::checked_cast<const FixedSizeBinaryScalar&>(*s.value));
    }

    if (UnwrapVarChar(*s.type)) {
      return FromBuffer(&Lit::set_uuid,
                        internal::checked_cast<const StringScalar&>(*s.value));
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

  st::Expression::Literal* type_;
};
}  // namespace

Result<std::unique_ptr<st::Expression::Literal>> ToProto(const Datum& datum) {
  if (!datum.is_scalar()) {
    return Status::NotImplemented("representing ", datum.ToString(),
                                  " as a substrait::Expression::Literal");
  }

  auto out = internal::make_unique<st::Expression::Literal>();

  if (datum.scalar()->is_valid) {
    RETURN_NOT_OK((ToProtoImpl{out.get()})(*datum.scalar()));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*datum.type()));
    out->set_allocated_null(type.release());
  }

  return std::move(out);
}

Result<std::unique_ptr<st::Expression>> ToProto(const compute::Expression& expr) {
  if (!expr.IsBound()) {
    return Status::Invalid("ToProto requires a bound Expression");
  }

  if (auto datum = expr.literal()) {
    ARROW_ASSIGN_OR_RAISE(auto literal, ToProto(*datum));
    auto out = internal::make_unique<st::Expression>();
    out->set_allocated_literal(literal.release());
    return std::move(out);
  }

  if (auto param = expr.parameter()) {
    /*
        DCHECK(!param->indices.empty());
        for (int i : param->indices) {
        }

        std::unique_ptr<st::ReferenceSegment> ref_segment;

        for (auto it = param->indices.rbegin(); it != param->indices.rend(); ++it) {
          auto struct_field = internal::make_unique<st::ReferenceSegment::StructField>();

          if (ref_segment) {
            struct_field->set_allocated_child(ref_segment.release());
          }
          struct_field->set_field(*it);

          ref_segment = internal::make_unique<st::ReferenceSegment>();
          ref_segment->set_allocated_struct_field(struct_field.release());
        }

        auto field_ref = internal::make_unique<st::FieldReference>();
        field_ref->set_allocated_direct_reference(ref_segment.release());

        auto out = internal::make_unique<st::Expression>();
        out->set_allocated_selection(field_ref.release());
        return std::move(out);
    */
  }

  auto call = CallNotNull(expr);

  // convert all arguments first
  std::vector<std::unique_ptr<st::Expression>> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(arguments[i], ToProto(call->arguments[i]));
  }

  if (call->function_name == "list_element") {
    /*
        // catch the special case of calls convertible to a ListElement
        if (arguments[0]->has_selection() &&
            arguments[0]->selection().has_direct_reference()) {
          if (arguments[1]->has_literal() && arguments[1]->literal().has_i32()) {
            auto list_element =
       internal::make_unique<st::ReferenceSegment::ListElement>();

            list_element->set_allocated_child(
                arguments[0]->mutable_selection()->release_direct_reference());
            list_element->set_offset(arguments[1]->literal().i32());

            auto ref_segment = internal::make_unique<st::ReferenceSegment>();
            ref_segment->set_allocated_list_element(list_element.release());

            auto field_ref = internal::make_unique<st::FieldReference>();
            field_ref->set_allocated_direct_reference(ref_segment.release());

            auto out = std::move(arguments[0]);  // reuse an emptied st::Expression
            out->set_allocated_selection(field_ref.release());
            return std::move(out);
          }
        }
    */
  }

  if (call->function_name == "if_else") {
    // catch the special case of calls convertible to IfThen
    auto if_clause = internal::make_unique<st::Expression::IfThen::IfClause>();
    if_clause->set_allocated_if_(arguments[0].release());
    if_clause->set_allocated_then(arguments[1].release());

    auto if_then = internal::make_unique<st::Expression::IfThen>();
    if_then->mutable_ifs()->AddAllocated(if_clause.release());
    if_then->set_allocated_else_(arguments[2].release());

    auto out = internal::make_unique<st::Expression>();
    out->set_allocated_if_then(if_then.release());
    return std::move(out);
  }

  // other expression types dive into extensions immediately
  /*
  if (call->function_name == "case_when") {
    auto conditions = call->arguments[0].call();
    if (conditions && conditions->function_name == "make_struct") {
      // catch the special case of calls convertible to SwitchExpression
      auto switch_ = internal::make_unique<st::Expression::SwitchExpression>();

      for (auto& cond : conditions->arguments) {
        auto if_value =
            internal::make_unique<st::Expression::SwitchExpression::IfValue>();
        if_value->set_allocated_if_(arguments[0].release());
        if_value->set_allocated_then(arguments[1].release());

        switch_->mutable_ifs()->AddAllocated(if_value.release());
      }

      switch_->set_allocated_else_(arguments[2].release());

      auto out = std::move(arguments[0]);  // reuse an emptied st::Expression
      out->set_allocated_switch_expression(switch_.release());
      return std::move(out);
    }
  }
  */

  return Status::NotImplemented("conversion to substrait::Expression from ",
                                expr.ToString());
}

}  // namespace engine
}  // namespace arrow
