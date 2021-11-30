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

#include "arrow/engine/substrait/type_internal.h"

#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace engine {
namespace {

template <typename TypeMessage>
Status CheckVariation(const TypeMessage& type) {
  if (type.type_variation_reference() != 0) return Status::OK();
  return Status::NotImplemented("Type variations for ", type.DebugString());
}

template <typename TypeMessage>
bool IsNullable(const TypeMessage& type) {
  return type.nullability() == st::Type_Nullability_NULLABLE;
}

template <typename ArrowType, typename TypeMessage, typename... A>
Result<std::pair<std::shared_ptr<DataType>, bool>> FromProtoImpl(const TypeMessage& type,
                                                                 A&&... args) {
  RETURN_NOT_OK(CheckVariation(type));

  return std::make_pair(std::static_pointer_cast<DataType>(
                            std::make_shared<ArrowType>(std::forward<A>(args)...)),
                        IsNullable(type));
}

template <typename TypeMessage, typename... A>
Result<std::pair<std::shared_ptr<DataType>, bool>> FromProtoImpl(
    const TypeMessage& type, std::shared_ptr<DataType> type_factory(A...), A&&... args) {
  RETURN_NOT_OK(CheckVariation(type));

  return std::make_pair(
      std::static_pointer_cast<DataType>(type_factory(std::forward<A>(args)...)),
      IsNullable(type));
}

template <typename Types, typename Names = const std::string*>
Result<FieldVector> FieldsFromProto(
    int size, const Types& types,
    const Names* names = static_cast<const Names*>(nullptr)) {
  FieldVector fields(size);
  for (int i = 0; i < size; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(types[i]));

    std::string name = names ? std::move((*names)[i]) : "";
    fields[i] =
        field(std::move(name), std::move(type_nullable.first), type_nullable.second);
  }
  return fields;
}

}  // namespace

Result<std::pair<std::shared_ptr<DataType>, bool>> FromProto(const st::Type& type) {
  switch (type.kind_case()) {
    case st::Type::kBool:
      return FromProtoImpl<BooleanType>(type.bool_());

    case st::Type::kI8:
      return FromProtoImpl<Int8Type>(type.i8());
    case st::Type::kI16:
      return FromProtoImpl<Int16Type>(type.i16());
    case st::Type::kI32:
      return FromProtoImpl<Int32Type>(type.i32());
    case st::Type::kI64:
      return FromProtoImpl<Int64Type>(type.i64());

    case st::Type::kFp32:
      return FromProtoImpl<FloatType>(type.fp32());
    case st::Type::kFp64:
      return FromProtoImpl<DoubleType>(type.fp64());

    case st::Type::kString:
      return FromProtoImpl<StringType>(type.string());
    case st::Type::kBinary:
      return FromProtoImpl<BinaryType>(type.binary());

    case st::Type::kTimestamp:
      return FromProtoImpl<TimestampType>(type.timestamp(), TimeUnit::MICRO);
    case st::Type::kTimestampTz:
      return FromProtoImpl<TimestampType>(type.timestamp_tz(), TimeUnit::MICRO,
                                          TimestampTzTimezoneString());
    case st::Type::kDate:
      // FIXME
      // Substrait uses uint32_t to store dates, and further restricts the allowed
      // range of dates to [1000-01-01..9999-12-31]. Does this mean the value should
      // be interpreted as an offset from 1000-01-01 instead of the epoch? Or should
      // the value be signed instead?
      // Furthermore, simple_logical_types.md states that the equivalent arrow type
      // is Date64 (which measures milliseconds rather than days). Is that incorrect?
      return FromProtoImpl<Date32Type>(type.date());

    case st::Type::kTime:
      return FromProtoImpl<Time64Type>(type.time(), TimeUnit::MICRO);

    case st::Type::kIntervalYear:
      // FIXME
      // None of MonthIntervalType, DayTimeIntervalType, MonthDayNanoIntervalType
      // corresponds; none has a year field. Lossy conversion to MonthIntervalType
      // would be possible...
      break;

    case st::Type::kIntervalDay:
      // FIXME
      // Documentation is inconsistent; the precision of the sub-day interval is
      // described as microsecond in simple_logical_types.md but IntervalDayToSecond has
      // the field `int32 seconds`. At microsecond precision it's minimally necessary to
      // store all values in the range `[0,24*60*60*1000_000)` in order to express all
      // possible sub-day intervals, but this is not possible for 32 bit integers.
      //
      // Possible fixes: amend that field to `int64 milliseconds`, then this type can be
      //                 converted to MonthDayNanoIntervalType (months will always be
      //                 0).
      //               : amend documentation to claim only second precision, then this
      //                 type can be converted to DayTimeIntervalType (milliseconds %
      //                 1000 will always be 0).
      break;

    case st::Type::kUuid:
      return FromProtoImpl(type.uuid(), uuid);

    case st::Type::kFixedChar:
      // need extension type to mark utf-8 constraint
      return FromProtoImpl(type.fixed_char(), fixed_char, type.fixed_char().length());

    case st::Type::kVarchar:
      // need extension type to hold type.varchar().length() constraint
      return FromProtoImpl(type.varchar(), varchar, type.varchar().length());

    case st::Type::kFixedBinary:
      return FromProtoImpl<FixedSizeBinaryType>(type.fixed_binary(),
                                                type.fixed_binary().length());

    case st::Type::kDecimal: {
      const auto& decimal = type.decimal();
      return FromProtoImpl<Decimal128Type>(decimal, decimal.precision(), decimal.scale());
    }

    case st::Type::kStruct: {
      const auto& struct_ = type.struct_();

      ARROW_ASSIGN_OR_RAISE(auto fields,
                            FieldsFromProto(struct_.types_size(), struct_.types()));

      return FromProtoImpl<StructType>(struct_, std::move(fields));
    }

    case st::Type::kList: {
      const auto& list = type.list();

      if (!list.has_type()) {
        return Status::Invalid(
            "While converting to ListType encountered a missing item type in ",
            list.DebugString());
      }

      ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(list.type()));
      return FromProtoImpl<ListType>(
          list, field("item", std::move(type_nullable.first), type_nullable.second));
    }

    case st::Type::kMap: {
      const auto& map = type.map();

      static const std::array<char const*, 4> kMissing = {"key and value", "value", "key",
                                                          nullptr};
      if (auto missing = kMissing[map.has_key() + map.has_value() * 2]) {
        return Status::Invalid("While converting to MapType encountered missing ",
                               missing, " type in ", map.DebugString());
      }

      ARROW_ASSIGN_OR_RAISE(auto key_nullable, FromProto(map.key()));
      ARROW_ASSIGN_OR_RAISE(auto value_nullable, FromProto(map.value()));

      if (key_nullable.second) {
        return Status::Invalid(
            "While converting to MapType encountered nullable key field in ",
            map.DebugString());
      }

      return FromProtoImpl<MapType>(
          map, std::move(key_nullable.first),
          field("value", std::move(value_nullable.first), value_nullable.second));
    }

    default:
      break;
  }

  return Status::NotImplemented("conversion to arrow::DataType from ",
                                type.DebugString());
}

Result<std::shared_ptr<Schema>> FromProto(const st::NamedStruct& named_struct) {
  if (named_struct.has_struct_()) {
    return Status::Invalid("While converting ", named_struct.DebugString(),
                           " no anonymous struct type was provided to which to names "
                           "could be attached.");
  }
  const auto& struct_ = named_struct.struct_();
  RETURN_NOT_OK(CheckVariation(struct_));

  if (struct_.types_size() != named_struct.names_size()) {
    return Status::Invalid("While converting ", named_struct.DebugString(), " received ",
                           struct_.types_size(), " types but ", named_struct.names_size(),
                           " names.");
  }

  ARROW_ASSIGN_OR_RAISE(
      auto fields,
      FieldsFromProto(struct_.types_size(), struct_.types(), &named_struct.names()));
  return schema(std::move(fields));
}

namespace {
struct ToProtoImpl {
  Status Visit(const NullType& t) { return NotImplemented(t); }

  Status Visit(const BooleanType& t) { return SetWith(&st::Type::set_allocated_bool_); }

  Status Visit(const Int8Type& t) { return SetWith(&st::Type::set_allocated_i8); }
  Status Visit(const Int16Type& t) { return SetWith(&st::Type::set_allocated_i16); }
  Status Visit(const Int32Type& t) { return SetWith(&st::Type::set_allocated_i32); }
  Status Visit(const Int64Type& t) { return SetWith(&st::Type::set_allocated_i64); }

  Status Visit(const UInt8Type& t) { return NotImplemented(t); }
  Status Visit(const UInt16Type& t) { return NotImplemented(t); }
  Status Visit(const UInt32Type& t) { return NotImplemented(t); }
  Status Visit(const UInt64Type& t) { return NotImplemented(t); }

  Status Visit(const HalfFloatType& t) { return NotImplemented(t); }
  Status Visit(const FloatType& t) { return SetWith(&st::Type::set_allocated_fp32); }
  Status Visit(const DoubleType& t) { return SetWith(&st::Type::set_allocated_fp64); }

  Status Visit(const StringType& t) { return SetWith(&st::Type::set_allocated_string); }
  Status Visit(const BinaryType& t) { return SetWith(&st::Type::set_allocated_binary); }

  Status Visit(const FixedSizeBinaryType& t) {
    SetWithThen(&st::Type::set_allocated_fixed_binary)->set_length(t.byte_width());
    return Status::OK();
  }

  Status Visit(const Date32Type& t) { return SetWith(&st::Type::set_allocated_date); }
  Status Visit(const Date64Type& t) { return NotImplemented(t); }

  Status Visit(const TimestampType& t) {
    if (t.unit() != TimeUnit::MICRO) return NotImplemented(t);

    if (t.timezone() == "") {
      return SetWith(&st::Type::set_allocated_timestamp);
    }
    if (t.timezone() == TimestampTzTimezoneString()) {
      return SetWith(&st::Type::set_allocated_timestamp_tz);
    }

    return NotImplemented(t);
  }

  Status Visit(const Time32Type& t) { return NotImplemented(t); }
  Status Visit(const Time64Type& t) {
    if (t.unit() != TimeUnit::MICRO) return NotImplemented(t);
    return SetWith(&st::Type::set_allocated_time);
  }

  Status Visit(const MonthIntervalType& t) { return NotImplemented(t); }
  Status Visit(const DayTimeIntervalType& t) { return NotImplemented(t); }

  Status Visit(const Decimal128Type& t) {
    auto dec = SetWithThen(&st::Type::set_allocated_decimal);
    dec->set_precision(t.precision());
    dec->set_scale(t.scale());
    return Status::OK();
  }
  Status Visit(const Decimal256Type& t) { return NotImplemented(t); }

  Status Visit(const ListType& t) {
    ARROW_ASSIGN_OR_RAISE(auto type,
                          ToProto(*t.value_type(), t.value_field()->nullable()));
    SetWithThen(&st::Type::set_allocated_list)->set_allocated_type(type.release());
    return Status::OK();
  }

  Status Visit(const StructType& t) {
    auto types = SetWithThen(&st::Type::set_allocated_struct_)->mutable_types();

    types->Reserve(t.num_fields());

    for (const auto& field : t.fields()) {
      if (field->name() != "") {
        return Status::Invalid("substrait::Type::Struct does not support named fields");
      }
      if (field->metadata() != nullptr) {
        return Status::Invalid("substrait::Type::Struct does not support field metadata");
      }
      ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*field->type(), field->nullable()));
      types->AddAllocated(type.release());
    }
    return Status::OK();
  }

  Status Visit(const SparseUnionType& t) { return NotImplemented(t); }
  Status Visit(const DenseUnionType& t) { return NotImplemented(t); }
  Status Visit(const DictionaryType& t) { return NotImplemented(t); }

  Status Visit(const MapType& t) {
    auto map = SetWithThen(&st::Type::set_allocated_map);

    ARROW_ASSIGN_OR_RAISE(auto key, ToProto(*t.key_type()));
    map->set_allocated_key(key.release());

    ARROW_ASSIGN_OR_RAISE(auto value,
                          ToProto(*t.value_type(), t.value_field()->nullable()));
    map->set_allocated_value(value.release());

    return Status::OK();
  }

  Status Visit(const ExtensionType& t) {
    if (UnwrapUuid(t)) {
      return SetWith(&st::Type::set_allocated_uuid);
    }

    if (auto length = UnwrapFixedChar(t)) {
      SetWithThen(&st::Type::set_allocated_fixed_char)->set_length(*length);
      return Status::OK();
    }

    if (auto length = UnwrapVarChar(t)) {
      SetWithThen(&st::Type::set_allocated_varchar)->set_length(*length);
      return Status::OK();
    }

    return NotImplemented(t);
  }

  Status Visit(const FixedSizeListType& t) { return NotImplemented(t); }
  Status Visit(const DurationType& t) { return NotImplemented(t); }
  Status Visit(const LargeStringType& t) { return NotImplemented(t); }
  Status Visit(const LargeBinaryType& t) { return NotImplemented(t); }
  Status Visit(const LargeListType& t) { return NotImplemented(t); }
  Status Visit(const MonthDayNanoIntervalType& t) { return NotImplemented(t); }

  template <typename Sub>
  Sub* SetWithThen(void (st::Type::*set_allocated_sub)(Sub*)) {
    auto sub = internal::make_unique<Sub>();
    sub->set_nullability(nullable_ ? st::Type_Nullability_NULLABLE
                                   : st::Type_Nullability_REQUIRED);

    auto out = sub.get();
    (type_->*set_allocated_sub)(sub.release());
    return out;
  }

  template <typename Sub>
  Status SetWith(void (st::Type::*set_allocated_sub)(Sub*)) {
    return SetWithThen(set_allocated_sub), Status::OK();
  }

  Status NotImplemented(const DataType& t) {
    return Status::NotImplemented("conversion to substrait::Type from ", t.ToString());
  }

  Status operator()(const DataType& type) { return VisitTypeInline(type, this); }

  st::Type* type_;
  bool nullable_;
};
}  // namespace

Result<std::unique_ptr<st::Type>> ToProto(const DataType& type, bool nullable) {
  auto out = internal::make_unique<st::Type>();
  RETURN_NOT_OK((ToProtoImpl{out.get(), nullable})(type));
  return std::move(out);
}

Result<std::unique_ptr<st::NamedStruct>> ToProto(const Schema& schema) {
  auto named_struct = internal::make_unique<st::NamedStruct>();
  auto names = named_struct->mutable_names();
  names->Reserve(schema.num_fields());

  auto struct_ = internal::make_unique<st::Type::Struct>();
  auto types = struct_->mutable_types();
  types->Reserve(schema.num_fields());

  for (const auto& field : schema.fields()) {
    *names->Add() = field->name();

    if (field->metadata() != nullptr) {
      return Status::Invalid("substrait::NamedStruct does not support field metadata");
    }

    ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*field->type(), field->nullable()));
    types->AddAllocated(type.release());
  }

  named_struct->set_allocated_struct_(struct_.release());
  return std::move(named_struct);
}

}  // namespace engine
}  // namespace arrow
