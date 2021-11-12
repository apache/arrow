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

#include "arrow/engine/protocol_internal.h"

#include <vector>

#include "arrow/engine/simple_extension_type_internal.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

#include "arrow/util/make_unique.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/message_lite.h"

namespace arrow {
namespace engine {

Status ParseFromBufferImpl(const Buffer& buf, const std::string& full_name,
                           google::protobuf::Message* message) {
  google::protobuf::io::ArrayInputStream buf_stream{buf.data(),
                                                    static_cast<int>(buf.size())};

  if (message->ParseFromZeroCopyStream(&buf_stream)) {
    return Status::OK();
  }
  return Status::IOError("ParseFromZeroCopyStream failed for ", full_name);
}

namespace {

template <typename TypeMessage>
Status CheckVariation(const TypeMessage& type) {
  if (!type.has_variation()) return Status::OK();
  return Status::NotImplemented("Type.Variation for ", type.DebugString());
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

template <typename ExtType, typename TypeMessage>
static Result<std::pair<std::shared_ptr<DataType>, bool>>
SimpleExtensionTypeFromProtoImpl(const TypeMessage& type,
                                 typename ExtType::ParamsType params) {
  RETURN_NOT_OK(CheckVariation(type));
  ARROW_ASSIGN_OR_RAISE(auto ext_type, ExtType::Make(std::move(params)))
  return std::make_pair(std::move(ext_type), IsNullable(type));
}

constexpr util::string_view kUuidExtensionName = "uuid";
struct UuidExtensionParams {};
Result<std::shared_ptr<DataType>> UuidGetStorage(const UuidExtensionParams&) {
  return fixed_size_binary(16);
}
static auto kUuidExtensionParamsProperties = internal::MakeProperties();

using UuidType = SimpleExtensionType<kUuidExtensionName, UuidExtensionParams,
                                     decltype(kUuidExtensionParamsProperties),
                                     kUuidExtensionParamsProperties, UuidGetStorage>;

constexpr util::string_view kFixedCharExtensionName = "fixed_char";
struct FixedCharExtensionParams {
  int32_t length;
};
Result<std::shared_ptr<DataType>> FixedCharGetStorage(
    const FixedCharExtensionParams& params) {
  return fixed_size_binary(params.length);
}
static auto kFixedCharExtensionParamsProperties = internal::MakeProperties(
    internal::DataMember("length", &FixedCharExtensionParams::length));

using FixedCharType =
    SimpleExtensionType<kFixedCharExtensionName, FixedCharExtensionParams,
                        decltype(kFixedCharExtensionParamsProperties),
                        kFixedCharExtensionParamsProperties, FixedCharGetStorage>;

constexpr util::string_view kVarCharExtensionName = "varchar";
struct VarCharExtensionParams {
  int32_t length;
};
Result<std::shared_ptr<DataType>> VarCharGetStorage(const VarCharExtensionParams&) {
  return utf8();
}
static auto kVarCharExtensionParamsProperties = internal::MakeProperties(
    internal::DataMember("length", &VarCharExtensionParams::length));

using VarCharType =
    SimpleExtensionType<kVarCharExtensionName, VarCharExtensionParams,
                        decltype(kVarCharExtensionParamsProperties),
                        kVarCharExtensionParamsProperties, VarCharGetStorage>;

}  // namespace

Result<std::pair<std::shared_ptr<DataType>, bool>> FromProto(const st::Type& type) {
  switch (type.kind_case()) {
    case st::Type::KindCase::kBool:
      return FromProtoImpl<BooleanType>(type.bool_());

    case st::Type::KindCase::kI8:
      return FromProtoImpl<Int8Type>(type.i8());
    case st::Type::KindCase::kI16:
      return FromProtoImpl<Int16Type>(type.i16());
    case st::Type::KindCase::kI32:
      return FromProtoImpl<Int32Type>(type.i32());
    case st::Type::KindCase::kI64:
      return FromProtoImpl<Int64Type>(type.i64());

    case st::Type::KindCase::kFp32:
      return FromProtoImpl<FloatType>(type.fp32());
    case st::Type::KindCase::kFp64:
      return FromProtoImpl<DoubleType>(type.fp64());

    case st::Type::KindCase::kString:
      return FromProtoImpl<StringType>(type.string());
    case st::Type::KindCase::kBinary:
      return FromProtoImpl<BinaryType>(type.binary());

    case st::Type::KindCase::kTimestamp:
      return FromProtoImpl<TimestampType>(type.timestamp(), TimeUnit::MICRO);
    case st::Type::KindCase::kTimestampTz:
      return FromProtoImpl<TimestampType>(type.timestamp_tz(), TimeUnit::MICRO, "UTC");
    case st::Type::KindCase::kDate:
      return FromProtoImpl<Date64Type>(type.date());
    case st::Type::KindCase::kTime:
      return FromProtoImpl<Time64Type>(type.time(), TimeUnit::MICRO);

    case st::Type::KindCase::kIntervalYear:
      // None of MonthIntervalType, DayTimeIntervalType, MonthDayNanoIntervalType
      // corresponds; none has a year field. Lossy conversion to MonthIntervalType
      // would be possible...
      break;

    case st::Type::KindCase::kIntervalDay:
      // Documentation is inconsistent; the precision of the sub-day interval is described
      // as microsecond in simple_logical_types.md but IntervalDayToSecond has the field
      // `int32 seconds`. At microsecond precision it's minimally necessary to store all
      // values in the range `[0,24*60*60*1000_000)` in order to express all possible
      // sub-day intervals, but this is not possible for 32 bit integers.
      //
      // Possible fixes: amend that field to `int64 milliseconds`, then this type can be
      //                 converted to MonthDayNanoIntervalType (months will always be 0).
      //               : amend documentation to claim only second precision, then this
      //                 type can be converted to DayTimeIntervalType (milliseconds % 1000
      //                 will always be 0).
      break;

    case st::Type::KindCase::kUuid:
      return SimpleExtensionTypeFromProtoImpl<UuidType>(type.uuid(), {});

    case st::Type::KindCase::kFixedChar:
      // need extension type to mark utf-8 constraint
      return SimpleExtensionTypeFromProtoImpl<FixedCharType>(
          type.fixed_char(), {type.fixed_char().length()});

    case st::Type::KindCase::kVarchar:
      // need extension type to hold type.varchar().length() constraint
      return SimpleExtensionTypeFromProtoImpl<VarCharType>(type.varchar(),
                                                           {type.varchar().length()});

    case st::Type::KindCase::kFixedBinary:
      return FromProtoImpl<FixedSizeBinaryType>(type.fixed_binary(),
                                                type.fixed_binary().length());

    case st::Type::KindCase::kDecimal: {
      const auto& decimal = type.decimal();
      return FromProtoImpl<Decimal128Type>(decimal, decimal.precision(), decimal.scale());
    }

    case st::Type::KindCase::kStruct: {
      const auto& struct_ = type.struct_();

      ARROW_ASSIGN_OR_RAISE(auto fields,
                            FieldsFromProto(struct_.types_size(), struct_.types()));

      return FromProtoImpl<StructType>(struct_, std::move(fields));
    }

      // NamedStruct is not currently enumerated in KindCase. This block of dead code is
      // here just to verify that it can be compiled.
      // case st::Type::KindCase::kNamedStruct: {
      if (false) {
        // const st::Type::NamedStruct& named_struct = type.named_struct();
        st::Type::NamedStruct named_struct;

        if (named_struct.has_struct_()) {
          return Status::Invalid(
              "While converting ", type.DebugString(),
              " no anonymous struct type was provided to which to names "
              "could be attached.");
        }
        const auto& struct_ = named_struct.struct_();
        RETURN_NOT_OK(CheckVariation(struct_));

        if (struct_.types_size() != named_struct.names_size()) {
          return Status::Invalid("While converting ", type.DebugString(), " received ",
                                 struct_.types_size(), " types but ",
                                 named_struct.names_size(), " names.");
        }

        ARROW_ASSIGN_OR_RAISE(auto fields,
                              FieldsFromProto(struct_.types_size(), struct_.types(),
                                              &named_struct.names()));

        return std::make_pair(arrow::struct_(std::move(fields)), IsNullable(struct_));
      }

    case st::Type::KindCase::kList: {
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

    case st::Type::KindCase::kMap: {
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

  Status Visit(const Date32Type& t) { return NotImplemented(t); }
  Status Visit(const Date64Type& t) { return SetWith(&st::Type::set_allocated_date); }

  Status Visit(const TimestampType& t) {
    if (t.unit() != TimeUnit::MICRO) return NotImplemented(t);
    if (t.timezone() == "") return SetWith(&st::Type::set_allocated_timestamp);
    if (t.timezone() == "UTC") return SetWith(&st::Type::set_allocated_timestamp_tz);
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
    auto ext_name = t.extension_name();

    if (auto params = UuidType::GetIf(t)) {
      return SetWith(&st::Type::set_allocated_uuid);
    }

    if (auto params = FixedCharType::GetIf(t)) {
      SetWithThen(&st::Type::set_allocated_fixed_char)->set_length(params->length);
      return Status::OK();
    }

    if (auto params = VarCharType::GetIf(t)) {
      SetWithThen(&st::Type::set_allocated_varchar)->set_length(params->length);
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

Result<std::unique_ptr<st::Type>> ToProto(const DataType& type, bool nullable) {
  auto out = internal::make_unique<st::Type>();
  RETURN_NOT_OK((ToProtoImpl{out.get(), nullable})(type));
  return std::move(out);
}

}  // namespace engine
}  // namespace arrow
