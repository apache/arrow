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

#include <memory>
#include <string>
#include <vector>

#include "arrow/engine/substrait/extension_types.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace engine {

namespace {

template <typename TypeMessage>
bool IsNullable(const TypeMessage& type) {
  // FIXME what can we do with NULLABILITY_UNSPECIFIED
  return type.nullability() != ::substrait::Type::NULLABILITY_REQUIRED;
}

template <typename ArrowType, typename TypeMessage, typename... A>
Result<std::pair<std::shared_ptr<DataType>, bool>> FromProtoImpl(const TypeMessage& type,
                                                                 A&&... args) {
  return std::make_pair(std::static_pointer_cast<DataType>(
                            std::make_shared<ArrowType>(std::forward<A>(args)...)),
                        IsNullable(type));
}

template <typename TypeMessage, typename... A>
Result<std::pair<std::shared_ptr<DataType>, bool>> FromProtoImpl(
    const TypeMessage& type, std::shared_ptr<DataType> type_factory(A...), A&&... args) {
  return std::make_pair(
      std::static_pointer_cast<DataType>(type_factory(std::forward<A>(args)...)),
      IsNullable(type));
}

template <typename Types, typename NextName>
Result<FieldVector> FieldsFromProto(int size, const Types& types,
                                    const NextName& next_name,
                                    const ExtensionSet& ext_set,
                                    const ConversionOptions& conversion_options) {
  FieldVector fields(size);
  for (int i = 0; i < size; ++i) {
    std::string name = next_name();
    std::shared_ptr<DataType> type;
    bool nullable;

    if (types.Get(i).has_struct_()) {
      const auto& struct_ = types.Get(i).struct_();

      ARROW_ASSIGN_OR_RAISE(type, FieldsFromProto(struct_.types_size(), struct_.types(),
                                                  next_name, ext_set, conversion_options)
                                      .Map(arrow::struct_));

      nullable = IsNullable(struct_);
    } else {
      ARROW_ASSIGN_OR_RAISE(std::tie(type, nullable),
                            FromProto(types.Get(i), ext_set, conversion_options));
    }

    fields[i] = field(std::move(name), std::move(type), nullable);
  }
  return fields;
}

}  // namespace

Result<std::pair<std::shared_ptr<DataType>, bool>> FromProto(
    const ::substrait::Type& type, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options) {
  switch (type.kind_case()) {
    case ::substrait::Type::kBool:
      return FromProtoImpl<BooleanType>(type.bool_());

    case ::substrait::Type::kI8:
      return FromProtoImpl<Int8Type>(type.i8());
    case ::substrait::Type::kI16:
      return FromProtoImpl<Int16Type>(type.i16());
    case ::substrait::Type::kI32:
      return FromProtoImpl<Int32Type>(type.i32());
    case ::substrait::Type::kI64:
      return FromProtoImpl<Int64Type>(type.i64());

    case ::substrait::Type::kFp32:
      return FromProtoImpl<FloatType>(type.fp32());
    case ::substrait::Type::kFp64:
      return FromProtoImpl<DoubleType>(type.fp64());

    case ::substrait::Type::kString:
      return FromProtoImpl<StringType>(type.string());
    case ::substrait::Type::kBinary:
      return FromProtoImpl<BinaryType>(type.binary());

    case ::substrait::Type::kTimestamp:
      return FromProtoImpl<TimestampType>(type.timestamp(), TimeUnit::MICRO);
    case ::substrait::Type::kTimestampTz:
      return FromProtoImpl<TimestampType>(type.timestamp_tz(), TimeUnit::MICRO,
                                          TimestampTzTimezoneString());
    case ::substrait::Type::kDate:
      return FromProtoImpl<Date32Type>(type.date());

    case ::substrait::Type::kTime:
      return FromProtoImpl<Time64Type>(type.time(), TimeUnit::MICRO);

    case ::substrait::Type::kIntervalYear:
      return FromProtoImpl(type.interval_year(), interval_year);

    case ::substrait::Type::kIntervalDay:
      return FromProtoImpl(type.interval_day(), interval_day);

    case ::substrait::Type::kUuid:
      return FromProtoImpl(type.uuid(), uuid);

    case ::substrait::Type::kFixedChar:
      return FromProtoImpl(type.fixed_char(), fixed_char, type.fixed_char().length());

    case ::substrait::Type::kVarchar:
      return FromProtoImpl(type.varchar(), varchar, type.varchar().length());

    case ::substrait::Type::kFixedBinary:
      return FromProtoImpl<FixedSizeBinaryType>(type.fixed_binary(),
                                                type.fixed_binary().length());

    case ::substrait::Type::kDecimal: {
      const auto& decimal = type.decimal();
      return FromProtoImpl<Decimal128Type>(decimal, decimal.precision(), decimal.scale());
    }

    case ::substrait::Type::kStruct: {
      const auto& struct_ = type.struct_();

      ARROW_ASSIGN_OR_RAISE(
          auto fields, FieldsFromProto(
                           struct_.types_size(), struct_.types(),
                           /*next_name=*/[] { return ""; }, ext_set, conversion_options));

      return FromProtoImpl<StructType>(struct_, std::move(fields));
    }

    case ::substrait::Type::kList: {
      const auto& list = type.list();

      if (!list.has_type()) {
        return Status::Invalid(
            "While converting to ListType encountered a missing item type in ",
            list.DebugString());
      }

      ARROW_ASSIGN_OR_RAISE(auto type_nullable,
                            FromProto(list.type(), ext_set, conversion_options));
      return FromProtoImpl<ListType>(
          list, field("item", std::move(type_nullable.first), type_nullable.second));
    }

    case ::substrait::Type::kMap: {
      const auto& map = type.map();

      static const std::array<char const*, 4> kMissing = {"key and value", "value", "key",
                                                          nullptr};
      if (auto missing = kMissing[map.has_key() + map.has_value() * 2]) {
        return Status::Invalid("While converting to MapType encountered missing ",
                               missing, " type in ", map.DebugString());
      }

      ARROW_ASSIGN_OR_RAISE(auto key_nullable,
                            FromProto(map.key(), ext_set, conversion_options));
      ARROW_ASSIGN_OR_RAISE(auto value_nullable,
                            FromProto(map.value(), ext_set, conversion_options));

      if (key_nullable.second) {
        return Status::Invalid(
            "While converting to MapType encountered nullable key field in ",
            map.DebugString());
      }

      return FromProtoImpl<MapType>(
          map, std::move(key_nullable.first),
          field("value", std::move(value_nullable.first), value_nullable.second));
    }

    case ::substrait::Type::kUserDefined: {
      const auto& user_defined = type.user_defined();
      uint32_t anchor = user_defined.type_reference();
      ARROW_ASSIGN_OR_RAISE(auto type_record, ext_set.DecodeType(anchor));
      return std::make_pair(std::move(type_record.type), IsNullable(user_defined));
    }

    default:
      break;
  }

  return Status::NotImplemented("conversion to arrow::DataType from Substrait type ",
                                type.DebugString());
}

namespace {

struct DataTypeToProtoImpl {
  Status Visit(const NullType& t) { return EncodeUserDefined(t); }

  Status Visit(const BooleanType& t) {
    return SetWith(&::substrait::Type::set_allocated_bool_);
  }

  Status Visit(const Int8Type& t) {
    return SetWith(&::substrait::Type::set_allocated_i8);
  }
  Status Visit(const Int16Type& t) {
    return SetWith(&::substrait::Type::set_allocated_i16);
  }
  Status Visit(const Int32Type& t) {
    return SetWith(&::substrait::Type::set_allocated_i32);
  }
  Status Visit(const Int64Type& t) {
    return SetWith(&::substrait::Type::set_allocated_i64);
  }

  Status Visit(const UInt8Type& t) { return EncodeUserDefined(t); }
  Status Visit(const UInt16Type& t) { return EncodeUserDefined(t); }
  Status Visit(const UInt32Type& t) { return EncodeUserDefined(t); }
  Status Visit(const UInt64Type& t) { return EncodeUserDefined(t); }

  Status Visit(const HalfFloatType& t) { return EncodeUserDefined(t); }
  Status Visit(const FloatType& t) {
    return SetWith(&::substrait::Type::set_allocated_fp32);
  }
  Status Visit(const DoubleType& t) {
    return SetWith(&::substrait::Type::set_allocated_fp64);
  }

  Status Visit(const StringType& t) {
    return SetWith(&::substrait::Type::set_allocated_string);
  }
  Status Visit(const BinaryType& t) {
    return SetWith(&::substrait::Type::set_allocated_binary);
  }

  Status Visit(const FixedSizeBinaryType& t) {
    SetWithThen(&::substrait::Type::set_allocated_fixed_binary)
        ->set_length(t.byte_width());
    return Status::OK();
  }

  Status Visit(const Date32Type& t) {
    return SetWith(&::substrait::Type::set_allocated_date);
  }
  Status Visit(const Date64Type& t) { return NotImplemented(t); }

  Status Visit(const TimestampType& t) {
    if (t.unit() != TimeUnit::MICRO) return NotImplemented(t);

    if (t.timezone() == "") {
      return SetWith(&::substrait::Type::set_allocated_timestamp);
    }
    if (t.timezone() == TimestampTzTimezoneString()) {
      return SetWith(&::substrait::Type::set_allocated_timestamp_tz);
    }

    return NotImplemented(t);
  }

  Status Visit(const Time32Type& t) { return NotImplemented(t); }
  Status Visit(const Time64Type& t) {
    if (t.unit() != TimeUnit::MICRO) return NotImplemented(t);
    return SetWith(&::substrait::Type::set_allocated_time);
  }

  Status Visit(const MonthIntervalType& t) { return EncodeUserDefined(t); }
  Status Visit(const DayTimeIntervalType& t) { return EncodeUserDefined(t); }

  Status Visit(const Decimal128Type& t) {
    auto dec = SetWithThen(&::substrait::Type::set_allocated_decimal);
    dec->set_precision(t.precision());
    dec->set_scale(t.scale());
    return Status::OK();
  }
  Status Visit(const Decimal256Type& t) { return NotImplemented(t); }

  Status Visit(const ListType& t) {
    // FIXME assert default field name; custom ones won't roundtrip
    ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*t.value_type(), t.value_field()->nullable(),
                                             ext_set_, conversion_options_));
    SetWithThen(&::substrait::Type::set_allocated_list)
        ->set_allocated_type(type.release());
    return Status::OK();
  }

  Status Visit(const StructType& t) {
    auto types = SetWithThen(&::substrait::Type::set_allocated_struct_)->mutable_types();

    types->Reserve(t.num_fields());

    for (const auto& field : t.fields()) {
      if (field->metadata() != nullptr) {
        return Status::Invalid(
            "::substrait::Type::Struct does not support field metadata");
      }
      ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*field->type(), field->nullable(),
                                               ext_set_, conversion_options_));
      types->AddAllocated(type.release());
    }
    return Status::OK();
  }

  Status Visit(const SparseUnionType& t) { return NotImplemented(t); }
  Status Visit(const DenseUnionType& t) { return NotImplemented(t); }
  Status Visit(const DictionaryType& t) { return NotImplemented(t); }
  Status Visit(const RunLengthEncodedType& t) { return NotImplemented(t); }

  Status Visit(const MapType& t) {
    // FIXME assert default field names; custom ones won't roundtrip
    auto map = SetWithThen(&::substrait::Type::set_allocated_map);

    ARROW_ASSIGN_OR_RAISE(auto key, ToProto(*t.key_type(), /*nullable=*/false, ext_set_,
                                            conversion_options_));
    map->set_allocated_key(key.release());

    ARROW_ASSIGN_OR_RAISE(auto value, ToProto(*t.item_type(), t.item_field()->nullable(),
                                              ext_set_, conversion_options_));
    map->set_allocated_value(value.release());

    return Status::OK();
  }

  Status Visit(const ExtensionType& t) {
    if (UnwrapUuid(t)) {
      return SetWith(&::substrait::Type::set_allocated_uuid);
    }

    if (auto length = UnwrapFixedChar(t)) {
      SetWithThen(&::substrait::Type::set_allocated_fixed_char)->set_length(*length);
      return Status::OK();
    }

    if (auto length = UnwrapVarChar(t)) {
      SetWithThen(&::substrait::Type::set_allocated_varchar)->set_length(*length);
      return Status::OK();
    }

    if (UnwrapIntervalYear(t)) {
      return SetWith(&::substrait::Type::set_allocated_interval_year);
    }

    if (UnwrapIntervalDay(t)) {
      return SetWith(&::substrait::Type::set_allocated_interval_day);
    }

    return NotImplemented(t);
  }

  Status Visit(const FixedSizeListType& t) { return NotImplemented(t); }
  Status Visit(const DurationType& t) { return NotImplemented(t); }
  Status Visit(const LargeStringType& t) { return NotImplemented(t); }
  Status Visit(const LargeBinaryType& t) { return NotImplemented(t); }
  Status Visit(const LargeListType& t) { return NotImplemented(t); }
  Status Visit(const MonthDayNanoIntervalType& t) { return EncodeUserDefined(t); }

  template <typename Sub>
  Sub* SetWithThen(void (::substrait::Type::*set_allocated_sub)(Sub*)) {
    auto sub = std::make_unique<Sub>();
    sub->set_nullability(nullable_ ? ::substrait::Type::NULLABILITY_NULLABLE
                                   : ::substrait::Type::NULLABILITY_REQUIRED);

    auto out = sub.get();
    (type_->*set_allocated_sub)(sub.release());
    return out;
  }

  template <typename Sub>
  Status SetWith(void (::substrait::Type::*set_allocated_sub)(Sub*)) {
    return SetWithThen(set_allocated_sub), Status::OK();
  }

  template <typename T>
  Status EncodeUserDefined(const T& t) {
    ARROW_ASSIGN_OR_RAISE(auto anchor, ext_set_->EncodeType(t));
    auto user_defined = std::make_unique<::substrait::Type::UserDefined>();
    user_defined->set_type_reference(anchor);
    user_defined->set_nullability(nullable_ ? ::substrait::Type::NULLABILITY_NULLABLE
                                            : ::substrait::Type::NULLABILITY_REQUIRED);
    type_->set_allocated_user_defined(user_defined.release());
    return Status::OK();
  }

  Status NotImplemented(const DataType& t) {
    return Status::NotImplemented("conversion to ::substrait::Type from ", t.ToString());
  }

  Status operator()(const DataType& type) { return VisitTypeInline(type, this); }

  ::substrait::Type* type_;
  bool nullable_;
  ExtensionSet* ext_set_;
  const ConversionOptions& conversion_options_;
};
}  // namespace

Result<std::unique_ptr<::substrait::Type>> ToProto(
    const DataType& type, bool nullable, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto out = std::make_unique<::substrait::Type>();
  RETURN_NOT_OK(
      (DataTypeToProtoImpl{out.get(), nullable, ext_set, conversion_options})(type));
  return std::move(out);
}

Result<std::shared_ptr<Schema>> FromProto(const ::substrait::NamedStruct& named_struct,
                                          const ExtensionSet& ext_set,
                                          const ConversionOptions& conversion_options) {
  if (!named_struct.has_struct_()) {
    return Status::Invalid("While converting ", named_struct.DebugString(),
                           " no anonymous struct type was provided to which names "
                           "could be attached.");
  }
  const auto& struct_ = named_struct.struct_();

  int requested_names_count = 0;
  ARROW_ASSIGN_OR_RAISE(auto fields, FieldsFromProto(
                                         struct_.types_size(), struct_.types(),
                                         /*next_name=*/
                                         [&] {
                                           int i = requested_names_count++;
                                           return i < named_struct.names_size()
                                                      ? named_struct.names().Get(i)
                                                      : "";
                                         },
                                         ext_set, conversion_options));

  if (requested_names_count != named_struct.names_size()) {
    return Status::Invalid("While converting ", named_struct.DebugString(), " received ",
                           named_struct.names_size(), " names but ",
                           requested_names_count, " struct fields");
  }

  return schema(std::move(fields));
}

namespace {
void ToProtoGetDepthFirstNames(const FieldVector& fields,
                               google::protobuf::RepeatedPtrField<std::string>* names) {
  for (const auto& field : fields) {
    *names->Add() = field->name();

    if (field->type()->id() == Type::STRUCT) {
      ToProtoGetDepthFirstNames(field->type()->fields(), names);
    }
  }
}
}  // namespace

Result<std::unique_ptr<::substrait::NamedStruct>> ToProto(
    const Schema& schema, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  if (schema.metadata()) {
    return Status::Invalid("::substrait::NamedStruct does not support schema metadata");
  }

  auto named_struct = std::make_unique<::substrait::NamedStruct>();

  auto names = named_struct->mutable_names();
  names->Reserve(schema.num_fields());
  ToProtoGetDepthFirstNames(schema.fields(), names);

  auto struct_ = std::make_unique<::substrait::Type::Struct>();
  auto types = struct_->mutable_types();
  types->Reserve(schema.num_fields());

  for (const auto& field : schema.fields()) {
    if (field->metadata() != nullptr) {
      return Status::Invalid("::substrait::NamedStruct does not support field metadata");
    }

    ARROW_ASSIGN_OR_RAISE(auto type, ToProto(*field->type(), field->nullable(), ext_set,
                                             conversion_options));
    types->AddAllocated(type.release());
  }

  named_struct->set_allocated_struct_(struct_.release());
  return std::move(named_struct);
}

}  // namespace engine
}  // namespace arrow
