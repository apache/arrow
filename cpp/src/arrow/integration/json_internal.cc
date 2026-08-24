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

#include "arrow/integration/json_internal.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/extension_type.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/json/json_writer_internal.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/formatting.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/range.h"
#include "arrow/util/simdjson_internal.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visit_array_inline.h"
#include "arrow/visit_type_inline.h"

using arrow::internal::checked_cast;
using arrow::internal::Enumerate;
using arrow::internal::ParseValue;
using arrow::internal::Zip;

using arrow::ipc::DictionaryFieldMapper;
using arrow::ipc::DictionaryMemo;
using arrow::ipc::internal::FieldPosition;

using JsonWriter = arrow::json::JsonWriter;

namespace arrow::internal::integration::json {

namespace {

constexpr char kData[] = "DATA";
constexpr char kDays[] = "days";
constexpr char kDayTime[] = "DAY_TIME";
constexpr char kDuration[] = "duration";
constexpr char kMilliseconds[] = "milliseconds";
constexpr char kMonths[] = "months";
constexpr char kNanoseconds[] = "nanoseconds";
constexpr char kYearMonth[] = "YEAR_MONTH";
constexpr char kMonthDayNano[] = "MONTH_DAY_NANO";

std::string GetFloatingPrecisionName(FloatingPointType::Precision precision) {
  switch (precision) {
    case FloatingPointType::HALF:
      return "HALF";
    case FloatingPointType::SINGLE:
      return "SINGLE";
    case FloatingPointType::DOUBLE:
      return "DOUBLE";
    default:
      break;
  }
  return "UNKNOWN";
}

std::string GetTimeUnitName(TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      return "SECOND";
    case TimeUnit::MILLI:
      return "MILLISECOND";
    case TimeUnit::MICRO:
      return "MICROSECOND";
    case TimeUnit::NANO:
      return "NANOSECOND";
    default:
      break;
  }
  return "UNKNOWN";
}

Result<std::string_view> GetStringView(const JsonValue& value) {
  ARROW_ASSIGN_OR_RAISE(
      auto string_view,
      internal::ResolveSimdjsonResult(value.get_string(), "field was not a string"));
  return string_view;
}

class SchemaWriter {
 public:
  explicit SchemaWriter(const Schema& schema, const DictionaryFieldMapper& mapper,
                        JsonWriter* writer)
      : schema_(schema), mapper_(mapper), writer_(writer) {}

  Status Write() {
    writer_->Key("schema");
    writer_->StartObject();
    writer_->Key("fields");
    writer_->StartArray();

    FieldPosition field_pos;
    for (auto [field, i] : Zip(schema_.fields(), Enumerate<int>)) {
      RETURN_NOT_OK(VisitField(field, field_pos.child(i)));
    }
    writer_->EndArray();
    WriteKeyValueMetadata(schema_.metadata());
    writer_->EndObject();
    return Status::OK();
  }

  void WriteKeyValueMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata,
      const std::vector<std::pair<std::string, std::string>>& additional_metadata = {}) {
    if ((metadata == nullptr || metadata->size() == 0) && additional_metadata.empty()) {
      return;
    }
    writer_->Key("metadata");

    writer_->StartArray();
    if (metadata != nullptr) {
      for (auto [key, value] : Zip(metadata->keys(), metadata->values())) {
        WriteKeyValue(key, value);
      }
    }
    for (const auto& [key, value] : additional_metadata) {
      WriteKeyValue(key, value);
    }
    writer_->EndArray();
  }

  void WriteKeyValue(const std::string& key, const std::string& value) {
    writer_->StartObject();

    writer_->Key("key");
    writer_->String(key.c_str());

    writer_->Key("value");
    writer_->String(value.c_str());

    writer_->EndObject();
  }

  Status WriteDictionaryMetadata(int64_t id, const DictionaryType& type) {
    writer_->Key("dictionary");

    // Emulate DictionaryEncoding from Schema.fbs
    writer_->StartObject();
    writer_->Key("id");
    writer_->Int(static_cast<int32_t>(id));
    writer_->Key("indexType");

    writer_->StartObject();
    RETURN_NOT_OK(VisitType(*type.index_type()));
    writer_->EndObject();

    writer_->Key("isOrdered");
    writer_->Bool(type.ordered());
    writer_->EndObject();

    return Status::OK();
  }

  Status VisitField(const std::shared_ptr<Field>& field, FieldPosition field_pos) {
    writer_->StartObject();

    writer_->Key("name");
    writer_->String(field->name().c_str());

    writer_->Key("nullable");
    writer_->Bool(field->nullable());

    const DataType* type = field->type().get();
    std::vector<std::pair<std::string, std::string>> additional_metadata;
    if (type->id() == Type::EXTENSION) {
      const auto& ext_type = checked_cast<const ExtensionType&>(*type);
      type = ext_type.storage_type().get();
      additional_metadata.emplace_back(kExtensionTypeKeyName, ext_type.extension_name());
      additional_metadata.emplace_back(kExtensionMetadataKeyName, ext_type.Serialize());
    }

    // Visit the type
    writer_->Key("type");
    writer_->StartObject();
    RETURN_NOT_OK(VisitType(*type));
    writer_->EndObject();

    if (type->id() == Type::DICTIONARY) {
      const auto& dict_type = checked_cast<const DictionaryType&>(*type);
      // Ensure we visit child fields first so that, in the case of nested
      // dictionaries, inner dictionaries get a smaller id than outer dictionaries.
      RETURN_NOT_OK(WriteChildren(dict_type.value_type()->fields(), field_pos));
      ARROW_ASSIGN_OR_RAISE(const int64_t dictionary_id,
                            mapper_.GetFieldId(field_pos.path()));
      RETURN_NOT_OK(WriteDictionaryMetadata(dictionary_id, dict_type));
    } else {
      RETURN_NOT_OK(WriteChildren(type->fields(), field_pos));
    }

    WriteKeyValueMetadata(field->metadata(), additional_metadata);
    writer_->EndObject();

    return Status::OK();
  }

  Status VisitType(const DataType& type);

  template <typename T>
  enable_if_t<is_null_type<T>::value || is_primitive_ctype<T>::value ||
              is_base_binary_type<T>::value || is_binary_view_like_type<T>::value ||
              is_var_length_list_type<T>::value || is_struct_type<T>::value ||
              is_run_end_encoded_type<T>::value || is_list_view_type<T>::value>
  WriteTypeMetadata(const T& type) {}

  void WriteTypeMetadata(const MapType& type) {
    writer_->Key("keysSorted");
    writer_->Bool(type.keys_sorted());
  }

  void WriteTypeMetadata(const IntegerType& type) {
    writer_->Key("bitWidth");
    writer_->Int(type.bit_width());
    writer_->Key("isSigned");
    writer_->Bool(type.is_signed());
  }

  void WriteTypeMetadata(const FloatingPointType& type) {
    writer_->Key("precision");
    writer_->String(GetFloatingPrecisionName(type.precision()));
  }

  void WriteTypeMetadata(const IntervalType& type) {
    writer_->Key("unit");
    switch (type.interval_type()) {
      case IntervalType::MONTHS:
        writer_->String(kYearMonth);
        break;
      case IntervalType::DAY_TIME:
        writer_->String(kDayTime);
        break;
      case IntervalType::MONTH_DAY_NANO:
        writer_->String(kMonthDayNano);
        break;
    }
  }

  void WriteTypeMetadata(const TimestampType& type) {
    writer_->Key("unit");
    writer_->String(GetTimeUnitName(type.unit()));
    if (type.timezone().size() > 0) {
      writer_->Key("timezone");
      writer_->String(type.timezone());
    }
  }

  void WriteTypeMetadata(const DurationType& type) {
    writer_->Key("unit");
    writer_->String(GetTimeUnitName(type.unit()));
  }

  void WriteTypeMetadata(const TimeType& type) {
    writer_->Key("unit");
    writer_->String(GetTimeUnitName(type.unit()));
    writer_->Key("bitWidth");
    writer_->Int(type.bit_width());
  }

  void WriteTypeMetadata(const DateType& type) {
    writer_->Key("unit");
    switch (type.unit()) {
      case DateUnit::DAY:
        writer_->String("DAY");
        break;
      case DateUnit::MILLI:
        writer_->String("MILLISECOND");
        break;
    }
  }

  void WriteTypeMetadata(const FixedSizeBinaryType& type) {
    writer_->Key("byteWidth");
    writer_->Int(type.byte_width());
  }

  void WriteTypeMetadata(const FixedSizeListType& type) {
    writer_->Key("listSize");
    writer_->Int(type.list_size());
  }

  void WriteTypeMetadata(const DecimalType& type) {
    writer_->Key("precision");
    writer_->Int(type.precision());
    writer_->Key("scale");
    writer_->Int(type.scale());
  }

  void WriteTypeMetadata(const UnionType& type) {
    writer_->Key("mode");
    switch (type.mode()) {
      case UnionMode::SPARSE:
        writer_->String("SPARSE");
        break;
      case UnionMode::DENSE:
        writer_->String("DENSE");
        break;
    }

    // Write type ids
    writer_->Key("typeIds");
    writer_->StartArray();
    for (int8_t i : type.type_codes()) {
      writer_->Int(i);
    }
    writer_->EndArray();
  }

  // TODO(wesm): Other Type metadata

  template <typename T>
  void WriteName(const std::string& typeclass, const T& type) {
    writer_->Key("name");
    writer_->String(typeclass);
    WriteTypeMetadata(type);
  }

  template <typename T>
  Status WritePrimitive(const std::string& typeclass, const T& type) {
    WriteName(typeclass, type);
    return Status::OK();
  }

  template <typename T>
  Status WriteVarBytes(const std::string& typeclass, const T& type) {
    WriteName(typeclass, type);
    return Status::OK();
  }

  Status WriteChildren(const FieldVector& children, FieldPosition field_pos) {
    writer_->Key("children");
    writer_->StartArray();
    for (auto [i, field] : Zip(Enumerate<int>, children)) {
      RETURN_NOT_OK(VisitField(field, field_pos.child(i)));
    }
    writer_->EndArray();
    return Status::OK();
  }

  Status Visit(const NullType& type) { return WritePrimitive("null", type); }
  Status Visit(const BooleanType& type) { return WritePrimitive("bool", type); }
  Status Visit(const IntegerType& type) { return WritePrimitive("int", type); }

  Status Visit(const FloatingPointType& type) {
    return WritePrimitive("floatingpoint", type);
  }

  Status Visit(const DateType& type) { return WritePrimitive("date", type); }
  Status Visit(const TimeType& type) { return WritePrimitive("time", type); }
  Status Visit(const StringType& type) { return WriteVarBytes("utf8", type); }
  Status Visit(const BinaryType& type) { return WriteVarBytes("binary", type); }
  Status Visit(const StringViewType& type) { return WritePrimitive("utf8view", type); }
  Status Visit(const BinaryViewType& type) { return WritePrimitive("binaryview", type); }
  Status Visit(const LargeStringType& type) { return WriteVarBytes("largeutf8", type); }
  Status Visit(const LargeBinaryType& type) { return WriteVarBytes("largebinary", type); }
  Status Visit(const FixedSizeBinaryType& type) {
    return WritePrimitive("fixedsizebinary", type);
  }

  Status Visit(const Decimal32Type& type) { return WritePrimitive("decimal32", type); }
  Status Visit(const Decimal64Type& type) { return WritePrimitive("decimal64", type); }
  Status Visit(const Decimal128Type& type) { return WritePrimitive("decimal", type); }
  Status Visit(const Decimal256Type& type) { return WritePrimitive("decimal256", type); }
  Status Visit(const TimestampType& type) { return WritePrimitive("timestamp", type); }
  Status Visit(const DurationType& type) { return WritePrimitive(kDuration, type); }
  Status Visit(const MonthIntervalType& type) { return WritePrimitive("interval", type); }
  Status Visit(const MonthDayNanoIntervalType& type) {
    return WritePrimitive("interval", type);
  }

  Status Visit(const DayTimeIntervalType& type) {
    return WritePrimitive("interval", type);
  }

  Status Visit(const ListType& type) {
    WriteName("list", type);
    return Status::OK();
  }

  Status Visit(const LargeListType& type) {
    WriteName("largelist", type);
    return Status::OK();
  }

  Status Visit(const ListViewType& type) {
    WriteName("listview", type);
    return Status::OK();
  }

  Status Visit(const LargeListViewType& type) {
    WriteName("largelistview", type);
    return Status::OK();
  }

  Status Visit(const MapType& type) {
    WriteName("map", type);
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    WriteName("fixedsizelist", type);
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    WriteName("struct", type);
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    WriteName("union", type);
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) { return VisitType(*type.value_type()); }

  Status Visit(const RunEndEncodedType& type) {
    WriteName("runendencoded", type);
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) { return Status::NotImplemented(type.name()); }

 private:
  const Schema& schema_;
  const DictionaryFieldMapper& mapper_;
  JsonWriter* writer_;
};

Status SchemaWriter::VisitType(const DataType& type) {
  return VisitTypeInline(type, this);
}

class ArrayWriter {
 public:
  ArrayWriter(const std::string& name, const Array& array, JsonWriter* writer)
      : name_(name), array_(array), writer_(writer) {}

  Status Write() { return VisitArray(name_, array_); }

  Status VisitArrayValues(const Array& arr) { return VisitArrayInline(arr, this); }

  Status VisitArray(const std::string& name, const Array& arr) {
    writer_->StartObject();
    writer_->Key("name");
    writer_->String(name);

    writer_->Key("count");
    writer_->Int(static_cast<int32_t>(arr.length()));

    RETURN_NOT_OK(VisitArrayValues(arr));

    writer_->EndObject();
    return Status::OK();
  }

  void WriteRawNumber(std::string_view v) { writer_->RawValue(v); }

  template <typename ArrayType, typename TypeClass = typename ArrayType::TypeClass,
            typename CType = typename TypeClass::c_type>
  enable_if_t<is_physical_integer_type<TypeClass>::value &&
              sizeof(CType) != sizeof(int64_t)>
  WriteDataValues(const ArrayType& arr) {
    static const std::string null_string = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        writer_->Int64(arr.Value(i));
      } else {
        WriteRawNumber(null_string);
      }
    }
  }

  template <typename ArrayType, typename TypeClass = typename ArrayType::TypeClass,
            typename CType = typename TypeClass::c_type>
  enable_if_t<is_physical_integer_type<TypeClass>::value &&
              sizeof(CType) == sizeof(int64_t)>
  WriteDataValues(const ArrayType& arr) {
    ::arrow::internal::StringFormatter<typename CTypeTraits<CType>::ArrowType> fmt;

    static const std::string null_string = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        fmt(arr.Value(i), [&](std::string_view repr) {
          writer_->String(std::string_view(repr.data(), repr.size()));
        });
      } else {
        writer_->String(std::string_view(null_string.data(), null_string.size()));
      }
    }
  }

  template <typename ArrayType>
  enable_if_physical_floating_point<typename ArrayType::TypeClass> WriteDataValues(
      const ArrayType& arr) {
    static const std::string null_string = "0";
    const auto data = arr.raw_values();
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        writer_->Double(data[i]);
      } else {
        WriteRawNumber(null_string);
      }
    }
  }

  template <typename ArrayType, typename Type = typename ArrayType::TypeClass>
  std::enable_if_t<is_base_binary_type<Type>::value ||
                   is_fixed_size_binary_type<Type>::value>
  WriteDataValues(const ArrayType& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      if constexpr (Type::is_utf8) {
        // UTF8 string, write as is
        auto view = arr.GetView(i);
        writer_->String(std::string_view(view.data(), view.size()));
      } else {
        // Binary, encode to hexadecimal.
        writer_->String(HexEncode(arr.GetView(i)));
      }
    }
  }

  void WriteDataValues(const MonthDayNanoIntervalArray& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      writer_->StartObject();
      if (arr.IsValid(i)) {
        const MonthDayNanoIntervalType::MonthDayNanos dm = arr.GetValue(i);
        writer_->Key(kMonths);
        writer_->Int(dm.months);
        writer_->Key(kDays);
        writer_->Int(dm.days);
        writer_->Key(kNanoseconds);
        writer_->Int64(dm.nanoseconds);
      }
      writer_->EndObject();
    }
  }

  void WriteDataValues(const DayTimeIntervalArray& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      writer_->StartObject();
      if (arr.IsValid(i)) {
        const DayTimeIntervalType::DayMilliseconds dm = arr.GetValue(i);
        writer_->Key(kDays);
        writer_->Int(dm.days);
        writer_->Key(kMilliseconds);
        writer_->Int(dm.milliseconds);
      }
      writer_->EndObject();
    }
  }

  void WriteDataValues(const Decimal32Array& arr) {
    static const char null_string[] = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        const Decimal32 value(arr.GetValue(i));
        writer_->String(value.ToIntegerString());
      } else {
        writer_->String(std::string_view(null_string));
      }
    }
  }

  void WriteDataValues(const Decimal64Array& arr) {
    static const char null_string[] = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        const Decimal64 value(arr.GetValue(i));
        writer_->String(value.ToIntegerString());
      } else {
        writer_->String(std::string_view(null_string));
      }
    }
  }

  void WriteDataValues(const Decimal128Array& arr) {
    static const char null_string[] = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        const Decimal128 value(arr.GetValue(i));
        writer_->String(value.ToIntegerString());
      } else {
        writer_->String(std::string_view(null_string));
      }
    }
  }

  void WriteDataValues(const Decimal256Array& arr) {
    static const char null_string[] = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        const Decimal256 value(arr.GetValue(i));
        writer_->String(value.ToIntegerString());
      } else {
        writer_->String(std::string_view(null_string));
      }
    }
  }

  void WriteDataValues(const BooleanArray& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        writer_->Bool(arr.Value(i));
      } else {
        writer_->Bool(false);
      }
    }
  }

  template <typename T>
  void WriteDataField(const T& arr) {
    writer_->Key(kData);
    writer_->StartArray();
    WriteDataValues(arr);
    writer_->EndArray();
  }

  template <typename T>
  void WriteIntegerField(const char* name, const T* values, int64_t length) {
    writer_->Key(name);
    writer_->StartArray();
    if (sizeof(T) < sizeof(int64_t)) {
      for (int i = 0; i < length; ++i) {
        writer_->Int64(values[i]);
      }
    } else {
      // Represent 64-bit integers as strings, as JSON numbers cannot represent
      // them exactly.
      ::arrow::internal::StringFormatter<typename CTypeTraits<T>::ArrowType> formatter;
      auto append = [this](std::string_view v) {
        writer_->String(std::string_view(v.data(), v.size()));
        return Status::OK();
      };
      for (int i = 0; i < length; ++i) {
        DCHECK_OK(formatter(values[i], append));
      }
    }
    writer_->EndArray();
  }

  template <typename ArrayType>
  void WriteBinaryViewField(const ArrayType& array) {
    writer_->Key("VIEWS");
    writer_->StartArray();
    for (int64_t i = 0; i < array.length(); ++i) {
      auto s = array.raw_values()[i];
      writer_->StartObject();
      writer_->Key("SIZE");
      writer_->Int64(s.size());
      if (s.is_inline()) {
        writer_->Key("INLINED");
        if constexpr (ArrayType::TypeClass::is_utf8) {
          writer_->String(
              std::string_view(reinterpret_cast<const char*>(s.inline_data()), s.size()));
        } else {
          writer_->String(HexEncode(s.inline_data(), s.size()));
        }
      } else {
        // Prefix is always 4 bytes so it may not be utf-8 even if the whole
        // string view is
        writer_->Key("PREFIX_HEX");
        writer_->String(HexEncode(s.inline_data(), BinaryViewType::kPrefixSize));
        writer_->Key("BUFFER_INDEX");
        writer_->Int64(s.ref.buffer_index);
        writer_->Key("OFFSET");
        writer_->Int64(s.ref.offset);
      }
      writer_->EndObject();
    }
    writer_->EndArray();
  }

  void WriteVariadicBuffersField(const BinaryViewArray& arr) {
    writer_->Key("VARIADIC_DATA_BUFFERS");
    writer_->StartArray();
    const auto& buffers = arr.data()->buffers;
    for (size_t i = 2; i < buffers.size(); ++i) {
      // Encode the data buffers into hexadecimal strings.
      // Even for arrays which contain utf-8, portions of the buffer not
      // referenced by any view may be invalid.
      writer_->String(buffers[i]->ToHexString());
    }
    writer_->EndArray();
  }

  void WriteValidityField(const Array& arr) {
    writer_->Key("VALIDITY");
    writer_->StartArray();
    if (arr.null_count() > 0) {
      for (int i = 0; i < arr.length(); ++i) {
        writer_->Int(arr.IsNull(i) ? 0 : 1);
      }
    } else {
      for (int i = 0; i < arr.length(); ++i) {
        writer_->Int(1);
      }
    }
    writer_->EndArray();
  }

  void SetNoChildren() {
    // Nothing.  We used to write an empty "children" array member,
    // but that fails the Java parser (ARROW-11483).
  }

  Status WriteChildren(const FieldVector& fields,
                       const std::vector<std::shared_ptr<Array>>& arrays) {
    // NOTE: the Java parser fails on an empty "children" member (ARROW-11483).
    if (fields.size() == 0) return Status::OK();

    writer_->Key("children");
    writer_->StartArray();
    for (auto [field, array] : Zip(fields, arrays)) {
      RETURN_NOT_OK(VisitArray(field->name(), *array));
    }
    writer_->EndArray();
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    SetNoChildren();
    return Status::OK();
  }

  template <typename ArrayType>
  enable_if_t<std::is_base_of<PrimitiveArray, ArrayType>::value &&
                  !is_binary_view_like_type<typename ArrayType::TypeClass>::value,
              Status>
  Visit(const ArrayType& array) {
    WriteValidityField(array);
    WriteDataField(array);
    SetNoChildren();
    return Status::OK();
  }

  template <typename ArrayType>
  enable_if_base_binary<typename ArrayType::TypeClass, Status> Visit(
      const ArrayType& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    WriteDataField(array);
    SetNoChildren();
    return Status::OK();
  }

  template <typename ArrayType>
  enable_if_binary_view_like<typename ArrayType::TypeClass, Status> Visit(
      const ArrayType& array) {
    WriteValidityField(array);
    WriteBinaryViewField(array);
    WriteVariadicBuffersField(array);

    SetNoChildren();
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    return VisitArrayValues(*array.indices());
  }

  template <typename ArrayType>
  enable_if_var_size_list<typename ArrayType::TypeClass, Status> Visit(
      const ArrayType& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    return WriteChildren(array.type()->fields(), {array.values()});
  }

  template <typename ArrayType>
  enable_if_list_view<typename ArrayType::TypeClass, Status> Visit(
      const ArrayType& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length());
    WriteIntegerField("SIZE", array.raw_value_sizes(), array.length());
    return WriteChildren(array.type()->fields(), {array.values()});
  }

  Status Visit(const FixedSizeListArray& array) {
    WriteValidityField(array);
    const auto& type = checked_cast<const FixedSizeListType&>(*array.type());
    return WriteChildren(type.fields(), {array.values()});
  }

  Status Visit(const StructArray& array) {
    WriteValidityField(array);
    const auto& type = checked_cast<const StructType&>(*array.type());
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return WriteChildren(type.fields(), children);
  }

  Status Visit(const UnionArray& array) {
    const auto& type = checked_cast<const UnionType&>(*array.type());
    WriteIntegerField("TYPE_ID", array.raw_type_codes(), array.length());
    if (type.mode() == UnionMode::DENSE) {
      auto offsets = checked_cast<const DenseUnionArray&>(array).raw_value_offsets();
      WriteIntegerField("OFFSET", offsets, array.length());
    }
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return WriteChildren(type.fields(), children);
  }

  Status Visit(const RunEndEncodedArray& array) {
    const auto& ree_type = checked_cast<const RunEndEncodedType&>(*array.type());
    ARROW_ASSIGN_OR_RAISE(auto run_ends, array.LogicalRunEnds(default_memory_pool()));
    const std::vector<std::shared_ptr<Array>> children = {
        std::move(run_ends),
        array.LogicalValues(),
    };
    return WriteChildren(ree_type.fields(), children);
  }

  Status Visit(const ExtensionArray& array) { return VisitArrayValues(*array.storage()); }

 private:
  const std::string& name_;
  const Array& array_;
  JsonWriter* writer_;
};

Result<TimeUnit::type> GetUnitFromString(const std::string& unit_str) {
  if (unit_str == "SECOND") {
    return TimeUnit::SECOND;
  } else if (unit_str == "MILLISECOND") {
    return TimeUnit::MILLI;
  } else if (unit_str == "MICROSECOND") {
    return TimeUnit::MICRO;
  } else if (unit_str == "NANOSECOND") {
    return TimeUnit::NANO;
  } else {
    return Status::Invalid("Invalid time unit: ", unit_str);
  }
}

template <typename IntType = int>
Result<IntType> GetMemberInt(const JsonObject& obj, std::string_view key) {
  ARROW_ASSIGN_OR_RAISE(
      auto value, internal::ResolveSimdjsonResult(obj[key], "Failed to get JSON field"));
  ARROW_ASSIGN_OR_RAISE(auto integer, internal::ResolveSimdjsonResult(
                                          value.get_int64(), "field was not an integer"));
  return static_cast<IntType>(integer);
}

Result<bool> GetMemberBool(const JsonObject& obj, std::string_view key) {
  ARROW_ASSIGN_OR_RAISE(
      auto value, internal::ResolveSimdjsonResult(obj[key], "Failed to get JSON field"));
  return internal::ResolveSimdjsonResult(value.get_bool(), "field was not a boolean");
}

Result<std::string> GetMemberString(const JsonObject& obj, std::string_view key) {
  ARROW_ASSIGN_OR_RAISE(
      auto value, internal::ResolveSimdjsonResult(obj[key], "Failed to get JSON field"));
  ARROW_ASSIGN_OR_RAISE(
      auto string_view,
      internal::ResolveSimdjsonResult(value.get_string(), "field was not a string"));
  return std::string(string_view);
}

Result<JsonObject> GetMemberObject(const JsonObject& obj, std::string_view key) {
  ARROW_ASSIGN_OR_RAISE(
      auto value, internal::ResolveSimdjsonResult(obj[key], "Failed to get JSON field"));
  return internal::ResolveSimdjsonResult(value.get_object(), "field was not an object");
}

Result<JsonArray> GetMemberArray(const JsonObject& obj, std::string_view key) {
  ARROW_ASSIGN_OR_RAISE(
      auto value, internal::ResolveSimdjsonResult(obj[key], "Failed to get JSON field"));

  return internal::ResolveSimdjsonResult(value.get_array(), "field was not an array");
}

Result<TimeUnit::type> GetMemberTimeUnit(const JsonObject& obj, std::string_view key) {
  ARROW_ASSIGN_OR_RAISE(const auto unit_str, GetMemberString(obj, key));
  return GetUnitFromString(unit_str);
}

Result<std::shared_ptr<DataType>> GetInteger(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const bool is_signed, GetMemberBool(json_type, "isSigned"));
  ARROW_ASSIGN_OR_RAISE(const int bit_width, GetMemberInt<int>(json_type, "bitWidth"));

  switch (bit_width) {
    case 8:
      return is_signed ? int8() : uint8();
    case 16:
      return is_signed ? int16() : uint16();
    case 32:
      return is_signed ? int32() : uint32();
    case 64:
      return is_signed ? int64() : uint64();
  }
  return Status::Invalid("Invalid bit width: ", bit_width);
}

Result<std::shared_ptr<DataType>> GetFloatingPoint(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const auto precision, GetMemberString(json_type, "precision"));

  if (precision == "DOUBLE") {
    return float64();
  } else if (precision == "SINGLE") {
    return float32();
  } else if (precision == "HALF") {
    return float16();
  }
  return Status::Invalid("Invalid precision: ", precision);
}

Result<std::shared_ptr<DataType>> GetMap(const JsonObject& json_type,
                                         const FieldVector& children) {
  if (children.size() != 1) {
    return Status::Invalid("Map must have exactly one child");
  }

  ARROW_ASSIGN_OR_RAISE(const bool keys_sorted, GetMemberBool(json_type, "keysSorted"));
  return MapType::Make(children[0], keys_sorted);
}

Result<std::shared_ptr<DataType>> GetFixedSizeBinary(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const int32_t byte_width,
                        GetMemberInt<int32_t>(json_type, "byteWidth"));
  return fixed_size_binary(byte_width);
}

Result<std::shared_ptr<DataType>> GetFixedSizeList(const JsonObject& json_type,
                                                   const FieldVector& children) {
  if (children.size() != 1) {
    return Status::Invalid("FixedSizeList must have exactly one child");
  }

  ARROW_ASSIGN_OR_RAISE(const int32_t list_size,
                        GetMemberInt<int32_t>(json_type, "listSize"));
  return fixed_size_list(children[0], list_size);
}

Result<std::shared_ptr<DataType>> GetDecimal(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const int32_t precision,
                        GetMemberInt<int32_t>(json_type, "precision"));
  ARROW_ASSIGN_OR_RAISE(const int32_t scale, GetMemberInt<int32_t>(json_type, "scale"));
  int32_t bit_width = 128;
  Result<int32_t> maybe_bit_width = GetMemberInt<int32_t>(json_type, "bitWidth");
  if (maybe_bit_width.ok()) {
    bit_width = maybe_bit_width.ValueOrDie();
  }

  switch (bit_width) {
    case 32:
      return decimal32(precision, scale);
    case 64:
      return decimal64(precision, scale);
    case 128:
      return decimal128(precision, scale);
    case 256:
      return decimal256(precision, scale);
  }

  return Status::Invalid("Only 32/64/128/256-bit Decimals are supported. Received ",
                         bit_width);
}

Result<std::shared_ptr<DataType>> GetDate(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const auto unit_str, GetMemberString(json_type, "unit"));

  if (unit_str == "DAY") {
    return date32();
  } else if (unit_str == "MILLISECOND") {
    return date64();
  }
  return Status::Invalid("Invalid date unit: ", unit_str);
}

Result<std::shared_ptr<DataType>> GetTime(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const auto unit_str, GetMemberString(json_type, "unit"));
  ARROW_ASSIGN_OR_RAISE(const int bit_width, GetMemberInt<int>(json_type, "bitWidth"));

  std::shared_ptr<DataType> type;

  if (unit_str == "SECOND") {
    type = time32(TimeUnit::SECOND);
  } else if (unit_str == "MILLISECOND") {
    type = time32(TimeUnit::MILLI);
  } else if (unit_str == "MICROSECOND") {
    type = time64(TimeUnit::MICRO);
  } else if (unit_str == "NANOSECOND") {
    type = time64(TimeUnit::NANO);
  } else {
    return Status::Invalid("Invalid time unit: ", unit_str);
  }

  const auto& fw_type = checked_cast<const FixedWidthType&>(*type);
  if (bit_width != fw_type.bit_width()) {
    return Status::Invalid("Indicated bit width does not match unit");
  }
  return type;
}

Result<std::shared_ptr<DataType>> GetDuration(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const TimeUnit::type unit, GetMemberTimeUnit(json_type, "unit"));
  return duration(unit);
}

Result<std::shared_ptr<DataType>> GetTimestamp(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const TimeUnit::type unit, GetMemberTimeUnit(json_type, "unit"));

  auto timezone_result = json_type["timezone"];
  if (timezone_result.error() == simdjson::NO_SUCH_FIELD) {
    return timestamp(unit);
  }

  ARROW_ASSIGN_OR_RAISE(auto timezone, internal::ResolveSimdjsonResult(
                                           timezone_result, "Failed to get JSON field"));
  ARROW_ASSIGN_OR_RAISE(
      auto timezone_string,
      internal::ResolveSimdjsonResult(timezone.get_string(), "field was not a string"));
  return timestamp(unit, std::string(timezone_string));
}

Result<std::shared_ptr<DataType>> GetInterval(const JsonObject& json_type) {
  ARROW_ASSIGN_OR_RAISE(const auto unit_str, GetMemberString(json_type, "unit"));

  if (unit_str == kDayTime) {
    return day_time_interval();
  } else if (unit_str == kYearMonth) {
    return month_interval();
  } else if (unit_str == kMonthDayNano) {
    return month_day_nano_interval();
  }
  return Status::Invalid("Invalid interval unit: " + unit_str);
}

Result<std::shared_ptr<DataType>> GetUnion(const JsonObject& json_type,
                                           const FieldVector& children) {
  ARROW_ASSIGN_OR_RAISE(const auto mode_str, GetMemberString(json_type, "mode"));

  UnionMode::type mode;
  if (mode_str == "SPARSE") {
    mode = UnionMode::SPARSE;
  } else if (mode_str == "DENSE") {
    mode = UnionMode::DENSE;
  } else {
    return Status::Invalid("Invalid union mode: ", mode_str);
  }

  ARROW_ASSIGN_OR_RAISE(const auto json_type_codes, GetMemberArray(json_type, "typeIds"));

  std::vector<int8_t> type_codes;
  type_codes.reserve(json_type_codes.size());

  for (const auto& value : json_type_codes) {
    ARROW_ASSIGN_OR_RAISE(auto integer,
                          internal::ResolveSimdjsonResult(
                              value.get_int64(), "Union type codes must be integers"));
    type_codes.push_back(static_cast<int8_t>(integer));
  }

  if (mode == UnionMode::SPARSE) {
    return sparse_union(children, std::move(type_codes));
  } else {
    return dense_union(children, std::move(type_codes));
  }
}

Result<std::shared_ptr<DataType>> GetRunEndEncoded(const JsonObject& json_type,
                                                   const FieldVector& children) {
  if (children.size() != 2) {
    return Status::Invalid("Run-end encoded array must have exactly 2 fields, but got ",
                           children.size());
  }
  if (children[0]->name() != "run_ends") {
    return Status::Invalid(
        "First child of run-end encoded array must be called run_ends, but got: ",
        children[0]->name());
  }
  if (children[1]->name() != "values") {
    return Status::Invalid(
        "Second child of run-end encoded array must be called values, but got: ",
        children[1]->name());
  }
  if (!is_run_end_type(children[0]->type()->id())) {
    return Status::Invalid(
        "Only int16, int32, and int64 types are supported"
        " as run ends array type, but got: ",
        children[0]->type());
  }
  if (children[0]->nullable()) {
    return Status::Invalid("Run ends array should not be nullable");
  }
  return run_end_encoded(children[0]->type(), children[1]->type());
}

Result<std::shared_ptr<DataType>> GetType(const JsonObject& json_type,
                                          const FieldVector& children) {
  ARROW_ASSIGN_OR_RAISE(const auto type_name, GetMemberString(json_type, "name"));

  if (type_name == "int") {
    return GetInteger(json_type);
  } else if (type_name == "floatingpoint") {
    return GetFloatingPoint(json_type);
  } else if (type_name == "bool") {
    return boolean();
  } else if (type_name == "utf8") {
    return utf8();
  } else if (type_name == "binary") {
    return binary();
  } else if (type_name == "utf8view") {
    return utf8_view();
  } else if (type_name == "binaryview") {
    return binary_view();
  } else if (type_name == "largeutf8") {
    return large_utf8();
  } else if (type_name == "largebinary") {
    return large_binary();
  } else if (type_name == "fixedsizebinary") {
    return GetFixedSizeBinary(json_type);
  } else if (type_name == "decimal") {
    return GetDecimal(json_type);
  } else if (type_name == "null") {
    return null();
  } else if (type_name == "date") {
    return GetDate(json_type);
  } else if (type_name == "time") {
    return GetTime(json_type);
  } else if (type_name == "timestamp") {
    return GetTimestamp(json_type);
  } else if (type_name == "interval") {
    return GetInterval(json_type);
  } else if (type_name == kDuration) {
    return GetDuration(json_type);
  } else if (type_name == "list") {
    if (children.size() != 1) {
      return Status::Invalid("List must have exactly one child");
    }
    return list(children[0]);
  } else if (type_name == "largelist") {
    if (children.size() != 1) {
      return Status::Invalid("Large list must have exactly one child");
    }
    return large_list(children[0]);
  } else if (type_name == "listview") {
    if (children.size() != 1) {
      return Status::Invalid("List-view must have exactly one child");
    }
    return list_view(children[0]);
  } else if (type_name == "largelistview") {
    if (children.size() != 1) {
      return Status::Invalid("Large list-view must have exactly one child");
    }
    return large_list_view(children[0]);
  } else if (type_name == "map") {
    return GetMap(json_type, children);
  } else if (type_name == "fixedsizelist") {
    return GetFixedSizeList(json_type, children);
  } else if (type_name == "struct") {
    return struct_(children);
  } else if (type_name == "union") {
    return GetUnion(json_type, children);
  } else if (type_name == "runendencoded") {
    return GetRunEndEncoded(json_type, children);
  }
  return Status::Invalid("Unrecognized type name: ", type_name);
}

Result<std::shared_ptr<Field>> GetField(const JsonValue& obj, FieldPosition field_pos,
                                        DictionaryMemo* dictionary_memo);

Result<FieldVector> GetFieldsFromArray(const JsonArray& json_fields,
                                       FieldPosition parent_pos,
                                       DictionaryMemo* dictionary_memo) {
  FieldVector fields(json_fields.size());
  for (auto [json_field, field, i] : Zip(json_fields, fields, Enumerate<int>)) {
    ARROW_ASSIGN_OR_RAISE(field,
                          GetField(json_field, parent_pos.child(i), dictionary_memo));
  }
  return fields;
}

Status ParseDictionary(const JsonObject& obj, int64_t* id, bool* is_ordered,
                       std::shared_ptr<DataType>* index_type) {
  ARROW_ASSIGN_OR_RAISE(*id, GetMemberInt<int64_t>(obj, "id"));
  ARROW_ASSIGN_OR_RAISE(*is_ordered, GetMemberBool(obj, "isOrdered"));

  ARROW_ASSIGN_OR_RAISE(const auto json_index_type, GetMemberObject(obj, "indexType"));

  ARROW_ASSIGN_OR_RAISE(const auto type_name, GetMemberString(json_index_type, "name"));
  if (type_name != "int") {
    return Status::Invalid("Dictionary indices can only be integers");
  }
  return GetInteger(json_index_type).Value(index_type);
}

template <typename FieldOrStruct>
Result<std::shared_ptr<KeyValueMetadata>> GetKeyValueMetadata(
    const FieldOrStruct& field_or_struct) {
  auto metadata = std::make_shared<KeyValueMetadata>();

  auto metadata_result = field_or_struct["metadata"];
  if (metadata_result.error() == simdjson::NO_SUCH_FIELD) {
    return metadata;
  }

  ARROW_ASSIGN_OR_RAISE(
      auto metadata_value,
      internal::ResolveSimdjsonResult(metadata_result, "Failed to get metadata"));

  if (metadata_value.is_null()) {
    return metadata;
  }

  ARROW_ASSIGN_OR_RAISE(auto metadata_array,
                        internal::ResolveSimdjsonResult(metadata_value.get_array(),
                                                        "Metadata was not a JSON array"));

  for (const auto& val : metadata_array) {
    ARROW_ASSIGN_OR_RAISE(
        auto key_value_pair,
        internal::ResolveSimdjsonResult(val.get_object(),
                                        "Metadata KeyValue was not a JSON object"));

    ARROW_ASSIGN_OR_RAISE(const auto key, GetMemberString(key_value_pair, "key"));
    ARROW_ASSIGN_OR_RAISE(const auto value, GetMemberString(key_value_pair, "value"));

    metadata->Append(std::move(key), std::move(value));
  }

  return metadata;
}

Result<std::shared_ptr<Field>> GetField(const JsonValue& obj, FieldPosition field_pos,
                                        DictionaryMemo* dictionary_memo) {
  ARROW_ASSIGN_OR_RAISE(
      auto json_field,
      internal::ResolveSimdjsonResult(obj.get_object(), "Field was not a JSON object"));

  ARROW_ASSIGN_OR_RAISE(const auto name, GetMemberString(json_field, "name"));
  ARROW_ASSIGN_OR_RAISE(const bool nullable, GetMemberBool(json_field, "nullable"));

  ARROW_ASSIGN_OR_RAISE(const auto json_type, GetMemberObject(json_field, "type"));
  ARROW_ASSIGN_OR_RAISE(const auto json_children, GetMemberArray(json_field, "children"));

  ARROW_ASSIGN_OR_RAISE(FieldVector children,
                        GetFieldsFromArray(json_children, field_pos, dictionary_memo));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> type, GetType(json_type, children));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<KeyValueMetadata> metadata,
                        GetKeyValueMetadata(json_field));

  // Is it a dictionary type?
  int64_t dictionary_id = -1;
  std::shared_ptr<DataType> dict_value_type;

  if (dictionary_memo != nullptr) {
    auto dictionary_result = json_field["dictionary"];

    if (dictionary_result.error() != simdjson::NO_SUCH_FIELD) {
      ARROW_ASSIGN_OR_RAISE(auto dictionary_value,
                            internal::ResolveSimdjsonResult(
                                dictionary_result, "Failed to get dictionary field"));

      ARROW_ASSIGN_OR_RAISE(
          auto dictionary_object,
          internal::ResolveSimdjsonResult(dictionary_value.get_object(),
                                          "dictionary was not a JSON object"));

      bool is_ordered{};
      std::shared_ptr<DataType> index_type;
      RETURN_NOT_OK(
          ParseDictionary(dictionary_object, &dictionary_id, &is_ordered, &index_type));

      dict_value_type = type;
      type = ::arrow::dictionary(index_type, type, is_ordered);
    }
  }

  // Is it an extension type?
  int ext_name_index = metadata->FindKey(kExtensionTypeKeyName);
  if (ext_name_index != -1) {
    const auto& ext_name = metadata->value(ext_name_index);
    ARROW_ASSIGN_OR_RAISE(auto ext_data, metadata->Get(kExtensionMetadataKeyName));

    auto ext_type = GetExtensionType(ext_name);
    if (ext_type == nullptr) {
      // Some integration tests check that unregistered extensions pass through
      auto maybe_value = metadata->Get("ARROW:integration:allow_unregistered_extension");
      if (!maybe_value.ok() || *maybe_value != "true") {
        return Status::KeyError("Extension type '", ext_name, "' not found");
      }
    } else {
      ARROW_ASSIGN_OR_RAISE(type, ext_type->Deserialize(type, ext_data));

      // Remove extension type metadata, for exact roundtripping
      RETURN_NOT_OK(metadata->Delete(kExtensionTypeKeyName));
      RETURN_NOT_OK(metadata->Delete(kExtensionMetadataKeyName));
    }
  }

  // Create field
  auto field = ::arrow::field(name, type, nullable, metadata);
  if (dictionary_id != -1) {
    RETURN_NOT_OK(dictionary_memo->fields().AddField(dictionary_id, field_pos.path()));
    RETURN_NOT_OK(dictionary_memo->AddDictionaryType(dictionary_id, dict_value_type));
  }

  return field;
}

template <typename T>
enable_if_boolean<T, Result<bool>> UnboxValue(const JsonValue& val) {
  return internal::ResolveSimdjsonResult(val.get_bool(), "Expected boolean value");
}

template <typename T, typename CType = typename T::c_type>
enable_if_t<is_physical_integer_type<T>::value && sizeof(CType) != sizeof(int64_t),
            Result<CType>>
UnboxValue(const JsonValue& val) {
  ARROW_ASSIGN_OR_RAISE(auto integer, internal::ResolveSimdjsonResult(
                                          val.get_int64(), "Expected integer value"));
  return static_cast<CType>(integer);
}

template <typename T, typename CType = typename T::c_type>
enable_if_t<is_physical_integer_type<T>::value && sizeof(CType) == sizeof(int64_t),
            Result<CType>>
UnboxValue(const JsonValue& val) {
  ARROW_ASSIGN_OR_RAISE(auto string, internal::ResolveSimdjsonResult(
                                         val.get_string(), "Expected string value"));

  CType out;
  if (!::arrow::internal::ParseValue<typename CTypeTraits<CType>::ArrowType>(
          string.data(), string.size(), &out)) {
    return Status::Invalid("Failed to parse integer value");
  }
  return out;
}

template <typename T>
enable_if_physical_floating_point<T, Result<typename T::c_type>> UnboxValue(
    const JsonValue& val) {
  ARROW_ASSIGN_OR_RAISE(
      auto floating_point,
      internal::ResolveSimdjsonResult(val.get_double(), "Expected floating point value"));
  return static_cast<typename T::c_type>(floating_point);
}

class ArrayReader {
 public:
  ArrayReader(const JsonObject& obj, MemoryPool* pool,
              const std::shared_ptr<Field>& field)
      : obj_(obj), pool_(pool), field_(field), type_(field->type()) {}

  template <typename BuilderType>
  Status FinishBuilder(BuilderType* builder) {
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(builder->Finish(&array));
    data_ = array->data();
    return Status::OK();
  }

  Result<JsonArray> GetDataArray(const JsonObject& obj, std::string_view key = kData) {
    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetMemberArray(obj, key));

    if (static_cast<int32_t>(json_data_arr.size()) != length_) {
      return Status::Invalid("JSON ", key, " array size ", json_data_arr.size(),
                             " differs from advertised array length ", length_);
    }

    return json_data_arr;
  }

  template <typename T>
  enable_if_has_c_type<T, Status> Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetDataArray(obj_));
    for (auto [is_valid, val] : Zip(is_valid_, json_data_arr)) {
      if (is_valid) {
        ARROW_ASSIGN_OR_RAISE(auto value, UnboxValue<T>(val));
        RETURN_NOT_OK(builder.Append(value));
      } else {
        RETURN_NOT_OK(builder.AppendNull());
      }
    }
    return FinishBuilder(&builder);
  }

  Result<int64_t> ParseOffset(const JsonValue& json_offset) {
    auto integer_result = json_offset.get_int64();
    if (integer_result.error() == simdjson::SUCCESS) {
      return internal::ResolveSimdjsonResult(integer_result, "Expected integer offset");
    }

    return UnboxValue<Int64Type>(json_offset);
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(pool_);
    using offset_type = typename T::offset_type;

    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetDataArray(obj_));
    ARROW_ASSIGN_OR_RAISE(const auto json_offsets, GetMemberArray(obj_, "OFFSET"));
    if (static_cast<int32_t>(json_offsets.size()) != (length_ + 1)) {
      return Status::Invalid(
          "JSON OFFSET array size differs from advertised array length + 1");
    }

    auto offset_it = json_offsets.begin();

    for (auto [is_valid, json_val] : Zip(is_valid_, json_data_arr)) {
      auto offset_start_json = *offset_it;
      ++offset_it;
      auto offset_end_json = *offset_it;

      if (!is_valid) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(auto val, GetStringView(json_val));

      ARROW_ASSIGN_OR_RAISE(auto offset_start, ParseOffset(offset_start_json));
      ARROW_ASSIGN_OR_RAISE(auto offset_end, ParseOffset(offset_end_json));
      DCHECK_GE(offset_end, offset_start);
      auto val_len = static_cast<size_t>(offset_end - offset_start);

      if constexpr (T::is_utf8) {
        if (val.size() != val_len) {
          return Status::Invalid("Value ", std::quoted(val),
                                 " differs from advertised length ", val_len);
        }
        RETURN_NOT_OK(builder.Append(val));
      } else {
        if (val.size() % 2 != 0) {
          return Status::Invalid("Expected base16 hex string");
        }
        if (val.size() / 2 != val_len) {
          return Status::Invalid("Value 0x", val, " differs from advertised byte length ",
                                 val_len);
        }

        ARROW_ASSIGN_OR_RAISE(auto byte_buffer, AllocateBuffer(val_len, pool_));

        uint8_t* byte_buffer_data = byte_buffer->mutable_data();
        for (size_t j = 0; j < val_len; ++j) {
          RETURN_NOT_OK(ParseHexValue(&val[j * 2], &byte_buffer_data[j]));
        }
        RETURN_NOT_OK(
            builder.Append(byte_buffer_data, static_cast<offset_type>(val_len)));
      }
    }

    return FinishBuilder(&builder);
  }

  template <typename ViewType>
  enable_if_binary_view_like<ViewType, Status> Visit(const ViewType& type) {
    ARROW_ASSIGN_OR_RAISE(const auto json_views, GetDataArray(obj_, "VIEWS"));
    ARROW_ASSIGN_OR_RAISE(const auto json_variadic_bufs,
                          GetMemberArray(obj_, "VARIADIC_DATA_BUFFERS"));

    using internal::Zip;

    BufferVector buffers;
    buffers.resize(json_variadic_bufs.size() + 2);
    for (auto [json_buf, buf] : Zip(json_variadic_bufs, std::span{buffers}.subspan(2))) {
      ARROW_ASSIGN_OR_RAISE(auto hex_string, GetStringView(json_buf));
      ARROW_ASSIGN_OR_RAISE(
          buf, AllocateBuffer(static_cast<int64_t>(hex_string.size()) / 2, pool_));
      RETURN_NOT_OK(ParseHexValues(hex_string, buf->mutable_data()));
    }

    TypedBufferBuilder<bool> validity_builder{pool_};
    RETURN_NOT_OK(validity_builder.Resize(length_));
    for (bool is_valid : is_valid_) {
      validity_builder.UnsafeAppend(is_valid);
    }
    ARROW_ASSIGN_OR_RAISE(buffers[0], validity_builder.Finish());

    ARROW_ASSIGN_OR_RAISE(
        buffers[1], AllocateBuffer(length_ * sizeof(BinaryViewType::c_type), pool_));

    std::span views{buffers[1]->mutable_data_as<BinaryViewType::c_type>(),
                    static_cast<size_t>(length_)};

    int64_t null_count = 0;
    for (auto [json_view, out_view, is_valid] : Zip(json_views, views, is_valid_)) {
      if (!is_valid) {
        out_view = {};
        ++null_count;
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(auto json_view_obj, internal::ResolveSimdjsonResult(
                                                    json_view.get_object(),
                                                    "Binary view was not a JSON object"));

      ARROW_ASSIGN_OR_RAISE(const int size, GetMemberInt<int>(json_view_obj, "SIZE"));
      if (size < 0) {
        return Status::Invalid("Invalid binary view SIZE: ", size,
                               ". Expected a non-negative value");
      }

      if (size <= BinaryViewType::kInlineSize) {
        ARROW_ASSIGN_OR_RAISE(const auto inlined,
                              GetMemberString(json_view_obj, "INLINED"));

        out_view.inlined = {size, {}};

        if constexpr (ViewType::is_utf8) {
          if (static_cast<int>(inlined.size()) != size) {
            return Status::Invalid("Invalid binary view INLINED length: ", inlined.size(),
                                   ". Expected exactly ", size, " bytes");
          }
          memcpy(&out_view.inlined.data, inlined.data(), size);
        } else {
          if (static_cast<int>(inlined.size()) != size * 2) {
            return Status::Invalid(
                "Invalid binary view INLINED hex length: ", inlined.size(),
                ". Expected exactly ", size * 2, " characters");
          }
          RETURN_NOT_OK(ParseHexValues(inlined, out_view.inlined.data.data()));
        }
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(const auto prefix,
                            GetMemberString(json_view_obj, "PREFIX_HEX"));
      ARROW_ASSIGN_OR_RAISE(const int buffer_index,
                            GetMemberInt<int>(json_view_obj, "BUFFER_INDEX"));
      ARROW_ASSIGN_OR_RAISE(const int offset, GetMemberInt<int>(json_view_obj, "OFFSET"));
      if (buffer_index < 0) {
        return Status::Invalid("Invalid binary view BUFFER_INDEX: ", buffer_index,
                               ". Expected a non-negative value");
      }
      if (offset < 0) {
        return Status::Invalid("Invalid binary view OFFSET: ", offset,
                               ". Expected a non-negative value");
      }
      if (prefix.size() != BinaryViewType::kPrefixSize * 2) {
        return Status::Invalid("Invalid binary view PREFIX_HEX length: ", prefix.size(),
                               ". Expected exactly ", BinaryViewType::kPrefixSize * 2,
                               " characters");
      }

      const int64_t num_variadic_buffers = static_cast<int64_t>(buffers.size()) - 2;
      if (buffer_index >= num_variadic_buffers) {
        return Status::Invalid("Invalid binary view BUFFER_INDEX: ", buffer_index,
                               ". Expected < ", num_variadic_buffers);
      }

      const int64_t data_buffer_size = buffers[buffer_index + 2]->size();
      if (static_cast<int64_t>(offset) > data_buffer_size ||
          static_cast<int64_t>(size) > data_buffer_size - static_cast<int64_t>(offset)) {
        return Status::Invalid("Invalid binary view range [offset=", offset,
                               ", size=", size, "] for data buffer of size ",
                               data_buffer_size);
      }

      out_view.ref = {
          size,
          {},
          buffer_index,
          offset,
      };

      RETURN_NOT_OK(ParseHexValues(prefix, out_view.ref.prefix.data()));
    }

    data_ = ArrayData::Make(type_, length_, std::move(buffers), null_count);
    return Status::OK();
  }

  Status Visit(const DayTimeIntervalType& type) {
    DayTimeIntervalBuilder builder(pool_);

    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetDataArray(obj_));

    for (auto [is_valid, val] : Zip(is_valid_, json_data_arr)) {
      if (!is_valid) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(
          auto object, internal::ResolveSimdjsonResult(
                           val.get_object(), "Interval value was not a JSON object"));

      DayTimeIntervalType::DayMilliseconds dm;
      ARROW_ASSIGN_OR_RAISE(dm.days, GetMemberInt<int32_t>(object, kDays));
      ARROW_ASSIGN_OR_RAISE(dm.milliseconds,
                            GetMemberInt<int32_t>(object, kMilliseconds));
      RETURN_NOT_OK(builder.Append(dm));
    }
    return FinishBuilder(&builder);
  }

  Status Visit(const MonthDayNanoIntervalType& type) {
    MonthDayNanoIntervalBuilder builder(pool_);

    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetDataArray(obj_));

    for (auto [is_valid, val] : Zip(is_valid_, json_data_arr)) {
      if (!is_valid) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(
          auto object, internal::ResolveSimdjsonResult(
                           val.get_object(), "Interval value was not a JSON object"));

      MonthDayNanoIntervalType::MonthDayNanos mdn;
      ARROW_ASSIGN_OR_RAISE(mdn.months, GetMemberInt<int32_t>(object, kMonths));
      ARROW_ASSIGN_OR_RAISE(mdn.days, GetMemberInt<int32_t>(object, kDays));
      ARROW_ASSIGN_OR_RAISE(mdn.nanoseconds, GetMemberInt<int64_t>(object, kNanoseconds));
      RETURN_NOT_OK(builder.Append(mdn));
    }
    return FinishBuilder(&builder);
  }

  template <typename T>
  enable_if_t<is_fixed_size_binary_type<T>::value && !is_decimal_type<T>::value, Status>
  Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetDataArray(obj_));

    int32_t byte_width = type.byte_width();

    // Allocate space for parsed values
    ARROW_ASSIGN_OR_RAISE(auto byte_buffer, AllocateBuffer(byte_width, pool_));
    uint8_t* byte_buffer_data = byte_buffer->mutable_data();

    for (auto [is_valid, json_val] : Zip(is_valid_, json_data_arr)) {
      if (!is_valid) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(
          auto val,
          internal::ResolveSimdjsonResult(
              json_val.get_string(),
              "Found non-string JSON value when parsing FixedSizeBinary value"));
      if (static_cast<int32_t>(val.size()) != byte_width * 2) {
        DCHECK(false) << "Expected size: " << byte_width * 2 << " got: " << val.size();
      }

      for (int32_t j = 0; j < byte_width; ++j) {
        RETURN_NOT_OK(ParseHexValue(&val[j * 2], &byte_buffer_data[j]));
      }
      RETURN_NOT_OK(builder.Append(byte_buffer_data));
    }
    return FinishBuilder(&builder);
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    ARROW_ASSIGN_OR_RAISE(const auto json_data_arr, GetDataArray(obj_));
    if (static_cast<int64_t>(json_data_arr.size()) != length_) {
      return Status::Invalid("Integer array had unexpected length ", json_data_arr.size(),
                             " (expected ", length_, ")");
    }

    for (auto [is_valid, val] : Zip(is_valid_, json_data_arr)) {
      if (!is_valid) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(auto string,
                            internal::ResolveSimdjsonResult(
                                val.get_string(), "Expected decimal value as string"));

      DCHECK_GT(string.size(), 0) << "Empty string found when parsing Decimal128 value";

      using Value = typename TypeTraits<T>::ScalarType::ValueType;
      ARROW_ASSIGN_OR_RAISE(Value decimal_val, Value::FromString(string));
      RETURN_NOT_OK(builder.Append(decimal_val));
    }

    return FinishBuilder(&builder);
  }

  template <typename T>
  Status GetIntArray(const JsonArray& json_array, const int32_t length,
                     std::shared_ptr<Buffer>* out) {
    if (static_cast<int32_t>(json_array.size()) != length) {
      return Status::Invalid("Integer array had unexpected length ", json_array.size(),
                             " (expected ", length, ")");
    }

    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(length * sizeof(T), pool_));

    T* values = reinterpret_cast<T*>(buffer->mutable_data());

    for (auto [i, val] : Zip(Enumerate<size_t>, json_array)) {
      if constexpr (sizeof(T) < sizeof(int64_t)) {
        ARROW_ASSIGN_OR_RAISE(
            auto integer,
            internal::ResolveSimdjsonResult(val.get_int64(), "Expected integer value"));
        values[i] = static_cast<T>(integer);
      } else {
        // Read 64-bit integers as strings, as JSON numbers cannot represent
        // them exactly.
        ARROW_ASSIGN_OR_RAISE(auto string,
                              internal::ResolveSimdjsonResult(
                                  val.get_string(), "Expected integer value as string"));

        using ArrowType = typename CTypeTraits<T>::ArrowType;
        if (!ParseValue<ArrowType>(string.data(), string.size(), &values[i])) {
          return Status::Invalid("Failed to parse integer: '", std::string(string), "'");
        }
      }
    }

    *out = std::move(buffer);
    return Status::OK();
  }

  template <typename T>
  Status CreateList(const std::shared_ptr<DataType>& type) {
    using offset_type = typename T::offset_type;

    RETURN_NOT_OK(InitializeData(2));

    RETURN_NOT_OK(GetNullBitmap());
    ARROW_ASSIGN_OR_RAISE(const auto json_offsets, GetMemberArray(obj_, "OFFSET"));
    RETURN_NOT_OK(
        GetIntArray<offset_type>(json_offsets, length_ + 1, &data_->buffers[1]));
    RETURN_NOT_OK(GetChildren(obj_, *type));
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    return CreateList<T>(type_);
  }

  template <typename T>
  Status CreateListView(const std::shared_ptr<DataType>& type) {
    using offset_type = typename T::offset_type;

    RETURN_NOT_OK(InitializeData(3));

    RETURN_NOT_OK(GetNullBitmap());
    ARROW_ASSIGN_OR_RAISE(const auto json_offsets, GetMemberArray(obj_, "OFFSET"));
    RETURN_NOT_OK(GetIntArray<offset_type>(json_offsets, length_, &data_->buffers[1]));
    ARROW_ASSIGN_OR_RAISE(const auto json_sizes, GetMemberArray(obj_, "SIZE"));
    RETURN_NOT_OK(GetIntArray<offset_type>(json_sizes, length_, &data_->buffers[2]));
    RETURN_NOT_OK(GetChildren(obj_, *type));
    return Status::OK();
  }

  template <typename T>
  enable_if_list_view<T, Status> Visit(const T& type) {
    return CreateListView<T>(type_);
  }

  Status Visit(const MapType& type) {
    auto list_type = std::make_shared<ListType>(type.value_field());
    RETURN_NOT_OK(CreateList<ListType>(list_type));
    data_->type = type_;
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    RETURN_NOT_OK(InitializeData(1));
    RETURN_NOT_OK(GetNullBitmap());

    RETURN_NOT_OK(GetChildren(obj_, type));
    DCHECK_EQ(data_->child_data[0]->length, type.list_size() * length_);
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    RETURN_NOT_OK(InitializeData(1));

    RETURN_NOT_OK(GetNullBitmap());
    RETURN_NOT_OK(GetChildren(obj_, type));
    return Status::OK();
  }

  Status GetUnionTypeIds() {
    ARROW_ASSIGN_OR_RAISE(const auto json_type_ids, GetMemberArray(obj_, "TYPE_ID"));
    return GetIntArray<uint8_t>(json_type_ids, length_, &data_->buffers[1]);
  }

  Status Visit(const SparseUnionType& type) {
    RETURN_NOT_OK(InitializeData(2));

    RETURN_NOT_OK(GetNullBitmap());
    RETURN_NOT_OK(GetUnionTypeIds());
    RETURN_NOT_OK(GetChildren(obj_, type));
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    RETURN_NOT_OK(InitializeData(3));

    RETURN_NOT_OK(GetNullBitmap());
    RETURN_NOT_OK(GetUnionTypeIds());
    RETURN_NOT_OK(GetChildren(obj_, type));

    ARROW_ASSIGN_OR_RAISE(const auto json_offsets, GetMemberArray(obj_, "OFFSET"));
    return GetIntArray<int32_t>(json_offsets, length_, &data_->buffers[2]);
  }

  Status Visit(const NullType& type) {
    data_ = std::make_shared<NullArray>(length_)->data();
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    ArrayReader parser(obj_, pool_, ::arrow::field("indices", type.index_type()));
    ARROW_ASSIGN_OR_RAISE(data_, parser.Parse());

    data_->type = field_->type();
    // data_->dictionary will be filled later by ResolveDictionaries()
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    RETURN_NOT_OK(InitializeData(1));
    RETURN_NOT_OK(GetNullBitmap());
    RETURN_NOT_OK(GetChildren(obj_, type));
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    ArrayReader parser(obj_, pool_, field_->WithType(type.storage_type()));
    ARROW_ASSIGN_OR_RAISE(data_, parser.Parse());
    data_->type = type_;
    // If the storage array is a dictionary array, lookup its dictionary id
    // using the extension field.
    // (the field is looked up by pointer, so the Field instance constructed
    //  above wouldn't work)
    return Status::OK();
  }

  Status InitializeData(int num_buffers) {
    data_ = std::make_shared<ArrayData>(type_, length_);
    data_->buffers.resize(num_buffers);
    return Status::OK();
  }

  Status GetNullBitmap() {
    const auto length = static_cast<int64_t>(is_valid_.size());

    ARROW_ASSIGN_OR_RAISE(data_->buffers[0], AllocateEmptyBitmap(length, pool_));
    uint8_t* bitmap = data_->buffers[0]->mutable_data();

    data_->null_count = 0;
    for (int64_t i = 0; i < length; ++i) {
      if (is_valid_[i]) {
        bit_util::SetBit(bitmap, i);
      } else {
        ++data_->null_count;
      }
    }
    if (data_->null_count == 0) {
      data_->buffers[0].reset();
    }

    return Status::OK();
  }

  Status GetChildren(const JsonObject& obj, const DataType& type) {
    auto children_result = obj["children"];

    if (children_result.error() == simdjson::NO_SUCH_FIELD) {
      if (type.num_fields() == 0) {
        data_->child_data.clear();
        return Status::OK();
      }
      return Status::Invalid("Expected ", type.num_fields(), " children, but got 0");
    }

    ARROW_ASSIGN_OR_RAISE(
        auto children_value,
        internal::ResolveSimdjsonResult(children_result, "Failed to get JSON field"));

    ARROW_ASSIGN_OR_RAISE(const auto json_children,
                          internal::ResolveSimdjsonResult(children_value.get_array(),
                                                          "field was not an array"));

    if (type.num_fields() != static_cast<int>(json_children.size())) {
      return Status::Invalid("Expected ", type.num_fields(), " children, but got ",
                             json_children.size());
    }

    data_->child_data.resize(type.num_fields());
    for (auto [json_child, child_field, child_data] :
         Zip(json_children, type.fields(), data_->child_data)) {
      ARROW_ASSIGN_OR_RAISE(
          auto child_obj, internal::ResolveSimdjsonResult(json_child.get_object(),
                                                          "Child was not a JSON object"));

      ARROW_ASSIGN_OR_RAISE(const auto name, GetMemberString(child_obj, "name"));
      DCHECK_EQ(name, child_field->name());

      ArrayReader child_reader(child_obj, pool_, child_field);
      ARROW_ASSIGN_OR_RAISE(child_data, child_reader.Parse());
    }

    return Status::OK();
  }

  Status ParseValidityBitmap() {
    ARROW_ASSIGN_OR_RAISE(const auto json_validity, GetMemberArray(obj_, "VALIDITY"));
    if (static_cast<int>(json_validity.size()) != length_) {
      return Status::Invalid("JSON VALIDITY size differs from advertised array length");
    }

    is_valid_.reserve(json_validity.size());
    for (const auto& value : json_validity) {
      ARROW_ASSIGN_OR_RAISE(auto integer,
                            internal::ResolveSimdjsonResult(
                                value.get_int64(), "VALIDITY value was not an integer"));
      is_valid_.push_back(integer != 0);
    }
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> Parse() {
    ARROW_ASSIGN_OR_RAISE(length_, GetMemberInt<int32_t>(obj_, "count"));

    if (::arrow::internal::may_have_validity_bitmap(type_->id())) {
      // Null and union types don't have a validity bitmap
      RETURN_NOT_OK(ParseValidityBitmap());
    }

    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return data_;
  }

 private:
  JsonObject obj_;
  MemoryPool* pool_;
  std::shared_ptr<Field> field_;
  std::shared_ptr<DataType> type_;

  // Parsed common attributes
  std::vector<bool> is_valid_;
  int32_t length_;
  std::shared_ptr<ArrayData> data_;
};

Result<std::shared_ptr<ArrayData>> ReadArrayData(MemoryPool* pool,
                                                 const JsonValue& json_array,
                                                 const std::shared_ptr<Field>& field) {
  ARROW_ASSIGN_OR_RAISE(
      auto obj, internal::ResolveSimdjsonResult(json_array.get_object(),
                                                "Array element was not a JSON object"));

  ArrayReader parser(obj, pool, field);
  return parser.Parse();
}

Status ReadDictionary(const JsonObject& obj, MemoryPool* pool,
                      DictionaryMemo* dictionary_memo) {
  ARROW_ASSIGN_OR_RAISE(int64_t dictionary_id, GetMemberInt<int64_t>(obj, "id"));

  ARROW_ASSIGN_OR_RAISE(const auto batch_obj, GetMemberObject(obj, "data"));

  ARROW_ASSIGN_OR_RAISE(auto value_type,
                        dictionary_memo->GetDictionaryType(dictionary_id));

  ARROW_ASSIGN_OR_RAISE(const int64_t num_rows,
                        GetMemberInt<int64_t>(batch_obj, "count"));
  ARROW_ASSIGN_OR_RAISE(const auto json_columns, GetMemberArray(batch_obj, "columns"));
  if (json_columns.size() != 1) {
    return Status::Invalid("Dictionary batch must contain only one column");
  }

  ARROW_ASSIGN_OR_RAISE(
      auto column, internal::ResolveSimdjsonResult(json_columns.at(0),
                                                   "Failed to get dictionary column"));

  ARROW_ASSIGN_OR_RAISE(auto dict_data,
                        ReadArrayData(pool, column, field("dummy", value_type)));
  if (num_rows != dict_data->length) {
    return Status::Invalid("Dictionary batch length mismatch: advertised (", num_rows,
                           ") != actual (", dict_data->length, ")");
  }
  return dictionary_memo->AddDictionary(dictionary_id, dict_data);
}

Status ReadDictionaries(const JsonValue& doc, MemoryPool* pool,
                        DictionaryMemo* dictionary_memo) {
  ARROW_ASSIGN_OR_RAISE(
      auto obj, internal::ResolveSimdjsonResult(doc.get_object(),
                                                "JSON document was not an object"));

  auto dictionaries_result = obj["dictionaries"];
  if (dictionaries_result.error() == simdjson::NO_SUCH_FIELD) {
    return Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(
      auto dictionaries_value,
      internal::ResolveSimdjsonResult(dictionaries_result, "Failed to get dictionaries"));
  ARROW_ASSIGN_OR_RAISE(auto dictionary_array, internal::ResolveSimdjsonResult(
                                                   dictionaries_value.get_array(),
                                                   "dictionaries was not a JSON array"));

  for (const auto& value : dictionary_array) {
    ARROW_ASSIGN_OR_RAISE(auto dictionary_object,
                          internal::ResolveSimdjsonResult(
                              value.get_object(), "Dictionary was not a JSON object"));
    RETURN_NOT_OK(ReadDictionary(dictionary_object, pool, dictionary_memo));
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<Schema>> ReadSchema(const JsonValue& json_schema, MemoryPool* pool,
                                           DictionaryMemo* dictionary_memo) {
  ARROW_ASSIGN_OR_RAISE(auto schema_document,
                        internal::ResolveSimdjsonResult(json_schema.get_object(),
                                                        "JSON schema was not an object"));

  ARROW_ASSIGN_OR_RAISE(const auto obj_schema,
                        GetMemberObject(schema_document, "schema"));

  ARROW_ASSIGN_OR_RAISE(const auto json_fields, GetMemberArray(obj_schema, "fields"));

  ARROW_ASSIGN_OR_RAISE(auto metadata, GetKeyValueMetadata(obj_schema));
  ARROW_ASSIGN_OR_RAISE(
      FieldVector fields,
      GetFieldsFromArray(json_fields, FieldPosition(), dictionary_memo));

  RETURN_NOT_OK(ReadDictionaries(json_schema, pool, dictionary_memo));

  return ::arrow::schema(fields, metadata);
}

Result<std::shared_ptr<Array>> ReadArray(MemoryPool* pool, const JsonValue& json_array,
                                         const std::shared_ptr<Field>& field) {
  ARROW_ASSIGN_OR_RAISE(auto data, ReadArrayData(pool, json_array, field));
  return MakeArray(data);
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const JsonValue& json_obj, const std::shared_ptr<Schema>& schema,
    DictionaryMemo* dictionary_memo, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto batch_obj,
                        internal::ResolveSimdjsonResult(
                            json_obj.get_object(), "Record batch was not a JSON object"));

  ARROW_ASSIGN_OR_RAISE(const int64_t num_rows,
                        GetMemberInt<int64_t>(batch_obj, "count"));

  ARROW_ASSIGN_OR_RAISE(const auto json_columns, GetMemberArray(batch_obj, "columns"));

  ArrayDataVector columns(json_columns.size());
  for (auto [column, json_column, field] : Zip(columns, json_columns, schema->fields())) {
    ARROW_ASSIGN_OR_RAISE(column, ReadArrayData(pool, json_column, field));
  }

  RETURN_NOT_OK(ResolveDictionaries(columns, *dictionary_memo, pool));

  return RecordBatch::Make(schema, num_rows, columns);
}

Status WriteSchema(const Schema& schema, const DictionaryFieldMapper& mapper,
                   JsonWriter* json_writer) {
  SchemaWriter converter(schema, mapper, json_writer);
  return converter.Write();
}

Status WriteDictionary(int64_t id, const std::shared_ptr<Array>& dictionary,
                       JsonWriter* writer) {
  writer->StartObject();
  writer->Key("id");
  writer->Int(static_cast<int32_t>(id));
  writer->Key("data");

  // Make a dummy record batch. A bit tedious as we have to make a schema
  auto schema = ::arrow::schema({arrow::field("dictionary", dictionary->type())});
  auto batch = RecordBatch::Make(schema, dictionary->length(), {dictionary});
  RETURN_NOT_OK(WriteRecordBatch(*batch, writer));
  writer->EndObject();
  return Status::OK();
}

Status WriteRecordBatch(const RecordBatch& batch, JsonWriter* writer) {
  writer->StartObject();
  writer->Key("count");
  writer->Int(static_cast<int32_t>(batch.num_rows()));

  writer->Key("columns");
  writer->StartArray();

  for (auto [column, i] : Zip(batch.columns(), Enumerate<int>)) {
    DCHECK_EQ(batch.num_rows(), column->length())
        << "Array length did not match record batch length: " << batch.num_rows()
        << " != " << column->length() << " " << batch.column_name(i);

    RETURN_NOT_OK(WriteArray(batch.column_name(i), *column, writer));
  }

  writer->EndArray();
  writer->EndObject();
  return Status::OK();
}

Status WriteArray(const std::string& name, const Array& array, JsonWriter* json_writer) {
  ArrayWriter converter(name, array, json_writer);
  return converter.Write();
}

}  // namespace arrow::internal::integration::json
