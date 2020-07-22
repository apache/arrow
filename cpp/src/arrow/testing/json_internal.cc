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

#include "arrow/testing/json_internal.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"  // IWYU pragma: keep
#include "arrow/extension_type.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/formatting.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace {
constexpr char kData[] = "DATA";
constexpr char kDays[] = "days";
constexpr char kDayTime[] = "DAY_TIME";
constexpr char kDuration[] = "duration";
constexpr char kMilliseconds[] = "milliseconds";
constexpr char kYearMonth[] = "YEAR_MONTH";
}  // namespace

class MemoryPool;

using internal::checked_cast;
using internal::ParseValue;

namespace testing {
namespace json {

using ::arrow::ipc::DictionaryMemo;

static std::string GetFloatingPrecisionName(FloatingPointType::Precision precision) {
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

static std::string GetTimeUnitName(TimeUnit::type unit) {
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

class SchemaWriter {
 public:
  explicit SchemaWriter(const Schema& schema, DictionaryMemo* dictionary_memo,
                        RjWriter* writer)
      : schema_(schema), dictionary_memo_(dictionary_memo), writer_(writer) {}

  Status Write() {
    writer_->Key("schema");
    writer_->StartObject();
    writer_->Key("fields");
    writer_->StartArray();
    for (const std::shared_ptr<Field>& field : schema_.fields()) {
      RETURN_NOT_OK(VisitField(field));
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
      for (int64_t i = 0; i < metadata->size(); ++i) {
        WriteKeyValue(metadata->key(i), metadata->value(i));
      }
    }
    for (const auto& kv : additional_metadata) {
      WriteKeyValue(kv.first, kv.second);
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

  Status VisitField(const std::shared_ptr<Field>& field) {
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
      RETURN_NOT_OK(WriteChildren(dict_type.value_type()->fields()));
      int64_t dictionary_id = -1;
      RETURN_NOT_OK(dictionary_memo_->GetOrAssignId(field, &dictionary_id));
      RETURN_NOT_OK(WriteDictionaryMetadata(dictionary_id, dict_type));
    } else {
      RETURN_NOT_OK(WriteChildren(type->fields()));
    }

    WriteKeyValueMetadata(field->metadata(), additional_metadata);
    writer_->EndObject();

    return Status::OK();
  }

  Status VisitType(const DataType& type);

  template <typename T>
  enable_if_t<is_null_type<T>::value || is_primitive_ctype<T>::value ||
              is_base_binary_type<T>::value || is_base_list_type<T>::value ||
              is_struct_type<T>::value>
  WriteTypeMetadata(const T& type) {}

  void WriteTypeMetadata(const MapType& type) {
    writer_->Key("keysSorted");
    writer_->Int(type.keys_sorted());
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

  void WriteTypeMetadata(const Decimal128Type& type) {
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
    for (size_t i = 0; i < type.type_codes().size(); ++i) {
      writer_->Int(type.type_codes()[i]);
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

  Status WriteChildren(const std::vector<std::shared_ptr<Field>>& children) {
    writer_->Key("children");
    writer_->StartArray();
    for (const std::shared_ptr<Field>& field : children) {
      RETURN_NOT_OK(VisitField(field));
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
  Status Visit(const LargeStringType& type) { return WriteVarBytes("largeutf8", type); }
  Status Visit(const LargeBinaryType& type) { return WriteVarBytes("largebinary", type); }
  Status Visit(const FixedSizeBinaryType& type) {
    return WritePrimitive("fixedsizebinary", type);
  }

  Status Visit(const Decimal128Type& type) { return WritePrimitive("decimal", type); }
  Status Visit(const TimestampType& type) { return WritePrimitive("timestamp", type); }
  Status Visit(const DurationType& type) { return WritePrimitive(kDuration, type); }
  Status Visit(const MonthIntervalType& type) { return WritePrimitive("interval", type); }

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

  Status Visit(const ExtensionType& type) { return Status::NotImplemented(type.name()); }

 private:
  const Schema& schema_;
  DictionaryMemo* dictionary_memo_;
  RjWriter* writer_;
};

Status SchemaWriter::VisitType(const DataType& type) {
  return VisitTypeInline(type, this);
}

class ArrayWriter {
 public:
  ArrayWriter(const std::string& name, const Array& array, RjWriter* writer)
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
        writer_->RawNumber(null_string.data(),
                           static_cast<rj::SizeType>(null_string.size()));
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
        fmt(arr.Value(i), [&](util::string_view repr) {
          writer_->String(repr.data(), static_cast<rj::SizeType>(repr.size()));
        });
      } else {
        writer_->String(null_string.data(),
                        static_cast<rj::SizeType>(null_string.size()));
      }
    }
  }

  template <typename ArrayType>
  enable_if_physical_floating_point<typename ArrayType::TypeClass> WriteDataValues(
      const ArrayType& arr) {
    static const char null_string[] = "0.";
    const auto data = arr.raw_values();
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        writer_->Double(data[i]);
      } else {
        writer_->RawNumber(null_string, sizeof(null_string));
      }
    }
  }

  // Binary, encode to hexadecimal.
  template <typename ArrayType>
  enable_if_binary_like<typename ArrayType::TypeClass> WriteDataValues(
      const ArrayType& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      writer_->String(HexEncode(arr.GetView(i)));
    }
  }

  // UTF8 string, write as is
  template <typename ArrayType>
  enable_if_string_like<typename ArrayType::TypeClass> WriteDataValues(
      const ArrayType& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      auto view = arr.GetView(i);
      writer_->String(view.data(), static_cast<rj::SizeType>(view.size()));
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

  void WriteDataValues(const Decimal128Array& arr) {
    static const char null_string[] = "0";
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        const Decimal128 value(arr.GetValue(i));
        writer_->String(value.ToIntegerString());
      } else {
        writer_->String(null_string, sizeof(null_string));
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
      auto append = [this](util::string_view v) {
        writer_->String(v.data(), static_cast<rj::SizeType>(v.size()));
        return Status::OK();
      };
      for (int i = 0; i < length; ++i) {
        DCHECK_OK(formatter(values[i], append));
      }
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
    writer_->Key("children");
    writer_->StartArray();
    writer_->EndArray();
  }

  Status WriteChildren(const std::vector<std::shared_ptr<Field>>& fields,
                       const std::vector<std::shared_ptr<Array>>& arrays) {
    writer_->Key("children");
    writer_->StartArray();
    for (size_t i = 0; i < fields.size(); ++i) {
      RETURN_NOT_OK(VisitArray(fields[i]->name(), *arrays[i]));
    }
    writer_->EndArray();
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    SetNoChildren();
    return Status::OK();
  }

  template <typename ArrayType>
  enable_if_t<std::is_base_of<PrimitiveArray, ArrayType>::value, Status> Visit(
      const ArrayType& array) {
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

  Status Visit(const ExtensionArray& array) { return VisitArrayValues(*array.storage()); }

 private:
  const std::string& name_;
  const Array& array_;
  RjWriter* writer_;
};

static Status GetObjectInt(const RjObject& obj, const std::string& key, int* out) {
  const auto& it = obj.FindMember(key);
  RETURN_NOT_INT(key, it, obj);
  *out = it->value.GetInt();
  return Status::OK();
}

static Status GetObjectBool(const RjObject& obj, const std::string& key, bool* out) {
  const auto& it = obj.FindMember(key);
  RETURN_NOT_BOOL(key, it, obj);
  *out = it->value.GetBool();
  return Status::OK();
}

static Status GetObjectString(const RjObject& obj, const std::string& key,
                              std::string* out) {
  const auto& it = obj.FindMember(key);
  RETURN_NOT_STRING(key, it, obj);
  *out = it->value.GetString();
  return Status::OK();
}

static Status GetInteger(const rj::Value::ConstObject& json_type,
                         std::shared_ptr<DataType>* type) {
  const auto& it_bit_width = json_type.FindMember("bitWidth");
  RETURN_NOT_INT("bitWidth", it_bit_width, json_type);

  const auto& it_is_signed = json_type.FindMember("isSigned");
  RETURN_NOT_BOOL("isSigned", it_is_signed, json_type);

  bool is_signed = it_is_signed->value.GetBool();
  int bit_width = it_bit_width->value.GetInt();

  switch (bit_width) {
    case 8:
      *type = is_signed ? int8() : uint8();
      break;
    case 16:
      *type = is_signed ? int16() : uint16();
      break;
    case 32:
      *type = is_signed ? int32() : uint32();
      break;
    case 64:
      *type = is_signed ? int64() : uint64();
      break;
    default:
      return Status::Invalid("Invalid bit width: ", bit_width);
  }
  return Status::OK();
}

static Status GetFloatingPoint(const RjObject& json_type,
                               std::shared_ptr<DataType>* type) {
  const auto& it_precision = json_type.FindMember("precision");
  RETURN_NOT_STRING("precision", it_precision, json_type);

  std::string precision = it_precision->value.GetString();

  if (precision == "DOUBLE") {
    *type = float64();
  } else if (precision == "SINGLE") {
    *type = float32();
  } else if (precision == "HALF") {
    *type = float16();
  } else {
    return Status::Invalid("Invalid precision: ", precision);
  }
  return Status::OK();
}

static Status GetMap(const RjObject& json_type,
                     const std::vector<std::shared_ptr<Field>>& children,
                     std::shared_ptr<DataType>* type) {
  if (children.size() != 1) {
    return Status::Invalid("Map must have exactly one child");
  }

  const auto& it_keys_sorted = json_type.FindMember("keysSorted");
  RETURN_NOT_BOOL("keysSorted", it_keys_sorted, json_type);
  bool keys_sorted = it_keys_sorted->value.GetBool();

  return MapType::Make(children[0], keys_sorted).Value(type);
}

static Status GetFixedSizeBinary(const RjObject& json_type,
                                 std::shared_ptr<DataType>* type) {
  const auto& it_byte_width = json_type.FindMember("byteWidth");
  RETURN_NOT_INT("byteWidth", it_byte_width, json_type);

  int32_t byte_width = it_byte_width->value.GetInt();
  *type = fixed_size_binary(byte_width);
  return Status::OK();
}

static Status GetFixedSizeList(const RjObject& json_type,
                               const std::vector<std::shared_ptr<Field>>& children,
                               std::shared_ptr<DataType>* type) {
  if (children.size() != 1) {
    return Status::Invalid("FixedSizeList must have exactly one child");
  }

  const auto& it_list_size = json_type.FindMember("listSize");
  RETURN_NOT_INT("listSize", it_list_size, json_type);

  int32_t list_size = it_list_size->value.GetInt();
  *type = fixed_size_list(children[0], list_size);
  return Status::OK();
}

static Status GetDecimal(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_precision = json_type.FindMember("precision");
  const auto& it_scale = json_type.FindMember("scale");

  RETURN_NOT_INT("precision", it_precision, json_type);
  RETURN_NOT_INT("scale", it_scale, json_type);

  *type = decimal(it_precision->value.GetInt(), it_scale->value.GetInt());
  return Status::OK();
}

static Status GetDate(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  std::string unit_str = it_unit->value.GetString();

  if (unit_str == "DAY") {
    *type = date32();
  } else if (unit_str == "MILLISECOND") {
    *type = date64();
  } else {
    return Status::Invalid("Invalid date unit: ", unit_str);
  }
  return Status::OK();
}

static Status GetTime(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  const auto& it_bit_width = json_type.FindMember("bitWidth");
  RETURN_NOT_INT("bitWidth", it_bit_width, json_type);

  std::string unit_str = it_unit->value.GetString();

  if (unit_str == "SECOND") {
    *type = time32(TimeUnit::SECOND);
  } else if (unit_str == "MILLISECOND") {
    *type = time32(TimeUnit::MILLI);
  } else if (unit_str == "MICROSECOND") {
    *type = time64(TimeUnit::MICRO);
  } else if (unit_str == "NANOSECOND") {
    *type = time64(TimeUnit::NANO);
  } else {
    return Status::Invalid("Invalid time unit: ", unit_str);
  }

  const auto& fw_type = checked_cast<const FixedWidthType&>(**type);

  int bit_width = it_bit_width->value.GetInt();
  if (bit_width != fw_type.bit_width()) {
    return Status::Invalid("Indicated bit width does not match unit");
  }

  return Status::OK();
}

static Status GetUnitFromString(const std::string& unit_str, TimeUnit::type* unit) {
  if (unit_str == "SECOND") {
    *unit = TimeUnit::SECOND;
  } else if (unit_str == "MILLISECOND") {
    *unit = TimeUnit::MILLI;
  } else if (unit_str == "MICROSECOND") {
    *unit = TimeUnit::MICRO;
  } else if (unit_str == "NANOSECOND") {
    *unit = TimeUnit::NANO;
  } else {
    return Status::Invalid("Invalid time unit: ", unit_str);
  }
  return Status::OK();
}

static Status GetDuration(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  std::string unit_str = it_unit->value.GetString();

  TimeUnit::type unit;
  RETURN_NOT_OK(GetUnitFromString(unit_str, &unit));

  *type = duration(unit);

  return Status::OK();
}

static Status GetTimestamp(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  std::string unit_str = it_unit->value.GetString();

  TimeUnit::type unit;
  RETURN_NOT_OK(GetUnitFromString(unit_str, &unit));

  const auto& it_tz = json_type.FindMember("timezone");
  if (it_tz == json_type.MemberEnd()) {
    *type = timestamp(unit);
  } else {
    *type = timestamp(unit, it_tz->value.GetString());
  }

  return Status::OK();
}

static Status GetInterval(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  std::string unit_name = it_unit->value.GetString();

  if (unit_name == kDayTime) {
    *type = day_time_interval();
  } else if (unit_name == kYearMonth) {
    *type = month_interval();
  } else {
    return Status::Invalid("Invalid interval unit: " + unit_name);
  }
  return Status::OK();
}

static Status GetUnion(const RjObject& json_type,
                       const std::vector<std::shared_ptr<Field>>& children,
                       std::shared_ptr<DataType>* type) {
  const auto& it_mode = json_type.FindMember("mode");
  RETURN_NOT_STRING("mode", it_mode, json_type);

  std::string mode_str = it_mode->value.GetString();
  UnionMode::type mode;

  if (mode_str == "SPARSE") {
    mode = UnionMode::SPARSE;
  } else if (mode_str == "DENSE") {
    mode = UnionMode::DENSE;
  } else {
    return Status::Invalid("Invalid union mode: ", mode_str);
  }

  const auto& it_type_codes = json_type.FindMember("typeIds");
  RETURN_NOT_ARRAY("typeIds", it_type_codes, json_type);

  std::vector<int8_t> type_codes;
  const auto& id_array = it_type_codes->value.GetArray();
  type_codes.reserve(id_array.Size());
  for (const rj::Value& val : id_array) {
    DCHECK(val.IsInt());
    type_codes.push_back(static_cast<int8_t>(val.GetInt()));
  }

  if (mode == UnionMode::SPARSE) {
    *type = sparse_union(std::move(children), std::move(type_codes));
  } else {
    *type = dense_union(std::move(children), std::move(type_codes));
  }

  return Status::OK();
}

static Status GetType(const RjObject& json_type,
                      const std::vector<std::shared_ptr<Field>>& children,
                      std::shared_ptr<DataType>* type) {
  const auto& it_type_name = json_type.FindMember("name");
  RETURN_NOT_STRING("name", it_type_name, json_type);

  std::string type_name = it_type_name->value.GetString();

  if (type_name == "int") {
    return GetInteger(json_type, type);
  } else if (type_name == "floatingpoint") {
    return GetFloatingPoint(json_type, type);
  } else if (type_name == "bool") {
    *type = boolean();
  } else if (type_name == "utf8") {
    *type = utf8();
  } else if (type_name == "binary") {
    *type = binary();
  } else if (type_name == "largeutf8") {
    *type = large_utf8();
  } else if (type_name == "largebinary") {
    *type = large_binary();
  } else if (type_name == "fixedsizebinary") {
    return GetFixedSizeBinary(json_type, type);
  } else if (type_name == "decimal") {
    return GetDecimal(json_type, type);
  } else if (type_name == "null") {
    *type = null();
  } else if (type_name == "date") {
    return GetDate(json_type, type);
  } else if (type_name == "time") {
    return GetTime(json_type, type);
  } else if (type_name == "timestamp") {
    return GetTimestamp(json_type, type);
  } else if (type_name == "interval") {
    return GetInterval(json_type, type);
  } else if (type_name == kDuration) {
    return GetDuration(json_type, type);
  } else if (type_name == "list") {
    if (children.size() != 1) {
      return Status::Invalid("List must have exactly one child");
    }
    *type = list(children[0]);
  } else if (type_name == "largelist") {
    if (children.size() != 1) {
      return Status::Invalid("Large list must have exactly one child");
    }
    *type = large_list(children[0]);
  } else if (type_name == "map") {
    return GetMap(json_type, children, type);
  } else if (type_name == "fixedsizelist") {
    return GetFixedSizeList(json_type, children, type);
  } else if (type_name == "struct") {
    *type = struct_(children);
  } else if (type_name == "union") {
    return GetUnion(json_type, children, type);
  } else {
    return Status::Invalid("Unrecognized type name: ", type_name);
  }
  return Status::OK();
}

static Status GetField(const rj::Value& obj, DictionaryMemo* dictionary_memo,
                       std::shared_ptr<Field>* field);

static Status GetFieldsFromArray(const rj::Value& obj, DictionaryMemo* dictionary_memo,
                                 std::vector<std::shared_ptr<Field>>* fields) {
  const auto& values = obj.GetArray();

  fields->resize(values.Size());
  for (rj::SizeType i = 0; i < fields->size(); ++i) {
    RETURN_NOT_OK(GetField(values[i], dictionary_memo, &(*fields)[i]));
  }
  return Status::OK();
}

static Status ParseDictionary(const RjObject& obj, int64_t* id, bool* is_ordered,
                              std::shared_ptr<DataType>* index_type) {
  int32_t int32_id;
  RETURN_NOT_OK(GetObjectInt(obj, "id", &int32_id));
  *id = int32_id;

  RETURN_NOT_OK(GetObjectBool(obj, "isOrdered", is_ordered));

  const auto& it_index_type = obj.FindMember("indexType");
  RETURN_NOT_OBJECT("indexType", it_index_type, obj);

  const auto& json_index_type = it_index_type->value.GetObject();

  std::string type_name;
  RETURN_NOT_OK(GetObjectString(json_index_type, "name", &type_name));
  if (type_name != "int") {
    return Status::Invalid("Dictionary indices can only be integers");
  }
  return GetInteger(json_index_type, index_type);
}

template <typename FieldOrStruct>
static Status GetKeyValueMetadata(const FieldOrStruct& field_or_struct,
                                  std::shared_ptr<KeyValueMetadata>* out) {
  out->reset(new KeyValueMetadata);
  auto it = field_or_struct.FindMember("metadata");
  if (it == field_or_struct.MemberEnd()) {
    return Status::OK();
  }

  if (it->value.IsNull()) {
    return Status::OK();
  }

  if (!it->value.IsArray()) {
    return Status::Invalid("Metadata was not a JSON array");
  }
  const auto& key_value_pairs = it->value.GetArray();

  for (auto it = key_value_pairs.Begin(); it != key_value_pairs.End(); ++it) {
    if (!it->IsObject()) {
      return Status::Invalid("Metadata KeyValue was not a JSON object");
    }
    const auto& key_value_pair = it->GetObject();

    std::string key, value;
    RETURN_NOT_OK(GetObjectString(key_value_pair, "key", &key));
    RETURN_NOT_OK(GetObjectString(key_value_pair, "value", &value));

    (*out)->Append(std::move(key), std::move(value));
  }
  return Status::OK();
}

static Status GetField(const rj::Value& obj, DictionaryMemo* dictionary_memo,
                       std::shared_ptr<Field>* field) {
  if (!obj.IsObject()) {
    return Status::Invalid("Field was not a JSON object");
  }
  const auto& json_field = obj.GetObject();

  std::string name;
  bool nullable;
  RETURN_NOT_OK(GetObjectString(json_field, "name", &name));
  RETURN_NOT_OK(GetObjectBool(json_field, "nullable", &nullable));

  std::shared_ptr<DataType> type;
  const auto& it_type = json_field.FindMember("type");
  RETURN_NOT_OBJECT("type", it_type, json_field);

  const auto& it_children = json_field.FindMember("children");
  RETURN_NOT_ARRAY("children", it_children, json_field);

  std::vector<std::shared_ptr<Field>> children;
  RETURN_NOT_OK(GetFieldsFromArray(it_children->value, dictionary_memo, &children));
  RETURN_NOT_OK(GetType(it_type->value.GetObject(), children, &type));

  std::shared_ptr<KeyValueMetadata> metadata;
  RETURN_NOT_OK(GetKeyValueMetadata(json_field, &metadata));

  // Is it a dictionary type?
  int64_t dictionary_id = -1;
  const auto& it_dictionary = json_field.FindMember("dictionary");
  if (dictionary_memo != nullptr && it_dictionary != json_field.MemberEnd()) {
    // Parse dictionary id in JSON and add dictionary field to the
    // memo, and parse the dictionaries later
    RETURN_NOT_OBJECT("dictionary", it_dictionary, json_field);
    bool is_ordered;
    std::shared_ptr<DataType> index_type;
    RETURN_NOT_OK(ParseDictionary(it_dictionary->value.GetObject(), &dictionary_id,
                                  &is_ordered, &index_type));

    type = ::arrow::dictionary(index_type, type, is_ordered);
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
  *field = ::arrow::field(name, type, nullable, metadata);
  if (dictionary_id != -1) {
    RETURN_NOT_OK(dictionary_memo->AddField(dictionary_id, *field));
  }

  return Status::OK();
}

template <typename T>
enable_if_boolean<T, bool> UnboxValue(const rj::Value& val) {
  DCHECK(val.IsBool());
  return val.GetBool();
}

template <typename T, typename CType = typename T::c_type>
enable_if_t<is_physical_integer_type<T>::value && sizeof(CType) != sizeof(int64_t), CType>
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsInt64());
  return static_cast<CType>(val.GetInt64());
}

template <typename T, typename CType = typename T::c_type>
enable_if_t<is_physical_integer_type<T>::value && sizeof(CType) == sizeof(int64_t), CType>
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsString());

  CType out;
  bool success = ::arrow::internal::ParseValue<typename CTypeTraits<CType>::ArrowType>(
      val.GetString(), val.GetStringLength(), &out);

  DCHECK(success);
  return out;
}

template <typename T>
enable_if_physical_floating_point<T, typename T::c_type> UnboxValue(
    const rj::Value& val) {
  DCHECK(val.IsFloat());
  return static_cast<typename T::c_type>(val.GetDouble());
}

class ArrayReader {
 public:
  ArrayReader(const RjObject& obj, MemoryPool* pool, const std::shared_ptr<Field>& field,
              DictionaryMemo* dictionary_memo)
      : obj_(obj),
        pool_(pool),
        field_(field),
        type_(field->type()),
        dictionary_memo_(dictionary_memo),
        dictionary_id_(-1) {}

  template <typename T>
  enable_if_has_c_type<T, Status> Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    const auto& json_data = obj_.FindMember(kData);
    RETURN_NOT_ARRAY(kData, json_data, obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);
    for (int i = 0; i < length_; ++i) {
      if (!is_valid_[i]) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      RETURN_NOT_OK(builder.Append(UnboxValue<T>(val)));
    }

    return builder.Finish(&result_);
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(pool_);
    using offset_type = typename T::offset_type;

    const auto& json_data = obj_.FindMember(kData);
    RETURN_NOT_ARRAY(kData, json_data, obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);

    for (int i = 0; i < length_; ++i) {
      if (!is_valid_[i]) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      DCHECK(val.IsString());

      if (T::is_utf8) {
        RETURN_NOT_OK(builder.Append(val.GetString()));
      } else {
        std::string hex_string = val.GetString();

        if (hex_string.size() % 2 != 0) {
          return Status::Invalid("Expected base16 hex string");
        }
        const auto value_len = static_cast<int64_t>(hex_string.size()) / 2;

        ARROW_ASSIGN_OR_RAISE(auto byte_buffer, AllocateBuffer(value_len, pool_));

        const char* hex_data = hex_string.c_str();
        uint8_t* byte_buffer_data = byte_buffer->mutable_data();
        for (int64_t j = 0; j < value_len; ++j) {
          RETURN_NOT_OK(ParseHexValue(hex_data + j * 2, &byte_buffer_data[j]));
        }
        RETURN_NOT_OK(
            builder.Append(byte_buffer_data, static_cast<offset_type>(value_len)));
      }
    }

    return builder.Finish(&result_);
  }

  Status Visit(const DayTimeIntervalType& type) {
    DayTimeIntervalBuilder builder(pool_);

    const auto& json_data = obj_.FindMember(kData);
    RETURN_NOT_ARRAY(kData, json_data, obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_)
        << "data length: " << json_data_arr.Size() << " != length_: " << length_;

    for (int i = 0; i < length_; ++i) {
      if (!is_valid_[i]) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      DCHECK(val.IsObject());
      DayTimeIntervalType::DayMilliseconds dm = {0, 0};
      dm.days = val[kDays].GetInt();
      dm.milliseconds = val[kMilliseconds].GetInt();
      RETURN_NOT_OK(builder.Append(dm));
    }
    return builder.Finish(&result_);
  }

  template <typename T>
  enable_if_t<is_fixed_size_binary_type<T>::value && !is_decimal_type<T>::value, Status>
  Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    const auto& json_data = obj_.FindMember(kData);
    RETURN_NOT_ARRAY(kData, json_data, obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);
    int32_t byte_width = type.byte_width();

    // Allocate space for parsed values
    ARROW_ASSIGN_OR_RAISE(auto byte_buffer, AllocateBuffer(byte_width, pool_));
    uint8_t* byte_buffer_data = byte_buffer->mutable_data();

    for (int i = 0; i < length_; ++i) {
      if (!is_valid_[i]) {
        RETURN_NOT_OK(builder.AppendNull());
      } else {
        const rj::Value& val = json_data_arr[i];
        DCHECK(val.IsString())
            << "Found non-string JSON value when parsing FixedSizeBinary value";
        std::string hex_string = val.GetString();
        if (static_cast<int32_t>(hex_string.size()) != byte_width * 2) {
          DCHECK(false) << "Expected size: " << byte_width * 2
                        << " got: " << hex_string.size();
        }
        const char* hex_data = hex_string.c_str();

        for (int32_t j = 0; j < byte_width; ++j) {
          RETURN_NOT_OK(ParseHexValue(hex_data + j * 2, &byte_buffer_data[j]));
        }
        RETURN_NOT_OK(builder.Append(byte_buffer_data));
      }
    }
    return builder.Finish(&result_);
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    const auto& json_data = obj_.FindMember(kData);
    RETURN_NOT_ARRAY(kData, json_data, obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);

    for (int i = 0; i < length_; ++i) {
      if (!is_valid_[i]) {
        RETURN_NOT_OK(builder.AppendNull());
      } else {
        const rj::Value& val = json_data_arr[i];
        DCHECK(val.IsString())
            << "Found non-string JSON value when parsing Decimal128 value";
        DCHECK_GT(val.GetStringLength(), 0)
            << "Empty string found when parsing Decimal128 value";

        Decimal128 value;
        ARROW_ASSIGN_OR_RAISE(value, Decimal128::FromString(val.GetString()));
        RETURN_NOT_OK(builder.Append(value));
      }
    }
    return builder.Finish(&result_);
  }

  template <typename T>
  Status GetIntArray(const RjArray& json_array, const int32_t length,
                     std::shared_ptr<Buffer>* out) {
    using ArrowType = typename CTypeTraits<T>::ArrowType;
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(length * sizeof(T), pool_));

    T* values = reinterpret_cast<T*>(buffer->mutable_data());
    if (sizeof(T) < sizeof(int64_t)) {
      for (int i = 0; i < length; ++i) {
        const rj::Value& val = json_array[i];
        DCHECK(val.IsInt() || val.IsInt64());
        if (val.IsInt()) {
          values[i] = static_cast<T>(val.GetInt());
        } else {
          values[i] = static_cast<T>(val.GetInt64());
        }
      }
    } else {
      // Read 64-bit integers as strings, as JSON numbers cannot represent
      // them exactly.

      for (int i = 0; i < length; ++i) {
        const rj::Value& val = json_array[i];
        DCHECK(val.IsString());
        if (!ParseValue<ArrowType>(val.GetString(), val.GetStringLength(), &values[i])) {
          return Status::Invalid("Failed to parse integer: '",
                                 std::string(val.GetString(), val.GetStringLength()),
                                 "'");
        }
      }
    }

    *out = std::move(buffer);
    return Status::OK();
  }

  template <typename T>
  Status CreateList(const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
    using offset_type = typename T::offset_type;
    using ArrayType = typename TypeTraits<T>::ArrayType;

    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid_, &null_count, &validity_buffer));

    const auto& json_offsets = obj_.FindMember("OFFSET");
    RETURN_NOT_ARRAY("OFFSET", json_offsets, obj_);
    std::shared_ptr<Buffer> offsets_buffer;
    RETURN_NOT_OK(GetIntArray<offset_type>(json_offsets->value.GetArray(), length_ + 1,
                                           &offsets_buffer));

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(obj_, *type, &children));
    DCHECK_EQ(children.size(), 1);

    out->reset(new ArrayType(type, length_, offsets_buffer, children[0], validity_buffer,
                             null_count));
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    return CreateList<T>(type_, &result_);
  }

  Status Visit(const MapType& type) {
    auto list_type = std::make_shared<ListType>(type.value_field());
    std::shared_ptr<Array> list_array;
    RETURN_NOT_OK(CreateList<ListType>(list_type, &list_array));
    auto map_data = list_array->data();
    map_data->type = type_;
    result_ = std::make_shared<MapArray>(map_data);
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid_, &null_count, &validity_buffer));

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(obj_, type, &children));
    DCHECK_EQ(children.size(), 1);
    DCHECK_EQ(children[0]->length(), type.list_size() * length_);

    result_ = std::make_shared<FixedSizeListArray>(type_, length_, children[0],
                                                   validity_buffer, null_count);

    return Status::OK();
  }

  Status Visit(const StructType& type) {
    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid_, &null_count, &validity_buffer));

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(GetChildren(obj_, type, &fields));

    result_ = std::make_shared<StructArray>(type_, length_, fields, validity_buffer,
                                            null_count);

    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    std::shared_ptr<Buffer> type_id_buffer;

    const auto& json_type_ids = obj_.FindMember("TYPE_ID");
    RETURN_NOT_ARRAY("TYPE_ID", json_type_ids, obj_);
    RETURN_NOT_OK(
        GetIntArray<uint8_t>(json_type_ids->value.GetArray(), length_, &type_id_buffer));

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(obj_, type, &children));

    if (type.mode() == UnionMode::SPARSE) {
      result_ =
          std::make_shared<SparseUnionArray>(type_, length_, children, type_id_buffer);
    } else {
      const auto& json_offsets = obj_.FindMember("OFFSET");
      RETURN_NOT_ARRAY("OFFSET", json_offsets, obj_);

      std::shared_ptr<Buffer> offsets_buffer;
      RETURN_NOT_OK(
          GetIntArray<int32_t>(json_offsets->value.GetArray(), length_, &offsets_buffer));

      result_ = std::make_shared<DenseUnionArray>(type_, length_, children,
                                                  type_id_buffer, offsets_buffer);
    }

    return Status::OK();
  }

  Status Visit(const NullType& type) {
    result_ = std::make_shared<NullArray>(length_);
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    std::shared_ptr<Array> indices;

    ArrayReader parser(obj_, pool_, ::arrow::field("indices", type.index_type()),
                       dictionary_memo_);
    RETURN_NOT_OK(parser.Parse(&indices));

    RETURN_NOT_OK(LookupDictionaryId(field_));
    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(dictionary_memo_->GetDictionary(dictionary_id_, &dictionary));

    result_ = std::make_shared<DictionaryArray>(field_->type(), indices, dictionary);
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    std::shared_ptr<Array> storage_array;

    ArrayReader parser(obj_, pool_, field_->WithType(type.storage_type()),
                       dictionary_memo_);
    // If the storage array is a dictionary array, lookup its dictionary id
    // using the extension field.
    // (the field is looked up by pointer, so the Field instance constructed
    //  above wouldn't work)
    if (parser.type_->id() == Type::DICTIONARY) {
      RETURN_NOT_OK(parser.LookupDictionaryId(field_));
    }
    RETURN_NOT_OK(parser.Parse(&storage_array));
    result_ = std::make_shared<ExtensionArray>(type_, storage_array);
    return Status::OK();
  }

  Status GetValidityBuffer(const std::vector<bool>& is_valid, int32_t* null_count,
                           std::shared_ptr<Buffer>* validity_buffer) {
    int length = static_cast<int>(is_valid.size());

    ARROW_ASSIGN_OR_RAISE(auto out_buffer, AllocateEmptyBitmap(length, pool_));
    uint8_t* bitmap = out_buffer->mutable_data();

    *null_count = 0;
    for (int i = 0; i < length; ++i) {
      if (!is_valid[i]) {
        ++(*null_count);
        continue;
      }
      BitUtil::SetBit(bitmap, i);
    }

    *validity_buffer = out_buffer;
    return Status::OK();
  }

  Status GetChildren(const RjObject& obj, const DataType& type,
                     std::vector<std::shared_ptr<Array>>* array) {
    const auto& json_children = obj.FindMember("children");
    RETURN_NOT_ARRAY("children", json_children, obj);
    const auto& json_children_arr = json_children->value.GetArray();

    if (type.num_fields() != static_cast<int>(json_children_arr.Size())) {
      return Status::Invalid("Expected ", type.num_fields(), " children, but got ",
                             json_children_arr.Size());
    }

    for (int i = 0; i < static_cast<int>(json_children_arr.Size()); ++i) {
      const rj::Value& json_child = json_children_arr[i];
      DCHECK(json_child.IsObject());

      std::shared_ptr<Field> child_field = type.field(i);

      auto it = json_child.FindMember("name");
      RETURN_NOT_STRING("name", it, json_child);

      DCHECK_EQ(it->value.GetString(), child_field->name());
      std::shared_ptr<Array> child;
      RETURN_NOT_OK(
          ReadArray(pool_, json_children_arr[i], child_field, dictionary_memo_, &child));
      array->emplace_back(child);
    }

    return Status::OK();
  }

  Status ParseValidityBitmap() {
    const auto& json_valid_iter = obj_.FindMember("VALIDITY");
    RETURN_NOT_ARRAY("VALIDITY", json_valid_iter, obj_);
    const auto& json_validity = json_valid_iter->value.GetArray();
    DCHECK_EQ(static_cast<int>(json_validity.Size()), length_);
    is_valid_.reserve(json_validity.Size());
    for (const rj::Value& val : json_validity) {
      DCHECK(val.IsInt());
      is_valid_.push_back(val.GetInt() != 0);
    }
    return Status::OK();
  }

  Status Parse(std::shared_ptr<Array>* out) {
    RETURN_NOT_OK(GetObjectInt(obj_, "count", &length_));

    if (::arrow::internal::HasValidityBitmap(type_->id())) {
      // Null and union types don't have a validity bitmap
      RETURN_NOT_OK(ParseValidityBitmap());
    }

    RETURN_NOT_OK(VisitTypeInline(*type_, this));

    *out = result_;
    return Status::OK();
  }

  Status LookupDictionaryId(const std::shared_ptr<Field>& field) {
    if (dictionary_id_ == -1) {
      RETURN_NOT_OK(dictionary_memo_->GetId(field.get(), &dictionary_id_));
    }
    return Status::OK();
  }

 private:
  const RjObject& obj_;
  MemoryPool* pool_;
  std::shared_ptr<Field> field_;
  std::shared_ptr<DataType> type_;
  DictionaryMemo* dictionary_memo_;
  int64_t dictionary_id_;

  // Parsed common attributes
  std::vector<bool> is_valid_;
  int32_t length_;
  std::shared_ptr<Array> result_;
};

Status WriteSchema(const Schema& schema, DictionaryMemo* dictionary_memo,
                   RjWriter* json_writer) {
  SchemaWriter converter(schema, dictionary_memo, json_writer);
  return converter.Write();
}

static Status ReadDictionary(const RjObject& obj, MemoryPool* pool,
                             DictionaryMemo* dictionary_memo) {
  int id;
  RETURN_NOT_OK(GetObjectInt(obj, "id", &id));

  const auto& it_data = obj.FindMember("data");
  RETURN_NOT_OBJECT("data", it_data, obj);

  std::shared_ptr<DataType> value_type;
  RETURN_NOT_OK(dictionary_memo->GetDictionaryType(id, &value_type));
  auto value_field = ::arrow::field("dummy", value_type);

  // We need a placeholder schema to read the record, because the dictionary
  // is embedded in a record batch with a single column.
  std::shared_ptr<RecordBatch> batch;
  RETURN_NOT_OK(ReadRecordBatch(it_data->value, ::arrow::schema({value_field}),
                                dictionary_memo, pool, &batch));

  if (batch->num_columns() != 1) {
    return Status::Invalid("Dictionary record batch must only contain one field");
  }
  return dictionary_memo->AddDictionary(id, batch->column(0));
}

static Status ReadDictionaries(const rj::Value& doc, MemoryPool* pool,
                               DictionaryMemo* dictionary_memo) {
  auto it = doc.FindMember("dictionaries");
  if (it == doc.MemberEnd()) {
    // No dictionaries
    return Status::OK();
  }

  RETURN_NOT_ARRAY("dictionaries", it, doc);
  const auto& dictionary_array = it->value.GetArray();

  for (const rj::Value& val : dictionary_array) {
    DCHECK(val.IsObject());
    RETURN_NOT_OK(ReadDictionary(val.GetObject(), pool, dictionary_memo));
  }
  return Status::OK();
}

Status ReadSchema(const rj::Value& json_schema, MemoryPool* pool,
                  DictionaryMemo* dictionary_memo, std::shared_ptr<Schema>* schema) {
  auto it = json_schema.FindMember("schema");
  RETURN_NOT_OBJECT("schema", it, json_schema);
  const auto& obj_schema = it->value.GetObject();

  const auto& it_fields = obj_schema.FindMember("fields");
  RETURN_NOT_ARRAY("fields", it_fields, obj_schema);

  std::shared_ptr<KeyValueMetadata> metadata;
  RETURN_NOT_OK(GetKeyValueMetadata(obj_schema, &metadata));

  std::vector<std::shared_ptr<Field>> fields;
  RETURN_NOT_OK(GetFieldsFromArray(it_fields->value, dictionary_memo, &fields));

  // Read the dictionaries (if any) and cache in the memo
  RETURN_NOT_OK(ReadDictionaries(json_schema, pool, dictionary_memo));

  *schema = ::arrow::schema(fields, metadata);
  return Status::OK();
}

Status ReadRecordBatch(const rj::Value& json_obj, const std::shared_ptr<Schema>& schema,
                       DictionaryMemo* dictionary_memo, MemoryPool* pool,
                       std::shared_ptr<RecordBatch>* batch) {
  DCHECK(json_obj.IsObject());
  const auto& batch_obj = json_obj.GetObject();

  auto it = batch_obj.FindMember("count");
  RETURN_NOT_INT("count", it, batch_obj);
  int32_t num_rows = static_cast<int32_t>(it->value.GetInt());

  it = batch_obj.FindMember("columns");
  RETURN_NOT_ARRAY("columns", it, batch_obj);
  const auto& json_columns = it->value.GetArray();

  std::vector<std::shared_ptr<Array>> columns(json_columns.Size());
  for (int i = 0; i < static_cast<int>(columns.size()); ++i) {
    RETURN_NOT_OK(
        ReadArray(pool, json_columns[i], schema->field(i), dictionary_memo, &columns[i]));
  }

  *batch = RecordBatch::Make(schema, num_rows, columns);
  return Status::OK();
}

Status WriteDictionary(int64_t id, const std::shared_ptr<Array>& dictionary,
                       RjWriter* writer) {
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

Status WriteRecordBatch(const RecordBatch& batch, RjWriter* writer) {
  writer->StartObject();
  writer->Key("count");
  writer->Int(static_cast<int32_t>(batch.num_rows()));

  writer->Key("columns");
  writer->StartArray();

  for (int i = 0; i < batch.num_columns(); ++i) {
    const std::shared_ptr<Array>& column = batch.column(i);

    DCHECK_EQ(batch.num_rows(), column->length())
        << "Array length did not match record batch length: " << batch.num_rows()
        << " != " << column->length() << " " << batch.column_name(i);

    RETURN_NOT_OK(WriteArray(batch.column_name(i), *column, writer));
  }

  writer->EndArray();
  writer->EndObject();
  return Status::OK();
}

Status WriteArray(const std::string& name, const Array& array, RjWriter* json_writer) {
  ArrayWriter converter(name, array, json_writer);
  return converter.Write();
}

Status ReadArray(MemoryPool* pool, const rj::Value& json_array,
                 const std::shared_ptr<Field>& field, DictionaryMemo* dictionary_memo,
                 std::shared_ptr<Array>* out) {
  if (!json_array.IsObject()) {
    return Status::Invalid("Array element was not a JSON object");
  }
  auto obj = json_array.GetObject();
  ArrayReader parser(obj, pool, field, dictionary_memo);
  return parser.Parse(out);
}

Status ReadArray(MemoryPool* pool, const rj::Value& json_array, const Schema& schema,
                 DictionaryMemo* dictionary_memo, std::shared_ptr<Array>* array) {
  if (!json_array.IsObject()) {
    return Status::Invalid("Element was not a JSON object");
  }

  const auto& json_obj = json_array.GetObject();

  const auto& it_name = json_obj.FindMember("name");
  RETURN_NOT_STRING("name", it_name, json_obj);

  std::string name = it_name->value.GetString();
  std::shared_ptr<Field> result = schema.GetFieldByName(name);
  if (result == nullptr) {
    return Status::KeyError("Field named ", name, " not found in schema");
  }

  return ReadArray(pool, json_array, result, dictionary_memo, array);
}

}  // namespace json
}  // namespace testing
}  // namespace arrow
