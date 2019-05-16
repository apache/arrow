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

#include "arrow/ipc/json-internal.h"

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"  // IWYU pragma: keep
#include "arrow/ipc/dictionary.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
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

namespace ipc {
namespace internal {
namespace json {

static std::string GetFloatingPrecisionName(FloatingPoint::Precision precision) {
  switch (precision) {
    case FloatingPoint::HALF:
      return "HALF";
    case FloatingPoint::SINGLE:
      return "SINGLE";
    case FloatingPoint::DOUBLE:
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
    writer_->EndObject();
    return Status::OK();
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

    const DataType& type = *field->type();

    // Visit the type
    writer_->Key("type");
    writer_->StartObject();
    RETURN_NOT_OK(VisitType(type));
    writer_->EndObject();

    if (type.id() == Type::DICTIONARY) {
      const auto& dict_type = checked_cast<const DictionaryType&>(type);
      int64_t dictionary_id = -1;
      RETURN_NOT_OK(dictionary_memo_->GetOrAssignId(field, &dictionary_id));
      RETURN_NOT_OK(WriteDictionaryMetadata(dictionary_id, dict_type));
      RETURN_NOT_OK(WriteChildren(dict_type.value_type()->children()));
    } else {
      RETURN_NOT_OK(WriteChildren(type.children()));
    }

    writer_->EndObject();

    return Status::OK();
  }

  Status VisitType(const DataType& type);

  template <typename T>
  typename std::enable_if<std::is_base_of<NoExtraMeta, T>::value ||
                              std::is_base_of<ListType, T>::value ||
                              std::is_base_of<StructType, T>::value,
                          void>::type
  WriteTypeMetadata(const T& type) {}

  void WriteTypeMetadata(const Integer& type) {
    writer_->Key("bitWidth");
    writer_->Int(type.bit_width());
    writer_->Key("isSigned");
    writer_->Bool(type.is_signed());
  }

  void WriteTypeMetadata(const FloatingPoint& type) {
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
      writer_->Uint(type.type_codes()[i]);
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
  Status Visit(const Integer& type) { return WritePrimitive("int", type); }

  Status Visit(const FloatingPoint& type) {
    return WritePrimitive("floatingpoint", type);
  }

  Status Visit(const DateType& type) { return WritePrimitive("date", type); }
  Status Visit(const TimeType& type) { return WritePrimitive("time", type); }
  Status Visit(const StringType& type) { return WriteVarBytes("utf8", type); }
  Status Visit(const BinaryType& type) { return WriteVarBytes("binary", type); }
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

  // Default case
  Status Visit(const DataType& type) { return Status::NotImplemented(type.name()); }

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

  template <typename T>
  typename std::enable_if<IsSignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    static const char null_string[] = "0";
    const auto data = arr.raw_values();
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        writer_->Int64(data[i]);
      } else {
        writer_->RawNumber(null_string, sizeof(null_string));
      }
    }
  }

  template <typename T>
  typename std::enable_if<IsUnsignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    static const char null_string[] = "0";
    const auto data = arr.raw_values();
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsValid(i)) {
        writer_->Uint64(data[i]);
      } else {
        writer_->RawNumber(null_string, sizeof(null_string));
      }
    }
  }

  template <typename T>
  typename std::enable_if<IsFloatingPoint<T>::value, void>::type WriteDataValues(
      const T& arr) {
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

  // Binary, encode to hexadecimal. UTF8 string write as is
  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryArray, T>::value, void>::type
  WriteDataValues(const T& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      int32_t length;
      const uint8_t* buf = arr.GetValue(i, &length);

      if (std::is_base_of<StringArray, T>::value) {
        // Presumed UTF-8
        writer_->String(reinterpret_cast<const char*>(buf), length);
      } else {
        writer_->String(HexEncode(buf, length));
      }
    }
  }

  void WriteDataValues(const FixedSizeBinaryArray& arr) {
    const int32_t width = arr.byte_width();

    for (int64_t i = 0; i < arr.length(); ++i) {
      const uint8_t* buf = arr.GetValue(i);
      std::string encoded = HexEncode(buf, width);
      writer_->String(encoded);
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
    for (int i = 0; i < length; ++i) {
      writer_->Int64(values[i]);
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

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveArray, T>::value, Status>::type Visit(
      const T& array) {
    WriteValidityField(array);
    WriteDataField(array);
    SetNoChildren();
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryArray, T>::value, Status>::type Visit(
      const T& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    WriteDataField(array);
    SetNoChildren();
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    return VisitArrayValues(*array.indices());
  }

  Status Visit(const ListArray& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    const auto& type = checked_cast<const ListType&>(*array.type());
    return WriteChildren(type.children(), {array.values()});
  }

  Status Visit(const FixedSizeListArray& array) {
    WriteValidityField(array);
    const auto& type = checked_cast<const FixedSizeListType&>(*array.type());
    return WriteChildren(type.children(), {array.values()});
  }

  Status Visit(const StructArray& array) {
    WriteValidityField(array);
    const auto& type = checked_cast<const StructType&>(*array.type());
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return WriteChildren(type.children(), children);
  }

  Status Visit(const UnionArray& array) {
    WriteValidityField(array);
    const auto& type = checked_cast<const UnionType&>(*array.type());

    WriteIntegerField("TYPE_ID", array.raw_type_ids(), array.length());
    if (type.mode() == UnionMode::DENSE) {
      WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length());
    }
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.child(i));
    }
    return WriteChildren(type.children(), children);
  }

  Status Visit(const ExtensionArray& array) {
    return Status::NotImplemented("extension array");
  }

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

  std::vector<uint8_t> type_codes;
  const auto& id_array = it_type_codes->value.GetArray();
  for (const rj::Value& val : id_array) {
    DCHECK(val.IsUint());
    type_codes.push_back(static_cast<uint8_t>(val.GetUint()));
  }

  *type = union_(children, type_codes, mode);

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

  const auto& it_dictionary = json_field.FindMember("dictionary");
  if (dictionary_memo != nullptr && it_dictionary != json_field.MemberEnd()) {
    // Parse dictionary id in JSON and add dictionary field to the
    // memo, and parse the dictionaries later
    RETURN_NOT_OBJECT("dictionary", it_dictionary, json_field);
    int64_t dictionary_id = -1;
    bool is_ordered;
    std::shared_ptr<DataType> index_type;
    RETURN_NOT_OK(ParseDictionary(it_dictionary->value.GetObject(), &dictionary_id,
                                  &is_ordered, &index_type));

    type = ::arrow::dictionary(index_type, type, is_ordered);
    *field = ::arrow::field(name, type, nullable);
    RETURN_NOT_OK(dictionary_memo->AddField(dictionary_id, *field));
  } else {
    *field = ::arrow::field(name, type, nullable);
  }

  return Status::OK();
}

template <typename T>
inline typename std::enable_if<IsSignedInt<T>::value, typename T::c_type>::type
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsInt64());
  return static_cast<typename T::c_type>(val.GetInt64());
}

template <typename T>
inline typename std::enable_if<IsUnsignedInt<T>::value, typename T::c_type>::type
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsUint());
  return static_cast<typename T::c_type>(val.GetUint64());
}

template <typename T>
inline typename std::enable_if<IsFloatingPoint<T>::value, typename T::c_type>::type
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsFloat());
  return static_cast<typename T::c_type>(val.GetDouble());
}

template <typename T>
inline typename std::enable_if<std::is_base_of<BooleanType, T>::value, bool>::type
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsBool());
  return val.GetBool();
}

class ArrayReader {
 public:
  ArrayReader(const RjObject& obj, MemoryPool* pool, const std::shared_ptr<Field>& field,
              DictionaryMemo* dictionary_memo)
      : obj_(obj),
        pool_(pool),
        field_(field),
        type_(field->type()),
        dictionary_memo_(dictionary_memo) {}

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value ||
                              is_temporal_type<T>::value ||
                              std::is_base_of<BooleanType, T>::value,
                          Status>::type
  Visit(const T& type) {
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
  typename std::enable_if<std::is_base_of<BinaryType, T>::value, Status>::type Visit(
      const T& type) {
    typename TypeTraits<T>::BuilderType builder(pool_);

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
      if (std::is_base_of<StringType, T>::value) {
        RETURN_NOT_OK(builder.Append(val.GetString()));
      } else {
        std::string hex_string = val.GetString();

        DCHECK(hex_string.size() % 2 == 0) << "Expected base16 hex string";
        int32_t length = static_cast<int>(hex_string.size()) / 2;

        std::shared_ptr<Buffer> byte_buffer;
        RETURN_NOT_OK(AllocateBuffer(pool_, length, &byte_buffer));

        const char* hex_data = hex_string.c_str();
        uint8_t* byte_buffer_data = byte_buffer->mutable_data();
        for (int32_t j = 0; j < length; ++j) {
          RETURN_NOT_OK(ParseHexValue(hex_data + j * 2, &byte_buffer_data[j]));
        }
        RETURN_NOT_OK(builder.Append(byte_buffer_data, length));
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
  typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T>::value &&
                              !std::is_base_of<Decimal128Type, T>::value,
                          Status>::type
  Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    const auto& json_data = obj_.FindMember(kData);
    RETURN_NOT_ARRAY(kData, json_data, obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);
    int32_t byte_width = type.byte_width();

    // Allocate space for parsed values
    std::shared_ptr<Buffer> byte_buffer;
    RETURN_NOT_OK(AllocateBuffer(pool_, byte_width, &byte_buffer));
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
  typename std::enable_if<std::is_base_of<Decimal128Type, T>::value, Status>::type Visit(
      const T& type) {
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
        RETURN_NOT_OK(Decimal128::FromString(val.GetString(), &value));
        RETURN_NOT_OK(builder.Append(value));
      }
    }
    return builder.Finish(&result_);
  }

  template <typename T>
  Status GetIntArray(const RjArray& json_array, const int32_t length,
                     std::shared_ptr<Buffer>* out) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(AllocateBuffer(pool_, length * sizeof(T), &buffer));

    T* values = reinterpret_cast<T*>(buffer->mutable_data());
    for (int i = 0; i < length; ++i) {
      const rj::Value& val = json_array[i];
      DCHECK(val.IsInt());
      values[i] = static_cast<T>(val.GetInt());
    }

    *out = buffer;
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid_, &null_count, &validity_buffer));

    const auto& json_offsets = obj_.FindMember("OFFSET");
    RETURN_NOT_ARRAY("OFFSET", json_offsets, obj_);
    std::shared_ptr<Buffer> offsets_buffer;
    RETURN_NOT_OK(GetIntArray<int32_t>(json_offsets->value.GetArray(), length_ + 1,
                                       &offsets_buffer));

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(obj_, type, &children));
    DCHECK_EQ(children.size(), 1);

    result_ = std::make_shared<ListArray>(type_, length_, offsets_buffer, children[0],
                                          validity_buffer, null_count);

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
    int32_t null_count = 0;

    std::shared_ptr<Buffer> validity_buffer;
    std::shared_ptr<Buffer> type_id_buffer;
    std::shared_ptr<Buffer> offsets_buffer;

    RETURN_NOT_OK(GetValidityBuffer(is_valid_, &null_count, &validity_buffer));

    const auto& json_type_ids = obj_.FindMember("TYPE_ID");
    RETURN_NOT_ARRAY("TYPE_ID", json_type_ids, obj_);
    RETURN_NOT_OK(
        GetIntArray<uint8_t>(json_type_ids->value.GetArray(), length_, &type_id_buffer));

    if (type.mode() == UnionMode::DENSE) {
      const auto& json_offsets = obj_.FindMember("OFFSET");
      RETURN_NOT_ARRAY("OFFSET", json_offsets, obj_);
      RETURN_NOT_OK(
          GetIntArray<int32_t>(json_offsets->value.GetArray(), length_, &offsets_buffer));
    }

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(obj_, type, &children));

    result_ = std::make_shared<UnionArray>(type_, length_, children, type_id_buffer,
                                           offsets_buffer, validity_buffer, null_count);

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

    // Look up dictionary
    int64_t dictionary_id = -1;
    RETURN_NOT_OK(dictionary_memo_->GetId(*field_, &dictionary_id));

    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(dictionary_memo_->GetDictionary(dictionary_id, &dictionary));

    result_ = std::make_shared<DictionaryArray>(field_->type(), indices, dictionary);
    return Status::OK();
  }

  // Default case
  Status Visit(const DataType& type) { return Status::NotImplemented(type.name()); }

  Status GetValidityBuffer(const std::vector<bool>& is_valid, int32_t* null_count,
                           std::shared_ptr<Buffer>* validity_buffer) {
    int length = static_cast<int>(is_valid.size());

    std::shared_ptr<Buffer> out_buffer;
    RETURN_NOT_OK(AllocateEmptyBitmap(pool_, length, &out_buffer));
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

    if (type.num_children() != static_cast<int>(json_children_arr.Size())) {
      return Status::Invalid("Expected ", type.num_children(), " children, but got ",
                             json_children_arr.Size());
    }

    for (int i = 0; i < static_cast<int>(json_children_arr.Size()); ++i) {
      const rj::Value& json_child = json_children_arr[i];
      DCHECK(json_child.IsObject());

      std::shared_ptr<Field> child_field = type.child(i);

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

  Status Parse(std::shared_ptr<Array>* out) {
    RETURN_NOT_OK(GetObjectInt(obj_, "count", &length_));

    const auto& json_valid_iter = obj_.FindMember("VALIDITY");
    RETURN_NOT_ARRAY("VALIDITY", json_valid_iter, obj_);

    const auto& json_validity = json_valid_iter->value.GetArray();
    DCHECK_EQ(static_cast<int>(json_validity.Size()), length_);
    for (const rj::Value& val : json_validity) {
      DCHECK(val.IsInt());
      is_valid_.push_back(val.GetInt() != 0);
    }

    RETURN_NOT_OK(VisitTypeInline(*type_, this));

    *out = result_;
    return Status::OK();
  }

 private:
  const RjObject& obj_;
  MemoryPool* pool_;
  const std::shared_ptr<Field>& field_;
  std::shared_ptr<DataType> type_;
  DictionaryMemo* dictionary_memo_;

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

  // We need placeholder schema and dictionary memo to read the record
  // batch, because the dictionary is embedded in a record batch with
  // a single column
  std::shared_ptr<RecordBatch> batch;
  DictionaryMemo dummy_memo;
  RETURN_NOT_OK(ReadRecordBatch(it_data->value, ::arrow::schema({value_field}),
                                &dummy_memo, pool, &batch));

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

  std::vector<std::shared_ptr<Field>> fields;
  RETURN_NOT_OK(GetFieldsFromArray(it_fields->value, dictionary_memo, &fields));

  // Read the dictionaries (if any) and cache in the memo
  RETURN_NOT_OK(ReadDictionaries(json_schema, pool, dictionary_memo));

  *schema = ::arrow::schema(fields);
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
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
