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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/metadata.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace ipc {
namespace json {
namespace internal {

static std::string GetBufferTypeName(BufferType type) {
  switch (type) {
    case BufferType::DATA:
      return "DATA";
    case BufferType::OFFSET:
      return "OFFSET";
    case BufferType::TYPE:
      return "TYPE";
    case BufferType::VALIDITY:
      return "VALIDITY";
    default:
      break;
  }
  return "UNKNOWN";
}

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
  explicit SchemaWriter(const Schema& schema, RjWriter* writer)
      : schema_(schema), writer_(writer) {}

  Status Write() {
    writer_->Key("schema");
    writer_->StartObject();
    writer_->Key("fields");
    writer_->StartArray();
    for (const std::shared_ptr<Field>& field : schema_.fields()) {
      RETURN_NOT_OK(VisitField(*field));
    }
    writer_->EndArray();
    writer_->EndObject();

    // Write dictionaries, if any
    if (dictionary_memo_.size() > 0) {
      writer_->Key("dictionaries");
      writer_->StartArray();
      for (const auto& entry : dictionary_memo_.id_to_dictionary()) {
        RETURN_NOT_OK(WriteDictionary(entry.first, entry.second));
      }
      writer_->EndArray();
    }
    return Status::OK();
  }

  Status WriteDictionary(int64_t id, const std::shared_ptr<Array>& dictionary) {
    writer_->StartObject();
    writer_->Key("id");
    writer_->Int(static_cast<int32_t>(id));
    writer_->Key("data");

    // Make a dummy record batch. A bit tedious as we have to make a schema
    auto schema = std::shared_ptr<Schema>(
        new Schema({arrow::field("dictionary", dictionary->type())}));
    RecordBatch batch(schema, dictionary->length(), {dictionary});
    RETURN_NOT_OK(WriteRecordBatch(batch, writer_));
    writer_->EndObject();
    return Status::OK();
  }

  Status WriteDictionaryMetadata(const DictionaryType& type) {
    int64_t dictionary_id = dictionary_memo_.GetId(type.dictionary());
    writer_->Key("dictionary");

    // Emulate DictionaryEncoding from Schema.fbs
    writer_->StartObject();
    writer_->Key("id");
    writer_->Int(static_cast<int32_t>(dictionary_id));
    writer_->Key("indexType");

    writer_->StartObject();
    RETURN_NOT_OK(VisitType(*type.index_type()));
    writer_->EndObject();

    writer_->Key("isOrdered");
    writer_->Bool(type.ordered());
    writer_->EndObject();

    return Status::OK();
  }

  Status VisitField(const Field& field) {
    writer_->StartObject();

    writer_->Key("name");
    writer_->String(field.name().c_str());

    writer_->Key("nullable");
    writer_->Bool(field.nullable());

    const DataType& type = *field.type();

    // Visit the type
    writer_->Key("type");
    writer_->StartObject();
    RETURN_NOT_OK(VisitType(type));
    writer_->EndObject();

    if (type.id() == Type::DICTIONARY) {
      const auto& dict_type = static_cast<const DictionaryType&>(type);
      RETURN_NOT_OK(WriteDictionaryMetadata(dict_type));

      const DataType& dictionary_type = *dict_type.dictionary()->type();
      const DataType& index_type = *dict_type.index_type();
      RETURN_NOT_OK(WriteChildren(dictionary_type.children()));
      WriteBufferLayout(index_type.GetBufferLayout());
    } else {
      RETURN_NOT_OK(WriteChildren(type.children()));
      WriteBufferLayout(type.GetBufferLayout());
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
    switch (type.unit()) {
      case IntervalType::Unit::YEAR_MONTH:
        writer_->String("YEAR_MONTH");
        break;
      case IntervalType::Unit::DAY_TIME:
        writer_->String("DAY_TIME");
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

  void WriteBufferLayout(const std::vector<BufferDescr>& buffer_layout) {
    writer_->Key("typeLayout");
    writer_->StartObject();
    writer_->Key("vectors");
    writer_->StartArray();

    for (const BufferDescr& buffer : buffer_layout) {
      writer_->StartObject();
      writer_->Key("type");
      writer_->String(GetBufferTypeName(buffer.type()));

      writer_->Key("typeBitWidth");
      writer_->Int(buffer.bit_width());

      writer_->EndObject();
    }
    writer_->EndArray();
    writer_->EndObject();
  }

  Status WriteChildren(const std::vector<std::shared_ptr<Field>>& children) {
    writer_->Key("children");
    writer_->StartArray();
    for (const std::shared_ptr<Field>& field : children) {
      RETURN_NOT_OK(VisitField(*field));
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

  Status Visit(const TimestampType& type) { return WritePrimitive("timestamp", type); }
  Status Visit(const IntervalType& type) { return WritePrimitive("interval", type); }

  Status Visit(const ListType& type) {
    WriteName("list", type);
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

  Status Visit(const DecimalType& type) { return Status::NotImplemented("decimal"); }

  Status Visit(const DictionaryType& type) {
    return VisitType(*type.dictionary()->type());
  }

 private:
  DictionaryMemo dictionary_memo_;

  const Schema& schema_;
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
    const auto data = arr.raw_values();
    for (int i = 0; i < arr.length(); ++i) {
      writer_->Int64(data[i]);
    }
  }

  template <typename T>
  typename std::enable_if<IsUnsignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_values();
    for (int i = 0; i < arr.length(); ++i) {
      writer_->Uint64(data[i]);
    }
  }

  template <typename T>
  typename std::enable_if<IsFloatingPoint<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_values();
    for (int i = 0; i < arr.length(); ++i) {
      writer_->Double(data[i]);
    }
  }

  // Binary, encode to hexadecimal. UTF8 string write as is
  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryArray, T>::value, void>::type
  WriteDataValues(const T& arr) {
    for (int64_t i = 0; i < arr.length(); ++i) {
      int32_t length;
      const char* buf = reinterpret_cast<const char*>(arr.GetValue(i, &length));

      if (std::is_base_of<StringArray, T>::value) {
        writer_->String(buf, length);
      } else {
        writer_->String(HexEncode(buf, length));
      }
    }
  }

  void WriteDataValues(const FixedSizeBinaryArray& arr) {
    int32_t width = arr.byte_width();
    for (int64_t i = 0; i < arr.length(); ++i) {
      const char* buf = reinterpret_cast<const char*>(arr.GetValue(i));
      writer_->String(HexEncode(buf, width));
    }
  }

  void WriteDataValues(const BooleanArray& arr) {
    for (int i = 0; i < arr.length(); ++i) {
      writer_->Bool(arr.Value(i));
    }
  }

  template <typename T>
  void WriteDataField(const T& arr) {
    writer_->Key("DATA");
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

  Status Visit(const DecimalArray& array) { return Status::NotImplemented("decimal"); }

  Status Visit(const DictionaryArray& array) {
    return VisitArrayValues(*array.indices());
  }

  Status Visit(const ListArray& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    const auto& type = static_cast<const ListType&>(*array.type());
    return WriteChildren(type.children(), {array.values()});
  }

  Status Visit(const StructArray& array) {
    WriteValidityField(array);
    const auto& type = static_cast<const StructType&>(*array.type());
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return WriteChildren(type.children(), children);
  }

  Status Visit(const UnionArray& array) {
    WriteValidityField(array);
    const auto& type = static_cast<const UnionType&>(*array.type());

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
      std::stringstream ss;
      ss << "Invalid bit width: " << bit_width;
      return Status::Invalid(ss.str());
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
    std::stringstream ss;
    ss << "Invalid precision: " << precision;
    return Status::Invalid(ss.str());
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

static Status GetDate(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  std::string unit_str = it_unit->value.GetString();

  if (unit_str == "DAY") {
    *type = date32();
  } else if (unit_str == "MILLISECOND") {
    *type = date64();
  } else {
    std::stringstream ss;
    ss << "Invalid date unit: " << unit_str;
    return Status::Invalid(ss.str());
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
    std::stringstream ss;
    ss << "Invalid time unit: " << unit_str;
    return Status::Invalid(ss.str());
  }

  const auto& fw_type = static_cast<const FixedWidthType&>(**type);

  int bit_width = it_bit_width->value.GetInt();
  if (bit_width != fw_type.bit_width()) {
    return Status::Invalid("Indicated bit width does not match unit");
  }

  return Status::OK();
}

static Status GetTimestamp(const RjObject& json_type, std::shared_ptr<DataType>* type) {
  const auto& it_unit = json_type.FindMember("unit");
  RETURN_NOT_STRING("unit", it_unit, json_type);

  std::string unit_str = it_unit->value.GetString();

  TimeUnit::type unit;
  if (unit_str == "SECOND") {
    unit = TimeUnit::SECOND;
  } else if (unit_str == "MILLISECOND") {
    unit = TimeUnit::MILLI;
  } else if (unit_str == "MICROSECOND") {
    unit = TimeUnit::MICRO;
  } else if (unit_str == "NANOSECOND") {
    unit = TimeUnit::NANO;
  } else {
    std::stringstream ss;
    ss << "Invalid time unit: " << unit_str;
    return Status::Invalid(ss.str());
  }

  const auto& it_tz = json_type.FindMember("timezone");
  if (it_tz == json_type.MemberEnd()) {
    *type = timestamp(unit);
  } else {
    *type = timestamp(unit, it_tz->value.GetString());
  }

  return Status::OK();
}

static Status GetUnion(const RjObject& json_type,
                       const std::vector<std::shared_ptr<Field>>& children,
                       std::shared_ptr<DataType>* type) {
  const auto& it_mode = json_type.FindMember("mode");
  RETURN_NOT_STRING("mode", it_mode, json_type);

  std::string mode_str = it_mode->value.GetString();
  UnionMode mode;

  if (mode_str == "SPARSE") {
    mode = UnionMode::SPARSE;
  } else if (mode_str == "DENSE") {
    mode = UnionMode::DENSE;
  } else {
    std::stringstream ss;
    ss << "Invalid union mode: " << mode_str;
    return Status::Invalid(ss.str());
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
  } else if (type_name == "null") {
    *type = null();
  } else if (type_name == "date") {
    return GetDate(json_type, type);
  } else if (type_name == "time") {
    return GetTime(json_type, type);
  } else if (type_name == "timestamp") {
    return GetTimestamp(json_type, type);
  } else if (type_name == "list") {
    if (children.size() != 1) {
      return Status::Invalid("List must have exactly one child");
    }
    *type = list(children[0]);
  } else if (type_name == "struct") {
    *type = struct_(children);
  } else {
    return GetUnion(json_type, children, type);
  }
  return Status::OK();
}

static Status GetField(const rj::Value& obj, const DictionaryMemo* dictionary_memo,
                       std::shared_ptr<Field>* field);

static Status GetFieldsFromArray(const rj::Value& obj,
                                 const DictionaryMemo* dictionary_memo,
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

static Status GetField(const rj::Value& obj, const DictionaryMemo* dictionary_memo,
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

  const auto& it_dictionary = json_field.FindMember("dictionary");
  if (dictionary_memo != nullptr && it_dictionary != json_field.MemberEnd()) {
    // Field is dictionary encoded. We must have already
    RETURN_NOT_OBJECT("dictionary", it_dictionary, json_field);
    int64_t dictionary_id;
    bool is_ordered;
    std::shared_ptr<DataType> index_type;
    RETURN_NOT_OK(ParseDictionary(it_dictionary->value.GetObject(), &dictionary_id,
                                  &is_ordered, &index_type));

    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(dictionary_memo->GetDictionary(dictionary_id, &dictionary));

    type = std::make_shared<DictionaryType>(index_type, dictionary, is_ordered);
  } else {
    // If the dictionary_memo was not passed, or if the field is not dictionary
    // encoded, we are interested in the complete type including all children

    const auto& it_type = json_field.FindMember("type");
    RETURN_NOT_OBJECT("type", it_type, json_field);

    const auto& it_children = json_field.FindMember("children");
    RETURN_NOT_ARRAY("children", it_children, json_field);

    std::vector<std::shared_ptr<Field>> children;
    RETURN_NOT_OK(GetFieldsFromArray(it_children->value, dictionary_memo, &children));
    RETURN_NOT_OK(GetType(it_type->value.GetObject(), children, &type));
  }

  *field = std::make_shared<Field>(name, type, nullable);
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
  explicit ArrayReader(const rj::Value& json_array, const std::shared_ptr<DataType>& type,
                       MemoryPool* pool)
      : json_array_(json_array), type_(type), pool_(pool) {}

  Status ParseTypeValues(const DataType& type);

  Status GetValidityBuffer(const std::vector<bool>& is_valid, int32_t* null_count,
                           std::shared_ptr<Buffer>* validity_buffer) {
    int length = static_cast<int>(is_valid.size());

    std::shared_ptr<MutableBuffer> out_buffer;
    RETURN_NOT_OK(GetEmptyBitmap(pool_, length, &out_buffer));
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

  template <typename T>
  typename std::enable_if<
      std::is_base_of<PrimitiveCType, T>::value || std::is_base_of<DateType, T>::value ||
          std::is_base_of<TimestampType, T>::value ||
          std::is_base_of<TimeType, T>::value || std::is_base_of<BooleanType, T>::value,
      Status>::type
  Visit(const T& type) {
    typename TypeTraits<T>::BuilderType builder(type_, pool_);

    const auto& json_data = obj_->FindMember("DATA");
    RETURN_NOT_ARRAY("DATA", json_data, *obj_);

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

    const auto& json_data = obj_->FindMember("DATA");
    RETURN_NOT_ARRAY("DATA", json_data, *obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);

    auto byte_buffer = std::make_shared<PoolBuffer>(pool_);
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

        if (byte_buffer->size() < length) {
          RETURN_NOT_OK(byte_buffer->Resize(length));
        }

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

  template <typename T>
  typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T>::value, Status>::type
  Visit(const T& type) {
    FixedSizeBinaryBuilder builder(type_, pool_);

    const auto& json_data = obj_->FindMember("DATA");
    RETURN_NOT_ARRAY("DATA", json_data, *obj_);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length_);
    int32_t byte_width = type.byte_width();

    // Allocate space for parsed values
    std::shared_ptr<MutableBuffer> byte_buffer;
    RETURN_NOT_OK(AllocateBuffer(pool_, byte_width, &byte_buffer));
    uint8_t* byte_buffer_data = byte_buffer->mutable_data();

    for (int i = 0; i < length_; ++i) {
      if (!is_valid_[i]) {
        RETURN_NOT_OK(builder.AppendNull());
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      DCHECK(val.IsString());
      std::string hex_string = val.GetString();
      DCHECK_EQ(static_cast<int32_t>(hex_string.size()), byte_width * 2)
          << "Expected size: " << byte_width * 2 << " got: " << hex_string.size();
      const char* hex_data = hex_string.c_str();

      for (int32_t j = 0; j < byte_width; ++j) {
        RETURN_NOT_OK(ParseHexValue(hex_data + j * 2, &byte_buffer_data[j]));
      }
      RETURN_NOT_OK(builder.Append(byte_buffer_data));
    }
    return builder.Finish(&result_);
  }

  template <typename T>
  Status GetIntArray(const RjArray& json_array, const int32_t length,
                     std::shared_ptr<Buffer>* out) {
    std::shared_ptr<MutableBuffer> buffer;
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

    const auto& json_offsets = obj_->FindMember("OFFSET");
    RETURN_NOT_ARRAY("OFFSET", json_offsets, *obj_);
    std::shared_ptr<Buffer> offsets_buffer;
    RETURN_NOT_OK(GetIntArray<int32_t>(json_offsets->value.GetArray(), length_ + 1,
                                       &offsets_buffer));

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(*obj_, type, &children));
    DCHECK_EQ(children.size(), 1);

    result_ = std::make_shared<ListArray>(type_, length_, offsets_buffer, children[0],
                                          validity_buffer, null_count);

    return Status::OK();
  }

  Status Visit(const StructType& type) {
    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid_, &null_count, &validity_buffer));

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(GetChildren(*obj_, type, &fields));

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

    const auto& json_type_ids = obj_->FindMember("TYPE_ID");
    RETURN_NOT_ARRAY("TYPE_ID", json_type_ids, *obj_);
    RETURN_NOT_OK(
        GetIntArray<uint8_t>(json_type_ids->value.GetArray(), length_, &type_id_buffer));

    if (type.mode() == UnionMode::DENSE) {
      const auto& json_offsets = obj_->FindMember("OFFSET");
      RETURN_NOT_ARRAY("OFFSET", json_offsets, *obj_);
      RETURN_NOT_OK(
          GetIntArray<int32_t>(json_offsets->value.GetArray(), length_, &offsets_buffer));
    }

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(*obj_, type, &children));

    result_ = std::make_shared<UnionArray>(type_, length_, children, type_id_buffer,
                                           offsets_buffer, validity_buffer, null_count);

    return Status::OK();
  }

  Status Visit(const NullType& type) {
    result_ = std::make_shared<NullArray>(length_);
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // This stores the indices in result_
    //
    // XXX(wesm): slight hack
    auto dict_type = type_;
    type_ = type.index_type();
    RETURN_NOT_OK(ParseTypeValues(*type_));
    type_ = dict_type;
    result_ = std::make_shared<DictionaryArray>(type_, result_);
    return Status::OK();
  }

  Status GetChildren(const RjObject& obj, const DataType& type,
                     std::vector<std::shared_ptr<Array>>* array) {
    const auto& json_children = obj.FindMember("children");
    RETURN_NOT_ARRAY("children", json_children, obj);
    const auto& json_children_arr = json_children->value.GetArray();

    if (type.num_children() != static_cast<int>(json_children_arr.Size())) {
      std::stringstream ss;
      ss << "Expected " << type.num_children() << " children, but got "
         << json_children_arr.Size();
      return Status::Invalid(ss.str());
    }

    for (int i = 0; i < static_cast<int>(json_children_arr.Size()); ++i) {
      const rj::Value& json_child = json_children_arr[i];
      DCHECK(json_child.IsObject());

      std::shared_ptr<Field> child_field = type.child(i);

      auto it = json_child.FindMember("name");
      RETURN_NOT_STRING("name", it, json_child);

      DCHECK_EQ(it->value.GetString(), child_field->name());
      std::shared_ptr<Array> child;
      RETURN_NOT_OK(ReadArray(pool_, json_children_arr[i], child_field->type(), &child));
      array->emplace_back(child);
    }

    return Status::OK();
  }

  Status GetArray(std::shared_ptr<Array>* out) {
    if (!json_array_.IsObject()) {
      return Status::Invalid("Array element was not a JSON object");
    }

    auto obj = json_array_.GetObject();
    obj_ = &obj;

    RETURN_NOT_OK(GetObjectInt(obj, "count", &length_));

    const auto& json_valid_iter = obj.FindMember("VALIDITY");
    RETURN_NOT_ARRAY("VALIDITY", json_valid_iter, obj);

    const auto& json_validity = json_valid_iter->value.GetArray();
    DCHECK_EQ(static_cast<int>(json_validity.Size()), length_);
    for (const rj::Value& val : json_validity) {
      DCHECK(val.IsInt());
      is_valid_.push_back(val.GetInt() != 0);
    }

    RETURN_NOT_OK(ParseTypeValues(*type_));
    *out = result_;
    return Status::OK();
  }

 private:
  const rj::Value& json_array_;
  const RjObject* obj_;
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;

  // Parsed common attributes
  std::vector<bool> is_valid_;
  int32_t length_;
  std::shared_ptr<Array> result_;
};

Status ArrayReader::ParseTypeValues(const DataType& type) {
  return VisitTypeInline(type, this);
}

Status WriteSchema(const Schema& schema, RjWriter* json_writer) {
  SchemaWriter converter(schema, json_writer);
  return converter.Write();
}

static Status LookForDictionaries(const rj::Value& obj, DictionaryTypeMap* id_to_field) {
  const auto& json_field = obj.GetObject();

  const auto& it_dictionary = json_field.FindMember("dictionary");
  if (it_dictionary == json_field.MemberEnd()) {
    // Not dictionary-encoded
    return Status::OK();
  }

  // Dictionary encoded. Construct the field and set in the type map
  std::shared_ptr<Field> dictionary_field;
  RETURN_NOT_OK(GetField(obj, nullptr, &dictionary_field));

  int id;
  RETURN_NOT_OK(GetObjectInt(it_dictionary->value.GetObject(), "id", &id));
  (*id_to_field)[id] = dictionary_field;
  return Status::OK();
}

static Status GetDictionaryTypes(const RjArray& fields, DictionaryTypeMap* id_to_field) {
  for (rj::SizeType i = 0; i < fields.Size(); ++i) {
    RETURN_NOT_OK(LookForDictionaries(fields[i], id_to_field));
  }
  return Status::OK();
}

static Status ReadDictionary(const RjObject& obj, const DictionaryTypeMap& id_to_field,
                             MemoryPool* pool, int64_t* dictionary_id,
                             std::shared_ptr<Array>* out) {
  int id;
  RETURN_NOT_OK(GetObjectInt(obj, "id", &id));

  const auto& it_data = obj.FindMember("data");
  RETURN_NOT_OBJECT("data", it_data, obj);

  auto it = id_to_field.find(id);
  if (it == id_to_field.end()) {
    std::stringstream ss;
    ss << "No dictionary with id " << id;
    return Status::Invalid(ss.str());
  }
  std::vector<std::shared_ptr<Field>> fields = {it->second};

  // We need a schema for the record batch
  auto dummy_schema = std::make_shared<Schema>(fields);

  // The dictionary is embedded in a record batch with a single column
  std::shared_ptr<RecordBatch> batch;
  RETURN_NOT_OK(ReadRecordBatch(it_data->value, dummy_schema, pool, &batch));

  if (batch->num_columns() != 1) {
    return Status::Invalid("Dictionary record batch must only contain one field");
  }

  *dictionary_id = id;
  *out = batch->column(0);
  return Status::OK();
}

static Status ReadDictionaries(const rj::Value& doc, const DictionaryTypeMap& id_to_field,
                               MemoryPool* pool, DictionaryMemo* dictionary_memo) {
  auto it = doc.FindMember("dictionaries");
  if (it == doc.MemberEnd()) {
    // No dictionaries
    return Status::OK();
  }

  RETURN_NOT_ARRAY("dictionaries", it, doc);
  const auto& dictionary_array = it->value.GetArray();

  for (const rj::Value& val : dictionary_array) {
    DCHECK(val.IsObject());
    int64_t dictionary_id;
    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(
        ReadDictionary(val.GetObject(), id_to_field, pool, &dictionary_id, &dictionary));

    RETURN_NOT_OK(dictionary_memo->AddDictionary(dictionary_id, dictionary));
  }
  return Status::OK();
}

Status ReadSchema(const rj::Value& json_schema, MemoryPool* pool,
                  std::shared_ptr<Schema>* schema) {
  auto it = json_schema.FindMember("schema");
  RETURN_NOT_OBJECT("schema", it, json_schema);
  const auto& obj_schema = it->value.GetObject();

  const auto& it_fields = obj_schema.FindMember("fields");
  RETURN_NOT_ARRAY("fields", it_fields, obj_schema);

  // Determine the dictionary types
  DictionaryTypeMap dictionary_types;
  RETURN_NOT_OK(GetDictionaryTypes(it_fields->value.GetArray(), &dictionary_types));

  // Read the dictionaries (if any) and cache in the memo
  DictionaryMemo dictionary_memo;
  RETURN_NOT_OK(ReadDictionaries(json_schema, dictionary_types, pool, &dictionary_memo));

  std::vector<std::shared_ptr<Field>> fields;
  RETURN_NOT_OK(GetFieldsFromArray(it_fields->value, &dictionary_memo, &fields));

  *schema = std::make_shared<Schema>(fields);
  return Status::OK();
}

Status ReadRecordBatch(const rj::Value& json_obj, const std::shared_ptr<Schema>& schema,
                       MemoryPool* pool, std::shared_ptr<RecordBatch>* batch) {
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
    const std::shared_ptr<DataType>& type = schema->field(i)->type();
    RETURN_NOT_OK(ReadArray(pool, json_columns[i], type, &columns[i]));
  }

  *batch = std::make_shared<RecordBatch>(schema, num_rows, columns);
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
        << "Array length did not match record batch length";

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
                 const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
  ArrayReader converter(json_array, type, pool);
  return converter.GetArray(array);
}

Status ReadArray(MemoryPool* pool, const rj::Value& json_array, const Schema& schema,
                 std::shared_ptr<Array>* array) {
  if (!json_array.IsObject()) {
    return Status::Invalid("Element was not a JSON object");
  }

  const auto& json_obj = json_array.GetObject();

  const auto& it_name = json_obj.FindMember("name");
  RETURN_NOT_STRING("name", it_name, json_obj);

  std::string name = it_name->value.GetString();

  std::shared_ptr<Field> result = nullptr;
  for (const std::shared_ptr<Field>& field : schema.fields()) {
    if (field->name() == name) {
      result = field;
      break;
    }
  }

  if (result == nullptr) {
    std::stringstream ss;
    ss << "Field named " << name << " not found in schema";
    return Status::KeyError(ss.str());
  }

  return ReadArray(pool, json_array, result->type(), array);
}

}  // namespace internal
}  // namespace json
}  // namespace ipc
}  // namespace arrow
