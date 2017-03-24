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
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {
namespace ipc {

using RjArray = rj::Value::ConstArray;
using RjObject = rj::Value::ConstObject;

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

static std::string GetFloatingPrecisionName(FloatingPointMeta::Precision precision) {
  switch (precision) {
    case FloatingPointMeta::HALF:
      return "HALF";
    case FloatingPointMeta::SINGLE:
      return "SINGLE";
    case FloatingPointMeta::DOUBLE:
      return "DOUBLE";
    default:
      break;
  }
  return "UNKNOWN";
}

static std::string GetTimeUnitName(TimeUnit unit) {
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

class JsonSchemaWriter : public TypeVisitor {
 public:
  explicit JsonSchemaWriter(const Schema& schema, RjWriter* writer)
      : schema_(schema), writer_(writer) {}

  Status Write() {
    writer_->StartObject();
    writer_->Key("fields");
    writer_->StartArray();
    for (const std::shared_ptr<Field>& field : schema_.fields()) {
      RETURN_NOT_OK(VisitField(*field.get()));
    }
    writer_->EndArray();
    writer_->EndObject();
    return Status::OK();
  }

  Status VisitField(const Field& field) {
    writer_->StartObject();

    writer_->Key("name");
    writer_->String(field.name.c_str());

    writer_->Key("nullable");
    writer_->Bool(field.nullable);

    // Visit the type
    RETURN_NOT_OK(field.type->Accept(this));
    writer_->EndObject();

    return Status::OK();
  }

  void SetNoChildren() {
    writer_->Key("children");
    writer_->StartArray();
    writer_->EndArray();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<NoExtraMeta, T>::value ||
                              std::is_base_of<ListType, T>::value ||
                              std::is_base_of<StructType, T>::value,
      void>::type
  WriteTypeMetadata(const T& type) {}

  template <typename T>
  typename std::enable_if<std::is_base_of<IntegerMeta, T>::value, void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("bitWidth");
    writer_->Int(type.bit_width());
    writer_->Key("isSigned");
    writer_->Bool(type.is_signed());
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<FloatingPointMeta, T>::value, void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("precision");
    writer_->String(GetFloatingPrecisionName(type.precision()));
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<IntervalType, T>::value, void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("unit");
    switch (type.unit) {
      case IntervalType::Unit::YEAR_MONTH:
        writer_->String("YEAR_MONTH");
        break;
      case IntervalType::Unit::DAY_TIME:
        writer_->String("DAY_TIME");
        break;
    }
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<Time32Type, T>::value ||
                              std::is_base_of<Time64Type, T>::value ||
                              std::is_base_of<TimestampType, T>::value,
      void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("unit");
    writer_->String(GetTimeUnitName(type.unit));
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<DecimalType, T>::value, void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("precision");
    writer_->Int(type.precision);
    writer_->Key("scale");
    writer_->Int(type.scale);
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<UnionType, T>::value, void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("mode");
    switch (type.mode) {
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
    for (size_t i = 0; i < type.type_codes.size(); ++i) {
      writer_->Uint(type.type_codes[i]);
    }
    writer_->EndArray();
  }

  // TODO(wesm): Other Type metadata

  template <typename T>
  void WriteName(const std::string& typeclass, const T& type) {
    writer_->Key("type");
    writer_->StartObject();
    writer_->Key("name");
    writer_->String(typeclass);
    WriteTypeMetadata(type);
    writer_->EndObject();
  }

  template <typename T>
  Status WritePrimitive(const std::string& typeclass, const T& type) {
    WriteName(typeclass, type);
    SetNoChildren();
    WriteBufferLayout(type.GetBufferLayout());
    return Status::OK();
  }

  template <typename T>
  Status WriteVarBytes(const std::string& typeclass, const T& type) {
    WriteName(typeclass, type);
    SetNoChildren();
    WriteBufferLayout(type.GetBufferLayout());
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
      RETURN_NOT_OK(VisitField(*field.get()));
    }
    writer_->EndArray();
    return Status::OK();
  }

  Status Visit(const NullType& type) override { return WritePrimitive("null", type); }

  Status Visit(const BooleanType& type) override { return WritePrimitive("bool", type); }

  Status Visit(const Int8Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const Int16Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const Int32Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const Int64Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const UInt8Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const UInt16Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const UInt32Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const UInt64Type& type) override { return WritePrimitive("int", type); }

  Status Visit(const HalfFloatType& type) override {
    return WritePrimitive("floatingpoint", type);
  }

  Status Visit(const FloatType& type) override {
    return WritePrimitive("floatingpoint", type);
  }

  Status Visit(const DoubleType& type) override {
    return WritePrimitive("floatingpoint", type);
  }

  Status Visit(const StringType& type) override { return WriteVarBytes("utf8", type); }

  Status Visit(const BinaryType& type) override { return WriteVarBytes("binary", type); }

  // TODO
  Status Visit(const Date32Type& type) override { return WritePrimitive("date", type); }

  Status Visit(const Date64Type& type) override { return WritePrimitive("date", type); }

  Status Visit(const Time32Type& type) override { return WritePrimitive("time", type); }

  Status Visit(const Time64Type& type) override { return WritePrimitive("time", type); }

  Status Visit(const TimestampType& type) override {
    return WritePrimitive("timestamp", type);
  }

  Status Visit(const IntervalType& type) override {
    return WritePrimitive("interval", type);
  }

  Status Visit(const ListType& type) override {
    WriteName("list", type);
    RETURN_NOT_OK(WriteChildren(type.children()));
    WriteBufferLayout(type.GetBufferLayout());
    return Status::OK();
  }

  Status Visit(const StructType& type) override {
    WriteName("struct", type);
    WriteChildren(type.children());
    WriteBufferLayout(type.GetBufferLayout());
    return Status::OK();
  }

  Status Visit(const UnionType& type) override {
    WriteName("union", type);
    WriteChildren(type.children());
    WriteBufferLayout(type.GetBufferLayout());
    return Status::OK();
  }

 private:
  const Schema& schema_;
  RjWriter* writer_;
};

class JsonArrayWriter : public ArrayVisitor {
 public:
  JsonArrayWriter(const std::string& name, const Array& array, RjWriter* writer)
      : name_(name), array_(array), writer_(writer) {}

  Status Write() { return VisitArray(name_, array_); }

  Status VisitArray(const std::string& name, const Array& arr) {
    writer_->StartObject();
    writer_->Key("name");
    writer_->String(name);

    writer_->Key("count");
    writer_->Int(static_cast<int32_t>(arr.length()));

    RETURN_NOT_OK(arr.Accept(this));

    writer_->EndObject();
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<IsSignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_data();
    for (int i = 0; i < arr.length(); ++i) {
      writer_->Int64(data[i]);
    }
  }

  template <typename T>
  typename std::enable_if<IsUnsignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_data();
    for (int i = 0; i < arr.length(); ++i) {
      writer_->Uint64(data[i]);
    }
  }

  template <typename T>
  typename std::enable_if<IsFloatingPoint<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_data();
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

  template <typename T>
  typename std::enable_if<std::is_base_of<BooleanArray, T>::value, void>::type
  WriteDataValues(const T& arr) {
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

  template <typename T>
  Status WritePrimitive(const T& array) {
    WriteValidityField(array);
    WriteDataField(array);
    SetNoChildren();
    return Status::OK();
  }

  template <typename T>
  Status WriteVarBytes(const T& array) {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    WriteDataField(array);
    SetNoChildren();
    return Status::OK();
  }

  Status WriteChildren(const std::vector<std::shared_ptr<Field>>& fields,
      const std::vector<std::shared_ptr<Array>>& arrays) {
    writer_->Key("children");
    writer_->StartArray();
    for (size_t i = 0; i < fields.size(); ++i) {
      RETURN_NOT_OK(VisitArray(fields[i]->name, *arrays[i].get()));
    }
    writer_->EndArray();
    return Status::OK();
  }

  Status Visit(const NullArray& array) override {
    SetNoChildren();
    return Status::OK();
  }

  Status Visit(const BooleanArray& array) override { return WritePrimitive(array); }

  Status Visit(const Int8Array& array) override { return WritePrimitive(array); }

  Status Visit(const Int16Array& array) override { return WritePrimitive(array); }

  Status Visit(const Int32Array& array) override { return WritePrimitive(array); }

  Status Visit(const Int64Array& array) override { return WritePrimitive(array); }

  Status Visit(const UInt8Array& array) override { return WritePrimitive(array); }

  Status Visit(const UInt16Array& array) override { return WritePrimitive(array); }

  Status Visit(const UInt32Array& array) override { return WritePrimitive(array); }

  Status Visit(const UInt64Array& array) override { return WritePrimitive(array); }

  Status Visit(const HalfFloatArray& array) override { return WritePrimitive(array); }

  Status Visit(const FloatArray& array) override { return WritePrimitive(array); }

  Status Visit(const DoubleArray& array) override { return WritePrimitive(array); }

  Status Visit(const StringArray& array) override { return WriteVarBytes(array); }

  Status Visit(const BinaryArray& array) override { return WriteVarBytes(array); }

  Status Visit(const ListArray& array) override {
    WriteValidityField(array);
    WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length() + 1);
    auto type = static_cast<const ListType*>(array.type().get());
    return WriteChildren(type->children(), {array.values()});
  }

  Status Visit(const StructArray& array) override {
    WriteValidityField(array);
    auto type = static_cast<const StructType*>(array.type().get());
    return WriteChildren(type->children(), array.fields());
  }

  Status Visit(const UnionArray& array) override {
    WriteValidityField(array);
    auto type = static_cast<const UnionType*>(array.type().get());

    WriteIntegerField("TYPE_ID", array.raw_type_ids(), array.length());
    if (type->mode == UnionMode::DENSE) {
      WriteIntegerField("OFFSET", array.raw_value_offsets(), array.length());
    }
    return WriteChildren(type->children(), array.children());
  }

 private:
  const std::string& name_;
  const Array& array_;
  RjWriter* writer_;
};

class JsonSchemaReader {
 public:
  explicit JsonSchemaReader(const rj::Value& json_schema) : json_schema_(json_schema) {}

  Status GetSchema(std::shared_ptr<Schema>* schema) {
    const auto& obj_schema = json_schema_.GetObject();

    const auto& json_fields = obj_schema.FindMember("fields");
    RETURN_NOT_ARRAY("fields", json_fields, obj_schema);

    std::vector<std::shared_ptr<Field>> fields;
    RETURN_NOT_OK(GetFieldsFromArray(json_fields->value, &fields));

    *schema = std::make_shared<Schema>(fields);
    return Status::OK();
  }

  Status GetFieldsFromArray(
      const rj::Value& obj, std::vector<std::shared_ptr<Field>>* fields) {
    const auto& values = obj.GetArray();

    fields->resize(values.Size());
    for (rj::SizeType i = 0; i < fields->size(); ++i) {
      RETURN_NOT_OK(GetField(values[i], &(*fields)[i]));
    }
    return Status::OK();
  }

  Status GetField(const rj::Value& obj, std::shared_ptr<Field>* field) {
    if (!obj.IsObject()) { return Status::Invalid("Field was not a JSON object"); }
    const auto& json_field = obj.GetObject();

    const auto& json_name = json_field.FindMember("name");
    RETURN_NOT_STRING("name", json_name, json_field);

    const auto& json_nullable = json_field.FindMember("nullable");
    RETURN_NOT_BOOL("nullable", json_nullable, json_field);

    const auto& json_type = json_field.FindMember("type");
    RETURN_NOT_OBJECT("type", json_type, json_field);

    const auto& json_children = json_field.FindMember("children");
    RETURN_NOT_ARRAY("children", json_children, json_field);

    std::vector<std::shared_ptr<Field>> children;
    RETURN_NOT_OK(GetFieldsFromArray(json_children->value, &children));

    std::shared_ptr<DataType> type;
    RETURN_NOT_OK(GetType(json_type->value.GetObject(), children, &type));

    *field = std::make_shared<Field>(
        json_name->value.GetString(), type, json_nullable->value.GetBool());
    return Status::OK();
  }

  Status GetInteger(
      const rj::Value::ConstObject& json_type, std::shared_ptr<DataType>* type) {
    const auto& json_bit_width = json_type.FindMember("bitWidth");
    RETURN_NOT_INT("bitWidth", json_bit_width, json_type);

    const auto& json_is_signed = json_type.FindMember("isSigned");
    RETURN_NOT_BOOL("isSigned", json_is_signed, json_type);

    bool is_signed = json_is_signed->value.GetBool();
    int bit_width = json_bit_width->value.GetInt();

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

  Status GetFloatingPoint(const RjObject& json_type, std::shared_ptr<DataType>* type) {
    const auto& json_precision = json_type.FindMember("precision");
    RETURN_NOT_STRING("precision", json_precision, json_type);

    std::string precision = json_precision->value.GetString();

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

  Status GetTime(const RjObject& json_type, std::shared_ptr<DataType>* type) {
    const auto& json_unit = json_type.FindMember("unit");
    RETURN_NOT_STRING("unit", json_unit, json_type);

    std::string unit_str = json_unit->value.GetString();

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
    return Status::OK();
  }

  Status GetTimestamp(const RjObject& json_type, std::shared_ptr<DataType>* type) {
    const auto& json_unit = json_type.FindMember("unit");
    RETURN_NOT_STRING("unit", json_unit, json_type);

    std::string unit_str = json_unit->value.GetString();

    TimeUnit unit;
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

    *type = timestamp(unit);

    return Status::OK();
  }

  Status GetUnion(const RjObject& json_type,
      const std::vector<std::shared_ptr<Field>>& children,
      std::shared_ptr<DataType>* type) {
    const auto& json_mode = json_type.FindMember("mode");
    RETURN_NOT_STRING("mode", json_mode, json_type);

    std::string mode_str = json_mode->value.GetString();
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

    const auto& json_type_codes = json_type.FindMember("typeIds");
    RETURN_NOT_ARRAY("typeIds", json_type_codes, json_type);

    std::vector<uint8_t> type_codes;
    const auto& id_array = json_type_codes->value.GetArray();
    for (const rj::Value& val : id_array) {
      DCHECK(val.IsUint());
      type_codes.push_back(static_cast<uint8_t>(val.GetUint()));
    }

    *type = union_(children, type_codes, mode);

    return Status::OK();
  }

  Status GetType(const RjObject& json_type,
      const std::vector<std::shared_ptr<Field>>& children,
      std::shared_ptr<DataType>* type) {
    const auto& json_type_name = json_type.FindMember("name");
    RETURN_NOT_STRING("name", json_type_name, json_type);

    std::string type_name = json_type_name->value.GetString();

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
    } else if (type_name == "null") {
      *type = null();
    } else if (type_name == "date") {
      // TODO
      *type = date64();
    } else if (type_name == "time") {
      return GetTime(json_type, type);
    } else if (type_name == "timestamp") {
      return GetTimestamp(json_type, type);
    } else if (type_name == "list") {
      *type = list(children[0]);
    } else if (type_name == "struct") {
      *type = struct_(children);
    } else {
      return GetUnion(json_type, children, type);
    }
    return Status::OK();
  }

 private:
  const rj::Value& json_schema_;
};

template <typename T>
inline typename std::enable_if<IsSignedInt<T>::value, typename T::c_type>::type
UnboxValue(const rj::Value& val) {
  DCHECK(val.IsInt());
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

class JsonArrayReader {
 public:
  explicit JsonArrayReader(MemoryPool* pool) : pool_(pool) {}

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
  typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value ||
                              std::is_base_of<BooleanType, T>::value,
      Status>::type
  ReadArray(const RjObject& json_array, int32_t length, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    typename TypeTraits<T>::BuilderType builder(pool_, type);

    const auto& json_data = json_array.FindMember("DATA");
    RETURN_NOT_ARRAY("DATA", json_data, json_array);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length);
    for (int i = 0; i < length; ++i) {
      if (!is_valid[i]) {
        builder.AppendNull();
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      builder.Append(UnboxValue<T>(val));
    }

    return builder.Finish(array);
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryType, T>::value, Status>::type ReadArray(
      const RjObject& json_array, int32_t length, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    typename TypeTraits<T>::BuilderType builder(pool_);

    const auto& json_data = json_array.FindMember("DATA");
    RETURN_NOT_ARRAY("DATA", json_data, json_array);

    const auto& json_data_arr = json_data->value.GetArray();

    DCHECK_EQ(static_cast<int32_t>(json_data_arr.Size()), length);

    auto byte_buffer = std::make_shared<PoolBuffer>(pool_);
    for (int i = 0; i < length; ++i) {
      if (!is_valid[i]) {
        builder.AppendNull();
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      DCHECK(val.IsString());
      if (std::is_base_of<StringType, T>::value) {
        builder.Append(val.GetString());
      } else {
        std::string hex_string = val.GetString();

        DCHECK(hex_string.size() % 2 == 0) << "Expected base16 hex string";
        int32_t length = static_cast<int>(hex_string.size()) / 2;

        if (byte_buffer->size() < length) { RETURN_NOT_OK(byte_buffer->Resize(length)); }

        const char* hex_data = hex_string.c_str();
        uint8_t* byte_buffer_data = byte_buffer->mutable_data();
        for (int32_t j = 0; j < length; ++j) {
          RETURN_NOT_OK(ParseHexValue(hex_data + j * 2, &byte_buffer_data[j]));
        }
        RETURN_NOT_OK(builder.Append(byte_buffer_data, length));
      }
    }

    return builder.Finish(array);
  }

  template <typename T>
  Status GetIntArray(
      const RjArray& json_array, const int32_t length, std::shared_ptr<Buffer>* out) {
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

  template <typename T>
  typename std::enable_if<std::is_base_of<ListType, T>::value, Status>::type ReadArray(
      const RjObject& json_array, int32_t length, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid, &null_count, &validity_buffer));

    const auto& json_offsets = json_array.FindMember("OFFSET");
    RETURN_NOT_ARRAY("OFFSET", json_offsets, json_array);
    std::shared_ptr<Buffer> offsets_buffer;
    RETURN_NOT_OK(GetIntArray<int32_t>(
        json_offsets->value.GetArray(), length + 1, &offsets_buffer));

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(json_array, type, &children));
    DCHECK_EQ(children.size(), 1);

    *array = std::make_shared<ListArray>(
        type, length, offsets_buffer, children[0], validity_buffer, null_count);

    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<StructType, T>::value, Status>::type ReadArray(
      const RjObject& json_array, int32_t length, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    int32_t null_count = 0;
    std::shared_ptr<Buffer> validity_buffer;
    RETURN_NOT_OK(GetValidityBuffer(is_valid, &null_count, &validity_buffer));

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(GetChildren(json_array, type, &fields));

    *array =
        std::make_shared<StructArray>(type, length, fields, validity_buffer, null_count);

    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<UnionType, T>::value, Status>::type ReadArray(
      const RjObject& json_array, int32_t length, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    int32_t null_count = 0;

    const auto& union_type = static_cast<const UnionType&>(*type.get());

    std::shared_ptr<Buffer> validity_buffer;
    std::shared_ptr<Buffer> type_id_buffer;
    std::shared_ptr<Buffer> offsets_buffer;

    RETURN_NOT_OK(GetValidityBuffer(is_valid, &null_count, &validity_buffer));

    const auto& json_type_ids = json_array.FindMember("TYPE_ID");
    RETURN_NOT_ARRAY("TYPE_ID", json_type_ids, json_array);
    RETURN_NOT_OK(
        GetIntArray<uint8_t>(json_type_ids->value.GetArray(), length, &type_id_buffer));

    if (union_type.mode == UnionMode::DENSE) {
      const auto& json_offsets = json_array.FindMember("OFFSET");
      RETURN_NOT_ARRAY("OFFSET", json_offsets, json_array);
      RETURN_NOT_OK(
          GetIntArray<int32_t>(json_offsets->value.GetArray(), length, &offsets_buffer));
    }

    std::vector<std::shared_ptr<Array>> children;
    RETURN_NOT_OK(GetChildren(json_array, type, &children));

    *array = std::make_shared<UnionArray>(type, length, children, type_id_buffer,
        offsets_buffer, validity_buffer, null_count);

    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<NullType, T>::value, Status>::type ReadArray(
      const RjObject& json_array, int32_t length, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    *array = std::make_shared<NullArray>(length);
    return Status::OK();
  }

  Status GetChildren(const RjObject& json_array, const std::shared_ptr<DataType>& type,
      std::vector<std::shared_ptr<Array>>* array) {
    const auto& json_children = json_array.FindMember("children");
    RETURN_NOT_ARRAY("children", json_children, json_array);
    const auto& json_children_arr = json_children->value.GetArray();

    if (type->num_children() != static_cast<int>(json_children_arr.Size())) {
      std::stringstream ss;
      ss << "Expected " << type->num_children() << " children, but got "
         << json_children_arr.Size();
      return Status::Invalid(ss.str());
    }

    for (int i = 0; i < static_cast<int>(json_children_arr.Size()); ++i) {
      const rj::Value& json_child = json_children_arr[i];
      DCHECK(json_child.IsObject());

      std::shared_ptr<Field> child_field = type->child(i);

      auto it = json_child.FindMember("name");
      RETURN_NOT_STRING("name", it, json_child);

      DCHECK_EQ(it->value.GetString(), child_field->name);
      std::shared_ptr<Array> child;
      RETURN_NOT_OK(GetArray(json_children_arr[i], child_field->type, &child));
      array->emplace_back(child);
    }

    return Status::OK();
  }

  Status GetArray(const rj::Value& obj, const std::shared_ptr<DataType>& type,
      std::shared_ptr<Array>* array) {
    if (!obj.IsObject()) {
      return Status::Invalid("Array element was not a JSON object");
    }
    const auto& json_array = obj.GetObject();

    const auto& json_length = json_array.FindMember("count");
    RETURN_NOT_INT("count", json_length, json_array);
    int32_t length = json_length->value.GetInt();

    const auto& json_valid_iter = json_array.FindMember("VALIDITY");
    RETURN_NOT_ARRAY("VALIDITY", json_valid_iter, json_array);

    const auto& json_validity = json_valid_iter->value.GetArray();

    DCHECK_EQ(static_cast<int>(json_validity.Size()), length);

    std::vector<bool> is_valid;
    for (const rj::Value& val : json_validity) {
      DCHECK(val.IsInt());
      is_valid.push_back(static_cast<bool>(val.GetInt()));
    }

#define TYPE_CASE(TYPE) \
  case TYPE::type_id:   \
    return ReadArray<TYPE>(json_array, length, is_valid, type, array);

#define NOT_IMPLEMENTED_CASE(TYPE_ENUM)      \
  case Type::TYPE_ENUM: {                    \
    std::stringstream ss;                    \
    ss << type->ToString();                  \
    return Status::NotImplemented(ss.str()); \
  }

    switch (type->type) {
      TYPE_CASE(NullType);
      TYPE_CASE(BooleanType);
      TYPE_CASE(UInt8Type);
      TYPE_CASE(Int8Type);
      TYPE_CASE(UInt16Type);
      TYPE_CASE(Int16Type);
      TYPE_CASE(UInt32Type);
      TYPE_CASE(Int32Type);
      TYPE_CASE(UInt64Type);
      TYPE_CASE(Int64Type);
      TYPE_CASE(HalfFloatType);
      TYPE_CASE(FloatType);
      TYPE_CASE(DoubleType);
      TYPE_CASE(StringType);
      TYPE_CASE(BinaryType);
      NOT_IMPLEMENTED_CASE(DATE32);
      NOT_IMPLEMENTED_CASE(DATE64);
      NOT_IMPLEMENTED_CASE(TIMESTAMP);
      NOT_IMPLEMENTED_CASE(TIME32);
      NOT_IMPLEMENTED_CASE(TIME64);
      NOT_IMPLEMENTED_CASE(INTERVAL);
      TYPE_CASE(ListType);
      TYPE_CASE(StructType);
      TYPE_CASE(UnionType);
      NOT_IMPLEMENTED_CASE(DICTIONARY);
      default:
        std::stringstream ss;
        ss << type->ToString();
        return Status::NotImplemented(ss.str());
    }

#undef TYPE_CASE
#undef NOT_IMPLEMENTED_CASE

    return Status::OK();
  }

 private:
  MemoryPool* pool_;
};

Status WriteJsonSchema(const Schema& schema, RjWriter* json_writer) {
  JsonSchemaWriter converter(schema, json_writer);
  return converter.Write();
}

Status ReadJsonSchema(const rj::Value& json_schema, std::shared_ptr<Schema>* schema) {
  JsonSchemaReader converter(json_schema);
  return converter.GetSchema(schema);
}

Status WriteJsonArray(
    const std::string& name, const Array& array, RjWriter* json_writer) {
  JsonArrayWriter converter(name, array, json_writer);
  return converter.Write();
}

Status ReadJsonArray(MemoryPool* pool, const rj::Value& json_array,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
  JsonArrayReader converter(pool);
  return converter.GetArray(json_array, type, array);
}

Status ReadJsonArray(MemoryPool* pool, const rj::Value& json_array, const Schema& schema,
    std::shared_ptr<Array>* array) {
  if (!json_array.IsObject()) { return Status::Invalid("Element was not a JSON object"); }

  const auto& json_obj = json_array.GetObject();

  const auto& json_name = json_obj.FindMember("name");
  RETURN_NOT_STRING("name", json_name, json_obj);

  std::string name = json_name->value.GetString();

  std::shared_ptr<Field> result = nullptr;
  for (const std::shared_ptr<Field>& field : schema.fields()) {
    if (field->name == name) {
      result = field;
      break;
    }
  }

  if (result == nullptr) {
    std::stringstream ss;
    ss << "Field named " << name << " not found in schema";
    return Status::KeyError(ss.str());
  }

  return ReadJsonArray(pool, json_array, result->type, array);
}

}  // namespace ipc
}  // namespace arrow
