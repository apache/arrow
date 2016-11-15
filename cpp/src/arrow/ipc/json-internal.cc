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
#include <sstream>
#include <string>
#include <type_traits>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "arrow/array.h"
#include "arrow/schema.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

enum class BufferType : char { DATA, OFFSET, TYPE, VALIDITY };

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
  };
  return "UNKNOWN";
}

class BufferLayout {
 public:
  BufferLayout(BufferType type, int bit_width) : type_(type), bit_width_(bit_width) {}

  BufferType type() const { return type_; }
  int bit_width() const { return bit_width_; }

 private:
  BufferType type_;
  int bit_width_;
};

static const BufferLayout kValidityBuffer(BufferType::VALIDITY, 1);
static const BufferLayout kOffsetBuffer(BufferType::OFFSET, 32);
static const BufferLayout kTypeBuffer(BufferType::TYPE, 32);
static const BufferLayout kBooleanBuffer(BufferType::DATA, 1);
static const BufferLayout kValues64(BufferType::DATA, 64);
static const BufferLayout kValues32(BufferType::DATA, 32);
static const BufferLayout kValues16(BufferType::DATA, 16);
static const BufferLayout kValues8(BufferType::DATA, 8);

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
                              std::is_base_of<BooleanType, T>::value ||
                              std::is_base_of<NullType, T>::value,
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
    switch (type.precision()) {
      case FloatingPointMeta::HALF:
        writer_->String("HALF");
        break;
      case FloatingPointMeta::SINGLE:
        writer_->String("SINGLE");
        break;
      case FloatingPointMeta::DOUBLE:
        writer_->String("DOUBLE");
        break;
      default:
        break;
    };
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
    };
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<TimeType, T>::value ||
                              std::is_base_of<TimestampType, T>::value,
      void>::type
  WriteTypeMetadata(const T& type) {
    writer_->Key("unit");
    switch (type.unit) {
      case TimeUnit::SECOND:
        writer_->String("SECOND");
        break;
      case TimeUnit::MILLI:
        writer_->String("MILLISECOND");
        break;
      case TimeUnit::MICRO:
        writer_->String("MICROSECOND");
        break;
      case TimeUnit::NANO:
        writer_->String("NANOSECOND");
        break;
    };
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
    };

    // Write type ids
    writer_->Key("typeIds");
    writer_->StartArray();
    for (size_t i = 0; i < type.type_ids.size(); ++i) {
      writer_->Uint(type.type_ids[i]);
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
  Status WritePrimitive(const std::string& typeclass, const T& type,
      const std::vector<BufferLayout>& buffer_layout) {
    WriteName(typeclass, type);
    SetNoChildren();
    WriteBufferLayout(buffer_layout);
    return Status::OK();
  }

  template <typename T>
  Status WriteVarBytes(const std::string& typeclass, const T& type) {
    WriteName(typeclass, type);
    SetNoChildren();
    WriteBufferLayout({kValidityBuffer, kOffsetBuffer, kValues8});
    return Status::OK();
  }

  void WriteBufferLayout(const std::vector<BufferLayout>& buffer_layout) {
    writer_->Key("typeLayout");
    writer_->StartArray();

    for (const BufferLayout& buffer : buffer_layout) {
      writer_->StartObject();
      writer_->Key("type");
      writer_->String(GetBufferTypeName(buffer.type()));

      writer_->Key("typeBitWidth");
      writer_->Int(buffer.bit_width());

      writer_->EndObject();
    }
    writer_->EndArray();
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

  Status Visit(const NullType& type) override { return WritePrimitive("null", type, {}); }

  Status Visit(const BooleanType& type) override {
    return WritePrimitive("bool", type, {kValidityBuffer, kBooleanBuffer});
  }

  Status Visit(const Int8Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues8});
  }

  Status Visit(const Int16Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues16});
  }

  Status Visit(const Int32Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues32});
  }

  Status Visit(const Int64Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const UInt8Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues8});
  }

  Status Visit(const UInt16Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues16});
  }

  Status Visit(const UInt32Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues32});
  }

  Status Visit(const UInt64Type& type) override {
    return WritePrimitive("int", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const HalfFloatType& type) override {
    return WritePrimitive("floatingpoint", type, {kValidityBuffer, kValues16});
  }

  Status Visit(const FloatType& type) override {
    return WritePrimitive("floatingpoint", type, {kValidityBuffer, kValues32});
  }

  Status Visit(const DoubleType& type) override {
    return WritePrimitive("floatingpoint", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const StringType& type) override { return WriteVarBytes("utf8", type); }

  Status Visit(const BinaryType& type) override { return WriteVarBytes("binary", type); }

  Status Visit(const DateType& type) override {
    return WritePrimitive("date", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const TimeType& type) override {
    return WritePrimitive("time", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const TimestampType& type) override {
    return WritePrimitive("timestamp", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const IntervalType& type) override {
    return WritePrimitive("interval", type, {kValidityBuffer, kValues64});
  }

  Status Visit(const DecimalType& type) override { return Status::NotImplemented("NYI"); }

  Status Visit(const ListType& type) override {
    WriteName("list", type);
    RETURN_NOT_OK(WriteChildren(type.children()));
    WriteBufferLayout({kValidityBuffer, kOffsetBuffer});
    return Status::OK();
  }

  Status Visit(const StructType& type) override {
    WriteName("struct", type);
    WriteChildren(type.children());
    WriteBufferLayout({kValidityBuffer, kTypeBuffer});
    return Status::OK();
  }

  Status Visit(const UnionType& type) override {
    WriteName("union", type);
    WriteChildren(type.children());

    if (type.mode == UnionMode::SPARSE) {
      WriteBufferLayout({kValidityBuffer, kTypeBuffer});
    } else {
      WriteBufferLayout({kValidityBuffer, kTypeBuffer, kOffsetBuffer});
    }
    return Status::OK();
  }

 private:
  const Schema& schema_;
  RjWriter* writer_;
};

class JsonArrayWriter : public ArrayVisitor {
 public:
  explicit JsonArrayWriter(const std::string& name, const Array& array, RjWriter* writer)
      : name_(name), array_(array), writer_(writer) {}

  Status Write() { return VisitArray(name_, array_); }

  Status VisitArray(const std::string& name, const Array& arr) {
    writer_->StartObject();
    writer_->Key("name");
    writer_->String(name);

    writer_->Key("count");
    writer_->Int(arr.length());

    RETURN_NOT_OK(arr.Accept(this));

    writer_->EndObject();
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<IsSignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_data();
    for (auto i = 0; i < arr.length(); ++i) {
      writer_->Int64(data[i]);
    }
  }

  template <typename T>
  typename std::enable_if<IsUnsignedInt<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_data();
    for (auto i = 0; i < arr.length(); ++i) {
      writer_->Uint64(data[i]);
    }
  }

  template <typename T>
  typename std::enable_if<IsFloatingPoint<T>::value, void>::type WriteDataValues(
      const T& arr) {
    const auto data = arr.raw_data();
    for (auto i = 0; i < arr.length(); ++i) {
      writer_->Double(data[i]);
    }
  }

  // String (Utf8), Binary
  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryArray, T>::value, void>::type
  WriteDataValues(const T& arr) {
    for (auto i = 0; i < arr.length(); ++i) {
      int32_t length;
      const char* buf = reinterpret_cast<const char*>(arr.GetValue(i, &length));
      writer_->String(buf, length);
    }
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<BooleanArray, T>::value, void>::type
  WriteDataValues(const T& arr) {
    for (auto i = 0; i < arr.length(); ++i) {
      writer_->Bool(arr.Value(i));
    }
  }

  template <typename T>
  void WriteDataField(const T& arr) {
    writer_->StartArray();
    WriteDataValues(arr);
    writer_->EndArray();
  }

  template <typename T>
  void WriteOffsetsField(const T* offsets, int32_t length) {
    writer_->Key("OFFSETS");
    writer_->StartArray();
    for (auto i = 0; i < length; ++i) {
      writer_->Int64(offsets[i]);
    }
    writer_->EndArray();
  }

  void WriteValidityField(const Array& arr) {
    writer_->Key("VALIDITY");
    writer_->StartArray();
    if (arr.null_count() > 0) {
      for (auto i = 0; i < arr.length(); ++i) {
        writer_->Int(arr.IsNull(i) ? 0 : 1);
      }
    } else {
      for (auto i = 0; i < arr.length(); ++i) {
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
    WriteOffsetsField(array.raw_offsets(), array.length() + 1);
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

  Status Visit(const DateArray& array) override { return Status::NotImplemented("date"); }

  Status Visit(const TimeArray& array) override { return Status::NotImplemented("time"); }

  Status Visit(const TimestampArray& array) override {
    return Status::NotImplemented("timestamp");
  }

  Status Visit(const IntervalArray& array) override {
    return Status::NotImplemented("interval");
  }

  Status Visit(const DecimalArray& array) override {
    return Status::NotImplemented("decimal");
  }

  Status Visit(const ListArray& array) override {
    WriteValidityField(array);
    WriteOffsetsField(array.raw_offsets(), array.length() + 1);
    auto type = static_cast<const ListType*>(array.type().get());
    return WriteChildren(type->children(), {array.values()});
  }

  Status Visit(const StructArray& array) override {
    WriteValidityField(array);
    auto type = static_cast<const StructType*>(array.type().get());
    return WriteChildren(type->children(), array.fields());
  }

  Status Visit(const UnionArray& array) override {
    return Status::NotImplemented("union");
  }

 private:
  const std::string& name_;
  const Array& array_;
  RjWriter* writer_;
};

#define RETURN_NOT_FOUND(TOK, NAME, PARENT) \
  if (NAME == PARENT.MemberEnd()) {         \
    std::stringstream ss;                   \
    ss << "field " << TOK << " not found";  \
    return Status::Invalid(ss.str());       \
  }

#define RETURN_NOT_STRING(TOK, NAME, PARENT) \
  RETURN_NOT_FOUND(TOK, NAME, PARENT);       \
  if (!NAME->value.IsString()) {             \
    std::stringstream ss;                    \
    ss << "field was not a string";          \
    return Status::Invalid(ss.str());        \
  }

#define RETURN_NOT_BOOL(TOK, NAME, PARENT) \
  RETURN_NOT_FOUND(TOK, NAME, PARENT);     \
  if (!NAME->value.IsBool()) {             \
    std::stringstream ss;                  \
    ss << "field was not a boolean";       \
    return Status::Invalid(ss.str());      \
  }

#define RETURN_NOT_INT(TOK, NAME, PARENT) \
  RETURN_NOT_FOUND(TOK, NAME, PARENT);    \
  if (!NAME->value.IsInt()) {             \
    std::stringstream ss;                 \
    ss << "field was not an int";         \
    return Status::Invalid(ss.str());     \
  }

#define RETURN_NOT_ARRAY(TOK, NAME, PARENT) \
  RETURN_NOT_FOUND(TOK, NAME, PARENT);      \
  if (!NAME->value.IsArray()) {             \
    std::stringstream ss;                   \
    ss << "field was not an array";         \
    return Status::Invalid(ss.str());       \
  }

#define RETURN_NOT_OBJECT(TOK, NAME, PARENT) \
  RETURN_NOT_FOUND(TOK, NAME, PARENT);       \
  if (!NAME->value.IsObject()) {             \
    std::stringstream ss;                    \
    ss << "field was not an object";         \
    return Status::Invalid(ss.str());        \
  }

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
    for (size_t i = 0; i < fields->size(); ++i) {
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
    RETURN_NOT_OK(GetType(json_type->value, children, &type));

    *field = std::make_shared<Field>(
        json_name->value.GetString(), type, json_nullable->value.GetBool());
    return Status::OK();
  }

  Status GetInteger(const rj::Value& obj, std::shared_ptr<DataType>* type) {
    const auto& json_type = obj.GetObject();

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

  Status GetFloatingPoint(const rj::Value& obj, std::shared_ptr<DataType>* type) {
    const auto& json_type = obj.GetObject();

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

  template <typename T>
  Status GetTimeLike(const rj::Value& obj, std::shared_ptr<DataType>* type) {
    const auto& json_type = obj.GetObject();

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

    *type = std::make_shared<T>(unit);

    return Status::OK();
  }

  Status GetUnion(const rj::Value& obj,
      const std::vector<std::shared_ptr<Field>>& children,
      std::shared_ptr<DataType>* type) {
    const auto& json_type = obj.GetObject();

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
      ;
      return Status::Invalid(ss.str());
    }

    const auto& json_type_ids = json_type.FindMember("typeIds");
    RETURN_NOT_ARRAY("typeIds", json_type_ids, json_type);

    std::vector<uint8_t> type_ids;
    const auto& id_array = json_type_ids->value.GetArray();
    for (const rj::Value& val : id_array) {
      type_ids.push_back(val.GetUint());
    }

    *type = union_(children, type_ids, mode);

    return Status::OK();
  }

  Status GetType(const rj::Value& obj,
      const std::vector<std::shared_ptr<Field>>& children,
      std::shared_ptr<DataType>* type) {
    const auto& json_type = obj.GetObject();

    const auto& json_type_name = json_type.FindMember("name");
    RETURN_NOT_STRING("name", json_type_name, json_type);

    std::string type_name = json_type_name->value.GetString();

    if (type_name == "int") {
      return GetInteger(obj, type);
    } else if (type_name == "floatingpoint") {
      return GetFloatingPoint(obj, type);
    } else if (type_name == "bool") {
      *type = boolean();
    } else if (type_name == "utf8") {
      *type = utf8();
    } else if (type_name == "binary") {
      *type = binary();
    } else if (type_name == "null") {
      *type = null();
    } else if (type_name == "date") {
      *type = date();
    } else if (type_name == "time") {
      return GetTimeLike<TimeType>(obj, type);
    } else if (type_name == "timestamp") {
      return GetTimeLike<TimestampType>(obj, type);
    } else if (type_name == "list") {
      *type = list(children[0]);
    } else if (type_name == "struct") {
      *type = struct_(children);
    } else {
      return GetUnion(obj, children, type);
    }
    return Status::OK();
  }

 private:
  const rj::Value& json_schema_;
};

class JsonArrayReader {
 public:
  explicit JsonArrayReader(
      MemoryPool* pool, const rj::Value& json_array, const Schema& schema)
      : pool_(pool), json_array_(json_array), schema_(schema) {}

  Status GetResult(std::shared_ptr<Array>* array) {
    if (!json_array_.IsObject()) {
      return Status::Invalid("Array was not a JSON object");
    }
    const auto& json_array = json_array_.GetObject();

    const auto& json_name = json_array.FindMember("name");
    RETURN_NOT_STRING("name", json_name, json_array);

    return GetArrayFromStruct(
        json_array_, json_name->value.GetString(), schema_.fields(), array);
  }

  Status GetArrayFromStruct(const rj::Value& obj, const std::string& name,
      const std::vector<std::shared_ptr<Field>>& fields, std::shared_ptr<Array>* array) {
    std::shared_ptr<Field> result = nullptr;

    for (const std::shared_ptr<Field>& field : fields) {
      if (field->name == name) {
        result = field;
        break;
      }
    }

    if (result == nullptr) {
      std::stringstream ss;
      ss << "Field named " << name << " not found in struct/schema";
      return Status::KeyError(ss.str());
    }

    return GetArray(obj, result->type, array);
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value ||
                              std::is_base_of<BooleanType, T>::value,
      Status>::type
  ReadArray(const rj::Value& obj, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    typename TypeTraits<T>::BuilderType builder(pool_, type);
    const auto& json_array = obj.GetObject();

    const auto& json_data = json_array.FindMember("DATA");
    RETURN_NOT_ARRAY("DATA", json_data, json_array);

    const auto& json_data_arr = json_data->value.GetArray();

    for (auto i = 0; i < json_data_arr.Size(); ++i) {
      if (!is_valid[i]) {
        builder.AppendNull();
        continue;
      }

      const rj::Value& val = json_data_arr[i];
      if (IsSignedInt<T>::value) {
        builder.Append(val.GetInt64());
      } else if (IsUnsignedInt<T>::value) {
        builder.Append(val.GetUint64());
      } else if (IsFloatingPoint<T>::value) {
        builder.Append(val.GetFloat());
      } else if (std::is_base_of<BooleanType, T>::value) {
        builder.Append(val.GetBool());
      } else {
        // We are in the wrong function
        return Status::Invalid(type->ToString());
      }
    }

    return builder.Finish(array);
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryType, T>::value, Status>::type ReadArray(
      const rj::Value& obj, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<ListType, T>::value, Status>::type ReadArray(
      const rj::Value& obj, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<StructType, T>::value, Status>::type ReadArray(
      const rj::Value& obj, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<NullType, T>::value, Status>::type ReadArray(
      const rj::Value& obj, const std::vector<bool>& is_valid,
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array) {
    return Status::NotImplemented("null");
  }

  Status GetArray(const rj::Value& obj, const std::shared_ptr<DataType>& type,
      std::shared_ptr<Array>* array) {
    if (!obj.IsObject()) { return Status::Invalid("Array was not a JSON object"); }
    const auto& json_array = obj.GetObject();

    const auto& json_length = json_array.FindMember("count");
    RETURN_NOT_INT("count", json_length, json_array);
    int32_t length = json_length->value.GetInt();

    const auto& json_valid_iter = json_array.FindMember("VALIDITY");
    RETURN_NOT_ARRAY("VALIDITY", json_valid_iter, json_array);

    const auto& json_validity = json_valid_iter->value.GetArray();

    DCHECK_EQ(static_cast<int>(json_validity.Size()), length);

    std::vector<bool> is_valid(length);
    for (const rj::Value& val : json_validity) {
      DCHECK(val.IsInt());
      is_valid.push_back(static_cast<bool>(val.GetInt()));
    }

#define TYPE_CASE(TYPE) \
  case TYPE::type_id:   \
    return ReadArray<TYPE>(obj, is_valid, type, array);

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
      NOT_IMPLEMENTED_CASE(DATE);
      NOT_IMPLEMENTED_CASE(TIMESTAMP);
      NOT_IMPLEMENTED_CASE(TIME);
      NOT_IMPLEMENTED_CASE(INTERVAL);
      TYPE_CASE(ListType);
      TYPE_CASE(StructType);
      NOT_IMPLEMENTED_CASE(UNION);
      default:
        std::stringstream ss;
        ss << type->ToString();
        return Status::NotImplemented(ss.str());
    };

#undef TYPE_CASE
#undef NOT_IMPLEMENTED_CASE

    return Status::OK();
  }

 private:
  MemoryPool* pool_;
  const rj::Value& json_array_;
  const Schema& schema_;
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

Status ReadJsonArray(MemoryPool* pool, const rj::Value& json_array, const Schema& schema,
    std::shared_ptr<Array>* array) {
  JsonArrayReader converter(pool, json_array, schema);
  return converter.GetResult(array);
}

}  // namespace ipc
}  // namespace arrow
