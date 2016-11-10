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

#include "arrow/schema.h"
#include "arrow/type.h"
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
      case UnionType::SPARSE:
        writer_->String("SPARSE");
        break;
      case UnionType::DENSE:
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
  void WritePrimitive(const std::string& typeclass, const T& type,
      const std::vector<BufferLayout>& buffer_layout) {
    WriteName(typeclass, type);
    SetNoChildren();
    WriteBufferLayout(buffer_layout);
  }

  template <typename T>
  void WriteVarBytes(const std::string& typeclass, const T& type) {
    WriteName(typeclass, type);
    SetNoChildren();
    WriteBufferLayout({kValidityBuffer, kOffsetBuffer, kValues8});
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

  Status Visit(const NullType& type) override {
    WritePrimitive("null", type, {});
    return Status::OK();
  }

  Status Visit(const BooleanType& type) override {
    WritePrimitive("bool", type, {kValidityBuffer, kBooleanBuffer});
    return Status::OK();
  }

  Status Visit(const Int8Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues8});
    return Status::OK();
  }

  Status Visit(const Int16Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues16});
    return Status::OK();
  }

  Status Visit(const Int32Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues32});
    return Status::OK();
  }

  Status Visit(const Int64Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const UInt8Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues8});
    return Status::OK();
  }

  Status Visit(const UInt16Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues16});
    return Status::OK();
  }

  Status Visit(const UInt32Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues32});
    return Status::OK();
  }

  Status Visit(const UInt64Type& type) override {
    WritePrimitive("int", type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) override {
    WritePrimitive("floatingpoint", type, {kValidityBuffer, kValues16});
    return Status::OK();
  }

  Status Visit(const FloatType& type) override {
    WritePrimitive("floatingpoint", type, {kValidityBuffer, kValues32});
    return Status::OK();
  }

  Status Visit(const DoubleType& type) override {
    WritePrimitive("floatingpoint", type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const StringType& type) override {
    WriteVarBytes("utf8", type);
    return Status::OK();
  }

  Status Visit(const BinaryType& type) override {
    WriteVarBytes("binary", type);
    return Status::OK();
  }

  Status Visit(const DateType& type) override {
    WritePrimitive("date", type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const TimeType& type) override {
    WritePrimitive("time", type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const TimestampType& type) override {
    WritePrimitive("timestamp", type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const IntervalType& type) override {
    WritePrimitive("interval", type, {kValidityBuffer, kValues64});
    return Status::OK();
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

    if (type.mode == UnionType::SPARSE) {
      WriteBufferLayout({kValidityBuffer, kTypeBuffer});
    } else {
      WriteBufferLayout({kValidityBuffer, kTypeBuffer, kOffsetBuffer});
    }
    return Status::NotImplemented("NYI");
  }

 private:
  const Schema& schema_;
  RjWriter* writer_;
};

#define RETURN_NOT_FOUND(NAME, PARENT) \
  if (NAME == PARENT.MemberEnd()) {    \
    std::stringstream ss;              \
    ss << "field not found";           \
    return Status::Invalid(ss.str());  \
  }

#define RETURN_NOT_STRING(NAME, PARENT) \
  RETURN_NOT_FOUND(NAME, PARENT);       \
  if (!NAME->value.IsString()) {        \
    std::stringstream ss;               \
    ss << "field was not a string";     \
    return Status::Invalid(ss.str());   \
  }

#define RETURN_NOT_BOOL(NAME, PARENT) \
  RETURN_NOT_FOUND(NAME, PARENT);     \
  if (!NAME->value.IsBool()) {        \
    std::stringstream ss;             \
    ss << "field was not a boolean";  \
    return Status::Invalid(ss.str()); \
  }

#define RETURN_NOT_INT(NAME, PARENT)  \
  RETURN_NOT_FOUND(NAME, PARENT);     \
  if (!NAME->value.IsInt()) {         \
    std::stringstream ss;             \
    ss << "field was not an int";     \
    return Status::Invalid(ss.str()); \
  }

#define RETURN_NOT_ARRAY(NAME, PARENT) \
  RETURN_NOT_FOUND(NAME, PARENT);      \
  if (!NAME->value.IsArray()) {        \
    std::stringstream ss;              \
    ss << "field was not an array";    \
    return Status::Invalid(ss.str());  \
  }

#define RETURN_NOT_OBJECT(NAME, PARENT) \
  RETURN_NOT_FOUND(NAME, PARENT);       \
  if (!NAME->value.IsObject()) {        \
    std::stringstream ss;               \
    ss << "field was not an object";    \
    return Status::Invalid(ss.str());   \
  }

class JsonSchemaReader {
 public:
  explicit JsonSchemaReader(const rj::Value& json_schema) : json_schema_(json_schema) {}

  Status GetSchema(std::shared_ptr<Schema>* schema) {
    const auto& obj_schema = json_schema_.GetObject();

    const auto& json_fields = obj_schema.FindMember("fields");
    RETURN_NOT_ARRAY(json_fields, obj_schema);

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
    RETURN_NOT_STRING(json_name, json_field);

    const auto& json_nullable = json_field.FindMember("nullable");
    RETURN_NOT_BOOL(json_nullable, json_field);

    const auto& json_type = json_field.FindMember("type");
    RETURN_NOT_OBJECT(json_type, json_field);

    const auto& json_children = json_field.FindMember("children");
    RETURN_NOT_ARRAY(json_children, json_field);

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
    RETURN_NOT_INT(json_bit_width, json_type);

    const auto& json_is_signed = json_type.FindMember("isSigned");
    RETURN_NOT_BOOL(json_is_signed, json_type);

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
        if (is_signed) {
          *type = std::make_shared<Int32Type>();
        } else {
          *type = std::make_shared<UInt32Type>();
        }
        break;
      case 64:
        if (is_signed) {
          *type = std::make_shared<Int64Type>();
        } else {
          *type = std::make_shared<UInt64Type>();
        }
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
    RETURN_NOT_STRING(json_precision, json_type);

    std::string precision = json_precision->value.GetString();

    if (precision == "DOUBLE") {
      *type = std::make_shared<DoubleType>();
    } else if (precision == "SINGLE") {
      *type = std::make_shared<FloatType>();
    } else if (precision == "HALF") {
      *type = std::make_shared<HalfFloatType>();
    } else {
      std::stringstream ss;
      ss << "Invalid precision: " << precision;
      return Status::Invalid(ss.str());
    }
    return Status::OK();
  }

  Status GetType(const rj::Value& obj,
      const std::vector<std::shared_ptr<Field>>& children,
      std::shared_ptr<DataType>* type) {
    const auto& json_type = obj.GetObject();

    const auto& json_type_name = json_type.FindMember("name");
    RETURN_NOT_STRING(json_type_name, json_type);

    std::string type_name = json_type_name->value.GetString();

    if (type_name == "int") {
      return GetInteger(obj, type);
    } else if (type_name == "floatingpoint") {
      return GetFloatingPoint(obj, type);
    } else if (type_name == "bool") {
      *type = std::make_shared<BooleanType>();
    } else if (type_name == "utf8") {
      *type = std::make_shared<StringType>();
    } else if (type_name == "binary") {
      *type = std::make_shared<BinaryType>();
    } else if (type_name == "null") {
      *type = std::make_shared<NullType>();
    } else if (type_name == "list") {
      *type = std::make_shared<ListType>(children[0]);
    } else if (type_name == "struct") {
      *type = std::make_shared<StructType>(children);
    } else {
      return Status::NotImplemented(type_name);
    }
    return Status::OK();
  }

 private:
  const rj::Value& json_schema_;
};

class JsonArrayReader {
 public:
  explicit JsonArrayReader(const rj::Value& json_array) : json_array_(json_array) {}

  Status GetArray(std::shared_ptr<Array>* array) {
    if (!json_array_.IsObject()) {
      return Status::Invalid("Array was not a JSON object");
    }

    return Status::OK();
  }

 private:
  const rj::Value& json_array_;
};

Status WriteJsonSchema(const Schema& schema, RjWriter* json_writer) {
  JsonSchemaWriter converter(schema, json_writer);
  return converter.Write();
}

Status ReadJsonSchema(const rj::Value& json_schema, std::shared_ptr<Schema>* schema) {
  JsonSchemaReader converter(json_schema);
  return converter.GetSchema(schema);
}

Status ReadJsonArray(const rj::Value& json_array, std::shared_ptr<Array>* array) {
  JsonArrayReader converter(json_array);
  return converter.GetArray(array);
}

}  // namespace ipc
}  // namespace arrow
