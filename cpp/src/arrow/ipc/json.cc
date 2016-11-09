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

#include "arrow/ipc/json.h"

#define RAPIDJSON_HAS_STDSTRING 1
#define RAPIDJSON_HAS_CXX11_RVALUE_REFS 1
#define RAPIDJSON_HAS_CXX11_RANGE_FOR 1

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "arrow/type.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

namespace rj = rapidjson;

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
  explicit JsonSchemaWriter(rj::Writer<rj::StringBuffer>* writer) : writer_(writer) {}

  void Start() {
    writer_->Key("schema");
    writer_->StartArray();
  }

  void Finish() { writer_->EndArray(); }

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

  template <typename T>
  void WritePrimitive(const T& type, const std::vector<BufferLayout>& buffer_layout) {
    WriteName(type);
    SetNoChildren();
    WriteBufferLayout(buffer_layout);
  }

  template <typename T>
  void WriteVarBytes(const T& type) {
    WriteName(type);
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

  void WriteChildren(const std::vector<std::shared_ptr<Field>>& children) {}

  void SetNoChildren() {
    writer_->Key("children");
    writer_->StartArray();
    writer_->EndArray();
  }

  template <typename T>
  void WriteName(const T& type) {
    writer_->Key("type");
    writer_->String(T::NAME);
  }

  Status Visit(const NullType& type) override {
    WritePrimitive(type, {});
    return Status::OK();
  }

  Status Visit(const BooleanType& type) override {
    WritePrimitive(type, {kValidityBuffer, kBooleanBuffer});
    return Status::OK();
  }

  Status Visit(const Int8Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues8});
    return Status::OK();
  }

  Status Visit(const Int16Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues16});
    return Status::OK();
  }

  Status Visit(const Int32Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues32});
    return Status::OK();
  }

  Status Visit(const Int64Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const UInt8Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues8});
    return Status::OK();
  }

  Status Visit(const UInt16Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues16});
    return Status::OK();
  }

  Status Visit(const UInt32Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues32});
    return Status::OK();
  }

  Status Visit(const UInt64Type& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues16});
    return Status::OK();
  }

  Status Visit(const FloatType& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues32});
    return Status::OK();
  }

  Status Visit(const DoubleType& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const StringType& type) override {
    WriteVarBytes(type);
    return Status::OK();
  }

  Status Visit(const BinaryType& type) override {
    WriteVarBytes(type);
    return Status::OK();
  }

  Status Visit(const DateType& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const TimeType& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const TimestampType& type) override {
    WritePrimitive(type, {kValidityBuffer, kValues64});
    return Status::OK();
  }

  Status Visit(const DecimalType& type) override { return Status::NotImplemented("NYI"); }

  Status Visit(const ListType& type) override {
    WriteName(type);
    WriteChildren(type.children());
    WriteBufferLayout({kValidityBuffer, kOffsetBuffer});
    return Status::OK();
  }

  Status Visit(const StructType& type) override {
    WriteName(type);
    WriteChildren(type.children());
    WriteBufferLayout({kValidityBuffer, kTypeBuffer});
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) override {
    WriteName(type);
    WriteChildren(type.children());
    WriteBufferLayout({kValidityBuffer, kTypeBuffer, kOffsetBuffer});
    return Status::NotImplemented("NYI");
  }

  Status Visit(const SparseUnionType& type) override {
    WriteName(type);
    WriteChildren(type.children());
    WriteBufferLayout({kValidityBuffer, kTypeBuffer});
    return Status::NotImplemented("NYI");
  }

  static void test() {
    rj::StringBuffer s;
    rj::Writer<rj::StringBuffer> writer(s);
    auto schema_writer = std::make_shared<JsonSchemaWriter>(&writer);
  }

 private:
  rj::Writer<rj::StringBuffer>* writer_;
};

}  // namespace ipc
}  // namespace arrow
