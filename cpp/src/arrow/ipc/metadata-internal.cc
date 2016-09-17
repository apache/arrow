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

#include "arrow/ipc/metadata-internal.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include "flatbuffers/flatbuffers.h"

#include "arrow/ipc/Message_generated.h"
#include "arrow/schema.h"
#include "arrow/type.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

typedef flatbuffers::FlatBufferBuilder FBB;
typedef flatbuffers::Offset<arrow::flatbuf::Field> FieldOffset;
typedef flatbuffers::Offset<void> Offset;

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

const std::shared_ptr<DataType> BOOL = std::make_shared<BooleanType>();
const std::shared_ptr<DataType> INT8 = std::make_shared<Int8Type>();
const std::shared_ptr<DataType> INT16 = std::make_shared<Int16Type>();
const std::shared_ptr<DataType> INT32 = std::make_shared<Int32Type>();
const std::shared_ptr<DataType> INT64 = std::make_shared<Int64Type>();
const std::shared_ptr<DataType> UINT8 = std::make_shared<UInt8Type>();
const std::shared_ptr<DataType> UINT16 = std::make_shared<UInt16Type>();
const std::shared_ptr<DataType> UINT32 = std::make_shared<UInt32Type>();
const std::shared_ptr<DataType> UINT64 = std::make_shared<UInt64Type>();
const std::shared_ptr<DataType> FLOAT = std::make_shared<FloatType>();
const std::shared_ptr<DataType> DOUBLE = std::make_shared<DoubleType>();

static Status IntFromFlatbuffer(
    const flatbuf::Int* int_data, std::shared_ptr<DataType>* out) {
  if (int_data->bitWidth() > 64) {
    return Status::NotImplemented("Integers with more than 64 bits not implemented");
  }
  if (int_data->bitWidth() < 8) {
    return Status::NotImplemented("Integers with less than 8 bits not implemented");
  }

  switch (int_data->bitWidth()) {
    case 8:
      *out = int_data->is_signed() ? INT8 : UINT8;
      break;
    case 16:
      *out = int_data->is_signed() ? INT16 : UINT16;
      break;
    case 32:
      *out = int_data->is_signed() ? INT32 : UINT32;
      break;
    case 64:
      *out = int_data->is_signed() ? INT64 : UINT64;
      break;
    default:
      return Status::NotImplemented("Integers not in cstdint are not implemented");
  }
  return Status::OK();
}

static Status FloatFromFlatuffer(
    const flatbuf::FloatingPoint* float_data, std::shared_ptr<DataType>* out) {
  if (float_data->precision() == flatbuf::Precision_SINGLE) {
    *out = FLOAT;
  } else {
    *out = DOUBLE;
  }
  return Status::OK();
}

static Status TypeFromFlatbuffer(flatbuf::Type type, const void* type_data,
    const std::vector<std::shared_ptr<Field>>& children, std::shared_ptr<DataType>* out) {
  switch (type) {
    case flatbuf::Type_NONE:
      return Status::Invalid("Type metadata cannot be none");
    case flatbuf::Type_Int:
      return IntFromFlatbuffer(static_cast<const flatbuf::Int*>(type_data), out);
    case flatbuf::Type_FloatingPoint:
      return FloatFromFlatuffer(
          static_cast<const flatbuf::FloatingPoint*>(type_data), out);
    case flatbuf::Type_Binary:
    case flatbuf::Type_Utf8:
      return Status::NotImplemented("Type is not implemented");
    case flatbuf::Type_Bool:
      *out = BOOL;
      return Status::OK();
    case flatbuf::Type_Decimal:
    case flatbuf::Type_Timestamp:
    case flatbuf::Type_List:
      if (children.size() != 1) {
        return Status::Invalid("List must have exactly 1 child field");
      }
      *out = std::make_shared<ListType>(children[0]);
      return Status::OK();
    case flatbuf::Type_Struct_:
      *out = std::make_shared<StructType>(children);
      return Status::OK();
    case flatbuf::Type_Union:
      return Status::NotImplemented("Type is not implemented");
    default:
      return Status::Invalid("Unrecognized type");
  }
}

// Forward declaration
static Status FieldToFlatbuffer(
    FBB& fbb, const std::shared_ptr<Field>& field, FieldOffset* offset);

static Offset IntToFlatbuffer(FBB& fbb, int bitWidth, bool is_signed) {
  return flatbuf::CreateInt(fbb, bitWidth, is_signed).Union();
}

static Offset FloatToFlatbuffer(FBB& fbb, flatbuf::Precision precision) {
  return flatbuf::CreateFloatingPoint(fbb, precision).Union();
}

static Status ListToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, Offset* offset) {
  FieldOffset field;
  RETURN_NOT_OK(FieldToFlatbuffer(fbb, type->child(0), &field));
  out_children->push_back(field);
  *offset = flatbuf::CreateList(fbb).Union();
  return Status::OK();
}

static Status StructToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, Offset* offset) {
  FieldOffset field;
  for (int i = 0; i < type->num_children(); ++i) {
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, type->child(i), &field));
    out_children->push_back(field);
  }
  *offset = flatbuf::CreateStruct_(fbb).Union();
  return Status::OK();
}

#define INT_TO_FB_CASE(BIT_WIDTH, IS_SIGNED)            \
  *out_type = flatbuf::Type_Int;                        \
  *offset = IntToFlatbuffer(fbb, BIT_WIDTH, IS_SIGNED); \
  break;

static Status TypeToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* children, flatbuf::Type* out_type, Offset* offset) {
  switch (type->type) {
    case Type::BOOL:
      *out_type = flatbuf::Type_Bool;
      *offset = flatbuf::CreateBool(fbb).Union();
      break;
    case Type::UINT8:
      INT_TO_FB_CASE(8, false);
    case Type::INT8:
      INT_TO_FB_CASE(8, true);
    case Type::UINT16:
      INT_TO_FB_CASE(16, false);
    case Type::INT16:
      INT_TO_FB_CASE(16, true);
    case Type::UINT32:
      INT_TO_FB_CASE(32, false);
    case Type::INT32:
      INT_TO_FB_CASE(32, true);
    case Type::UINT64:
      INT_TO_FB_CASE(64, false);
    case Type::INT64:
      INT_TO_FB_CASE(64, true);
    case Type::FLOAT:
      *out_type = flatbuf::Type_FloatingPoint;
      *offset = FloatToFlatbuffer(fbb, flatbuf::Precision_SINGLE);
      break;
    case Type::DOUBLE:
      *out_type = flatbuf::Type_FloatingPoint;
      *offset = FloatToFlatbuffer(fbb, flatbuf::Precision_DOUBLE);
      break;
    case Type::LIST:
      *out_type = flatbuf::Type_List;
      return ListToFlatbuffer(fbb, type, children, offset);
    case Type::STRUCT:
      *out_type = flatbuf::Type_Struct_;
      return StructToFlatbuffer(fbb, type, children, offset);
    default:
      *out_type = flatbuf::Type_NONE;  // Make clang-tidy happy
      std::stringstream ss;
      ss << "Unable to convert type: " << type->ToString() << std::endl;
      return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

static Status FieldToFlatbuffer(
    FBB& fbb, const std::shared_ptr<Field>& field, FieldOffset* offset) {
  auto fb_name = fbb.CreateString(field->name);

  flatbuf::Type type_enum;
  Offset type_data;
  std::vector<FieldOffset> children;

  RETURN_NOT_OK(TypeToFlatbuffer(fbb, field->type, &children, &type_enum, &type_data));
  auto fb_children = fbb.CreateVector(children);

  // TODO: produce the list of VectorTypes
  *offset = flatbuf::CreateField(fbb, fb_name, field->nullable, type_enum, type_data,
      field->dictionary, fb_children);

  return Status::OK();
}

Status FieldFromFlatbuffer(const flatbuf::Field* field, std::shared_ptr<Field>* out) {
  std::shared_ptr<DataType> type;

  auto children = field->children();
  std::vector<std::shared_ptr<Field>> child_fields(children->size());
  for (size_t i = 0; i < children->size(); ++i) {
    RETURN_NOT_OK(FieldFromFlatbuffer(children->Get(i), &child_fields[i]));
  }

  RETURN_NOT_OK(
      TypeFromFlatbuffer(field->type_type(), field->type(), child_fields, &type));

  *out = std::make_shared<Field>(field->name()->str(), type);
  return Status::OK();
}

// Implement MessageBuilder

// will return the endianness of the system we are running on
// based the NUMPY_API function. See NOTICE.txt
flatbuf::Endianness endianness() {
  union {
    uint32_t i;
    char c[4];
  } bint = {0x01020304};

  return bint.c[0] == 1 ? flatbuf::Endianness_Big : flatbuf::Endianness_Little;
}

Status MessageBuilder::SetSchema(const Schema* schema) {
  header_type_ = flatbuf::MessageHeader_Schema;

  std::vector<FieldOffset> field_offsets;
  for (int i = 0; i < schema->num_fields(); ++i) {
    const std::shared_ptr<Field>& field = schema->field(i);
    FieldOffset offset;
    RETURN_NOT_OK(FieldToFlatbuffer(fbb_, field, &offset));
    field_offsets.push_back(offset);
  }

  header_ =
      flatbuf::CreateSchema(fbb_, endianness(), fbb_.CreateVector(field_offsets)).Union();
  body_length_ = 0;
  return Status::OK();
}

Status MessageBuilder::SetRecordBatch(int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers) {
  header_type_ = flatbuf::MessageHeader_RecordBatch;
  header_ = flatbuf::CreateRecordBatch(fbb_, length, fbb_.CreateVectorOfStructs(nodes),
                fbb_.CreateVectorOfStructs(buffers))
                .Union();
  body_length_ = body_length;

  return Status::OK();
}

Status WriteDataHeader(int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers, std::shared_ptr<Buffer>* out) {
  MessageBuilder message;
  RETURN_NOT_OK(message.SetRecordBatch(length, body_length, nodes, buffers));
  RETURN_NOT_OK(message.Finish());
  return message.GetBuffer(out);
}

Status MessageBuilder::Finish() {
  auto message =
      flatbuf::CreateMessage(fbb_, kMetadataVersion, header_type_, header_, body_length_);
  fbb_.Finish(message);
  return Status::OK();
}

Status MessageBuilder::GetBuffer(std::shared_ptr<Buffer>* out) {
  // The message buffer is prefixed by the size of the complete flatbuffer as
  // int32_t
  // <int32_t: flatbuffer size><uint8_t*: flatbuffer data>
  int32_t size = fbb_.GetSize();

  auto result = std::make_shared<PoolBuffer>();
  RETURN_NOT_OK(result->Resize(size + sizeof(int32_t)));

  uint8_t* dst = result->mutable_data();
  memcpy(dst, reinterpret_cast<int32_t*>(&size), sizeof(int32_t));
  memcpy(dst + sizeof(int32_t), fbb_.GetBufferPointer(), size);

  *out = result;
  return Status::OK();
}

}  // namespace ipc
}  // namespace arrow
