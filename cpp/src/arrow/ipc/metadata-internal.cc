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

#include "arrow/buffer.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

// ----------------------------------------------------------------------
// Memoization data structure for handling shared dictionaries

DictionaryMemo::DictionaryMemo() {}

// Returns KeyError if dictionary not found
Status DictionaryMemo::GetDictionary(
    int32_t id, std::shared_ptr<Array>* dictionary) const {
  auto it = id_to_dictionary_.find(id);
  if (it == id_to_dictionary_.end()) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " not found";
    return Status::KeyError(ss.str());
  }
  *dictionary = it->second;
  return Status::OK();
}

int32_t DictionaryMemo::GetId(const std::shared_ptr<Array> dictionary) {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  if (it != dictionary_to_id_.end()) {
    // Dictionary already observed, return the id
    return it->second;
  } else {
    int32_t new_id = static_cast<int32_t>(dictionary_to_id_.size()) + 1;
    dictionary_to_id_[address] = new_id;
    id_to_dictionary_[new_id] = dictionary;
    return new_id;
  }
}

bool DictionaryMemo::HasDictionary(const std::shared_ptr<Array> dictionary) const {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  return it != dictionary_to_id_.end();
}

bool DictionaryMemo::HasDictionaryId(int32_t id) const {
  auto it = id_to_dictionary_.find(id);
  return it != id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(
    int32_t id, const std::shared_ptr<Array>& dictionary) {
  if (HasDictionaryId(id)) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " already exists";
    return Status::KeyError(ss.str());
  }
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  id_to_dictionary_[id] = dictionary;
  dictionary_to_id_[address] = id;
  return Status::OK();
}

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
      *out = int_data->is_signed() ? int8() : uint8();
      break;
    case 16:
      *out = int_data->is_signed() ? int16() : uint16();
      break;
    case 32:
      *out = int_data->is_signed() ? int32() : uint32();
      break;
    case 64:
      *out = int_data->is_signed() ? int64() : uint64();
      break;
    default:
      return Status::NotImplemented("Integers not in cstdint are not implemented");
  }
  return Status::OK();
}

static Status FloatFromFlatuffer(
    const flatbuf::FloatingPoint* float_data, std::shared_ptr<DataType>* out) {
  if (float_data->precision() == flatbuf::Precision_HALF) {
    *out = float16();
  } else if (float_data->precision() == flatbuf::Precision_SINGLE) {
    *out = float32();
  } else {
    *out = float64();
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
      *out = binary();
      return Status::OK();
    case flatbuf::Type_Utf8:
      *out = utf8();
      return Status::OK();
    case flatbuf::Type_Bool:
      *out = boolean();
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
static Status FieldToFlatbuffer(FBB& fbb, const std::shared_ptr<Field>& field,
    DictionaryMemo* dictionary_memo, FieldOffset* offset);

static Offset IntToFlatbuffer(FBB& fbb, int bitWidth, bool is_signed) {
  return flatbuf::CreateInt(fbb, bitWidth, is_signed).Union();
}

static Offset FloatToFlatbuffer(FBB& fbb, flatbuf::Precision precision) {
  return flatbuf::CreateFloatingPoint(fbb, precision).Union();
}

static Status ListToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, DictionaryMemo* dictionary_memo,
    Offset* offset) {
  FieldOffset field;
  RETURN_NOT_OK(FieldToFlatbuffer(fbb, type->child(0), dictionary_memo, &field));
  out_children->push_back(field);
  *offset = flatbuf::CreateList(fbb).Union();
  return Status::OK();
}

static Status StructToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, DictionaryMemo* dictionary_memo,
    Offset* offset) {
  FieldOffset field;
  for (int i = 0; i < type->num_children(); ++i) {
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, type->child(i), dictionary_memo, &field));
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
    std::vector<FieldOffset>* children, std::vector<VectorLayoutOffset>* layout,
    flatbuf::Type* out_type, DictionaryMemo* dictionary_memo, Offset* offset) {
  std::vector<BufferDescr> buffer_layout = type->GetBufferLayout();
  for (const BufferDescr& descr : buffer_layout) {
    flatbuf::VectorType vector_type;
    switch (descr.type()) {
      case BufferType::OFFSET:
        vector_type = flatbuf::VectorType_OFFSET;
        break;
      case BufferType::DATA:
        vector_type = flatbuf::VectorType_DATA;
        break;
      case BufferType::VALIDITY:
        vector_type = flatbuf::VectorType_VALIDITY;
        break;
      case BufferType::TYPE:
        vector_type = flatbuf::VectorType_TYPE;
        break;
      default:
        vector_type = flatbuf::VectorType_DATA;
        break;
    }
    auto offset = flatbuf::CreateVectorLayout(fbb, descr.bit_width(), vector_type);
    layout->push_back(offset);
  }

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
    case Type::BINARY:
      *out_type = flatbuf::Type_Binary;
      *offset = flatbuf::CreateBinary(fbb).Union();
      break;
    case Type::STRING:
      *out_type = flatbuf::Type_Utf8;
      *offset = flatbuf::CreateUtf8(fbb).Union();
      break;
    case Type::LIST:
      *out_type = flatbuf::Type_List;
      return ListToFlatbuffer(fbb, type, children, dictionary_memo, offset);
    case Type::STRUCT:
      *out_type = flatbuf::Type_Struct_;
      return StructToFlatbuffer(fbb, type, children, dictionary_memo, offset);
    default:
      *out_type = flatbuf::Type_NONE;  // Make clang-tidy happy
      std::stringstream ss;
      ss << "Unable to convert type: " << type->ToString() << std::endl;
      return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

using DictionaryOffset = flatbuffers::Offset<flatbuf::DictionaryEncoding>;

static DictionaryOffset GetDictionaryEncoding(
    FBB& fbb, const DictionaryType& type, DictionaryMemo* memo) {
  int64_t dictionary_id = memo->GetId(type.dictionary());

  // We assume that the dictionary index type (as an integer) has already been
  // validated elsewhere, and can safely assume we are dealing with signed
  // integers
  const auto& fw_index_type = static_cast<const FixedWidthType&>(*type.index_type());

  auto index_type_offset = flatbuf::CreateInt(fbb, fw_index_type.bit_width(), true);

  // TODO(wesm): ordered dictionaries
  return flatbuf::CreateDictionaryEncoding(fbb, dictionary_id, index_type_offset);
}

static Status FieldToFlatbuffer(FBB& fbb, const std::shared_ptr<Field>& field,
    DictionaryMemo* dictionary_memo, FieldOffset* offset) {
  auto fb_name = fbb.CreateString(field->name);

  flatbuf::Type type_enum;
  Offset type_offset;
  Offset type_layout;
  std::vector<FieldOffset> children;
  std::vector<VectorLayoutOffset> layout;

  RETURN_NOT_OK(TypeToFlatbuffer(
      fbb, field->type, &children, &layout, &type_enum, dictionary_memo, &type_offset));
  auto fb_children = fbb.CreateVector(children);
  auto fb_layout = fbb.CreateVector(layout);

  DictionaryOffset dictionary = 0;
  if (field->type->type == Type::DICTIONARY) {
    dictionary = GetDictionaryEncoding(
        fbb, static_cast<const DictionaryType&>(*field->type), dictionary_memo);
  }

  // TODO: produce the list of VectorTypes
  *offset = flatbuf::CreateField(fbb, fb_name, field->nullable, type_enum, type_offset,
      dictionary, fb_children, fb_layout);

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

  *out = std::make_shared<Field>(field->name()->str(), type, field->nullable());
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

Status SchemaToFlatbuffer(
    FBB& fbb, const Schema& schema, flatbuffers::Offset<flatbuf::Schema>* out) {
  std::vector<FieldOffset> field_offsets;
  DictionaryMemo dictionary_memo;
  for (int i = 0; i < schema.num_fields(); ++i) {
    std::shared_ptr<Field> field = schema.field(i);
    FieldOffset offset;
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, field, &dictionary_memo, &offset));
    field_offsets.push_back(offset);
  }

  *out = flatbuf::CreateSchema(fbb, endianness(), fbb.CreateVector(field_offsets));
  return Status::OK();
}

Status MessageBuilder::SetSchema(const Schema& schema) {
  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb_, schema, &fb_schema));

  header_type_ = flatbuf::MessageHeader_Schema;
  header_ = fb_schema.Union();
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

Status WriteRecordBatchMetadata(int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers, std::shared_ptr<Buffer>* out) {
  MessageBuilder builder;
  RETURN_NOT_OK(builder.SetRecordBatch(length, body_length, nodes, buffers));
  RETURN_NOT_OK(builder.Finish());
  return builder.GetBuffer(out);
}

Status MessageBuilder::Finish() {
  auto message =
      flatbuf::CreateMessage(fbb_, kMetadataVersion, header_type_, header_, body_length_);
  fbb_.Finish(message);
  return Status::OK();
}

Status MessageBuilder::GetBuffer(std::shared_ptr<Buffer>* out) {
  int32_t size = fbb_.GetSize();

  auto result = std::make_shared<PoolBuffer>();
  RETURN_NOT_OK(result->Resize(size));

  uint8_t* dst = result->mutable_data();
  memcpy(dst, fbb_.GetBufferPointer(), size);

  *out = result;
  return Status::OK();
}

}  // namespace ipc
}  // namespace arrow
