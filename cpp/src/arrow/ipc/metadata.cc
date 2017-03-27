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

#include "arrow/ipc/metadata.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/File_generated.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

using FBB = flatbuffers::FlatBufferBuilder;
using DictionaryOffset = flatbuffers::Offset<flatbuf::DictionaryEncoding>;
using FieldOffset = flatbuffers::Offset<flatbuf::Field>;
using RecordBatchOffset = flatbuffers::Offset<flatbuf::RecordBatch>;
using VectorLayoutOffset = flatbuffers::Offset<arrow::flatbuf::VectorLayout>;
using Offset = flatbuffers::Offset<void>;
using FBString = flatbuffers::Offset<flatbuffers::String>;

static constexpr flatbuf::MetadataVersion kMetadataVersion = flatbuf::MetadataVersion_V2;

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

// Forward declaration
static Status FieldToFlatbuffer(FBB& fbb, const std::shared_ptr<Field>& field,
    DictionaryMemo* dictionary_memo, FieldOffset* offset);

static Offset IntToFlatbuffer(FBB& fbb, int bitWidth, bool is_signed) {
  return flatbuf::CreateInt(fbb, bitWidth, is_signed).Union();
}

static Offset FloatToFlatbuffer(FBB& fbb, flatbuf::Precision precision) {
  return flatbuf::CreateFloatingPoint(fbb, precision).Union();
}

static Status AppendChildFields(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, DictionaryMemo* dictionary_memo) {
  FieldOffset field;
  for (int i = 0; i < type->num_children(); ++i) {
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, type->child(i), dictionary_memo, &field));
    out_children->push_back(field);
  }
  return Status::OK();
}

static Status ListToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, DictionaryMemo* dictionary_memo,
    Offset* offset) {
  RETURN_NOT_OK(AppendChildFields(fbb, type, out_children, dictionary_memo));
  *offset = flatbuf::CreateList(fbb).Union();
  return Status::OK();
}

static Status StructToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, DictionaryMemo* dictionary_memo,
    Offset* offset) {
  RETURN_NOT_OK(AppendChildFields(fbb, type, out_children, dictionary_memo));
  *offset = flatbuf::CreateStruct_(fbb).Union();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Union implementation

static Status UnionFromFlatbuffer(const flatbuf::Union* union_data,
    const std::vector<std::shared_ptr<Field>>& children, std::shared_ptr<DataType>* out) {
  UnionMode mode = union_data->mode() == flatbuf::UnionMode_Sparse ? UnionMode::SPARSE
                                                                   : UnionMode::DENSE;

  std::vector<uint8_t> type_codes;

  const flatbuffers::Vector<int32_t>* fb_type_ids = union_data->typeIds();
  if (fb_type_ids == nullptr) {
    for (uint8_t i = 0; i < children.size(); ++i) {
      type_codes.push_back(i);
    }
  } else {
    for (int32_t id : (*fb_type_ids)) {
      // TODO(wesm): can these values exceed 255?
      type_codes.push_back(static_cast<uint8_t>(id));
    }
  }

  *out = union_(children, type_codes, mode);
  return Status::OK();
}

static Status UnionToFlatBuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* out_children, DictionaryMemo* dictionary_memo,
    Offset* offset) {
  RETURN_NOT_OK(AppendChildFields(fbb, type, out_children, dictionary_memo));

  const auto& union_type = static_cast<const UnionType&>(*type);

  flatbuf::UnionMode mode = union_type.mode == UnionMode::SPARSE
                                ? flatbuf::UnionMode_Sparse
                                : flatbuf::UnionMode_Dense;

  std::vector<int32_t> type_ids;
  type_ids.reserve(union_type.type_codes.size());
  for (uint8_t code : union_type.type_codes) {
    type_ids.push_back(code);
  }

  auto fb_type_ids = fbb.CreateVector(type_ids);

  *offset = flatbuf::CreateUnion(fbb, mode, fb_type_ids).Union();
  return Status::OK();
}

#define INT_TO_FB_CASE(BIT_WIDTH, IS_SIGNED)            \
  *out_type = flatbuf::Type_Int;                        \
  *offset = IntToFlatbuffer(fbb, BIT_WIDTH, IS_SIGNED); \
  break;

static inline flatbuf::TimeUnit ToFlatbufferUnit(TimeUnit unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      return flatbuf::TimeUnit_SECOND;
    case TimeUnit::MILLI:
      return flatbuf::TimeUnit_MILLISECOND;
    case TimeUnit::MICRO:
      return flatbuf::TimeUnit_MICROSECOND;
    case TimeUnit::NANO:
      return flatbuf::TimeUnit_NANOSECOND;
    default:
      break;
  }
  return flatbuf::TimeUnit_MIN;
}

static inline TimeUnit FromFlatbufferUnit(flatbuf::TimeUnit unit) {
  switch (unit) {
    case flatbuf::TimeUnit_SECOND:
      return TimeUnit::SECOND;
    case flatbuf::TimeUnit_MILLISECOND:
      return TimeUnit::MILLI;
    case flatbuf::TimeUnit_MICROSECOND:
      return TimeUnit::MICRO;
    case flatbuf::TimeUnit_NANOSECOND:
      return TimeUnit::NANO;
    default:
      break;
  }
  // cannot reach
  return TimeUnit::SECOND;
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
    case flatbuf::Type_FixedWidthBinary: {
      auto fw_binary = static_cast<const flatbuf::FixedWidthBinary*>(type_data);
      *out = fixed_width_binary(fw_binary->byteWidth());
      return Status::OK();
    }
    case flatbuf::Type_Utf8:
      *out = utf8();
      return Status::OK();
    case flatbuf::Type_Bool:
      *out = boolean();
      return Status::OK();
    case flatbuf::Type_Decimal:
      return Status::NotImplemented("Decimal");
    case flatbuf::Type_Date: {
      auto date_type = static_cast<const flatbuf::Date*>(type_data);
      if (date_type->unit() == flatbuf::DateUnit_DAY) {
        *out = date32();
      } else {
        *out = date64();
      }
      return Status::OK();
    }
    case flatbuf::Type_Time: {
      auto time_type = static_cast<const flatbuf::Time*>(type_data);
      TimeUnit unit = FromFlatbufferUnit(time_type->unit());
      switch (unit) {
        case TimeUnit::SECOND:
        case TimeUnit::MILLI:
          *out = time32(unit);
          break;
        default:
          *out = time64(unit);
          break;
      }
      return Status::OK();
    }
    case flatbuf::Type_Timestamp: {
      auto ts_type = static_cast<const flatbuf::Timestamp*>(type_data);
      TimeUnit unit = FromFlatbufferUnit(ts_type->unit());
      if (ts_type->timezone() != 0 && ts_type->timezone()->Length() > 0) {
        *out = timestamp(unit, ts_type->timezone()->str());
      } else {
        *out = timestamp(unit);
      }
      return Status::OK();
    }
    case flatbuf::Type_Interval:
      return Status::NotImplemented("Interval");
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
      return UnionFromFlatbuffer(
          static_cast<const flatbuf::Union*>(type_data), children, out);
    default:
      return Status::Invalid("Unrecognized type");
  }
}

// TODO(wesm): Convert this to visitor pattern
static Status TypeToFlatbuffer(FBB& fbb, const std::shared_ptr<DataType>& type,
    std::vector<FieldOffset>* children, std::vector<VectorLayoutOffset>* layout,
    flatbuf::Type* out_type, DictionaryMemo* dictionary_memo, Offset* offset) {
  if (type->type == Type::DICTIONARY) {
    // In this library, the dictionary "type" is a logical construct. Here we
    // pass through to the value type, as we've already captured the index
    // type in the DictionaryEncoding metadata in the parent field
    const auto& dict_type = static_cast<const DictionaryType&>(*type);
    return TypeToFlatbuffer(fbb, dict_type.dictionary()->type(), children, layout,
        out_type, dictionary_memo, offset);
  }

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
    auto offset = flatbuf::CreateVectorLayout(
        fbb, static_cast<int16_t>(descr.bit_width()), vector_type);
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
    case Type::FIXED_WIDTH_BINARY: {
      const auto& fw_type = static_cast<const FixedWidthBinaryType&>(*type);
      *out_type = flatbuf::Type_FixedWidthBinary;
      *offset = flatbuf::CreateFixedWidthBinary(fbb, fw_type.byte_width()).Union();
    } break;
    case Type::BINARY:
      *out_type = flatbuf::Type_Binary;
      *offset = flatbuf::CreateBinary(fbb).Union();
      break;
    case Type::STRING:
      *out_type = flatbuf::Type_Utf8;
      *offset = flatbuf::CreateUtf8(fbb).Union();
      break;
    case Type::DATE32:
      *out_type = flatbuf::Type_Date;
      *offset = flatbuf::CreateDate(fbb, flatbuf::DateUnit_DAY).Union();
      break;
    case Type::DATE64:
      *out_type = flatbuf::Type_Date;
      *offset = flatbuf::CreateDate(fbb, flatbuf::DateUnit_MILLISECOND).Union();
      break;
    case Type::TIME32: {
      const auto& time_type = static_cast<const Time32Type&>(*type);
      *out_type = flatbuf::Type_Time;
      *offset = flatbuf::CreateTime(fbb, ToFlatbufferUnit(time_type.unit)).Union();
    } break;
    case Type::TIME64: {
      const auto& time_type = static_cast<const Time64Type&>(*type);
      *out_type = flatbuf::Type_Time;
      *offset = flatbuf::CreateTime(fbb, ToFlatbufferUnit(time_type.unit)).Union();
    } break;
    case Type::TIMESTAMP: {
      const auto& ts_type = static_cast<const TimestampType&>(*type);
      *out_type = flatbuf::Type_Timestamp;

      flatbuf::TimeUnit fb_unit = ToFlatbufferUnit(ts_type.unit);
      FBString fb_timezone = 0;
      if (ts_type.timezone.size() > 0) {
        fb_timezone = fbb.CreateString(ts_type.timezone);
      }
      *offset = flatbuf::CreateTimestamp(fbb, fb_unit, fb_timezone).Union();
    } break;
    case Type::LIST:
      *out_type = flatbuf::Type_List;
      return ListToFlatbuffer(fbb, type, children, dictionary_memo, offset);
    case Type::STRUCT:
      *out_type = flatbuf::Type_Struct_;
      return StructToFlatbuffer(fbb, type, children, dictionary_memo, offset);
    case Type::UNION:
      *out_type = flatbuf::Type_Union;
      return UnionToFlatBuffer(fbb, type, children, dictionary_memo, offset);
    default:
      *out_type = flatbuf::Type_NONE;  // Make clang-tidy happy
      std::stringstream ss;
      ss << "Unable to convert type: " << type->ToString() << std::endl;
      return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

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

static Status FieldFromFlatbuffer(const flatbuf::Field* field,
    const DictionaryMemo& dictionary_memo, std::shared_ptr<Field>* out) {
  std::shared_ptr<DataType> type;

  const flatbuf::DictionaryEncoding* encoding = field->dictionary();

  if (encoding == nullptr) {
    // The field is not dictionary encoded. We must potentially visit its
    // children to fully reconstruct the data type
    auto children = field->children();
    std::vector<std::shared_ptr<Field>> child_fields(children->size());
    for (int i = 0; i < static_cast<int>(children->size()); ++i) {
      RETURN_NOT_OK(
          FieldFromFlatbuffer(children->Get(i), dictionary_memo, &child_fields[i]));
    }
    RETURN_NOT_OK(
        TypeFromFlatbuffer(field->type_type(), field->type(), child_fields, &type));
  } else {
    // The field is dictionary encoded. The type of the dictionary values has
    // been determined elsewhere, and is stored in the DictionaryMemo. Here we
    // construct the logical DictionaryType object

    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(dictionary_memo.GetDictionary(encoding->id(), &dictionary));

    std::shared_ptr<DataType> index_type;
    RETURN_NOT_OK(IntFromFlatbuffer(encoding->indexType(), &index_type));
    type = std::make_shared<DictionaryType>(index_type, dictionary);
  }
  *out = std::make_shared<Field>(field->name()->str(), type, field->nullable());
  return Status::OK();
}

static Status FieldFromFlatbufferDictionary(
    const flatbuf::Field* field, std::shared_ptr<Field>* out) {
  // Need an empty memo to pass down for constructing children
  DictionaryMemo dummy_memo;

  // Any DictionaryEncoding set is ignored here

  std::shared_ptr<DataType> type;
  auto children = field->children();
  std::vector<std::shared_ptr<Field>> child_fields(children->size());
  for (int i = 0; i < static_cast<int>(children->size()); ++i) {
    RETURN_NOT_OK(FieldFromFlatbuffer(children->Get(i), dummy_memo, &child_fields[i]));
  }

  RETURN_NOT_OK(
      TypeFromFlatbuffer(field->type_type(), field->type(), child_fields, &type));

  *out = std::make_shared<Field>(field->name()->str(), type, field->nullable());
  return Status::OK();
}

// will return the endianness of the system we are running on
// based the NUMPY_API function. See NOTICE.txt
flatbuf::Endianness endianness() {
  union {
    uint32_t i;
    char c[4];
  } bint = {0x01020304};

  return bint.c[0] == 1 ? flatbuf::Endianness_Big : flatbuf::Endianness_Little;
}

static Status SchemaToFlatbuffer(FBB& fbb, const Schema& schema,
    DictionaryMemo* dictionary_memo, flatbuffers::Offset<flatbuf::Schema>* out) {
  std::vector<FieldOffset> field_offsets;
  for (int i = 0; i < schema.num_fields(); ++i) {
    std::shared_ptr<Field> field = schema.field(i);
    FieldOffset offset;
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, field, dictionary_memo, &offset));
    field_offsets.push_back(offset);
  }

  *out = flatbuf::CreateSchema(fbb, endianness(), fbb.CreateVector(field_offsets));
  return Status::OK();
}

static Status WriteFlatbufferBuilder(FBB& fbb, std::shared_ptr<Buffer>* out) {
  int32_t size = fbb.GetSize();

  auto result = std::make_shared<PoolBuffer>();
  RETURN_NOT_OK(result->Resize(size));

  uint8_t* dst = result->mutable_data();
  memcpy(dst, fbb.GetBufferPointer(), size);
  *out = result;
  return Status::OK();
}

static Status WriteMessage(FBB& fbb, flatbuf::MessageHeader header_type,
    flatbuffers::Offset<void> header, int64_t body_length, std::shared_ptr<Buffer>* out) {
  auto message =
      flatbuf::CreateMessage(fbb, kMetadataVersion, header_type, header, body_length);
  fbb.Finish(message);
  return WriteFlatbufferBuilder(fbb, out);
}

Status WriteSchemaMessage(
    const Schema& schema, DictionaryMemo* dictionary_memo, std::shared_ptr<Buffer>* out) {
  FBB fbb;
  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb, schema, dictionary_memo, &fb_schema));
  return WriteMessage(fbb, flatbuf::MessageHeader_Schema, fb_schema.Union(), 0, out);
}

using FieldNodeVector =
    flatbuffers::Offset<flatbuffers::Vector<const flatbuf::FieldNode*>>;
using BufferVector = flatbuffers::Offset<flatbuffers::Vector<const flatbuf::Buffer*>>;

static Status WriteFieldNodes(
    FBB& fbb, const std::vector<FieldMetadata>& nodes, FieldNodeVector* out) {
  std::vector<flatbuf::FieldNode> fb_nodes;
  fb_nodes.reserve(nodes.size());

  for (size_t i = 0; i < nodes.size(); ++i) {
    const FieldMetadata& node = nodes[i];
    if (node.offset != 0) {
      return Status::Invalid("Field metadata for IPC must have offset 0");
    }
    fb_nodes.emplace_back(node.length, node.null_count);
  }
  *out = fbb.CreateVectorOfStructs(fb_nodes);
  return Status::OK();
}

static Status WriteBuffers(
    FBB& fbb, const std::vector<BufferMetadata>& buffers, BufferVector* out) {
  std::vector<flatbuf::Buffer> fb_buffers;
  fb_buffers.reserve(buffers.size());

  for (size_t i = 0; i < buffers.size(); ++i) {
    const BufferMetadata& buffer = buffers[i];
    fb_buffers.emplace_back(buffer.page, buffer.offset, buffer.length);
  }
  *out = fbb.CreateVectorOfStructs(fb_buffers);
  return Status::OK();
}

static Status MakeRecordBatch(FBB& fbb, int64_t length, int64_t body_length,
    const std::vector<FieldMetadata>& nodes, const std::vector<BufferMetadata>& buffers,
    RecordBatchOffset* offset) {
  FieldNodeVector fb_nodes;
  BufferVector fb_buffers;

  RETURN_NOT_OK(WriteFieldNodes(fbb, nodes, &fb_nodes));
  RETURN_NOT_OK(WriteBuffers(fbb, buffers, &fb_buffers));

  *offset = flatbuf::CreateRecordBatch(fbb, length, fb_nodes, fb_buffers);
  return Status::OK();
}

Status WriteRecordBatchMessage(int64_t length, int64_t body_length,
    const std::vector<FieldMetadata>& nodes, const std::vector<BufferMetadata>& buffers,
    std::shared_ptr<Buffer>* out) {
  FBB fbb;
  RecordBatchOffset record_batch;
  RETURN_NOT_OK(MakeRecordBatch(fbb, length, body_length, nodes, buffers, &record_batch));
  return WriteMessage(
      fbb, flatbuf::MessageHeader_RecordBatch, record_batch.Union(), body_length, out);
}

Status WriteDictionaryMessage(int64_t id, int64_t length, int64_t body_length,
    const std::vector<FieldMetadata>& nodes, const std::vector<BufferMetadata>& buffers,
    std::shared_ptr<Buffer>* out) {
  FBB fbb;
  RecordBatchOffset record_batch;
  RETURN_NOT_OK(MakeRecordBatch(fbb, length, body_length, nodes, buffers, &record_batch));
  auto dictionary_batch = flatbuf::CreateDictionaryBatch(fbb, id, record_batch).Union();
  return WriteMessage(
      fbb, flatbuf::MessageHeader_DictionaryBatch, dictionary_batch, body_length, out);
}

static flatbuffers::Offset<flatbuffers::Vector<const flatbuf::Block*>>
FileBlocksToFlatbuffer(FBB& fbb, const std::vector<FileBlock>& blocks) {
  std::vector<flatbuf::Block> fb_blocks;

  for (const FileBlock& block : blocks) {
    fb_blocks.emplace_back(block.offset, block.metadata_length, block.body_length);
  }

  return fbb.CreateVectorOfStructs(fb_blocks);
}

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
    const std::vector<FileBlock>& record_batches, DictionaryMemo* dictionary_memo,
    io::OutputStream* out) {
  FBB fbb;

  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb, schema, dictionary_memo, &fb_schema));

  auto fb_dictionaries = FileBlocksToFlatbuffer(fbb, dictionaries);
  auto fb_record_batches = FileBlocksToFlatbuffer(fbb, record_batches);

  auto footer = flatbuf::CreateFooter(
      fbb, kMetadataVersion, fb_schema, fb_dictionaries, fb_record_batches);

  fbb.Finish(footer);

  int32_t size = fbb.GetSize();

  return out->Write(fbb.GetBufferPointer(), size);
}

// ----------------------------------------------------------------------
// Memoization data structure for handling shared dictionaries

DictionaryMemo::DictionaryMemo() {}

// Returns KeyError if dictionary not found
Status DictionaryMemo::GetDictionary(
    int64_t id, std::shared_ptr<Array>* dictionary) const {
  auto it = id_to_dictionary_.find(id);
  if (it == id_to_dictionary_.end()) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " not found";
    return Status::KeyError(ss.str());
  }
  *dictionary = it->second;
  return Status::OK();
}

int64_t DictionaryMemo::GetId(const std::shared_ptr<Array>& dictionary) {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  if (it != dictionary_to_id_.end()) {
    // Dictionary already observed, return the id
    return it->second;
  } else {
    int64_t new_id = static_cast<int64_t>(dictionary_to_id_.size()) + 1;
    dictionary_to_id_[address] = new_id;
    id_to_dictionary_[new_id] = dictionary;
    return new_id;
  }
}

bool DictionaryMemo::HasDictionary(const std::shared_ptr<Array>& dictionary) const {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  return it != dictionary_to_id_.end();
}

bool DictionaryMemo::HasDictionaryId(int64_t id) const {
  auto it = id_to_dictionary_.find(id);
  return it != id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(
    int64_t id, const std::shared_ptr<Array>& dictionary) {
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

//----------------------------------------------------------------------
// Message reader

class Message::MessageImpl {
 public:
  explicit MessageImpl(const std::shared_ptr<Buffer>& buffer, int64_t offset)
      : buffer_(buffer), offset_(offset), message_(nullptr) {}

  Status Open() {
    message_ = flatbuf::GetMessage(buffer_->data() + offset_);

    // TODO(wesm): verify the message
    return Status::OK();
  }

  Message::Type type() const {
    switch (message_->header_type()) {
      case flatbuf::MessageHeader_Schema:
        return Message::SCHEMA;
      case flatbuf::MessageHeader_DictionaryBatch:
        return Message::DICTIONARY_BATCH;
      case flatbuf::MessageHeader_RecordBatch:
        return Message::RECORD_BATCH;
      default:
        return Message::NONE;
    }
  }

  const void* header() const { return message_->header(); }

  int64_t body_length() const { return message_->bodyLength(); }

 private:
  // Retain reference to memory
  std::shared_ptr<Buffer> buffer_;
  int64_t offset_;

  const flatbuf::Message* message_;
};

Message::Message(const std::shared_ptr<Buffer>& buffer, int64_t offset) {
  impl_.reset(new MessageImpl(buffer, offset));
}

Status Message::Open(const std::shared_ptr<Buffer>& buffer, int64_t offset,
    std::shared_ptr<Message>* out) {
  // ctor is private

  *out = std::shared_ptr<Message>(new Message(buffer, offset));
  return (*out)->impl_->Open();
}

Message::Type Message::type() const {
  return impl_->type();
}

int64_t Message::body_length() const {
  return impl_->body_length();
}

const void* Message::header() const {
  return impl_->header();
}

// ----------------------------------------------------------------------
// SchemaMetadata

class MessageHolder {
 public:
  void set_message(const std::shared_ptr<Message>& message) { message_ = message; }
  void set_buffer(const std::shared_ptr<Buffer>& buffer) { buffer_ = buffer; }

 protected:
  // Possible parents, owns the flatbuffer data
  std::shared_ptr<Message> message_;
  std::shared_ptr<Buffer> buffer_;
};

class SchemaMetadata::SchemaMetadataImpl : public MessageHolder {
 public:
  explicit SchemaMetadataImpl(const void* schema)
      : schema_(static_cast<const flatbuf::Schema*>(schema)) {}

  const flatbuf::Field* get_field(int i) const { return schema_->fields()->Get(i); }

  int num_fields() const { return schema_->fields()->size(); }

  Status VisitField(const flatbuf::Field* field, DictionaryTypeMap* id_to_field) const {
    const flatbuf::DictionaryEncoding* dict_metadata = field->dictionary();
    if (dict_metadata == nullptr) {
      // Field is not dictionary encoded. Visit children
      auto children = field->children();
      for (flatbuffers::uoffset_t i = 0; i < children->size(); ++i) {
        RETURN_NOT_OK(VisitField(children->Get(i), id_to_field));
      }
    } else {
      // Field is dictionary encoded. Construct the data type for the
      // dictionary (no descendents can be dictionary encoded)
      std::shared_ptr<Field> dictionary_field;
      RETURN_NOT_OK(FieldFromFlatbufferDictionary(field, &dictionary_field));
      (*id_to_field)[dict_metadata->id()] = dictionary_field;
    }
    return Status::OK();
  }

  Status GetDictionaryTypes(DictionaryTypeMap* id_to_field) const {
    for (int i = 0; i < num_fields(); ++i) {
      RETURN_NOT_OK(VisitField(get_field(i), id_to_field));
    }
    return Status::OK();
  }

 private:
  const flatbuf::Schema* schema_;
};

SchemaMetadata::SchemaMetadata(const std::shared_ptr<Message>& message)
    : SchemaMetadata(message->impl_->header()) {
  impl_->set_message(message);
}

SchemaMetadata::SchemaMetadata(const void* header) {
  impl_.reset(new SchemaMetadataImpl(header));
}

SchemaMetadata::SchemaMetadata(const std::shared_ptr<Buffer>& buffer, int64_t offset)
    : SchemaMetadata(buffer->data() + offset) {
  // Preserve ownership
  impl_->set_buffer(buffer);
}

SchemaMetadata::~SchemaMetadata() {}

int SchemaMetadata::num_fields() const {
  return impl_->num_fields();
}

Status SchemaMetadata::GetDictionaryTypes(DictionaryTypeMap* id_to_field) const {
  return impl_->GetDictionaryTypes(id_to_field);
}

Status SchemaMetadata::GetSchema(
    const DictionaryMemo& dictionary_memo, std::shared_ptr<Schema>* out) const {
  std::vector<std::shared_ptr<Field>> fields(num_fields());
  for (int i = 0; i < this->num_fields(); ++i) {
    const flatbuf::Field* field = impl_->get_field(i);
    RETURN_NOT_OK(FieldFromFlatbuffer(field, dictionary_memo, &fields[i]));
  }
  *out = std::make_shared<Schema>(fields);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Conveniences

Status ReadMessage(int64_t offset, int32_t metadata_length, io::RandomAccessFile* file,
    std::shared_ptr<Message>* message) {
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(file->ReadAt(offset, metadata_length, &buffer));

  int32_t flatbuffer_size = *reinterpret_cast<const int32_t*>(buffer->data());

  if (flatbuffer_size + static_cast<int>(sizeof(int32_t)) > metadata_length) {
    std::stringstream ss;
    ss << "flatbuffer size " << metadata_length << " invalid. File offset: " << offset
       << ", metadata length: " << metadata_length;
    return Status::Invalid(ss.str());
  }
  return Message::Open(buffer, 4, message);
}

}  // namespace ipc
}  // namespace arrow
