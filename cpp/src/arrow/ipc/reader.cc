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

#include "arrow/ipc/reader.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>  // IWYU pragma: export

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/util.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/parallel.h"
#include "arrow/util/ubsan.h"
#include "arrow/visitor_inline.h"

#include "generated/File_generated.h"  // IWYU pragma: export
#include "generated/Message_generated.h"
#include "generated/Schema_generated.h"
#include "generated/SparseTensor_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::GetByteWidth;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

namespace {

Status InvalidMessageType(MessageType expected, MessageType actual) {
  return Status::IOError("Expected IPC message of type ", FormatMessageType(expected),
                         " but got ", FormatMessageType(actual));
}

#define CHECK_MESSAGE_TYPE(expected, actual)           \
  do {                                                 \
    if ((actual) != (expected)) {                      \
      return InvalidMessageType((expected), (actual)); \
    }                                                  \
  } while (0)

#define CHECK_HAS_BODY(message)                                       \
  do {                                                                \
    if ((message).body() == nullptr) {                                \
      return Status::IOError("Expected body in IPC message of type ", \
                             FormatMessageType((message).type()));    \
    }                                                                 \
  } while (0)

#define CHECK_HAS_NO_BODY(message)                                      \
  do {                                                                  \
    if ((message).body_length() != 0) {                                 \
      return Status::IOError("Unexpected body in IPC message of type ", \
                             FormatMessageType((message).type()));      \
    }                                                                   \
  } while (0)

}  // namespace

// ----------------------------------------------------------------------
// Record batch read path

/// The field_index and buffer_index are incremented based on how much of the
/// batch is "consumed" (through nested data reconstruction, for example)
class ArrayLoader {
 public:
  explicit ArrayLoader(const flatbuf::RecordBatch* metadata,
                       MetadataVersion metadata_version,
                       const DictionaryMemo* dictionary_memo,
                       const IpcReadOptions& options, io::RandomAccessFile* file)
      : metadata_(metadata),
        metadata_version_(metadata_version),
        file_(file),
        dictionary_memo_(dictionary_memo),
        max_recursion_depth_(options.max_recursion_depth) {}

  Status ReadBuffer(int64_t offset, int64_t length, std::shared_ptr<Buffer>* out) {
    if (skip_io_) {
      return Status::OK();
    }
    if (offset < 0) {
      return Status::Invalid("Negative offset for reading buffer ", buffer_index_);
    }
    if (length < 0) {
      return Status::Invalid("Negative length for reading buffer ", buffer_index_);
    }
    // This construct permits overriding GetBuffer at compile time
    if (!BitUtil::IsMultipleOf8(offset)) {
      return Status::Invalid("Buffer ", buffer_index_,
                             " did not start on 8-byte aligned offset: ", offset);
    }
    return file_->ReadAt(offset, length).Value(out);
  }

  Status LoadType(const DataType& type) { return VisitTypeInline(type, this); }

  Status Load(const Field* field, ArrayData* out) {
    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    field_ = field;
    out_ = out;
    out_->type = field_->type();
    return LoadType(*field_->type());
  }

  Status SkipField(const Field* field) {
    ArrayData dummy;
    skip_io_ = true;
    Status status = Load(field, &dummy);
    skip_io_ = false;
    return status;
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    auto buffers = metadata_->buffers();
    CHECK_FLATBUFFERS_NOT_NULL(buffers, "RecordBatch.buffers");
    if (buffer_index >= static_cast<int>(buffers->size())) {
      return Status::IOError("buffer_index out of range.");
    }
    const flatbuf::Buffer* buffer = buffers->Get(buffer_index);
    if (buffer->length() == 0) {
      // Should never return a null buffer here.
      // (zero-sized buffer allocations are cheap)
      return AllocateBuffer(0).Value(out);
    } else {
      return ReadBuffer(buffer->offset(), buffer->length(), out);
    }
  }

  Status GetFieldMetadata(int field_index, ArrayData* out) {
    auto nodes = metadata_->nodes();
    CHECK_FLATBUFFERS_NOT_NULL(nodes, "Table.nodes");
    // pop off a field
    if (field_index >= static_cast<int>(nodes->size())) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }
    const flatbuf::FieldNode* node = nodes->Get(field_index);

    out->length = node->length();
    out->null_count = node->null_count();
    out->offset = 0;
    return Status::OK();
  }

  Status LoadCommon(Type::type type_id) {
    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    RETURN_NOT_OK(GetFieldMetadata(field_index_++, out_));

    if (internal::HasValidityBitmap(type_id, metadata_version_)) {
      // Extract null_bitmap which is common to all arrays except for unions
      // and nulls.
      if (out_->null_count != 0) {
        RETURN_NOT_OK(GetBuffer(buffer_index_, &out_->buffers[0]));
      }
      buffer_index_++;
    }
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadPrimitive(Type::type type_id) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon(type_id));
    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(buffer_index_++, &out_->buffers[1]));
    } else {
      buffer_index_++;
      out_->buffers[1].reset(new Buffer(nullptr, 0));
    }
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadBinary(Type::type type_id) {
    out_->buffers.resize(3);

    RETURN_NOT_OK(LoadCommon(type_id));
    RETURN_NOT_OK(GetBuffer(buffer_index_++, &out_->buffers[1]));
    return GetBuffer(buffer_index_++, &out_->buffers[2]);
  }

  template <typename TYPE>
  Status LoadList(const TYPE& type) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon(type.id()));
    RETURN_NOT_OK(GetBuffer(buffer_index_++, &out_->buffers[1]));

    const int num_children = type.num_fields();
    if (num_children != 1) {
      return Status::Invalid("Wrong number of children: ", num_children);
    }

    return LoadChildren(type.fields());
  }

  Status LoadChildren(std::vector<std::shared_ptr<Field>> child_fields) {
    ArrayData* parent = out_;
    parent->child_data.reserve(static_cast<int>(child_fields.size()));
    for (const auto& child_field : child_fields) {
      auto field_array = std::make_shared<ArrayData>();
      --max_recursion_depth_;
      RETURN_NOT_OK(Load(child_field.get(), field_array.get()));
      ++max_recursion_depth_;
      parent->child_data.emplace_back(field_array);
    }
    out_ = parent;
    return Status::OK();
  }

  Status Visit(const NullType& type) {
    out_->buffers.resize(1);

    // ARROW-6379: NullType has no buffers in the IPC payload
    return GetFieldMetadata(field_index_++, out_);
  }

  template <typename T>
  enable_if_t<std::is_base_of<FixedWidthType, T>::value &&
                  !std::is_base_of<FixedSizeBinaryType, T>::value &&
                  !std::is_base_of<DictionaryType, T>::value,
              Status>
  Visit(const T& type) {
    return LoadPrimitive<T>(type.id());
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& type) {
    return LoadBinary<T>(type.id());
  }

  Status Visit(const FixedSizeBinaryType& type) {
    out_->buffers.resize(2);
    RETURN_NOT_OK(LoadCommon(type.id()));
    return GetBuffer(buffer_index_++, &out_->buffers[1]);
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    return LoadList(type);
  }

  Status Visit(const MapType& type) {
    RETURN_NOT_OK(LoadList(type));
    return MapArray::ValidateChildData(out_->child_data);
  }

  Status Visit(const FixedSizeListType& type) {
    out_->buffers.resize(1);

    RETURN_NOT_OK(LoadCommon(type.id()));

    const int num_children = type.num_fields();
    if (num_children != 1) {
      return Status::Invalid("Wrong number of children: ", num_children);
    }

    return LoadChildren(type.fields());
  }

  Status Visit(const StructType& type) {
    out_->buffers.resize(1);
    RETURN_NOT_OK(LoadCommon(type.id()));
    return LoadChildren(type.fields());
  }

  Status Visit(const UnionType& type) {
    int n_buffers = type.mode() == UnionMode::SPARSE ? 2 : 3;
    out_->buffers.resize(n_buffers);

    RETURN_NOT_OK(LoadCommon(type.id()));

    // With metadata V4, we can get a validity bitmap.
    // Trying to fix up union data to do without the top-level validity bitmap
    // is hairy:
    // - type ids must be rewritten to all have valid values (even for former
    //   null slots)
    // - sparse union children must have their validity bitmaps rewritten
    //   by ANDing the top-level validity bitmap
    // - dense union children must be rewritten (at least one of them)
    //   to insert the required null slots that were formerly omitted
    // So instead we bail out.
    if (out_->null_count != 0 && out_->buffers[0] != nullptr) {
      return Status::Invalid(
          "Cannot read pre-1.0.0 Union array with top-level validity bitmap");
    }
    out_->buffers[0] = nullptr;
    out_->null_count = 0;

    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(buffer_index_, &out_->buffers[1]));
      if (type.mode() == UnionMode::DENSE) {
        RETURN_NOT_OK(GetBuffer(buffer_index_ + 1, &out_->buffers[2]));
      }
    }
    buffer_index_ += n_buffers - 1;
    return LoadChildren(type.fields());
  }

  Status Visit(const DictionaryType& type) {
    RETURN_NOT_OK(LoadType(*type.index_type()));

    // Look up dictionary
    int64_t id = -1;
    RETURN_NOT_OK(dictionary_memo_->GetId(field_, &id));

    std::shared_ptr<Array> boxed_dict;
    RETURN_NOT_OK(dictionary_memo_->GetDictionary(id, &boxed_dict));
    out_->dictionary = boxed_dict->data();
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) { return LoadType(*type.storage_type()); }

 private:
  const flatbuf::RecordBatch* metadata_;
  const MetadataVersion metadata_version_;
  io::RandomAccessFile* file_;
  const DictionaryMemo* dictionary_memo_;
  int max_recursion_depth_;
  int buffer_index_ = 0;
  int field_index_ = 0;
  bool skip_io_ = false;

  const Field* field_;
  ArrayData* out_;
};

Result<std::shared_ptr<Buffer>> DecompressBuffer(const std::shared_ptr<Buffer>& buf,
                                                 const IpcReadOptions& options,
                                                 util::Codec* codec) {
  if (buf == nullptr || buf->size() == 0) {
    return buf;
  }

  if (buf->size() < 8) {
    return Status::Invalid(
        "Likely corrupted message, compressed buffers "
        "are larger than 8 bytes by construction");
  }

  const uint8_t* data = buf->data();
  int64_t compressed_size = buf->size() - sizeof(int64_t);
  int64_t uncompressed_size = BitUtil::FromLittleEndian(util::SafeLoadAs<int64_t>(data));

  ARROW_ASSIGN_OR_RAISE(auto uncompressed,
                        AllocateBuffer(uncompressed_size, options.memory_pool));

  ARROW_ASSIGN_OR_RAISE(
      int64_t actual_decompressed,
      codec->Decompress(compressed_size, data + sizeof(int64_t), uncompressed_size,
                        uncompressed->mutable_data()));
  if (actual_decompressed != uncompressed_size) {
    return Status::Invalid("Failed to fully decompress buffer, expected ",
                           uncompressed_size, " bytes but decompressed ",
                           actual_decompressed);
  }

  return std::move(uncompressed);
}

Status DecompressBuffers(Compression::type compression, const IpcReadOptions& options,
                         std::vector<std::shared_ptr<ArrayData>>* fields) {
  struct BufferAccumulator {
    using BufferPtrVector = std::vector<std::shared_ptr<Buffer>*>;

    void AppendFrom(const std::vector<std::shared_ptr<ArrayData>>& fields) {
      for (const auto& field : fields) {
        for (auto& buffer : field->buffers) {
          buffers_.push_back(&buffer);
        }
        AppendFrom(field->child_data);
      }
    }

    BufferPtrVector Get(const std::vector<std::shared_ptr<ArrayData>>& fields) && {
      AppendFrom(fields);
      return std::move(buffers_);
    }

    BufferPtrVector buffers_;
  };

  // flatten all buffers
  auto buffers = BufferAccumulator{}.Get(*fields);

  std::unique_ptr<util::Codec> codec;
  ARROW_ASSIGN_OR_RAISE(codec, util::Codec::Create(compression));

  return ::arrow::internal::OptionalParallelFor(
      options.use_threads, static_cast<int>(buffers.size()), [&](int i) {
        ARROW_ASSIGN_OR_RAISE(*buffers[i],
                              DecompressBuffer(*buffers[i], options, codec.get()));
        return Status::OK();
      });
}

Result<std::shared_ptr<RecordBatch>> LoadRecordBatchSubset(
    const flatbuf::RecordBatch* metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, const DictionaryMemo* dictionary_memo,
    const IpcReadOptions& options, MetadataVersion metadata_version,
    Compression::type compression, io::RandomAccessFile* file) {
  ArrayLoader loader(metadata, metadata_version, dictionary_memo, options, file);

  std::vector<std::shared_ptr<ArrayData>> field_data;
  std::vector<std::shared_ptr<Field>> schema_fields;

  for (int i = 0; i < schema->num_fields(); ++i) {
    if (inclusion_mask[i]) {
      // Read field
      auto arr = std::make_shared<ArrayData>();
      RETURN_NOT_OK(loader.Load(schema->field(i).get(), arr.get()));
      if (metadata->length() != arr->length) {
        return Status::IOError("Array length did not match record batch length");
      }
      field_data.emplace_back(std::move(arr));
      schema_fields.emplace_back(schema->field(i));
    } else {
      // Skip field. This logic must be executed to advance the state of the
      // loader to the next field
      RETURN_NOT_OK(loader.SkipField(schema->field(i).get()));
    }
  }

  if (compression != Compression::UNCOMPRESSED) {
    RETURN_NOT_OK(DecompressBuffers(compression, options, &field_data));
  }

  return RecordBatch::Make(::arrow::schema(std::move(schema_fields), schema->metadata()),
                           metadata->length(), std::move(field_data));
}

Result<std::shared_ptr<RecordBatch>> LoadRecordBatch(
    const flatbuf::RecordBatch* metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, const DictionaryMemo* dictionary_memo,
    const IpcReadOptions& options, MetadataVersion metadata_version,
    Compression::type compression, io::RandomAccessFile* file) {
  if (inclusion_mask.size() > 0) {
    return LoadRecordBatchSubset(metadata, schema, inclusion_mask, dictionary_memo,
                                 options, metadata_version, compression, file);
  }

  ArrayLoader loader(metadata, metadata_version, dictionary_memo, options, file);
  std::vector<std::shared_ptr<ArrayData>> arrays(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto arr = std::make_shared<ArrayData>();
    RETURN_NOT_OK(loader.Load(schema->field(i).get(), arr.get()));
    if (metadata->length() != arr->length) {
      return Status::IOError("Array length did not match record batch length");
    }
    arrays[i] = std::move(arr);
  }
  if (compression != Compression::UNCOMPRESSED) {
    RETURN_NOT_OK(DecompressBuffers(compression, options, &arrays));
  }
  return RecordBatch::Make(schema, metadata->length(), std::move(arrays));
}

// ----------------------------------------------------------------------
// Array loading

Status GetCompression(const flatbuf::RecordBatch* batch, Compression::type* out) {
  *out = Compression::UNCOMPRESSED;
  const flatbuf::BodyCompression* compression = batch->compression();
  if (compression != nullptr) {
    if (compression->method() != flatbuf::BodyCompressionMethod::BUFFER) {
      // Forward compatibility
      return Status::Invalid("This library only supports BUFFER compression method");
    }

    if (compression->codec() == flatbuf::CompressionType::LZ4_FRAME) {
      *out = Compression::LZ4_FRAME;
    } else if (compression->codec() == flatbuf::CompressionType::ZSTD) {
      *out = Compression::ZSTD;
    } else {
      return Status::Invalid("Unsupported codec in RecordBatch::compression metadata");
    }
    return Status::OK();
  }
  return Status::OK();
}

Status GetCompressionExperimental(const flatbuf::Message* message,
                                  Compression::type* out) {
  *out = Compression::UNCOMPRESSED;
  if (message->custom_metadata() != nullptr) {
    // TODO: Ensure this deserialization only ever happens once
    std::shared_ptr<KeyValueMetadata> metadata;
    RETURN_NOT_OK(internal::GetKeyValueMetadata(message->custom_metadata(), &metadata));
    int index = metadata->FindKey("ARROW:experimental_compression");
    if (index != -1) {
      ARROW_ASSIGN_OR_RAISE(*out,
                            util::Codec::GetCompressionType(metadata->value(index)));
    }
    return internal::CheckCompressionSupported(*out);
  }
  return Status::OK();
}

static Status ReadContiguousPayload(io::InputStream* file,
                                    std::unique_ptr<Message>* message) {
  ARROW_ASSIGN_OR_RAISE(*message, ReadMessage(file));
  if (*message == nullptr) {
    return Status::Invalid("Unable to read metadata at offset");
  }
  return Status::OK();
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const std::shared_ptr<Schema>& schema, const DictionaryMemo* dictionary_memo,
    const IpcReadOptions& options, io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  CHECK_HAS_BODY(*message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
  return ReadRecordBatch(*message->metadata(), schema, dictionary_memo, options,
                         reader.get());
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Message& message, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options) {
  CHECK_MESSAGE_TYPE(MessageType::RECORD_BATCH, message.type());
  CHECK_HAS_BODY(message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message.body()));
  return ReadRecordBatch(*message.metadata(), schema, dictionary_memo, options,
                         reader.get());
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatchInternal(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, const DictionaryMemo* dictionary_memo,
    const IpcReadOptions& options, io::RandomAccessFile* file) {
  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  auto batch = message->header_as_RecordBatch();
  if (batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not RecordBatch.");
  }

  Compression::type compression;
  RETURN_NOT_OK(GetCompression(batch, &compression));
  if (compression == Compression::UNCOMPRESSED &&
      message->version() == flatbuf::MetadataVersion::V4) {
    // Possibly obtain codec information from experimental serialization format
    // in 0.17.x
    RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
  }
  return LoadRecordBatch(batch, schema, inclusion_mask, dictionary_memo, options,
                         internal::GetMetadataVersion(message->version()), compression,
                         file);
}

// If we are selecting only certain fields, populate an inclusion mask for fast lookups.
// Additionally, drop deselected fields from the reader's schema.
Status GetInclusionMaskAndOutSchema(const std::shared_ptr<Schema>& full_schema,
                                    const std::vector<int>& included_indices,
                                    std::vector<bool>* inclusion_mask,
                                    std::shared_ptr<Schema>* out_schema) {
  inclusion_mask->clear();
  if (included_indices.empty()) {
    *out_schema = full_schema;
    return Status::OK();
  }

  inclusion_mask->resize(full_schema->num_fields(), false);

  auto included_indices_sorted = included_indices;
  std::sort(included_indices_sorted.begin(), included_indices_sorted.end());

  FieldVector included_fields;
  for (int i : included_indices_sorted) {
    // Ignore out of bounds indices
    if (i < 0 || i >= full_schema->num_fields()) {
      return Status::Invalid("Out of bounds field index: ", i);
    }

    if (inclusion_mask->at(i)) continue;

    inclusion_mask->at(i) = true;
    included_fields.push_back(full_schema->field(i));
  }

  *out_schema = schema(std::move(included_fields), full_schema->metadata());
  return Status::OK();
}

Status UnpackSchemaMessage(const void* opaque_schema, const IpcReadOptions& options,
                           DictionaryMemo* dictionary_memo,
                           std::shared_ptr<Schema>* schema,
                           std::shared_ptr<Schema>* out_schema,
                           std::vector<bool>* field_inclusion_mask) {
  RETURN_NOT_OK(internal::GetSchema(opaque_schema, dictionary_memo, schema));

  // If we are selecting only certain fields, populate the inclusion mask now
  // for fast lookups
  return GetInclusionMaskAndOutSchema(*schema, options.included_fields,
                                      field_inclusion_mask, out_schema);
}

Status UnpackSchemaMessage(const Message& message, const IpcReadOptions& options,
                           DictionaryMemo* dictionary_memo,
                           std::shared_ptr<Schema>* schema,
                           std::shared_ptr<Schema>* out_schema,
                           std::vector<bool>* field_inclusion_mask) {
  CHECK_MESSAGE_TYPE(MessageType::SCHEMA, message.type());
  CHECK_HAS_NO_BODY(message);

  return UnpackSchemaMessage(message.header(), options, dictionary_memo, schema,
                             out_schema, field_inclusion_mask);
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options,
    io::RandomAccessFile* file) {
  std::shared_ptr<Schema> out_schema;
  // Empty means do not use
  std::vector<bool> inclusion_mask;
  RETURN_NOT_OK(GetInclusionMaskAndOutSchema(schema, options.included_fields,
                                             &inclusion_mask, &out_schema));
  return ReadRecordBatchInternal(metadata, schema, inclusion_mask, dictionary_memo,
                                 options, file);
}

Status ReadDictionary(const Buffer& metadata, DictionaryMemo* dictionary_memo,
                      const IpcReadOptions& options, io::RandomAccessFile* file) {
  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  auto dictionary_batch = message->header_as_DictionaryBatch();
  if (dictionary_batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not DictionaryBatch.");
  }

  // The dictionary is embedded in a record batch with a single column
  auto batch_meta = dictionary_batch->data();

  CHECK_FLATBUFFERS_NOT_NULL(batch_meta, "DictionaryBatch.data");

  Compression::type compression;
  RETURN_NOT_OK(GetCompression(batch_meta, &compression));
  if (compression == Compression::UNCOMPRESSED &&
      message->version() == flatbuf::MetadataVersion::V4) {
    // Possibly obtain codec information from experimental serialization format
    // in 0.17.x
    RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
  }

  int64_t id = dictionary_batch->id();

  // Look up the field, which must have been added to the
  // DictionaryMemo already prior to invoking this function
  std::shared_ptr<DataType> value_type;
  RETURN_NOT_OK(dictionary_memo->GetDictionaryType(id, &value_type));

  auto value_field = ::arrow::field("dummy", value_type);

  std::shared_ptr<RecordBatch> batch;
  ARROW_ASSIGN_OR_RAISE(
      batch, LoadRecordBatch(batch_meta, ::arrow::schema({value_field}),
                             /*field_inclusion_mask=*/{}, dictionary_memo, options,
                             internal::GetMetadataVersion(message->version()),
                             compression, file));
  if (batch->num_columns() != 1) {
    return Status::Invalid("Dictionary record batch must only contain one field");
  }
  auto dictionary = batch->column(0);
  // Validate the dictionary for safe delta concatenation
  RETURN_NOT_OK(dictionary->Validate());
  if (dictionary_batch->isDelta()) {
    return dictionary_memo->AddDictionaryDelta(id, dictionary, options.memory_pool);
  }
  return dictionary_memo->AddOrReplaceDictionary(id, dictionary);
}

Status ParseDictionary(const Message& message, DictionaryMemo* dictionary_memo,
                       const IpcReadOptions& options) {
  // Only invoke this method if we already know we have a dictionary message
  DCHECK_EQ(message.type(), MessageType::DICTIONARY_BATCH);
  CHECK_HAS_BODY(message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message.body()));
  return ReadDictionary(*message.metadata(), dictionary_memo, options, reader.get());
}

Status UpdateDictionaries(const Message& message, DictionaryMemo* dictionary_memo,
                          const IpcReadOptions& options) {
  return ParseDictionary(message, dictionary_memo, options);
}

// ----------------------------------------------------------------------
// RecordBatchStreamReader implementation

class RecordBatchStreamReaderImpl : public RecordBatchStreamReader {
 public:
  Status Open(std::unique_ptr<MessageReader> message_reader,
              const IpcReadOptions& options) {
    message_reader_ = std::move(message_reader);
    options_ = options;

    // Read schema
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Message> message,
                          message_reader_->ReadNextMessage());
    if (!message) {
      return Status::Invalid("Tried reading schema message, was null or length 0");
    }

    return UnpackSchemaMessage(*message, options, &dictionary_memo_, &schema_,
                               &out_schema_, &field_inclusion_mask_);
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    if (!have_read_initial_dictionaries_) {
      RETURN_NOT_OK(ReadInitialDictionaries());
    }

    if (empty_stream_) {
      // ARROW-6006: Degenerate case where stream contains no data, we do not
      // bother trying to read a RecordBatch message from the stream
      *batch = nullptr;
      return Status::OK();
    }

    // Continue to read other dictionaries, if any
    std::unique_ptr<Message> message;
    ARROW_ASSIGN_OR_RAISE(message, message_reader_->ReadNextMessage());

    while (message != nullptr && message->type() == MessageType::DICTIONARY_BATCH) {
      RETURN_NOT_OK(UpdateDictionaries(*message, &dictionary_memo_, options_));
      ARROW_ASSIGN_OR_RAISE(message, message_reader_->ReadNextMessage());
    }

    if (message == nullptr) {
      // End of stream
      *batch = nullptr;
      return Status::OK();
    }

    CHECK_HAS_BODY(*message);
    ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
    return ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                   &dictionary_memo_, options_, reader.get())
        .Value(batch);
  }

  std::shared_ptr<Schema> schema() const override { return out_schema_; }

 private:
  Status ReadInitialDictionaries() {
    // We must receive all dictionaries before reconstructing the
    // first record batch. Subsequent dictionary deltas modify the memo
    std::unique_ptr<Message> message;

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    for (int i = 0; i < dictionary_memo_.num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(message, message_reader_->ReadNextMessage());
      if (!message) {
        if (i == 0) {
          /// ARROW-6006: If we fail to find any dictionaries in the stream, then
          /// it may be that the stream has a schema but no actual data. In such
          /// case we communicate that we were unable to find the dictionaries
          /// (but there was no failure otherwise), so the caller can decide what
          /// to do
          empty_stream_ = true;
          break;
        } else {
          // ARROW-6126, the stream terminated before receiving the expected
          // number of dictionaries
          return Status::Invalid("IPC stream ended without reading the expected number (",
                                 dictionary_memo_.num_fields(), ") of dictionaries");
        }
      }

      if (message->type() != MessageType::DICTIONARY_BATCH) {
        return Status::Invalid("IPC stream did not have the expected number (",
                               dictionary_memo_.num_fields(),
                               ") of dictionaries at the start of the stream");
      }
      RETURN_NOT_OK(ParseDictionary(*message, &dictionary_memo_, options_));
    }

    have_read_initial_dictionaries_ = true;
    return Status::OK();
  }

  std::unique_ptr<MessageReader> message_reader_;
  IpcReadOptions options_;
  std::vector<bool> field_inclusion_mask_;

  bool have_read_initial_dictionaries_ = false;

  // Flag to set in case where we fail to observe all dictionaries in a stream,
  // and so the reader should not attempt to parse any messages
  bool empty_stream_ = false;

  DictionaryMemo dictionary_memo_;
  std::shared_ptr<Schema> schema_, out_schema_;
};

// ----------------------------------------------------------------------
// Stream reader constructors

Result<std::shared_ptr<RecordBatchReader>> RecordBatchStreamReader::Open(
    std::unique_ptr<MessageReader> message_reader, const IpcReadOptions& options) {
  // Private ctor
  auto result = std::make_shared<RecordBatchStreamReaderImpl>();
  RETURN_NOT_OK(result->Open(std::move(message_reader), options));
  return result;
}

Result<std::shared_ptr<RecordBatchReader>> RecordBatchStreamReader::Open(
    io::InputStream* stream, const IpcReadOptions& options) {
  return Open(MessageReader::Open(stream), options);
}

Result<std::shared_ptr<RecordBatchReader>> RecordBatchStreamReader::Open(
    const std::shared_ptr<io::InputStream>& stream, const IpcReadOptions& options) {
  return Open(MessageReader::Open(stream), options);
}

// ----------------------------------------------------------------------
// Reader implementation

static inline FileBlock FileBlockFromFlatbuffer(const flatbuf::Block* block) {
  return FileBlock{block->offset(), block->metaDataLength(), block->bodyLength()};
}

class RecordBatchFileReaderImpl : public RecordBatchFileReader {
 public:
  RecordBatchFileReaderImpl() : file_(NULLPTR), footer_offset_(0), footer_(NULLPTR) {}

  int num_record_batches() const override {
    return static_cast<int>(internal::FlatBuffersVectorSize(footer_->recordBatches()));
  }

  MetadataVersion version() const override {
    return internal::GetMetadataVersion(footer_->version());
  }

  Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(int i) override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());

    if (!read_dictionaries_) {
      RETURN_NOT_OK(ReadDictionaries());
      read_dictionaries_ = true;
    }

    std::unique_ptr<Message> message;
    RETURN_NOT_OK(ReadMessageFromBlock(GetRecordBatchBlock(i), &message));

    CHECK_HAS_BODY(*message);
    ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
    return ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                   &dictionary_memo_, options_, reader.get());
  }

  Status Open(const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset,
              const IpcReadOptions& options) {
    owned_file_ = file;
    return Open(file.get(), footer_offset, options);
  }

  Status Open(io::RandomAccessFile* file, int64_t footer_offset,
              const IpcReadOptions& options) {
    file_ = file;
    options_ = options;
    footer_offset_ = footer_offset;
    RETURN_NOT_OK(ReadFooter());

    // Get the schema and record any observed dictionaries
    return UnpackSchemaMessage(footer_->schema(), options, &dictionary_memo_, &schema_,
                               &out_schema_, &field_inclusion_mask_);
  }

  std::shared_ptr<Schema> schema() const override { return out_schema_; }

  std::shared_ptr<const KeyValueMetadata> metadata() const override { return metadata_; }

 private:
  FileBlock GetRecordBatchBlock(int i) const {
    return FileBlockFromFlatbuffer(footer_->recordBatches()->Get(i));
  }

  FileBlock GetDictionaryBlock(int i) const {
    return FileBlockFromFlatbuffer(footer_->dictionaries()->Get(i));
  }

  Status ReadMessageFromBlock(const FileBlock& block, std::unique_ptr<Message>* out) {
    if (!BitUtil::IsMultipleOf8(block.offset) ||
        !BitUtil::IsMultipleOf8(block.metadata_length) ||
        !BitUtil::IsMultipleOf8(block.body_length)) {
      return Status::Invalid("Unaligned block in IPC file");
    }

    // TODO(wesm): this breaks integration tests, see ARROW-3256
    // DCHECK_EQ((*out)->body_length(), block.body_length);

    return ReadMessage(block.offset, block.metadata_length, file_).Value(out);
  }

  Status ReadDictionaries() {
    // Read all the dictionaries
    for (int i = 0; i < num_dictionaries(); ++i) {
      std::unique_ptr<Message> message;
      RETURN_NOT_OK(ReadMessageFromBlock(GetDictionaryBlock(i), &message));

      CHECK_HAS_BODY(*message);
      ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
      RETURN_NOT_OK(ReadDictionary(*message->metadata(), &dictionary_memo_, options_,
                                   reader.get()));
    }
    return Status::OK();
  }

  Status ReadFooter() {
    const int32_t magic_size = static_cast<int>(strlen(kArrowMagicBytes));

    if (footer_offset_ <= magic_size * 2 + 4) {
      return Status::Invalid("File is too small: ", footer_offset_);
    }

    int file_end_size = static_cast<int>(magic_size + sizeof(int32_t));
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          file_->ReadAt(footer_offset_ - file_end_size, file_end_size));

    const int64_t expected_footer_size = magic_size + sizeof(int32_t);
    if (buffer->size() < expected_footer_size) {
      return Status::Invalid("Unable to read ", expected_footer_size, "from end of file");
    }

    if (memcmp(buffer->data() + sizeof(int32_t), kArrowMagicBytes, magic_size)) {
      return Status::Invalid("Not an Arrow file");
    }

    int32_t footer_length =
        BitUtil::FromLittleEndian(*reinterpret_cast<const int32_t*>(buffer->data()));

    if (footer_length <= 0 || footer_length > footer_offset_ - magic_size * 2 - 4) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }

    // Now read the footer
    ARROW_ASSIGN_OR_RAISE(
        footer_buffer_,
        file_->ReadAt(footer_offset_ - footer_length - file_end_size, footer_length));

    auto data = footer_buffer_->data();
    flatbuffers::Verifier verifier(data, footer_buffer_->size(), 128);
    if (!flatbuf::VerifyFooterBuffer(verifier)) {
      return Status::IOError("Verification of flatbuffer-encoded Footer failed.");
    }
    footer_ = flatbuf::GetFooter(data);

    auto fb_metadata = footer_->custom_metadata();
    if (fb_metadata != nullptr) {
      std::shared_ptr<KeyValueMetadata> md;
      RETURN_NOT_OK(internal::GetKeyValueMetadata(fb_metadata, &md));
      metadata_ = std::move(md);  // const-ify
    }

    return Status::OK();
  }

  int num_dictionaries() const {
    return static_cast<int>(internal::FlatBuffersVectorSize(footer_->dictionaries()));
  }

  io::RandomAccessFile* file_;
  IpcReadOptions options_;
  std::vector<bool> field_inclusion_mask_;

  std::shared_ptr<io::RandomAccessFile> owned_file_;

  // The location where the Arrow file layout ends. May be the end of the file
  // or some other location if embedded in a larger file.
  int64_t footer_offset_;

  // Footer metadata
  std::shared_ptr<Buffer> footer_buffer_;
  const flatbuf::Footer* footer_;
  std::shared_ptr<const KeyValueMetadata> metadata_;

  bool read_dictionaries_ = false;
  DictionaryMemo dictionary_memo_;

  // Reconstructed schema, including any read dictionaries
  std::shared_ptr<Schema> schema_;
  // Schema with deselected fields dropped
  std::shared_ptr<Schema> out_schema_;
};

Result<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::Open(
    io::RandomAccessFile* file, const IpcReadOptions& options) {
  ARROW_ASSIGN_OR_RAISE(int64_t footer_offset, file->GetSize());
  return Open(file, footer_offset, options);
}

Result<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::Open(
    io::RandomAccessFile* file, int64_t footer_offset, const IpcReadOptions& options) {
  auto result = std::make_shared<RecordBatchFileReaderImpl>();
  RETURN_NOT_OK(result->Open(file, footer_offset, options));
  return result;
}

Result<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::Open(
    const std::shared_ptr<io::RandomAccessFile>& file, const IpcReadOptions& options) {
  ARROW_ASSIGN_OR_RAISE(int64_t footer_offset, file->GetSize());
  return Open(file, footer_offset, options);
}

Result<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::Open(
    const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset,
    const IpcReadOptions& options) {
  auto result = std::make_shared<RecordBatchFileReaderImpl>();
  RETURN_NOT_OK(result->Open(file, footer_offset, options));
  return result;
}

Status Listener::OnEOS() { return Status::OK(); }

Status Listener::OnSchemaDecoded(std::shared_ptr<Schema> schema) { return Status::OK(); }

Status Listener::OnRecordBatchDecoded(std::shared_ptr<RecordBatch> record_batch) {
  return Status::NotImplemented("OnRecordBatchDecoded() callback isn't implemented");
}

class StreamDecoder::StreamDecoderImpl : public MessageDecoderListener {
 private:
  enum State {
    SCHEMA,
    INITIAL_DICTIONARIES,
    RECORD_BATCHES,
    EOS,
  };

 public:
  explicit StreamDecoderImpl(std::shared_ptr<Listener> listener,
                             const IpcReadOptions& options)
      : MessageDecoderListener(),
        listener_(std::move(listener)),
        options_(options),
        state_(State::SCHEMA),
        message_decoder_(std::shared_ptr<StreamDecoderImpl>(this, [](void*) {}),
                         options_.memory_pool),
        field_inclusion_mask_(),
        n_required_dictionaries_(0),
        dictionary_memo_(),
        schema_() {}

  Status OnMessageDecoded(std::unique_ptr<Message> message) override {
    switch (state_) {
      case State::SCHEMA:
        ARROW_RETURN_NOT_OK(OnSchemaMessageDecoded(std::move(message)));
        break;
      case State::INITIAL_DICTIONARIES:
        ARROW_RETURN_NOT_OK(OnInitialDictionaryMessageDecoded(std::move(message)));
        break;
      case State::RECORD_BATCHES:
        ARROW_RETURN_NOT_OK(OnRecordBatchMessageDecoded(std::move(message)));
        break;
      case State::EOS:
        break;
    }
    return Status::OK();
  }

  Status OnEOS() override {
    state_ = State::EOS;
    return listener_->OnEOS();
  }

  Status Consume(const uint8_t* data, int64_t size) {
    return message_decoder_.Consume(data, size);
  }

  Status Consume(std::shared_ptr<Buffer> buffer) {
    return message_decoder_.Consume(std::move(buffer));
  }

  std::shared_ptr<Schema> schema() const { return out_schema_; }

  int64_t next_required_size() const { return message_decoder_.next_required_size(); }

 private:
  Status OnSchemaMessageDecoded(std::unique_ptr<Message> message) {
    RETURN_NOT_OK(UnpackSchemaMessage(*message, options_, &dictionary_memo_, &schema_,
                                      &out_schema_, &field_inclusion_mask_));

    n_required_dictionaries_ = dictionary_memo_.num_fields();
    if (n_required_dictionaries_ == 0) {
      state_ = State::RECORD_BATCHES;
      RETURN_NOT_OK(listener_->OnSchemaDecoded(schema_));
    } else {
      state_ = State::INITIAL_DICTIONARIES;
    }
    return Status::OK();
  }

  Status OnInitialDictionaryMessageDecoded(std::unique_ptr<Message> message) {
    if (message->type() != MessageType::DICTIONARY_BATCH) {
      return Status::Invalid("IPC stream did not have the expected number (",
                             dictionary_memo_.num_fields(),
                             ") of dictionaries at the start of the stream");
    }
    RETURN_NOT_OK(ParseDictionary(*message, &dictionary_memo_, options_));
    n_required_dictionaries_--;
    if (n_required_dictionaries_ == 0) {
      state_ = State::RECORD_BATCHES;
      ARROW_RETURN_NOT_OK(listener_->OnSchemaDecoded(schema_));
    }
    return Status::OK();
  }

  Status OnRecordBatchMessageDecoded(std::unique_ptr<Message> message) {
    if (message->type() == MessageType::DICTIONARY_BATCH) {
      return UpdateDictionaries(*message, &dictionary_memo_, options_);
    } else {
      CHECK_HAS_BODY(*message);
      ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
      ARROW_ASSIGN_OR_RAISE(
          auto batch,
          ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                  &dictionary_memo_, options_, reader.get()));
      return listener_->OnRecordBatchDecoded(std::move(batch));
    }
  }

  std::shared_ptr<Listener> listener_;
  IpcReadOptions options_;
  State state_;
  MessageDecoder message_decoder_;
  std::vector<bool> field_inclusion_mask_;
  int n_required_dictionaries_;
  DictionaryMemo dictionary_memo_;
  std::shared_ptr<Schema> schema_, out_schema_;
};

StreamDecoder::StreamDecoder(std::shared_ptr<Listener> listener,
                             const IpcReadOptions& options) {
  impl_.reset(new StreamDecoderImpl(std::move(listener), options));
}

StreamDecoder::~StreamDecoder() {}

Status StreamDecoder::Consume(const uint8_t* data, int64_t size) {
  return impl_->Consume(data, size);
}
Status StreamDecoder::Consume(std::shared_ptr<Buffer> buffer) {
  return impl_->Consume(std::move(buffer));
}

std::shared_ptr<Schema> StreamDecoder::schema() const { return impl_->schema(); }

int64_t StreamDecoder::next_required_size() const { return impl_->next_required_size(); }

Result<std::shared_ptr<Schema>> ReadSchema(io::InputStream* stream,
                                           DictionaryMemo* dictionary_memo) {
  std::unique_ptr<MessageReader> reader = MessageReader::Open(stream);
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Message> message, reader->ReadNextMessage());
  if (!message) {
    return Status::Invalid("Tried reading schema message, was null or length 0");
  }
  CHECK_MESSAGE_TYPE(MessageType::SCHEMA, message->type());
  return ReadSchema(*message, dictionary_memo);
}

Result<std::shared_ptr<Schema>> ReadSchema(const Message& message,
                                           DictionaryMemo* dictionary_memo) {
  std::shared_ptr<Schema> result;
  RETURN_NOT_OK(internal::GetSchema(message.header(), dictionary_memo, &result));
  return result;
}

Result<std::shared_ptr<Tensor>> ReadTensor(io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  return ReadTensor(*message);
}

Result<std::shared_ptr<Tensor>> ReadTensor(const Message& message) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<int64_t> strides;
  std::vector<std::string> dim_names;
  CHECK_HAS_BODY(message);
  RETURN_NOT_OK(internal::GetTensorMetadata(*message.metadata(), &type, &shape, &strides,
                                            &dim_names));
  return Tensor::Make(type, message.body(), shape, strides, dim_names);
}

namespace {

Result<std::shared_ptr<SparseIndex>> ReadSparseCOOIndex(
    const flatbuf::SparseTensor* sparse_tensor, const std::vector<int64_t>& shape,
    int64_t non_zero_length, io::RandomAccessFile* file) {
  auto* sparse_index = sparse_tensor->sparseIndex_as_SparseTensorIndexCOO();
  const auto ndim = static_cast<int64_t>(shape.size());

  std::shared_ptr<DataType> indices_type;
  RETURN_NOT_OK(internal::GetSparseCOOIndexMetadata(sparse_index, &indices_type));
  const int64_t indices_elsize = GetByteWidth(*indices_type);

  auto* indices_buffer = sparse_index->indicesBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indices_data,
                        file->ReadAt(indices_buffer->offset(), indices_buffer->length()));
  std::vector<int64_t> indices_shape({non_zero_length, ndim});
  auto* indices_strides = sparse_index->indicesStrides();
  std::vector<int64_t> strides(2);
  if (indices_strides && indices_strides->size() > 0) {
    if (indices_strides->size() != 2) {
      return Status::Invalid("Wrong size for indicesStrides in SparseCOOIndex");
    }
    strides[0] = indices_strides->Get(0);
    strides[1] = indices_strides->Get(1);
  } else {
    // Row-major by default
    strides[0] = indices_elsize * ndim;
    strides[1] = indices_elsize;
  }
  return SparseCOOIndex::Make(
      std::make_shared<Tensor>(indices_type, indices_data, indices_shape, strides),
      sparse_index->isCanonical());
}

Result<std::shared_ptr<SparseIndex>> ReadSparseCSXIndex(
    const flatbuf::SparseTensor* sparse_tensor, const std::vector<int64_t>& shape,
    int64_t non_zero_length, io::RandomAccessFile* file) {
  if (shape.size() != 2) {
    return Status::Invalid("Invalid shape length for a sparse matrix");
  }

  auto* sparse_index = sparse_tensor->sparseIndex_as_SparseMatrixIndexCSX();

  std::shared_ptr<DataType> indptr_type, indices_type;
  RETURN_NOT_OK(
      internal::GetSparseCSXIndexMetadata(sparse_index, &indptr_type, &indices_type));
  const int indptr_byte_width = GetByteWidth(*indptr_type);

  auto* indptr_buffer = sparse_index->indptrBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indptr_data,
                        file->ReadAt(indptr_buffer->offset(), indptr_buffer->length()));

  auto* indices_buffer = sparse_index->indicesBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indices_data,
                        file->ReadAt(indices_buffer->offset(), indices_buffer->length()));

  std::vector<int64_t> indices_shape({non_zero_length});
  const auto indices_minimum_bytes = indices_shape[0] * GetByteWidth(*indices_type);
  if (indices_minimum_bytes > indices_buffer->length()) {
    return Status::Invalid("shape is inconsistent to the size of indices buffer");
  }

  switch (sparse_index->compressedAxis()) {
    case flatbuf::SparseMatrixCompressedAxis::Row: {
      std::vector<int64_t> indptr_shape({shape[0] + 1});
      const int64_t indptr_minimum_bytes = indptr_shape[0] * indptr_byte_width;
      if (indptr_minimum_bytes > indptr_buffer->length()) {
        return Status::Invalid("shape is inconsistent to the size of indptr buffer");
      }
      return std::make_shared<SparseCSRIndex>(
          std::make_shared<Tensor>(indptr_type, indptr_data, indptr_shape),
          std::make_shared<Tensor>(indices_type, indices_data, indices_shape));
    }
    case flatbuf::SparseMatrixCompressedAxis::Column: {
      std::vector<int64_t> indptr_shape({shape[1] + 1});
      const int64_t indptr_minimum_bytes = indptr_shape[0] * indptr_byte_width;
      if (indptr_minimum_bytes > indptr_buffer->length()) {
        return Status::Invalid("shape is inconsistent to the size of indptr buffer");
      }
      return std::make_shared<SparseCSCIndex>(
          std::make_shared<Tensor>(indptr_type, indptr_data, indptr_shape),
          std::make_shared<Tensor>(indices_type, indices_data, indices_shape));
    }
    default:
      return Status::Invalid("Invalid value of SparseMatrixCompressedAxis");
  }
}

Result<std::shared_ptr<SparseIndex>> ReadSparseCSFIndex(
    const flatbuf::SparseTensor* sparse_tensor, const std::vector<int64_t>& shape,
    io::RandomAccessFile* file) {
  auto* sparse_index = sparse_tensor->sparseIndex_as_SparseTensorIndexCSF();
  const auto ndim = static_cast<int64_t>(shape.size());
  auto* indptr_buffers = sparse_index->indptrBuffers();
  auto* indices_buffers = sparse_index->indicesBuffers();
  std::vector<std::shared_ptr<Buffer>> indptr_data(ndim - 1);
  std::vector<std::shared_ptr<Buffer>> indices_data(ndim);

  std::shared_ptr<DataType> indptr_type, indices_type;
  std::vector<int64_t> axis_order, indices_size;

  RETURN_NOT_OK(internal::GetSparseCSFIndexMetadata(
      sparse_index, &axis_order, &indices_size, &indptr_type, &indices_type));
  for (int i = 0; i < static_cast<int>(indptr_buffers->size()); ++i) {
    ARROW_ASSIGN_OR_RAISE(indptr_data[i], file->ReadAt(indptr_buffers->Get(i)->offset(),
                                                       indptr_buffers->Get(i)->length()));
  }
  for (int i = 0; i < static_cast<int>(indices_buffers->size()); ++i) {
    ARROW_ASSIGN_OR_RAISE(indices_data[i],
                          file->ReadAt(indices_buffers->Get(i)->offset(),
                                       indices_buffers->Get(i)->length()));
  }

  return SparseCSFIndex::Make(indptr_type, indices_type, indices_size, axis_order,
                              indptr_data, indices_data);
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCOOIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCOOIndex>& sparse_index, int64_t non_zero_length,
    const std::shared_ptr<Buffer>& data) {
  return SparseCOOTensor::Make(sparse_index, type, data, shape, dim_names);
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCSRIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCSRIndex>& sparse_index, int64_t non_zero_length,
    const std::shared_ptr<Buffer>& data) {
  return SparseCSRMatrix::Make(sparse_index, type, data, shape, dim_names);
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCSCIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCSCIndex>& sparse_index, int64_t non_zero_length,
    const std::shared_ptr<Buffer>& data) {
  return SparseCSCMatrix::Make(sparse_index, type, data, shape, dim_names);
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCSFIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCSFIndex>& sparse_index,
    const std::shared_ptr<Buffer>& data) {
  return SparseCSFTensor::Make(sparse_index, type, data, shape, dim_names);
}

Status ReadSparseTensorMetadata(const Buffer& metadata,
                                std::shared_ptr<DataType>* out_type,
                                std::vector<int64_t>* out_shape,
                                std::vector<std::string>* out_dim_names,
                                int64_t* out_non_zero_length,
                                SparseTensorFormat::type* out_format_id,
                                const flatbuf::SparseTensor** out_fb_sparse_tensor,
                                const flatbuf::Buffer** out_buffer) {
  RETURN_NOT_OK(internal::GetSparseTensorMetadata(
      metadata, out_type, out_shape, out_dim_names, out_non_zero_length, out_format_id));

  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));

  auto sparse_tensor = message->header_as_SparseTensor();
  if (sparse_tensor == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not SparseTensor.");
  }
  *out_fb_sparse_tensor = sparse_tensor;

  auto buffer = sparse_tensor->data();
  if (!BitUtil::IsMultipleOf8(buffer->offset())) {
    return Status::Invalid(
        "Buffer of sparse index data did not start on 8-byte aligned offset: ",
        buffer->offset());
  }
  *out_buffer = buffer;

  return Status::OK();
}

}  // namespace

namespace internal {

namespace {

Result<size_t> GetSparseTensorBodyBufferCount(SparseTensorFormat::type format_id,
                                              const size_t ndim) {
  switch (format_id) {
    case SparseTensorFormat::COO:
      return 2;

    case SparseTensorFormat::CSR:
      return 3;

    case SparseTensorFormat::CSC:
      return 3;

    case SparseTensorFormat::CSF:
      return 2 * ndim;

    default:
      return Status::Invalid("Unrecognized sparse tensor format");
  }
}

Status CheckSparseTensorBodyBufferCount(const IpcPayload& payload,
                                        SparseTensorFormat::type sparse_tensor_format_id,
                                        const size_t ndim) {
  size_t expected_body_buffer_count = 0;
  ARROW_ASSIGN_OR_RAISE(expected_body_buffer_count,
                        GetSparseTensorBodyBufferCount(sparse_tensor_format_id, ndim));
  if (payload.body_buffers.size() != expected_body_buffer_count) {
    return Status::Invalid("Invalid body buffer count for a sparse tensor");
  }

  return Status::OK();
}

}  // namespace

Result<size_t> ReadSparseTensorBodyBufferCount(const Buffer& metadata) {
  SparseTensorFormat::type format_id;
  std::vector<int64_t> shape;

  RETURN_NOT_OK(internal::GetSparseTensorMetadata(metadata, nullptr, &shape, nullptr,
                                                  nullptr, &format_id));

  return GetSparseTensorBodyBufferCount(format_id, static_cast<size_t>(shape.size()));
}

Result<std::shared_ptr<SparseTensor>> ReadSparseTensorPayload(const IpcPayload& payload) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<std::string> dim_names;
  int64_t non_zero_length;
  SparseTensorFormat::type sparse_tensor_format_id;
  const flatbuf::SparseTensor* sparse_tensor;
  const flatbuf::Buffer* buffer;

  RETURN_NOT_OK(ReadSparseTensorMetadata(*payload.metadata, &type, &shape, &dim_names,
                                         &non_zero_length, &sparse_tensor_format_id,
                                         &sparse_tensor, &buffer));

  RETURN_NOT_OK(CheckSparseTensorBodyBufferCount(payload, sparse_tensor_format_id,
                                                 static_cast<size_t>(shape.size())));

  switch (sparse_tensor_format_id) {
    case SparseTensorFormat::COO: {
      std::shared_ptr<SparseCOOIndex> sparse_index;
      std::shared_ptr<DataType> indices_type;
      RETURN_NOT_OK(internal::GetSparseCOOIndexMetadata(
          sparse_tensor->sparseIndex_as_SparseTensorIndexCOO(), &indices_type));
      ARROW_ASSIGN_OR_RAISE(sparse_index,
                            SparseCOOIndex::Make(indices_type, shape, non_zero_length,
                                                 payload.body_buffers[0]));
      return MakeSparseTensorWithSparseCOOIndex(type, shape, dim_names, sparse_index,
                                                non_zero_length, payload.body_buffers[1]);
    }
    case SparseTensorFormat::CSR: {
      std::shared_ptr<SparseCSRIndex> sparse_index;
      std::shared_ptr<DataType> indptr_type;
      std::shared_ptr<DataType> indices_type;
      RETURN_NOT_OK(internal::GetSparseCSXIndexMetadata(
          sparse_tensor->sparseIndex_as_SparseMatrixIndexCSX(), &indptr_type,
          &indices_type));
      ARROW_CHECK_EQ(indptr_type, indices_type);
      ARROW_ASSIGN_OR_RAISE(
          sparse_index,
          SparseCSRIndex::Make(indices_type, shape, non_zero_length,
                               payload.body_buffers[0], payload.body_buffers[1]));
      return MakeSparseTensorWithSparseCSRIndex(type, shape, dim_names, sparse_index,
                                                non_zero_length, payload.body_buffers[2]);
    }
    case SparseTensorFormat::CSC: {
      std::shared_ptr<SparseCSCIndex> sparse_index;
      std::shared_ptr<DataType> indptr_type;
      std::shared_ptr<DataType> indices_type;
      RETURN_NOT_OK(internal::GetSparseCSXIndexMetadata(
          sparse_tensor->sparseIndex_as_SparseMatrixIndexCSX(), &indptr_type,
          &indices_type));
      ARROW_CHECK_EQ(indptr_type, indices_type);
      ARROW_ASSIGN_OR_RAISE(
          sparse_index,
          SparseCSCIndex::Make(indices_type, shape, non_zero_length,
                               payload.body_buffers[0], payload.body_buffers[1]));
      return MakeSparseTensorWithSparseCSCIndex(type, shape, dim_names, sparse_index,
                                                non_zero_length, payload.body_buffers[2]);
    }
    case SparseTensorFormat::CSF: {
      std::shared_ptr<SparseCSFIndex> sparse_index;
      std::shared_ptr<DataType> indptr_type, indices_type;
      std::vector<int64_t> axis_order, indices_size;

      RETURN_NOT_OK(internal::GetSparseCSFIndexMetadata(
          sparse_tensor->sparseIndex_as_SparseTensorIndexCSF(), &axis_order,
          &indices_size, &indptr_type, &indices_type));
      ARROW_CHECK_EQ(indptr_type, indices_type);

      const int64_t ndim = shape.size();
      std::vector<std::shared_ptr<Buffer>> indptr_data(ndim - 1);
      std::vector<std::shared_ptr<Buffer>> indices_data(ndim);

      for (int64_t i = 0; i < ndim - 1; ++i) {
        indptr_data[i] = payload.body_buffers[i];
      }
      for (int64_t i = 0; i < ndim; ++i) {
        indices_data[i] = payload.body_buffers[i + ndim - 1];
      }

      ARROW_ASSIGN_OR_RAISE(sparse_index,
                            SparseCSFIndex::Make(indptr_type, indices_type, indices_size,
                                                 axis_order, indptr_data, indices_data));
      return MakeSparseTensorWithSparseCSFIndex(type, shape, dim_names, sparse_index,
                                                payload.body_buffers[2 * ndim - 1]);
    }
    default:
      return Status::Invalid("Unsupported sparse index format");
  }
}

}  // namespace internal

Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(const Buffer& metadata,
                                                       io::RandomAccessFile* file) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<std::string> dim_names;
  int64_t non_zero_length;
  SparseTensorFormat::type sparse_tensor_format_id;
  const flatbuf::SparseTensor* sparse_tensor;
  const flatbuf::Buffer* buffer;

  RETURN_NOT_OK(ReadSparseTensorMetadata(metadata, &type, &shape, &dim_names,
                                         &non_zero_length, &sparse_tensor_format_id,
                                         &sparse_tensor, &buffer));

  ARROW_ASSIGN_OR_RAISE(auto data, file->ReadAt(buffer->offset(), buffer->length()));

  std::shared_ptr<SparseIndex> sparse_index;
  switch (sparse_tensor_format_id) {
    case SparseTensorFormat::COO: {
      ARROW_ASSIGN_OR_RAISE(
          sparse_index, ReadSparseCOOIndex(sparse_tensor, shape, non_zero_length, file));
      return MakeSparseTensorWithSparseCOOIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCOOIndex>(sparse_index),
          non_zero_length, data);
    }
    case SparseTensorFormat::CSR: {
      ARROW_ASSIGN_OR_RAISE(
          sparse_index, ReadSparseCSXIndex(sparse_tensor, shape, non_zero_length, file));
      return MakeSparseTensorWithSparseCSRIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCSRIndex>(sparse_index),
          non_zero_length, data);
    }
    case SparseTensorFormat::CSC: {
      ARROW_ASSIGN_OR_RAISE(
          sparse_index, ReadSparseCSXIndex(sparse_tensor, shape, non_zero_length, file));
      return MakeSparseTensorWithSparseCSCIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCSCIndex>(sparse_index),
          non_zero_length, data);
    }
    case SparseTensorFormat::CSF: {
      ARROW_ASSIGN_OR_RAISE(sparse_index, ReadSparseCSFIndex(sparse_tensor, shape, file));
      return MakeSparseTensorWithSparseCSFIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCSFIndex>(sparse_index),
          data);
    }
    default:
      return Status::Invalid("Unsupported sparse index format");
  }
}

Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(const Message& message) {
  CHECK_HAS_BODY(message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message.body()));
  return ReadSparseTensor(*message.metadata(), reader.get());
}

Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  CHECK_MESSAGE_TYPE(MessageType::SPARSE_TENSOR, message->type());
  CHECK_HAS_BODY(*message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
  return ReadSparseTensor(*message->metadata(), reader.get());
}

///////////////////////////////////////////////////////////////////////////
// Helpers for fuzzing

namespace internal {

Status FuzzIpcStream(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<RecordBatchReader> batch_reader;
  ARROW_ASSIGN_OR_RAISE(batch_reader, RecordBatchStreamReader::Open(&buffer_reader));

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_NOT_OK(batch_reader->ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    RETURN_NOT_OK(batch->ValidateFull());
  }

  return Status::OK();
}

Status FuzzIpcFile(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<RecordBatchFileReader> batch_reader;
  ARROW_ASSIGN_OR_RAISE(batch_reader, RecordBatchFileReader::Open(&buffer_reader));

  const int n_batches = batch_reader->num_record_batches();
  for (int i = 0; i < n_batches; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->ReadRecordBatch(i));
    RETURN_NOT_OK(batch->ValidateFull());
  }

  return Status::OK();
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
