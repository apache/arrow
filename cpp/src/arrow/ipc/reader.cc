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
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>  // IWYU pragma: export

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/reader_internal.h"
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
#include "arrow/util/endian.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/parallel.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/vector.h"
#include "arrow/visit_type_inline.h"

#include "generated/File_generated.h"  // IWYU pragma: export
#include "generated/Message_generated.h"
#include "generated/Schema_generated.h"
#include "generated/SparseTensor_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

namespace {

enum class DictionaryKind { New, Delta, Replacement };

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

/// \brief Structure to keep common arguments to be passed
struct IpcReadContext {
  IpcReadContext(DictionaryMemo* memo, const IpcReadOptions& option, bool swap,
                 MetadataVersion version = MetadataVersion::V5,
                 Compression::type kind = Compression::UNCOMPRESSED)
      : dictionary_memo(memo),
        options(option),
        metadata_version(version),
        compression(kind),
        swap_endian(swap) {}

  DictionaryMemo* dictionary_memo;

  const IpcReadOptions& options;

  MetadataVersion metadata_version;

  Compression::type compression;

  /// \brief LoadRecordBatch() or LoadRecordBatchSubset() swaps endianness of elements
  /// if this flag is true
  const bool swap_endian;
};

/// A collection of ranges to read and pointers to set to those ranges when they are
/// available.  This allows the ArrayLoader to utilize a two pass cache-then-read
/// strategy with a ReadRangeCache
class BatchDataReadRequest {
 public:
  const std::vector<io::ReadRange>& ranges_to_read() const { return ranges_to_read_; }

  void RequestRange(int64_t offset, int64_t length, std::shared_ptr<Buffer>* out) {
    ranges_to_read_.push_back({offset, length});
    destinations_.push_back(out);
  }

  void FulfillRequest(const std::vector<std::shared_ptr<Buffer>>& buffers) {
    for (std::size_t i = 0; i < buffers.size(); i++) {
      *destinations_[i] = buffers[i];
    }
  }

 private:
  std::vector<io::ReadRange> ranges_to_read_;
  std::vector<std::shared_ptr<Buffer>*> destinations_;
};

/// The field_index and buffer_index are incremented based on how much of the
/// batch is "consumed" (through nested data reconstruction, for example)
class ArrayLoader {
 public:
  explicit ArrayLoader(const flatbuf::RecordBatch* metadata,
                       MetadataVersion metadata_version, const IpcReadOptions& options,
                       io::RandomAccessFile* file)
      : metadata_(metadata),
        metadata_version_(metadata_version),
        file_(file),
        file_offset_(0),
        max_recursion_depth_(options.max_recursion_depth) {}

  explicit ArrayLoader(const flatbuf::RecordBatch* metadata,
                       MetadataVersion metadata_version, const IpcReadOptions& options,
                       int64_t file_offset)
      : metadata_(metadata),
        metadata_version_(metadata_version),
        file_(nullptr),
        file_offset_(file_offset),
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
    if (!bit_util::IsMultipleOf8(offset)) {
      return Status::Invalid("Buffer ", buffer_index_,
                             " did not start on 8-byte aligned offset: ", offset);
    }
    if (file_) {
      return file_->ReadAt(offset, length).Value(out);
    } else {
      read_request_.RequestRange(offset + file_offset_, length, out);
      return Status::OK();
    }
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

  Status LoadChildren(const std::vector<std::shared_ptr<Field>>& child_fields) {
    ArrayData* parent = out_;

    parent->child_data.resize(child_fields.size());
    for (int i = 0; i < static_cast<int>(child_fields.size()); ++i) {
      parent->child_data[i] = std::make_shared<ArrayData>();
      --max_recursion_depth_;
      RETURN_NOT_OK(Load(child_fields[i].get(), parent->child_data[i].get()));
      ++max_recursion_depth_;
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
    // out_->dictionary will be filled later in ResolveDictionaries()
    return LoadType(*type.index_type());
  }

  Status Visit(const RunLengthEncodedType& type) {
    return Status::NotImplemented("run-length encoded array in ipc");
  }

  Status Visit(const ExtensionType& type) { return LoadType(*type.storage_type()); }

  BatchDataReadRequest& read_request() { return read_request_; }

 private:
  const flatbuf::RecordBatch* metadata_;
  const MetadataVersion metadata_version_;
  io::RandomAccessFile* file_;
  int64_t file_offset_;
  int max_recursion_depth_;
  int buffer_index_ = 0;
  int field_index_ = 0;
  bool skip_io_ = false;

  BatchDataReadRequest read_request_;
  const Field* field_ = nullptr;
  ArrayData* out_ = nullptr;
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
  int64_t uncompressed_size = bit_util::FromLittleEndian(util::SafeLoadAs<int64_t>(data));

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
                         ArrayDataVector* fields) {
  struct BufferAccumulator {
    using BufferPtrVector = std::vector<std::shared_ptr<Buffer>*>;

    void AppendFrom(const ArrayDataVector& fields) {
      for (const auto& field : fields) {
        for (auto& buffer : field->buffers) {
          buffers_.push_back(&buffer);
        }
        AppendFrom(field->child_data);
      }
    }

    BufferPtrVector Get(const ArrayDataVector& fields) && {
      AppendFrom(fields);
      return std::move(buffers_);
    }

    BufferPtrVector buffers_;
  };

  // Flatten all buffers
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
    const std::vector<bool>* inclusion_mask, const IpcReadContext& context,
    io::RandomAccessFile* file) {
  ArrayLoader loader(metadata, context.metadata_version, context.options, file);

  ArrayDataVector columns(schema->num_fields());
  ArrayDataVector filtered_columns;
  FieldVector filtered_fields;
  std::shared_ptr<Schema> filtered_schema;

  for (int i = 0; i < schema->num_fields(); ++i) {
    const Field& field = *schema->field(i);
    if (!inclusion_mask || (*inclusion_mask)[i]) {
      // Read field
      auto column = std::make_shared<ArrayData>();
      RETURN_NOT_OK(loader.Load(&field, column.get()));
      if (metadata->length() != column->length) {
        return Status::IOError("Array length did not match record batch length");
      }
      columns[i] = std::move(column);
      if (inclusion_mask) {
        filtered_columns.push_back(columns[i]);
        filtered_fields.push_back(schema->field(i));
      }
    } else {
      // Skip field. This logic must be executed to advance the state of the
      // loader to the next field
      RETURN_NOT_OK(loader.SkipField(&field));
    }
  }

  // Dictionary resolution needs to happen on the unfiltered columns,
  // because fields are mapped structurally (by path in the original schema).
  RETURN_NOT_OK(ResolveDictionaries(columns, *context.dictionary_memo,
                                    context.options.memory_pool));

  if (inclusion_mask) {
    filtered_schema = ::arrow::schema(std::move(filtered_fields), schema->metadata());
    columns.clear();
  } else {
    filtered_schema = schema;
    filtered_columns = std::move(columns);
  }
  if (context.compression != Compression::UNCOMPRESSED) {
    RETURN_NOT_OK(
        DecompressBuffers(context.compression, context.options, &filtered_columns));
  }

  // swap endian in a set of ArrayData if necessary (swap_endian == true)
  if (context.swap_endian) {
    for (int i = 0; i < static_cast<int>(filtered_columns.size()); ++i) {
      ARROW_ASSIGN_OR_RAISE(filtered_columns[i],
                            arrow::internal::SwapEndianArrayData(filtered_columns[i]));
    }
  }
  return RecordBatch::Make(std::move(filtered_schema), metadata->length(),
                           std::move(filtered_columns));
}

Result<std::shared_ptr<RecordBatch>> LoadRecordBatch(
    const flatbuf::RecordBatch* metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, const IpcReadContext& context,
    io::RandomAccessFile* file) {
  if (inclusion_mask.size() > 0) {
    return LoadRecordBatchSubset(metadata, schema, &inclusion_mask, context, file);
  } else {
    return LoadRecordBatchSubset(metadata, schema, /*inclusion_mask=*/nullptr, context,
                                 file);
  }
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
      // Arrow 0.17 stored string in upper case, internal utils now require lower case
      auto name = arrow::internal::AsciiToLower(metadata->value(index));
      ARROW_ASSIGN_OR_RAISE(*out, util::Codec::GetCompressionType(name));
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

Result<RecordBatchWithMetadata> ReadRecordBatchInternal(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, IpcReadContext& context,
    io::RandomAccessFile* file) {
  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  auto batch = message->header_as_RecordBatch();
  if (batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not RecordBatch.");
  }

  Compression::type compression;
  RETURN_NOT_OK(GetCompression(batch, &compression));
  if (context.compression == Compression::UNCOMPRESSED &&
      message->version() == flatbuf::MetadataVersion::V4) {
    // Possibly obtain codec information from experimental serialization format
    // in 0.17.x
    RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
  }
  context.compression = compression;
  context.metadata_version = internal::GetMetadataVersion(message->version());

  std::shared_ptr<KeyValueMetadata> custom_metadata;
  if (message->custom_metadata() != nullptr) {
    RETURN_NOT_OK(
        internal::GetKeyValueMetadata(message->custom_metadata(), &custom_metadata));
  }
  ARROW_ASSIGN_OR_RAISE(auto record_batch,
                        LoadRecordBatch(batch, schema, inclusion_mask, context, file));
  return RecordBatchWithMetadata{record_batch, custom_metadata};
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

  *out_schema = schema(std::move(included_fields), full_schema->endianness(),
                       full_schema->metadata());
  return Status::OK();
}

Status UnpackSchemaMessage(const void* opaque_schema, const IpcReadOptions& options,
                           DictionaryMemo* dictionary_memo,
                           std::shared_ptr<Schema>* schema,
                           std::shared_ptr<Schema>* out_schema,
                           std::vector<bool>* field_inclusion_mask, bool* swap_endian) {
  RETURN_NOT_OK(internal::GetSchema(opaque_schema, dictionary_memo, schema));

  // If we are selecting only certain fields, populate the inclusion mask now
  // for fast lookups
  RETURN_NOT_OK(GetInclusionMaskAndOutSchema(*schema, options.included_fields,
                                             field_inclusion_mask, out_schema));
  *swap_endian = options.ensure_native_endian && !out_schema->get()->is_native_endian();
  if (*swap_endian) {
    // create a new schema with native endianness before swapping endian in ArrayData
    *schema = schema->get()->WithEndianness(Endianness::Native);
    *out_schema = out_schema->get()->WithEndianness(Endianness::Native);
  }
  return Status::OK();
}

Status UnpackSchemaMessage(const Message& message, const IpcReadOptions& options,
                           DictionaryMemo* dictionary_memo,
                           std::shared_ptr<Schema>* schema,
                           std::shared_ptr<Schema>* out_schema,
                           std::vector<bool>* field_inclusion_mask, bool* swap_endian) {
  CHECK_MESSAGE_TYPE(MessageType::SCHEMA, message.type());
  CHECK_HAS_NO_BODY(message);

  return UnpackSchemaMessage(message.header(), options, dictionary_memo, schema,
                             out_schema, field_inclusion_mask, swap_endian);
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options,
    io::RandomAccessFile* file) {
  std::shared_ptr<Schema> out_schema;
  // Empty means do not use
  std::vector<bool> inclusion_mask;
  IpcReadContext context(const_cast<DictionaryMemo*>(dictionary_memo), options, false);
  RETURN_NOT_OK(GetInclusionMaskAndOutSchema(schema, context.options.included_fields,
                                             &inclusion_mask, &out_schema));
  ARROW_ASSIGN_OR_RAISE(
      auto batch_and_custom_metadata,
      ReadRecordBatchInternal(metadata, schema, inclusion_mask, context, file));
  return batch_and_custom_metadata.batch;
}

Status ReadDictionary(const Buffer& metadata, const IpcReadContext& context,
                      DictionaryKind* kind, io::RandomAccessFile* file) {
  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  const auto dictionary_batch = message->header_as_DictionaryBatch();
  if (dictionary_batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not DictionaryBatch.");
  }

  // The dictionary is embedded in a record batch with a single column
  const auto batch_meta = dictionary_batch->data();

  CHECK_FLATBUFFERS_NOT_NULL(batch_meta, "DictionaryBatch.data");

  Compression::type compression;
  RETURN_NOT_OK(GetCompression(batch_meta, &compression));
  if (compression == Compression::UNCOMPRESSED &&
      message->version() == flatbuf::MetadataVersion::V4) {
    // Possibly obtain codec information from experimental serialization format
    // in 0.17.x
    RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
  }

  const int64_t id = dictionary_batch->id();

  // Look up the dictionary value type, which must have been added to the
  // DictionaryMemo already prior to invoking this function
  ARROW_ASSIGN_OR_RAISE(auto value_type, context.dictionary_memo->GetDictionaryType(id));

  // Load the dictionary data from the dictionary batch
  ArrayLoader loader(batch_meta, internal::GetMetadataVersion(message->version()),
                     context.options, file);
  auto dict_data = std::make_shared<ArrayData>();
  const Field dummy_field("", value_type);
  RETURN_NOT_OK(loader.Load(&dummy_field, dict_data.get()));

  if (compression != Compression::UNCOMPRESSED) {
    ArrayDataVector dict_fields{dict_data};
    RETURN_NOT_OK(DecompressBuffers(compression, context.options, &dict_fields));
  }

  // swap endian in dict_data if necessary (swap_endian == true)
  if (context.swap_endian) {
    ARROW_ASSIGN_OR_RAISE(dict_data, ::arrow::internal::SwapEndianArrayData(dict_data));
  }

  if (dictionary_batch->isDelta()) {
    if (kind != nullptr) {
      *kind = DictionaryKind::Delta;
    }
    return context.dictionary_memo->AddDictionaryDelta(id, dict_data);
  }
  ARROW_ASSIGN_OR_RAISE(bool inserted,
                        context.dictionary_memo->AddOrReplaceDictionary(id, dict_data));
  if (kind != nullptr) {
    *kind = inserted ? DictionaryKind::New : DictionaryKind::Replacement;
  }
  return Status::OK();
}

Status ReadDictionary(const Message& message, const IpcReadContext& context,
                      DictionaryKind* kind) {
  // Only invoke this method if we already know we have a dictionary message
  DCHECK_EQ(message.type(), MessageType::DICTIONARY_BATCH);
  CHECK_HAS_BODY(message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message.body()));
  return ReadDictionary(*message.metadata(), context, kind, reader.get());
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
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Message> message, ReadNextMessage());
    if (!message) {
      return Status::Invalid("Tried reading schema message, was null or length 0");
    }

    RETURN_NOT_OK(UnpackSchemaMessage(*message, options, &dictionary_memo_, &schema_,
                                      &out_schema_, &field_inclusion_mask_,
                                      &swap_endian_));
    return Status::OK();
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    ARROW_ASSIGN_OR_RAISE(auto batch_with_metadata, ReadNext());
    *batch = std::move(batch_with_metadata.batch);
    return Status::OK();
  }

  Result<RecordBatchWithMetadata> ReadNext() override {
    if (!have_read_initial_dictionaries_) {
      RETURN_NOT_OK(ReadInitialDictionaries());
    }

    RecordBatchWithMetadata batch_with_metadata;
    if (empty_stream_) {
      // ARROW-6006: Degenerate case where stream contains no data, we do not
      // bother trying to read a RecordBatch message from the stream
      return batch_with_metadata;
    }

    // Continue to read other dictionaries, if any
    std::unique_ptr<Message> message;
    ARROW_ASSIGN_OR_RAISE(message, ReadNextMessage());

    while (message != nullptr && message->type() == MessageType::DICTIONARY_BATCH) {
      RETURN_NOT_OK(ReadDictionary(*message));
      ARROW_ASSIGN_OR_RAISE(message, ReadNextMessage());
    }

    if (message == nullptr) {
      // End of stream
      return batch_with_metadata;
    }

    CHECK_HAS_BODY(*message);
    ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
    IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
    return ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                   context, reader.get());
  }

  std::shared_ptr<Schema> schema() const override { return out_schema_; }

  ReadStats stats() const override { return stats_; }

 private:
  Result<std::unique_ptr<Message>> ReadNextMessage() {
    ARROW_ASSIGN_OR_RAISE(auto message, message_reader_->ReadNextMessage());
    if (message) {
      ++stats_.num_messages;
      switch (message->type()) {
        case MessageType::RECORD_BATCH:
          ++stats_.num_record_batches;
          break;
        case MessageType::DICTIONARY_BATCH:
          ++stats_.num_dictionary_batches;
          break;
        default:
          break;
      }
    }
    return std::move(message);
  }

  // Read dictionary from dictionary batch
  Status ReadDictionary(const Message& message) {
    DictionaryKind kind;
    IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
    RETURN_NOT_OK(::arrow::ipc::ReadDictionary(message, context, &kind));
    switch (kind) {
      case DictionaryKind::New:
        break;
      case DictionaryKind::Delta:
        ++stats_.num_dictionary_deltas;
        break;
      case DictionaryKind::Replacement:
        ++stats_.num_replaced_dictionaries;
        break;
    }
    return Status::OK();
  }

  Status ReadInitialDictionaries() {
    // We must receive all dictionaries before reconstructing the
    // first record batch. Subsequent dictionary deltas modify the memo
    std::unique_ptr<Message> message;

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    const auto num_dicts = dictionary_memo_.fields().num_dicts();
    for (int i = 0; i < num_dicts; ++i) {
      ARROW_ASSIGN_OR_RAISE(message, ReadNextMessage());
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
                                 num_dicts, ") of dictionaries");
        }
      }

      if (message->type() != MessageType::DICTIONARY_BATCH) {
        return Status::Invalid("IPC stream did not have the expected number (", num_dicts,
                               ") of dictionaries at the start of the stream");
      }
      RETURN_NOT_OK(ReadDictionary(*message));
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

  ReadStats stats_;

  DictionaryMemo dictionary_memo_;
  std::shared_ptr<Schema> schema_, out_schema_;

  bool swap_endian_;
};

// ----------------------------------------------------------------------
// Stream reader constructors

Result<std::shared_ptr<RecordBatchStreamReader>> RecordBatchStreamReader::Open(
    std::unique_ptr<MessageReader> message_reader, const IpcReadOptions& options) {
  // Private ctor
  auto result = std::make_shared<RecordBatchStreamReaderImpl>();
  RETURN_NOT_OK(result->Open(std::move(message_reader), options));
  return result;
}

Result<std::shared_ptr<RecordBatchStreamReader>> RecordBatchStreamReader::Open(
    io::InputStream* stream, const IpcReadOptions& options) {
  return Open(MessageReader::Open(stream), options);
}

Result<std::shared_ptr<RecordBatchStreamReader>> RecordBatchStreamReader::Open(
    const std::shared_ptr<io::InputStream>& stream, const IpcReadOptions& options) {
  return Open(MessageReader::Open(stream), options);
}

// ----------------------------------------------------------------------
// Reader implementation

// Common functions used in both the random-access file reader and the
// asynchronous generator
static inline FileBlock FileBlockFromFlatbuffer(const flatbuf::Block* block) {
  return FileBlock{block->offset(), block->metaDataLength(), block->bodyLength()};
}

Status CheckAligned(const FileBlock& block) {
  if (!bit_util::IsMultipleOf8(block.offset) ||
      !bit_util::IsMultipleOf8(block.metadata_length) ||
      !bit_util::IsMultipleOf8(block.body_length)) {
    return Status::Invalid("Unaligned block in IPC file");
  }
  return Status::OK();
}

static Result<std::unique_ptr<Message>> ReadMessageFromBlock(
    const FileBlock& block, io::RandomAccessFile* file,
    const FieldsLoaderFunction& fields_loader) {
  RETURN_NOT_OK(CheckAligned(block));
  // TODO(wesm): this breaks integration tests, see ARROW-3256
  // DCHECK_EQ((*out)->body_length(), block.body_length);

  ARROW_ASSIGN_OR_RAISE(auto message, ReadMessage(block.offset, block.metadata_length,
                                                  file, fields_loader));
  return std::move(message);
}

static Future<std::shared_ptr<Message>> ReadMessageFromBlockAsync(
    const FileBlock& block, io::RandomAccessFile* file, const io::IOContext& io_context) {
  if (!bit_util::IsMultipleOf8(block.offset) ||
      !bit_util::IsMultipleOf8(block.metadata_length) ||
      !bit_util::IsMultipleOf8(block.body_length)) {
    return Status::Invalid("Unaligned block in IPC file");
  }

  // TODO(wesm): this breaks integration tests, see ARROW-3256
  // DCHECK_EQ((*out)->body_length(), block.body_length);

  return ReadMessageAsync(block.offset, block.metadata_length, block.body_length, file,
                          io_context);
}

class RecordBatchFileReaderImpl;

/// A generator of record batches.
///
/// All batches are yielded in order.
class ARROW_EXPORT WholeIpcFileRecordBatchGenerator {
 public:
  using Item = std::shared_ptr<RecordBatch>;

  explicit WholeIpcFileRecordBatchGenerator(
      std::shared_ptr<RecordBatchFileReaderImpl> state,
      std::shared_ptr<io::internal::ReadRangeCache> cached_source,
      const io::IOContext& io_context, arrow::internal::Executor* executor)
      : state_(std::move(state)),
        cached_source_(std::move(cached_source)),
        io_context_(io_context),
        executor_(executor),
        index_(0) {}

  Future<Item> operator()();
  Future<std::shared_ptr<Message>> ReadBlock(const FileBlock& block);

  static Status ReadDictionaries(
      RecordBatchFileReaderImpl* state,
      std::vector<std::shared_ptr<Message>> dictionary_messages);
  static Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
      RecordBatchFileReaderImpl* state, Message* message);

 private:
  std::shared_ptr<RecordBatchFileReaderImpl> state_;
  std::shared_ptr<io::internal::ReadRangeCache> cached_source_;
  io::IOContext io_context_;
  arrow::internal::Executor* executor_;
  int index_;
  // Odd Future type, but this lets us use All() easily
  Future<> read_dictionaries_;
};

/// A generator of record batches for use when reading
/// a subset of columns from the file.
///
/// All batches are yielded in order.
class ARROW_EXPORT SelectiveIpcFileRecordBatchGenerator {
 public:
  using Item = std::shared_ptr<RecordBatch>;

  explicit SelectiveIpcFileRecordBatchGenerator(
      std::shared_ptr<RecordBatchFileReaderImpl> state)
      : state_(std::move(state)), index_(0) {}

  Future<Item> operator()();

 private:
  std::shared_ptr<RecordBatchFileReaderImpl> state_;
  int index_;
};

class RecordBatchFileReaderImpl : public RecordBatchFileReader {
 public:
  RecordBatchFileReaderImpl() : file_(NULLPTR), footer_offset_(0), footer_(NULLPTR) {}

  int num_record_batches() const override {
    return static_cast<int>(internal::FlatBuffersVectorSize(footer_->recordBatches()));
  }

  MetadataVersion version() const override {
    return internal::GetMetadataVersion(footer_->version());
  }

  static Status LoadFieldsSubset(const flatbuf::RecordBatch* metadata,
                                 const IpcReadOptions& options,
                                 io::RandomAccessFile* file,
                                 const std::shared_ptr<Schema>& schema,
                                 const std::vector<bool>* inclusion_mask,
                                 MetadataVersion metadata_version = MetadataVersion::V5) {
    ArrayLoader loader(metadata, metadata_version, options, file);
    for (int i = 0; i < schema->num_fields(); ++i) {
      const Field& field = *schema->field(i);
      if (!inclusion_mask || (*inclusion_mask)[i]) {
        // Read field
        ArrayData column;
        RETURN_NOT_OK(loader.Load(&field, &column));
        if (metadata->length() != column.length) {
          return Status::IOError("Array length did not match record batch length");
        }
      } else {
        // Skip field. This logic must be executed to advance the state of the
        // loader to the next field
        RETURN_NOT_OK(loader.SkipField(&field));
      }
    }
    return Status::OK();
  }

  Future<std::shared_ptr<RecordBatch>> ReadRecordBatchAsync(int i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());

    auto cached_metadata = cached_metadata_.find(i);
    if (cached_metadata != cached_metadata_.end()) {
      return ReadCachedRecordBatch(i, cached_metadata->second);
    }

    return Status::Invalid(
        "Asynchronous record batch reading is only supported after a call to "
        "PreBufferMetadata or PreBufferBatches");
  }

  Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(int i) override {
    ARROW_ASSIGN_OR_RAISE(auto batch_with_metadata, ReadRecordBatchWithCustomMetadata(i));
    return batch_with_metadata.batch;
  }

  Result<RecordBatchWithMetadata> ReadRecordBatchWithCustomMetadata(int i) override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());

    auto cached_metadata = cached_metadata_.find(i);
    if (cached_metadata != cached_metadata_.end()) {
      auto result = ReadCachedRecordBatch(i, cached_metadata->second).result();
      ARROW_ASSIGN_OR_RAISE(auto batch, result);
      ARROW_ASSIGN_OR_RAISE(auto message_obj, cached_metadata->second.result());
      ARROW_ASSIGN_OR_RAISE(auto message, GetFlatbufMessage(message_obj));
      std::shared_ptr<KeyValueMetadata> custom_metadata;
      if (message->custom_metadata() != nullptr) {
        RETURN_NOT_OK(
            internal::GetKeyValueMetadata(message->custom_metadata(), &custom_metadata));
      }
      return RecordBatchWithMetadata{std::move(batch), std::move(custom_metadata)};
    }

    RETURN_NOT_OK(WaitForDictionaryReadFinished());

    FieldsLoaderFunction fields_loader = {};
    if (!field_inclusion_mask_.empty()) {
      auto& schema = schema_;
      auto& inclusion_mask = field_inclusion_mask_;
      auto& read_options = options_;
      fields_loader = [schema, inclusion_mask, read_options](const void* metadata,
                                                             io::RandomAccessFile* file) {
        return LoadFieldsSubset(static_cast<const flatbuf::RecordBatch*>(metadata),
                                read_options, file, schema, &inclusion_mask);
      };
    }
    ARROW_ASSIGN_OR_RAISE(auto message,
                          ReadMessageFromBlock(GetRecordBatchBlock(i), fields_loader));

    CHECK_HAS_BODY(*message);
    ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
    IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
    ARROW_ASSIGN_OR_RAISE(
        auto batch_with_metadata,
        ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                context, reader.get()));
    stats_.num_record_batches.fetch_add(1, std::memory_order_relaxed);
    return batch_with_metadata;
  }

  Result<int64_t> CountRows() override {
    int64_t total = 0;
    for (int i = 0; i < num_record_batches(); i++) {
      ARROW_ASSIGN_OR_RAISE(auto outer_message,
                            ReadMessageFromBlock(GetRecordBatchBlock(i)));
      auto metadata = outer_message->metadata();
      const flatbuf::Message* message = nullptr;
      RETURN_NOT_OK(
          internal::VerifyMessage(metadata->data(), metadata->size(), &message));
      auto batch = message->header_as_RecordBatch();
      if (batch == nullptr) {
        return Status::IOError(
            "Header-type of flatbuffer-encoded Message is not RecordBatch.");
      }
      total += batch->length();
    }
    return total;
  }

  Status Open(const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset,
              const IpcReadOptions& options) {
    owned_file_ = file;
    metadata_cache_ = std::make_shared<io::internal::ReadRangeCache>(
        file, file->io_context(), options.pre_buffer_cache_options);
    return Open(file.get(), footer_offset, options);
  }

  Status Open(io::RandomAccessFile* file, int64_t footer_offset,
              const IpcReadOptions& options) {
    // The metadata_cache_ may have already been constructed with an owned file in the
    // owning overload of Open
    if (!metadata_cache_) {
      metadata_cache_ = std::make_shared<io::internal::ReadRangeCache>(
          file, file->io_context(), options.pre_buffer_cache_options);
    }
    file_ = file;
    options_ = options;
    footer_offset_ = footer_offset;
    RETURN_NOT_OK(ReadFooter());

    // Get the schema and record any observed dictionaries
    RETURN_NOT_OK(UnpackSchemaMessage(footer_->schema(), options, &dictionary_memo_,
                                      &schema_, &out_schema_, &field_inclusion_mask_,
                                      &swap_endian_));
    stats_.num_messages.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }

  Future<> OpenAsync(const std::shared_ptr<io::RandomAccessFile>& file,
                     int64_t footer_offset, const IpcReadOptions& options) {
    owned_file_ = file;
    metadata_cache_ = std::make_shared<io::internal::ReadRangeCache>(
        file, file->io_context(), options.pre_buffer_cache_options);
    return OpenAsync(file.get(), footer_offset, options);
  }

  Future<> OpenAsync(io::RandomAccessFile* file, int64_t footer_offset,
                     const IpcReadOptions& options) {
    // The metadata_cache_ may have already been constructed with an owned file in the
    // owning overload of OpenAsync
    if (!metadata_cache_) {
      metadata_cache_ = std::make_shared<io::internal::ReadRangeCache>(
          file, file->io_context(), options.pre_buffer_cache_options);
    }
    file_ = file;
    options_ = options;
    footer_offset_ = footer_offset;
    auto cpu_executor = ::arrow::internal::GetCpuThreadPool();
    auto self = std::dynamic_pointer_cast<RecordBatchFileReaderImpl>(shared_from_this());
    return ReadFooterAsync(cpu_executor).Then([self, options]() -> Status {
      // Get the schema and record any observed dictionaries
      RETURN_NOT_OK(UnpackSchemaMessage(
          self->footer_->schema(), options, &self->dictionary_memo_, &self->schema_,
          &self->out_schema_, &self->field_inclusion_mask_, &self->swap_endian_));
      self->stats_.num_messages.fetch_add(1, std::memory_order_relaxed);
      return Status::OK();
    });
  }

  std::shared_ptr<Schema> schema() const override { return out_schema_; }

  std::shared_ptr<const KeyValueMetadata> metadata() const override { return metadata_; }

  ReadStats stats() const override { return stats_.poll(); }

  Result<AsyncGenerator<std::shared_ptr<RecordBatch>>> GetRecordBatchGenerator(
      const bool coalesce, const io::IOContext& io_context,
      const io::CacheOptions cache_options,
      arrow::internal::Executor* executor) override {
    auto state = std::dynamic_pointer_cast<RecordBatchFileReaderImpl>(shared_from_this());
    // Prebuffering causes us to use a lot of futures which, at the moment,
    // can only slow things down when we are doing zero-copy in-memory reads.
    //
    // Prebuffering's read patterns are also slightly worse than the alternative
    // when doing whole-file reads because the logic is not in place to recognize
    // we can just read the entire file up-front
    if (options_.included_fields.size() != 0 &&
        options_.included_fields.size() != schema_->fields().size() &&
        !file_->supports_zero_copy()) {
      RETURN_NOT_OK(state->PreBufferMetadata({}));
      return SelectiveIpcFileRecordBatchGenerator(std::move(state));
    }

    std::shared_ptr<io::internal::ReadRangeCache> cached_source;
    if (coalesce && !file_->supports_zero_copy()) {
      if (!owned_file_) return Status::Invalid("Cannot coalesce without an owned file");
      // Since the user is asking for all fields then we can cache the entire
      // file (up to the footer)
      cached_source = std::make_shared<io::internal::ReadRangeCache>(file_, io_context,
                                                                     cache_options);
      RETURN_NOT_OK(cached_source->Cache({{0, footer_offset_}}));
    }
    return WholeIpcFileRecordBatchGenerator(std::move(state), std::move(cached_source),
                                            io_context, executor);
  }

  Status DoPreBufferMetadata(const std::vector<int>& indices) {
    RETURN_NOT_OK(CacheMetadata(indices));
    EnsureDictionaryReadStarted();
    Future<> all_metadata_ready = WaitForMetadatas(indices);
    for (int index : indices) {
      Future<std::shared_ptr<Message>> metadata_loaded =
          all_metadata_ready.Then([this, index]() -> Result<std::shared_ptr<Message>> {
            stats_.num_messages.fetch_add(1, std::memory_order_relaxed);
            FileBlock block = GetRecordBatchBlock(index);
            ARROW_ASSIGN_OR_RAISE(
                std::shared_ptr<Buffer> metadata,
                metadata_cache_->Read({block.offset, block.metadata_length}));
            return ReadMessage(std::move(metadata), nullptr);
          });
      cached_metadata_.emplace(index, metadata_loaded);
    }
    return Status::OK();
  }

  std::vector<int> AllIndices() const {
    std::vector<int> all_indices(num_record_batches());
    std::iota(all_indices.begin(), all_indices.end(), 0);
    return all_indices;
  }

  Status PreBufferMetadata(const std::vector<int>& indices) override {
    if (indices.size() == 0) {
      return DoPreBufferMetadata(AllIndices());
    } else {
      return DoPreBufferMetadata(indices);
    }
  }

 private:
  friend class WholeIpcFileRecordBatchGenerator;

  struct AtomicReadStats {
    std::atomic<int64_t> num_messages{0};
    std::atomic<int64_t> num_record_batches{0};
    std::atomic<int64_t> num_dictionary_batches{0};
    std::atomic<int64_t> num_dictionary_deltas{0};
    std::atomic<int64_t> num_replaced_dictionaries{0};

    /// \brief Capture a copy of the current counters
    ReadStats poll() const {
      ReadStats stats;
      stats.num_messages = num_messages.load(std::memory_order_relaxed);
      stats.num_record_batches = num_record_batches.load(std::memory_order_relaxed);
      stats.num_dictionary_batches =
          num_dictionary_batches.load(std::memory_order_relaxed);
      stats.num_dictionary_deltas = num_dictionary_deltas.load(std::memory_order_relaxed);
      stats.num_replaced_dictionaries =
          num_replaced_dictionaries.load(std::memory_order_relaxed);
      return stats;
    }
  };

  FileBlock GetRecordBatchBlock(int i) const {
    return FileBlockFromFlatbuffer(footer_->recordBatches()->Get(i));
  }

  FileBlock GetDictionaryBlock(int i) const {
    return FileBlockFromFlatbuffer(footer_->dictionaries()->Get(i));
  }

  Result<std::unique_ptr<Message>> ReadMessageFromBlock(
      const FileBlock& block, const FieldsLoaderFunction& fields_loader = {}) {
    ARROW_ASSIGN_OR_RAISE(auto message,
                          arrow::ipc::ReadMessageFromBlock(block, file_, fields_loader));
    stats_.num_messages.fetch_add(1, std::memory_order_relaxed);
    return std::move(message);
  }

  Status ReadDictionaries() {
    // Read all the dictionaries
    IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
    for (int i = 0; i < num_dictionaries(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto message, ReadMessageFromBlock(GetDictionaryBlock(i)));
      RETURN_NOT_OK(ReadOneDictionary(message.get(), context));
      stats_.num_dictionary_batches.fetch_add(1, std::memory_order_relaxed);
    }
    return Status::OK();
  }

  Status ReadOneDictionary(Message* message, const IpcReadContext& context) {
    CHECK_HAS_BODY(*message);
    ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
    DictionaryKind kind;
    RETURN_NOT_OK(ReadDictionary(*message->metadata(), context, &kind, reader.get()));
    if (kind == DictionaryKind::Replacement) {
      return Status::Invalid("Unsupported dictionary replacement in IPC file");
    } else if (kind == DictionaryKind::Delta) {
      stats_.num_dictionary_deltas.fetch_add(1, std::memory_order_relaxed);
    }
    return Status::OK();
  }

  void AddDictionaryRanges(std::vector<io::ReadRange>* ranges) const {
    // Adds all dictionaries to the range cache
    for (int i = 0; i < num_dictionaries(); ++i) {
      FileBlock block = GetDictionaryBlock(i);
      ranges->push_back({block.offset, block.metadata_length + block.body_length});
    }
  }

  void AddMetadataRanges(const std::vector<int>& indices,
                         std::vector<io::ReadRange>* ranges) {
    for (int index : indices) {
      FileBlock block = GetRecordBatchBlock(static_cast<int>(index));
      ranges->push_back({block.offset, block.metadata_length});
    }
  }

  Status CacheMetadata(const std::vector<int>& indices) {
    std::vector<io::ReadRange> ranges;
    if (!read_dictionaries_) {
      AddDictionaryRanges(&ranges);
    }
    AddMetadataRanges(indices, &ranges);
    return metadata_cache_->Cache(std::move(ranges));
  }

  void EnsureDictionaryReadStarted() {
    if (!dictionary_load_finished_.is_valid()) {
      read_dictionaries_ = true;
      std::vector<io::ReadRange> ranges;
      AddDictionaryRanges(&ranges);
      dictionary_load_finished_ =
          metadata_cache_->WaitFor(std::move(ranges)).Then([this] {
            return ReadDictionaries();
          });
    }
  }

  Status WaitForDictionaryReadFinished() {
    if (!read_dictionaries_) {
      RETURN_NOT_OK(ReadDictionaries());
      read_dictionaries_ = true;
      return Status::OK();
    }
    if (dictionary_load_finished_.is_valid()) {
      return dictionary_load_finished_.status();
    }
    // Dictionaries were previously loaded synchronously
    return Status::OK();
  }

  Future<> WaitForMetadatas(const std::vector<int>& indices) {
    std::vector<io::ReadRange> ranges;
    AddMetadataRanges(indices, &ranges);
    return metadata_cache_->WaitFor(std::move(ranges));
  }

  Result<IpcReadContext> GetIpcReadContext(const flatbuf::Message* message,
                                           const flatbuf::RecordBatch* batch) {
    IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
    Compression::type compression;
    RETURN_NOT_OK(GetCompression(batch, &compression));
    if (context.compression == Compression::UNCOMPRESSED &&
        message->version() == flatbuf::MetadataVersion::V4) {
      // Possibly obtain codec information from experimental serialization format
      // in 0.17.x
      RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
    }
    context.compression = compression;
    context.metadata_version = internal::GetMetadataVersion(message->version());
    return std::move(context);
  }

  Result<const flatbuf::RecordBatch*> GetBatchFromMessage(
      const flatbuf::Message* message) {
    auto batch = message->header_as_RecordBatch();
    if (batch == nullptr) {
      return Status::IOError(
          "Header-type of flatbuffer-encoded Message is not RecordBatch.");
    }
    return batch;
  }

  Result<const flatbuf::Message*> GetFlatbufMessage(
      const std::shared_ptr<Message>& message) {
    const Buffer& metadata = *message->metadata();
    const flatbuf::Message* flatbuf_message = nullptr;
    RETURN_NOT_OK(
        internal::VerifyMessage(metadata.data(), metadata.size(), &flatbuf_message));
    return flatbuf_message;
  }

  struct CachedRecordBatchReadContext {
    CachedRecordBatchReadContext(std::shared_ptr<Schema> sch,
                                 const flatbuf::RecordBatch* batch,
                                 IpcReadContext context, io::RandomAccessFile* file,
                                 std::shared_ptr<io::RandomAccessFile> owned_file,
                                 int64_t block_data_offset)
        : schema(std::move(sch)),
          context(std::move(context)),
          file(file),
          owned_file(std::move(owned_file)),
          loader(batch, context.metadata_version, context.options, block_data_offset),
          columns(schema->num_fields()),
          cache(file, file->io_context(), io::CacheOptions::LazyDefaults()),
          length(batch->length()) {}

    Status CalculateLoadRequest() {
      std::shared_ptr<Schema> out_schema;
      RETURN_NOT_OK(GetInclusionMaskAndOutSchema(schema, context.options.included_fields,
                                                 &inclusion_mask, &out_schema));

      for (int i = 0; i < schema->num_fields(); ++i) {
        const Field& field = *schema->field(i);
        if (inclusion_mask.size() == 0 || inclusion_mask[i]) {
          // Read field
          auto column = std::make_shared<ArrayData>();
          RETURN_NOT_OK(loader.Load(&field, column.get()));
          if (length != column->length) {
            return Status::IOError("Array length did not match record batch length");
          }
          columns[i] = std::move(column);
          if (inclusion_mask.size() > 0) {
            filtered_columns.push_back(columns[i]);
            filtered_fields.push_back(schema->field(i));
          }
        } else {
          // Skip field. This logic must be executed to advance the state of the
          // loader to the next field
          RETURN_NOT_OK(loader.SkipField(&field));
        }
      }
      if (inclusion_mask.size() > 0) {
        filtered_schema = ::arrow::schema(std::move(filtered_fields), schema->metadata());
      } else {
        filtered_schema = schema;
      }
      return Status::OK();
    }

    Future<> ReadAsync() {
      RETURN_NOT_OK(cache.Cache(loader.read_request().ranges_to_read()));
      return cache.WaitFor(loader.read_request().ranges_to_read());
    }

    Result<std::shared_ptr<RecordBatch>> CreateRecordBatch() {
      std::vector<std::shared_ptr<Buffer>> buffers;
      for (const auto& range_to_read : loader.read_request().ranges_to_read()) {
        ARROW_ASSIGN_OR_RAISE(auto buffer, cache.Read(range_to_read));
        buffers.push_back(std::move(buffer));
      }
      loader.read_request().FulfillRequest(buffers);

      // Dictionary resolution needs to happen on the unfiltered columns,
      // because fields are mapped structurally (by path in the original schema).
      RETURN_NOT_OK(ResolveDictionaries(columns, *context.dictionary_memo,
                                        context.options.memory_pool));
      if (inclusion_mask.size() > 0) {
        columns.clear();
      } else {
        filtered_columns = std::move(columns);
      }

      if (context.compression != Compression::UNCOMPRESSED) {
        RETURN_NOT_OK(
            DecompressBuffers(context.compression, context.options, &filtered_columns));
      }

      // swap endian in a set of ArrayData if necessary (swap_endian == true)
      if (context.swap_endian) {
        for (int i = 0; i < static_cast<int>(filtered_columns.size()); ++i) {
          ARROW_ASSIGN_OR_RAISE(filtered_columns[i], arrow::internal::SwapEndianArrayData(
                                                         filtered_columns[i]));
        }
      }
      return RecordBatch::Make(std::move(filtered_schema), length,
                               std::move(filtered_columns));
    }

    std::shared_ptr<Schema> schema;
    IpcReadContext context;
    io::RandomAccessFile* file;
    std::shared_ptr<io::RandomAccessFile> owned_file;

    ArrayLoader loader;
    ArrayDataVector columns;
    io::internal::ReadRangeCache cache;
    int64_t length;
    ArrayDataVector filtered_columns;
    FieldVector filtered_fields;
    std::shared_ptr<Schema> filtered_schema;
    std::vector<bool> inclusion_mask;
  };

  Future<std::shared_ptr<RecordBatch>> ReadCachedRecordBatch(
      int index, Future<std::shared_ptr<Message>> message_fut) {
    stats_.num_record_batches.fetch_add(1, std::memory_order_relaxed);
    return dictionary_load_finished_.Then([message_fut] { return message_fut; })
        .Then([this, index](const std::shared_ptr<Message>& message_obj)
                  -> Future<std::shared_ptr<RecordBatch>> {
          FileBlock block = GetRecordBatchBlock(index);
          ARROW_ASSIGN_OR_RAISE(auto message, GetFlatbufMessage(message_obj));
          ARROW_ASSIGN_OR_RAISE(auto batch, GetBatchFromMessage(message));
          ARROW_ASSIGN_OR_RAISE(auto context, GetIpcReadContext(message, batch));

          auto read_context = std::make_shared<CachedRecordBatchReadContext>(
              schema_, batch, std::move(context), file_, owned_file_,
              block.offset + static_cast<int64_t>(block.metadata_length));
          RETURN_NOT_OK(read_context->CalculateLoadRequest());
          return read_context->ReadAsync().Then(
              [read_context] { return read_context->CreateRecordBatch(); });
        });
  }

  Status ReadFooter() {
    auto fut = ReadFooterAsync(/*executor=*/nullptr);
    return fut.status();
  }

  Future<> ReadFooterAsync(arrow::internal::Executor* executor) {
    const int32_t magic_size = static_cast<int>(strlen(kArrowMagicBytes));

    if (footer_offset_ <= magic_size * 2 + 4) {
      return Status::Invalid("File is too small: ", footer_offset_);
    }

    int file_end_size = static_cast<int>(magic_size + sizeof(int32_t));
    auto self = std::dynamic_pointer_cast<RecordBatchFileReaderImpl>(shared_from_this());
    auto read_magic = file_->ReadAsync(footer_offset_ - file_end_size, file_end_size);
    if (executor) read_magic = executor->Transfer(std::move(read_magic));
    return read_magic
        .Then([=](const std::shared_ptr<Buffer>& buffer)
                  -> Future<std::shared_ptr<Buffer>> {
          const int64_t expected_footer_size = magic_size + sizeof(int32_t);
          if (buffer->size() < expected_footer_size) {
            return Status::Invalid("Unable to read ", expected_footer_size,
                                   "from end of file");
          }

          if (memcmp(buffer->data() + sizeof(int32_t), kArrowMagicBytes, magic_size)) {
            return Status::Invalid("Not an Arrow file");
          }

          int32_t footer_length = bit_util::FromLittleEndian(
              *reinterpret_cast<const int32_t*>(buffer->data()));

          if (footer_length <= 0 ||
              footer_length > self->footer_offset_ - magic_size * 2 - 4) {
            return Status::Invalid("File is smaller than indicated metadata size");
          }

          // Now read the footer
          auto read_footer = self->file_->ReadAsync(
              self->footer_offset_ - footer_length - file_end_size, footer_length);
          if (executor) read_footer = executor->Transfer(std::move(read_footer));
          return read_footer;
        })
        .Then([=](const std::shared_ptr<Buffer>& buffer) -> Status {
          self->footer_buffer_ = buffer;
          const auto data = self->footer_buffer_->data();
          const auto size = self->footer_buffer_->size();
          if (!internal::VerifyFlatbuffers<flatbuf::Footer>(data, size)) {
            return Status::IOError("Verification of flatbuffer-encoded Footer failed.");
          }
          self->footer_ = flatbuf::GetFooter(data);

          auto fb_metadata = self->footer_->custom_metadata();
          if (fb_metadata != nullptr) {
            std::shared_ptr<KeyValueMetadata> md;
            RETURN_NOT_OK(internal::GetKeyValueMetadata(fb_metadata, &md));
            self->metadata_ = std::move(md);  // const-ify
          }
          return Status::OK();
        });
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

  AtomicReadStats stats_;
  std::shared_ptr<io::internal::ReadRangeCache> metadata_cache_;
  std::unordered_set<int> cached_data_blocks_;
  Future<> dictionary_load_finished_;
  std::unordered_map<int, Future<std::shared_ptr<Message>>> cached_metadata_;
  std::unordered_map<int, Future<>> cached_data_requests_;

  bool swap_endian_;
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

Future<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::OpenAsync(
    const std::shared_ptr<io::RandomAccessFile>& file, const IpcReadOptions& options) {
  ARROW_ASSIGN_OR_RAISE(int64_t footer_offset, file->GetSize());
  return OpenAsync(std::move(file), footer_offset, options);
}

Future<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::OpenAsync(
    io::RandomAccessFile* file, const IpcReadOptions& options) {
  ARROW_ASSIGN_OR_RAISE(int64_t footer_offset, file->GetSize());
  return OpenAsync(file, footer_offset, options);
}

Future<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::OpenAsync(
    const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset,
    const IpcReadOptions& options) {
  auto result = std::make_shared<RecordBatchFileReaderImpl>();
  return result->OpenAsync(file, footer_offset, options)
      .Then([=]() -> Result<std::shared_ptr<RecordBatchFileReader>> { return result; });
}

Future<std::shared_ptr<RecordBatchFileReader>> RecordBatchFileReader::OpenAsync(
    io::RandomAccessFile* file, int64_t footer_offset, const IpcReadOptions& options) {
  auto result = std::make_shared<RecordBatchFileReaderImpl>();
  return result->OpenAsync(file, footer_offset, options)
      .Then([=]() -> Result<std::shared_ptr<RecordBatchFileReader>> { return result; });
}

Future<SelectiveIpcFileRecordBatchGenerator::Item>
SelectiveIpcFileRecordBatchGenerator::operator()() {
  int index = index_++;
  if (index >= state_->num_record_batches()) {
    return IterationEnd<SelectiveIpcFileRecordBatchGenerator::Item>();
  }
  return state_->ReadRecordBatchAsync(index);
}

Future<WholeIpcFileRecordBatchGenerator::Item>
WholeIpcFileRecordBatchGenerator::operator()() {
  auto state = state_;
  if (!read_dictionaries_.is_valid()) {
    std::vector<Future<std::shared_ptr<Message>>> messages(state->num_dictionaries());
    for (int i = 0; i < state->num_dictionaries(); i++) {
      auto block = FileBlockFromFlatbuffer(state->footer_->dictionaries()->Get(i));
      messages[i] = ReadBlock(block);
    }
    auto read_messages = All(std::move(messages));
    if (executor_) read_messages = executor_->Transfer(read_messages);
    read_dictionaries_ = read_messages.Then(
        [=](const std::vector<Result<std::shared_ptr<Message>>>& maybe_messages)
            -> Status {
          ARROW_ASSIGN_OR_RAISE(auto messages,
                                arrow::internal::UnwrapOrRaise(maybe_messages));
          return ReadDictionaries(state.get(), std::move(messages));
        });
  }
  if (index_ >= state_->num_record_batches()) {
    return Future<Item>::MakeFinished(IterationTraits<Item>::End());
  }
  auto block = FileBlockFromFlatbuffer(state->footer_->recordBatches()->Get(index_++));
  auto read_message = ReadBlock(block);
  auto read_messages = read_dictionaries_.Then([read_message]() { return read_message; });
  // Force transfer. This may be wasteful in some cases, but ensures we get off the
  // I/O threads as soon as possible, and ensures we don't decode record batches
  // synchronously in the case that the message read has already finished.
  if (executor_) {
    auto executor = executor_;
    return read_messages.Then(
        [=](const std::shared_ptr<Message>& message) -> Future<Item> {
          return DeferNotOk(executor->Submit(
              [=]() { return ReadRecordBatch(state.get(), message.get()); }));
        });
  }
  return read_messages.Then([=](const std::shared_ptr<Message>& message) -> Result<Item> {
    return ReadRecordBatch(state.get(), message.get());
  });
}

Future<std::shared_ptr<Message>> WholeIpcFileRecordBatchGenerator::ReadBlock(
    const FileBlock& block) {
  if (cached_source_) {
    auto cached_source = cached_source_;
    io::ReadRange range{block.offset, block.metadata_length + block.body_length};
    auto pool = state_->options_.memory_pool;
    return cached_source->WaitFor({range}).Then(
        [cached_source, pool, range]() -> Result<std::shared_ptr<Message>> {
          ARROW_ASSIGN_OR_RAISE(auto buffer, cached_source->Read(range));
          io::BufferReader stream(std::move(buffer));
          return ReadMessage(&stream, pool);
        });
  } else {
    return ReadMessageFromBlockAsync(block, state_->file_, io_context_);
  }
}

Status WholeIpcFileRecordBatchGenerator::ReadDictionaries(
    RecordBatchFileReaderImpl* state,
    std::vector<std::shared_ptr<Message>> dictionary_messages) {
  IpcReadContext context(&state->dictionary_memo_, state->options_, state->swap_endian_);
  for (const auto& message : dictionary_messages) {
    RETURN_NOT_OK(state->ReadOneDictionary(message.get(), context));
  }
  return Status::OK();
}

Result<std::shared_ptr<RecordBatch>> WholeIpcFileRecordBatchGenerator::ReadRecordBatch(
    RecordBatchFileReaderImpl* state, Message* message) {
  CHECK_HAS_BODY(*message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
  IpcReadContext context(&state->dictionary_memo_, state->options_, state->swap_endian_);
  ARROW_ASSIGN_OR_RAISE(
      auto batch_with_metadata,
      ReadRecordBatchInternal(*message->metadata(), state->schema_,
                              state->field_inclusion_mask_, context, reader.get()));
  return batch_with_metadata.batch;
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
  explicit StreamDecoderImpl(std::shared_ptr<Listener> listener, IpcReadOptions options)
      : listener_(std::move(listener)),
        options_(std::move(options)),
        state_(State::SCHEMA),
        message_decoder_(std::shared_ptr<StreamDecoderImpl>(this, [](void*) {}),
                         options_.memory_pool),
        n_required_dictionaries_(0) {}

  Status OnMessageDecoded(std::unique_ptr<Message> message) override {
    ++stats_.num_messages;
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

  ReadStats stats() const { return stats_; }

 private:
  Status OnSchemaMessageDecoded(std::unique_ptr<Message> message) {
    RETURN_NOT_OK(UnpackSchemaMessage(*message, options_, &dictionary_memo_, &schema_,
                                      &out_schema_, &field_inclusion_mask_,
                                      &swap_endian_));

    n_required_dictionaries_ = dictionary_memo_.fields().num_fields();
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
                             dictionary_memo_.fields().num_fields(),
                             ") of dictionaries at the start of the stream");
    }
    RETURN_NOT_OK(ReadDictionary(*message));
    n_required_dictionaries_--;
    if (n_required_dictionaries_ == 0) {
      state_ = State::RECORD_BATCHES;
      ARROW_RETURN_NOT_OK(listener_->OnSchemaDecoded(schema_));
    }
    return Status::OK();
  }

  Status OnRecordBatchMessageDecoded(std::unique_ptr<Message> message) {
    if (message->type() == MessageType::DICTIONARY_BATCH) {
      return ReadDictionary(*message);
    } else {
      CHECK_HAS_BODY(*message);
      ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
      IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
      ARROW_ASSIGN_OR_RAISE(
          auto batch_with_metadata,
          ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                  context, reader.get()));
      ++stats_.num_record_batches;
      return listener_->OnRecordBatchDecoded(std::move(batch_with_metadata.batch));
    }
  }

  // Read dictionary from dictionary batch
  Status ReadDictionary(const Message& message) {
    DictionaryKind kind;
    IpcReadContext context(&dictionary_memo_, options_, swap_endian_);
    RETURN_NOT_OK(::arrow::ipc::ReadDictionary(message, context, &kind));
    ++stats_.num_dictionary_batches;
    switch (kind) {
      case DictionaryKind::New:
        break;
      case DictionaryKind::Delta:
        ++stats_.num_dictionary_deltas;
        break;
      case DictionaryKind::Replacement:
        ++stats_.num_replaced_dictionaries;
        break;
    }
    return Status::OK();
  }

  std::shared_ptr<Listener> listener_;
  const IpcReadOptions options_;
  State state_;
  MessageDecoder message_decoder_;
  std::vector<bool> field_inclusion_mask_;
  int n_required_dictionaries_;
  DictionaryMemo dictionary_memo_;
  std::shared_ptr<Schema> schema_, out_schema_;
  ReadStats stats_;
  bool swap_endian_;
};

StreamDecoder::StreamDecoder(std::shared_ptr<Listener> listener, IpcReadOptions options) {
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

ReadStats StreamDecoder::stats() const { return impl_->stats(); }

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
  const int64_t indices_elsize = indices_type->byte_width();

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
  const int indptr_byte_width = indptr_type->byte_width();

  auto* indptr_buffer = sparse_index->indptrBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indptr_data,
                        file->ReadAt(indptr_buffer->offset(), indptr_buffer->length()));

  auto* indices_buffer = sparse_index->indicesBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indices_data,
                        file->ReadAt(indices_buffer->offset(), indices_buffer->length()));

  std::vector<int64_t> indices_shape({non_zero_length});
  const auto indices_minimum_bytes = indices_shape[0] * indices_type->byte_width();
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
  if (!bit_util::IsMultipleOf8(buffer->offset())) {
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
  SparseTensorFormat::type format_id{};
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
namespace {

Status ValidateFuzzBatch(const RecordBatch& batch) {
  auto st = batch.ValidateFull();
  if (st.ok()) {
    // If the batch is valid, printing should succeed
    batch.ToString();
  }
  return st;
}

}  // namespace

Status FuzzIpcStream(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<RecordBatchReader> batch_reader;
  ARROW_ASSIGN_OR_RAISE(batch_reader, RecordBatchStreamReader::Open(&buffer_reader));
  Status st;

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_NOT_OK(batch_reader->ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    st &= ValidateFuzzBatch(*batch);
  }

  return st;
}

Status FuzzIpcFile(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<RecordBatchFileReader> batch_reader;
  ARROW_ASSIGN_OR_RAISE(batch_reader, RecordBatchFileReader::Open(&buffer_reader));
  Status st;

  const int n_batches = batch_reader->num_record_batches();
  for (int i = 0; i < n_batches; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->ReadRecordBatch(i));
    st &= ValidateFuzzBatch(*batch);
  }

  return st;
}

Status FuzzIpcTensorStream(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<Tensor> tensor;

  while (true) {
    ARROW_ASSIGN_OR_RAISE(tensor, ReadTensor(&buffer_reader));
    if (tensor == nullptr) {
      break;
    }
    RETURN_NOT_OK(tensor->Validate());
  }

  return Status::OK();
}

Result<int64_t> IoRecordedRandomAccessFile::GetSize() { return file_size_; }

Result<int64_t> IoRecordedRandomAccessFile::ReadAt(int64_t position, int64_t nbytes,
                                                   void* out) {
  auto num_bytes_read = std::min(file_size_, position + nbytes) - position;

  if (!read_ranges_.empty() &&
      position == read_ranges_.back().offset + read_ranges_.back().length) {
    // merge continuous IOs into one if possible
    read_ranges_.back().length += num_bytes_read;
  } else {
    // no real IO is performed, it is only saved into a vector for replaying later
    read_ranges_.emplace_back(io::ReadRange{position, num_bytes_read});
  }
  return num_bytes_read;
}

Result<std::shared_ptr<Buffer>> IoRecordedRandomAccessFile::ReadAt(int64_t position,
                                                                   int64_t nbytes) {
  std::shared_ptr<Buffer> out;
  auto result = ReadAt(position, nbytes, &out);
  return out;
}

Status IoRecordedRandomAccessFile::Close() {
  closed_ = true;
  return Status::OK();
}

Status IoRecordedRandomAccessFile::Abort() { return Status::OK(); }

Result<int64_t> IoRecordedRandomAccessFile::Tell() const { return position_; }

bool IoRecordedRandomAccessFile::closed() const { return closed_; }

Status IoRecordedRandomAccessFile::Seek(int64_t position) {
  position_ = position;
  return Status::OK();
}

Result<int64_t> IoRecordedRandomAccessFile::Read(int64_t nbytes, void* out) {
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(position_, nbytes, out));
  position_ += bytes_read;
  return bytes_read;
}

Result<std::shared_ptr<Buffer>> IoRecordedRandomAccessFile::Read(int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, ReadAt(position_, nbytes));
  auto num_bytes_read = std::min(file_size_, position_ + nbytes) - position_;
  position_ += num_bytes_read;
  return std::move(buffer);
}

const io::IOContext& IoRecordedRandomAccessFile::io_context() const {
  return io_context_;
}

const std::vector<io::ReadRange>& IoRecordedRandomAccessFile::GetReadRanges() const {
  return read_ranges_;
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
