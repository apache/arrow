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

#include "arrow/ipc/writer.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/extension_type.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/util.h"
#include "arrow/record_batch.h"
#include "arrow/result_internal.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/table.h"
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
#include "arrow/visit_array_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::CopyBitmap;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

namespace {

bool HasNestedDict(const ArrayData& data) {
  if (data.type->id() == Type::DICTIONARY) {
    return true;
  }
  for (const auto& child : data.child_data) {
    if (HasNestedDict(*child)) {
      return true;
    }
  }
  return false;
}

Status GetTruncatedBitmap(int64_t offset, int64_t length,
                          const std::shared_ptr<Buffer> input, MemoryPool* pool,
                          std::shared_ptr<Buffer>* buffer) {
  if (!input) {
    *buffer = input;
    return Status::OK();
  }
  int64_t min_length = PaddedLength(bit_util::BytesForBits(length));
  if (offset != 0 || min_length < input->size()) {
    // With a sliced array / non-zero offset, we must copy the bitmap
    ARROW_ASSIGN_OR_RAISE(*buffer, CopyBitmap(pool, input->data(), offset, length));
  } else {
    *buffer = input;
  }
  return Status::OK();
}

Status GetTruncatedBuffer(int64_t offset, int64_t length, int32_t byte_width,
                          const std::shared_ptr<Buffer> input, MemoryPool* pool,
                          std::shared_ptr<Buffer>* buffer) {
  if (!input) {
    *buffer = input;
    return Status::OK();
  }
  int64_t padded_length = PaddedLength(length * byte_width);
  if (offset != 0 || padded_length < input->size()) {
    *buffer =
        SliceBuffer(input, offset * byte_width, std::min(padded_length, input->size()));
  } else {
    *buffer = input;
  }
  return Status::OK();
}

static inline bool NeedTruncate(int64_t offset, const Buffer* buffer,
                                int64_t min_length) {
  // buffer can be NULL
  if (buffer == nullptr) {
    return false;
  }
  return offset != 0 || min_length < buffer->size();
}

class RecordBatchSerializer {
 public:
  RecordBatchSerializer(int64_t buffer_start_offset,
                        const std::shared_ptr<const KeyValueMetadata>& custom_metadata,
                        const IpcWriteOptions& options, IpcPayload* out)
      : out_(out),
        custom_metadata_(custom_metadata),
        options_(options),
        max_recursion_depth_(options.max_recursion_depth),
        buffer_start_offset_(buffer_start_offset) {
    DCHECK_GT(max_recursion_depth_, 0);
  }

  virtual ~RecordBatchSerializer() = default;

  Status VisitArray(const Array& arr) {
    static std::shared_ptr<Buffer> kNullBuffer = std::make_shared<Buffer>(nullptr, 0);

    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    if (!options_.allow_64bit && arr.length() > std::numeric_limits<int32_t>::max()) {
      return Status::CapacityError("Cannot write arrays larger than 2^31 - 1 in length");
    }

    // push back all common elements
    field_nodes_.push_back({arr.length(), arr.null_count(), 0});

    // In V4, null types have no validity bitmap
    // In V5 and later, null and union types have no validity bitmap
    if (internal::HasValidityBitmap(arr.type_id(), options_.metadata_version)) {
      if (arr.null_count() > 0) {
        std::shared_ptr<Buffer> bitmap;
        RETURN_NOT_OK(GetTruncatedBitmap(arr.offset(), arr.length(), arr.null_bitmap(),
                                         options_.memory_pool, &bitmap));
        out_->body_buffers.emplace_back(bitmap);
      } else {
        // Push a dummy zero-length buffer, not to be copied
        out_->body_buffers.emplace_back(kNullBuffer);
      }
    }
    return VisitType(arr);
  }

  // Override this for writing dictionary metadata
  virtual Status SerializeMetadata(int64_t num_rows) {
    return WriteRecordBatchMessage(num_rows, out_->body_length, custom_metadata_,
                                   field_nodes_, buffer_meta_, options_, &out_->metadata);
  }

  Status CompressBuffer(const Buffer& buffer, util::Codec* codec,
                        std::shared_ptr<Buffer>* out) {
    // Convert buffer to uncompressed-length-prefixed compressed buffer
    int64_t maximum_length = codec->MaxCompressedLen(buffer.size(), buffer.data());
    ARROW_ASSIGN_OR_RAISE(auto result, AllocateBuffer(maximum_length + sizeof(int64_t)));

    int64_t actual_length;
    ARROW_ASSIGN_OR_RAISE(actual_length,
                          codec->Compress(buffer.size(), buffer.data(), maximum_length,
                                          result->mutable_data() + sizeof(int64_t)));
    *reinterpret_cast<int64_t*>(result->mutable_data()) =
        bit_util::ToLittleEndian(buffer.size());
    *out = SliceBuffer(std::move(result), /*offset=*/0, actual_length + sizeof(int64_t));
    return Status::OK();
  }

  Status CompressBodyBuffers() {
    RETURN_NOT_OK(
        internal::CheckCompressionSupported(options_.codec->compression_type()));

    auto CompressOne = [&](size_t i) {
      if (out_->body_buffers[i]->size() > 0) {
        RETURN_NOT_OK(CompressBuffer(*out_->body_buffers[i], options_.codec.get(),
                                     &out_->body_buffers[i]));
      }
      return Status::OK();
    };

    return ::arrow::internal::OptionalParallelFor(
        options_.use_threads, static_cast<int>(out_->body_buffers.size()), CompressOne);
  }

  Status Assemble(const RecordBatch& batch) {
    if (field_nodes_.size() > 0) {
      field_nodes_.clear();
      buffer_meta_.clear();
      out_->body_buffers.clear();
    }

    // Perform depth-first traversal of the row-batch
    for (int i = 0; i < batch.num_columns(); ++i) {
      RETURN_NOT_OK(VisitArray(*batch.column(i)));
    }

    // calculate initial body length using all buffer sizes
    int64_t raw_size = 0;
    for (const auto& buf : out_->body_buffers) {
      if (buf) {
        raw_size += buf->size();
      }
    }
    out_->raw_body_length = raw_size;

    if (options_.codec != nullptr) {
      RETURN_NOT_OK(CompressBodyBuffers());
    }

    // The position for the start of a buffer relative to the passed frame of
    // reference. May be 0 or some other position in an address space
    int64_t offset = buffer_start_offset_;

    buffer_meta_.reserve(out_->body_buffers.size());

    // Construct the buffer metadata for the record batch header
    for (const auto& buffer : out_->body_buffers) {
      int64_t size = 0;
      int64_t padding = 0;

      // The buffer might be null if we are handling zero row lengths.
      if (buffer) {
        size = buffer->size();
        padding = bit_util::RoundUpToMultipleOf8(size) - size;
      }

      buffer_meta_.push_back({offset, size});
      offset += size + padding;
    }

    out_->body_length = offset - buffer_start_offset_;
    DCHECK(bit_util::IsMultipleOf8(out_->body_length));

    // Now that we have computed the locations of all of the buffers in shared
    // memory, the data header can be converted to a flatbuffer and written out
    //
    // Note: The memory written here is prefixed by the size of the flatbuffer
    // itself as an int32_t.
    return SerializeMetadata(batch.num_rows());
  }

  template <typename ArrayType>
  Status GetZeroBasedValueOffsets(const ArrayType& array,
                                  std::shared_ptr<Buffer>* value_offsets) {
    // Share slicing logic between ListArray, BinaryArray and LargeBinaryArray
    using offset_type = typename ArrayType::offset_type;

    auto offsets = array.value_offsets();

    int64_t required_bytes = sizeof(offset_type) * (array.length() + 1);
    if (array.offset() != 0) {
      // If we have a non-zero offset, then the value offsets do not start at
      // zero. We must a) create a new offsets array with shifted offsets and
      // b) slice the values array accordingly

      ARROW_ASSIGN_OR_RAISE(auto shifted_offsets,
                            AllocateBuffer(required_bytes, options_.memory_pool));

      offset_type* dest_offsets =
          reinterpret_cast<offset_type*>(shifted_offsets->mutable_data());
      const offset_type start_offset = array.value_offset(0);

      for (int i = 0; i < array.length(); ++i) {
        dest_offsets[i] = array.value_offset(i) - start_offset;
      }
      // Final offset
      dest_offsets[array.length()] = array.value_offset(array.length()) - start_offset;
      offsets = std::move(shifted_offsets);
    } else {
      // ARROW-6046: Slice offsets to used extent, in case we have a truncated
      // slice
      if (offsets != nullptr && offsets->size() > required_bytes) {
        offsets = SliceBuffer(offsets, 0, required_bytes);
      }
    }
    *value_offsets = std::move(offsets);
    return Status::OK();
  }

  Status Visit(const BooleanArray& array) {
    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(GetTruncatedBitmap(array.offset(), array.length(), array.values(),
                                     options_.memory_pool, &data));
    out_->body_buffers.emplace_back(data);
    return Status::OK();
  }

  Status Visit(const NullArray& array) { return Status::OK(); }

  template <typename T>
  typename std::enable_if<is_number_type<typename T::TypeClass>::value ||
                              is_temporal_type<typename T::TypeClass>::value ||
                              is_fixed_size_binary_type<typename T::TypeClass>::value,
                          Status>::type
  Visit(const T& array) {
    std::shared_ptr<Buffer> data = array.values();

    const int64_t type_width = array.type()->byte_width();
    int64_t min_length = PaddedLength(array.length() * type_width);

    if (NeedTruncate(array.offset(), data.get(), min_length)) {
      // Non-zero offset, slice the buffer
      const int64_t byte_offset = array.offset() * type_width;

      // Send padding if it's available
      const int64_t buffer_length =
          std::min(bit_util::RoundUpToMultipleOf8(array.length() * type_width),
                   data->size() - byte_offset);
      data = SliceBuffer(data, byte_offset, buffer_length);
    }
    out_->body_buffers.emplace_back(data);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<typename T::TypeClass, Status> Visit(const T& array) {
    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<T>(array, &value_offsets));
    auto data = array.value_data();

    int64_t total_data_bytes = 0;
    if (value_offsets) {
      total_data_bytes = array.value_offset(array.length()) - array.value_offset(0);
    }
    if (NeedTruncate(array.offset(), data.get(), total_data_bytes)) {
      // Slice the data buffer to include only the range we need now
      const int64_t start_offset = array.value_offset(0);
      const int64_t slice_length =
          std::min(PaddedLength(total_data_bytes), data->size() - start_offset);
      data = SliceBuffer(data, start_offset, slice_length);
    }

    out_->body_buffers.emplace_back(value_offsets);
    out_->body_buffers.emplace_back(data);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_list<typename T::TypeClass, Status> Visit(const T& array) {
    using offset_type = typename T::offset_type;

    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<T>(array, &value_offsets));
    out_->body_buffers.emplace_back(value_offsets);

    --max_recursion_depth_;
    std::shared_ptr<Array> values = array.values();

    offset_type values_offset = 0;
    offset_type values_length = 0;
    if (value_offsets) {
      values_offset = array.value_offset(0);
      values_length = array.value_offset(array.length()) - values_offset;
    }

    if (array.offset() != 0 || values_length < values->length()) {
      // Must also slice the values
      values = values->Slice(values_offset, values_length);
    }
    RETURN_NOT_OK(VisitArray(*values));
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const FixedSizeListArray& array) {
    --max_recursion_depth_;
    auto size = array.list_type()->list_size();
    auto values = array.values()->Slice(array.offset() * size, array.length() * size);

    RETURN_NOT_OK(VisitArray(*values));
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const StructArray& array) {
    --max_recursion_depth_;
    for (int i = 0; i < array.num_fields(); ++i) {
      std::shared_ptr<Array> field = array.field(i);
      RETURN_NOT_OK(VisitArray(*field));
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const SparseUnionArray& array) {
    const int64_t offset = array.offset();
    const int64_t length = array.length();

    std::shared_ptr<Buffer> type_codes;
    RETURN_NOT_OK(GetTruncatedBuffer(
        offset, length, static_cast<int32_t>(sizeof(UnionArray::type_code_t)),
        array.type_codes(), options_.memory_pool, &type_codes));
    out_->body_buffers.emplace_back(type_codes);

    --max_recursion_depth_;
    for (int i = 0; i < array.num_fields(); ++i) {
      // Sparse union, slicing is done for us by field()
      RETURN_NOT_OK(VisitArray(*array.field(i)));
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const DenseUnionArray& array) {
    const int64_t offset = array.offset();
    const int64_t length = array.length();

    std::shared_ptr<Buffer> type_codes;
    RETURN_NOT_OK(GetTruncatedBuffer(
        offset, length, static_cast<int32_t>(sizeof(UnionArray::type_code_t)),
        array.type_codes(), options_.memory_pool, &type_codes));
    out_->body_buffers.emplace_back(type_codes);

    --max_recursion_depth_;
    const auto& type = checked_cast<const UnionType&>(*array.type());

    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(
        GetTruncatedBuffer(offset, length, static_cast<int32_t>(sizeof(int32_t)),
                           array.value_offsets(), options_.memory_pool, &value_offsets));

    // The Union type codes are not necessary 0-indexed
    int8_t max_code = 0;
    for (int8_t code : type.type_codes()) {
      if (code > max_code) {
        max_code = code;
      }
    }

    // Allocate an array of child offsets. Set all to -1 to indicate that we
    // haven't observed a first occurrence of a particular child yet
    std::vector<int32_t> child_offsets(max_code + 1, -1);
    std::vector<int32_t> child_lengths(max_code + 1, 0);

    if (offset != 0) {
      // This is an unpleasant case. Because the offsets are different for
      // each child array, when we have a sliced array, we need to "rebase"
      // the value_offsets for each array

      const int32_t* unshifted_offsets = array.raw_value_offsets();
      const int8_t* type_codes = array.raw_type_codes();

      // Allocate the shifted offsets
      ARROW_ASSIGN_OR_RAISE(
          auto shifted_offsets_buffer,
          AllocateBuffer(length * sizeof(int32_t), options_.memory_pool));
      int32_t* shifted_offsets =
          reinterpret_cast<int32_t*>(shifted_offsets_buffer->mutable_data());

      // Offsets may not be ascending, so we need to find out the start offset
      // for each child
      for (int64_t i = 0; i < length; ++i) {
        const uint8_t code = type_codes[i];
        if (child_offsets[code] == -1) {
          child_offsets[code] = unshifted_offsets[i];
        } else {
          child_offsets[code] = std::min(child_offsets[code], unshifted_offsets[i]);
        }
      }

      // Now compute shifted offsets by subtracting child offset
      for (int64_t i = 0; i < length; ++i) {
        const int8_t code = type_codes[i];
        shifted_offsets[i] = unshifted_offsets[i] - child_offsets[code];
        // Update the child length to account for observed value
        child_lengths[code] = std::max(child_lengths[code], shifted_offsets[i] + 1);
      }

      value_offsets = std::move(shifted_offsets_buffer);
    }
    out_->body_buffers.emplace_back(value_offsets);

    // Visit children and slice accordingly
    for (int i = 0; i < type.num_fields(); ++i) {
      std::shared_ptr<Array> child = array.field(i);

      // TODO: ARROW-809, for sliced unions, tricky to know how much to
      // truncate the children. For now, we are truncating the children to be
      // no longer than the parent union.
      if (offset != 0) {
        const int8_t code = type.type_codes()[i];
        const int64_t child_offset = child_offsets[code];
        const int64_t child_length = child_lengths[code];

        if (child_offset > 0) {
          child = child->Slice(child_offset, child_length);
        } else if (child_length < child->length()) {
          // This case includes when child is not encountered at all
          child = child->Slice(0, child_length);
        }
      }
      RETURN_NOT_OK(VisitArray(*child));
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    // Dictionary written out separately. Slice offset contained in the indices
    return VisitType(*array.indices());
  }

  Status Visit(const RunLengthEncodedArray& type) {
    return Status::NotImplemented("run-length encoded array in ipc");
  }

  Status Visit(const ExtensionArray& array) { return VisitType(*array.storage()); }

  Status VisitType(const Array& values) { return VisitArrayInline(values, this); }

 protected:
  // Destination for output buffers
  IpcPayload* out_;

  std::shared_ptr<const KeyValueMetadata> custom_metadata_;

  std::vector<internal::FieldMetadata> field_nodes_;
  std::vector<internal::BufferMetadata> buffer_meta_;

  const IpcWriteOptions& options_;
  int64_t max_recursion_depth_;
  int64_t buffer_start_offset_;
};

class DictionarySerializer : public RecordBatchSerializer {
 public:
  DictionarySerializer(int64_t dictionary_id, bool is_delta, int64_t buffer_start_offset,
                       const IpcWriteOptions& options, IpcPayload* out)
      : RecordBatchSerializer(buffer_start_offset, NULLPTR, options, out),
        dictionary_id_(dictionary_id),
        is_delta_(is_delta) {}

  Status SerializeMetadata(int64_t num_rows) override {
    return WriteDictionaryMessage(dictionary_id_, is_delta_, num_rows, out_->body_length,
                                  custom_metadata_, field_nodes_, buffer_meta_, options_,
                                  &out_->metadata);
  }

  Status Assemble(const std::shared_ptr<Array>& dictionary) {
    // Make a dummy record batch. A bit tedious as we have to make a schema
    auto schema = arrow::schema({arrow::field("dictionary", dictionary->type())});
    auto batch = RecordBatch::Make(std::move(schema), dictionary->length(), {dictionary});
    return RecordBatchSerializer::Assemble(*batch);
  }

 private:
  int64_t dictionary_id_;
  bool is_delta_;
};

}  // namespace

Status WriteIpcPayload(const IpcPayload& payload, const IpcWriteOptions& options,
                       io::OutputStream* dst, int32_t* metadata_length) {
  RETURN_NOT_OK(WriteMessage(*payload.metadata, options, dst, metadata_length));

#ifndef NDEBUG
  RETURN_NOT_OK(CheckAligned(dst));
#endif

  // Now write the buffers
  for (size_t i = 0; i < payload.body_buffers.size(); ++i) {
    const std::shared_ptr<Buffer>& buffer = payload.body_buffers[i];
    int64_t size = 0;
    int64_t padding = 0;

    // The buffer might be null if we are handling zero row lengths.
    if (buffer) {
      size = buffer->size();
      padding = bit_util::RoundUpToMultipleOf8(size) - size;
    }

    if (size > 0) {
      RETURN_NOT_OK(dst->Write(buffer));
    }

    if (padding > 0) {
      RETURN_NOT_OK(dst->Write(kPaddingBytes, padding));
    }
  }

#ifndef NDEBUG
  RETURN_NOT_OK(CheckAligned(dst));
#endif

  return Status::OK();
}

Status GetSchemaPayload(const Schema& schema, const IpcWriteOptions& options,
                        const DictionaryFieldMapper& mapper, IpcPayload* out) {
  out->type = MessageType::SCHEMA;
  return internal::WriteSchemaMessage(schema, mapper, options, &out->metadata);
}

Status GetDictionaryPayload(int64_t id, const std::shared_ptr<Array>& dictionary,
                            const IpcWriteOptions& options, IpcPayload* out) {
  return GetDictionaryPayload(id, false, dictionary, options, out);
}

Status GetDictionaryPayload(int64_t id, bool is_delta,
                            const std::shared_ptr<Array>& dictionary,
                            const IpcWriteOptions& options, IpcPayload* out) {
  out->type = MessageType::DICTIONARY_BATCH;
  // Frame of reference is 0, see ARROW-384
  DictionarySerializer assembler(id, is_delta, /*buffer_start_offset=*/0, options, out);
  return assembler.Assemble(dictionary);
}

Status GetRecordBatchPayload(const RecordBatch& batch, const IpcWriteOptions& options,
                             IpcPayload* out) {
  return GetRecordBatchPayload(batch, NULLPTR, options, out);
}

Status GetRecordBatchPayload(
    const RecordBatch& batch,
    const std::shared_ptr<const KeyValueMetadata>& custom_metadata,
    const IpcWriteOptions& options, IpcPayload* out) {
  out->type = MessageType::RECORD_BATCH;
  RecordBatchSerializer assembler(/*buffer_start_offset=*/0, custom_metadata, options,
                                  out);
  return assembler.Assemble(batch);
}

Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
                        io::OutputStream* dst, int32_t* metadata_length,
                        int64_t* body_length, const IpcWriteOptions& options) {
  IpcPayload payload;
  RecordBatchSerializer assembler(buffer_start_offset, NULLPTR, options, &payload);
  RETURN_NOT_OK(assembler.Assemble(batch));

  // TODO: it's a rough edge that the metadata and body length here are
  // computed separately

  // The body size is computed in the payload
  *body_length = payload.body_length;

  return WriteIpcPayload(payload, options, dst, metadata_length);
}

Status WriteRecordBatchStream(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                              const IpcWriteOptions& options, io::OutputStream* dst) {
  ASSIGN_OR_RAISE(std::shared_ptr<RecordBatchWriter> writer,
                  MakeStreamWriter(dst, batches[0]->schema(), options));
  for (const auto& batch : batches) {
    DCHECK(batch->schema()->Equals(*batches[0]->schema())) << "Schemas unequal";
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  RETURN_NOT_OK(writer->Close());
  return Status::OK();
}

namespace {

Status WriteTensorHeader(const Tensor& tensor, io::OutputStream* dst,
                         int32_t* metadata_length) {
  IpcWriteOptions options;
  options.alignment = kTensorAlignment;
  std::shared_ptr<Buffer> metadata;
  ARROW_ASSIGN_OR_RAISE(metadata, internal::WriteTensorMessage(tensor, 0, options));
  return WriteMessage(*metadata, options, dst, metadata_length);
}

Status WriteStridedTensorData(int dim_index, int64_t offset, int elem_size,
                              const Tensor& tensor, uint8_t* scratch_space,
                              io::OutputStream* dst) {
  if (dim_index == tensor.ndim() - 1) {
    const uint8_t* data_ptr = tensor.raw_data() + offset;
    const int64_t stride = tensor.strides()[dim_index];
    for (int64_t i = 0; i < tensor.shape()[dim_index]; ++i) {
      memcpy(scratch_space + i * elem_size, data_ptr, elem_size);
      data_ptr += stride;
    }
    return dst->Write(scratch_space, elem_size * tensor.shape()[dim_index]);
  }
  for (int64_t i = 0; i < tensor.shape()[dim_index]; ++i) {
    RETURN_NOT_OK(WriteStridedTensorData(dim_index + 1, offset, elem_size, tensor,
                                         scratch_space, dst));
    offset += tensor.strides()[dim_index];
  }
  return Status::OK();
}

Status GetContiguousTensor(const Tensor& tensor, MemoryPool* pool,
                           std::unique_ptr<Tensor>* out) {
  const int elem_size = tensor.type()->byte_width();

  ARROW_ASSIGN_OR_RAISE(
      auto scratch_space,
      AllocateBuffer(tensor.shape()[tensor.ndim() - 1] * elem_size, pool));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> contiguous_data,
                        AllocateResizableBuffer(tensor.size() * elem_size, pool));

  io::BufferOutputStream stream(contiguous_data);
  RETURN_NOT_OK(WriteStridedTensorData(0, 0, elem_size, tensor,
                                       scratch_space->mutable_data(), &stream));

  out->reset(new Tensor(tensor.type(), contiguous_data, tensor.shape()));

  return Status::OK();
}

}  // namespace

Status WriteTensor(const Tensor& tensor, io::OutputStream* dst, int32_t* metadata_length,
                   int64_t* body_length) {
  const int elem_size = tensor.type()->byte_width();

  *body_length = tensor.size() * elem_size;

  // Tensor metadata accounts for padding
  if (tensor.is_contiguous()) {
    RETURN_NOT_OK(WriteTensorHeader(tensor, dst, metadata_length));
    auto data = tensor.data();
    if (data && data->data()) {
      RETURN_NOT_OK(dst->Write(data->data(), *body_length));
    } else {
      *body_length = 0;
    }
  } else {
    // The tensor written is made contiguous
    Tensor dummy(tensor.type(), nullptr, tensor.shape());
    RETURN_NOT_OK(WriteTensorHeader(dummy, dst, metadata_length));

    // TODO: Do we care enough about this temporary allocation to pass in a
    // MemoryPool to this function?
    ARROW_ASSIGN_OR_RAISE(auto scratch_space,
                          AllocateBuffer(tensor.shape()[tensor.ndim() - 1] * elem_size));

    RETURN_NOT_OK(WriteStridedTensorData(0, 0, elem_size, tensor,
                                         scratch_space->mutable_data(), dst));
  }

  return Status::OK();
}

Result<std::unique_ptr<Message>> GetTensorMessage(const Tensor& tensor,
                                                  MemoryPool* pool) {
  const Tensor* tensor_to_write = &tensor;
  std::unique_ptr<Tensor> temp_tensor;

  if (!tensor.is_contiguous()) {
    RETURN_NOT_OK(GetContiguousTensor(tensor, pool, &temp_tensor));
    tensor_to_write = temp_tensor.get();
  }

  IpcWriteOptions options;
  options.alignment = kTensorAlignment;
  std::shared_ptr<Buffer> metadata;
  ARROW_ASSIGN_OR_RAISE(metadata,
                        internal::WriteTensorMessage(*tensor_to_write, 0, options));
  return std::make_unique<Message>(metadata, tensor_to_write->data());
}

namespace internal {

class SparseTensorSerializer {
 public:
  SparseTensorSerializer(int64_t buffer_start_offset, IpcPayload* out)
      : out_(out),
        buffer_start_offset_(buffer_start_offset),
        options_(IpcWriteOptions::Defaults()) {}

  ~SparseTensorSerializer() = default;

  Status VisitSparseIndex(const SparseIndex& sparse_index) {
    switch (sparse_index.format_id()) {
      case SparseTensorFormat::COO:
        RETURN_NOT_OK(
            VisitSparseCOOIndex(checked_cast<const SparseCOOIndex&>(sparse_index)));
        break;

      case SparseTensorFormat::CSR:
        RETURN_NOT_OK(
            VisitSparseCSRIndex(checked_cast<const SparseCSRIndex&>(sparse_index)));
        break;

      case SparseTensorFormat::CSC:
        RETURN_NOT_OK(
            VisitSparseCSCIndex(checked_cast<const SparseCSCIndex&>(sparse_index)));
        break;

      case SparseTensorFormat::CSF:
        RETURN_NOT_OK(
            VisitSparseCSFIndex(checked_cast<const SparseCSFIndex&>(sparse_index)));
        break;

      default:
        std::stringstream ss;
        ss << "Unable to convert type: " << sparse_index.ToString() << std::endl;
        return Status::NotImplemented(ss.str());
    }

    return Status::OK();
  }

  Status SerializeMetadata(const SparseTensor& sparse_tensor) {
    return WriteSparseTensorMessage(sparse_tensor, out_->body_length, buffer_meta_,
                                    options_)
        .Value(&out_->metadata);
  }

  Status Assemble(const SparseTensor& sparse_tensor) {
    if (buffer_meta_.size() > 0) {
      buffer_meta_.clear();
      out_->body_buffers.clear();
    }

    RETURN_NOT_OK(VisitSparseIndex(*sparse_tensor.sparse_index()));
    out_->body_buffers.emplace_back(sparse_tensor.data());

    int64_t offset = buffer_start_offset_;
    buffer_meta_.reserve(out_->body_buffers.size());
    int64_t raw_size = 0;

    for (size_t i = 0; i < out_->body_buffers.size(); ++i) {
      const Buffer* buffer = out_->body_buffers[i].get();
      int64_t size = buffer->size();
      int64_t padding = bit_util::RoundUpToMultipleOf8(size) - size;
      buffer_meta_.push_back({offset, size + padding});
      offset += size + padding;
      raw_size += size;
    }

    out_->body_length = offset - buffer_start_offset_;
    DCHECK(bit_util::IsMultipleOf8(out_->body_length));
    out_->raw_body_length = raw_size;

    return SerializeMetadata(sparse_tensor);
  }

 private:
  Status VisitSparseCOOIndex(const SparseCOOIndex& sparse_index) {
    out_->body_buffers.emplace_back(sparse_index.indices()->data());
    return Status::OK();
  }

  Status VisitSparseCSRIndex(const SparseCSRIndex& sparse_index) {
    out_->body_buffers.emplace_back(sparse_index.indptr()->data());
    out_->body_buffers.emplace_back(sparse_index.indices()->data());
    return Status::OK();
  }

  Status VisitSparseCSCIndex(const SparseCSCIndex& sparse_index) {
    out_->body_buffers.emplace_back(sparse_index.indptr()->data());
    out_->body_buffers.emplace_back(sparse_index.indices()->data());
    return Status::OK();
  }

  Status VisitSparseCSFIndex(const SparseCSFIndex& sparse_index) {
    for (const std::shared_ptr<arrow::Tensor>& indptr : sparse_index.indptr()) {
      out_->body_buffers.emplace_back(indptr->data());
    }
    for (const std::shared_ptr<arrow::Tensor>& indices : sparse_index.indices()) {
      out_->body_buffers.emplace_back(indices->data());
    }
    return Status::OK();
  }

  IpcPayload* out_;

  std::vector<internal::BufferMetadata> buffer_meta_;
  int64_t buffer_start_offset_;
  IpcWriteOptions options_;
};

}  // namespace internal

Status WriteSparseTensor(const SparseTensor& sparse_tensor, io::OutputStream* dst,
                         int32_t* metadata_length, int64_t* body_length) {
  IpcPayload payload;
  internal::SparseTensorSerializer writer(0, &payload);
  RETURN_NOT_OK(writer.Assemble(sparse_tensor));

  *body_length = payload.body_length;
  return WriteIpcPayload(payload, IpcWriteOptions::Defaults(), dst, metadata_length);
}

Status GetSparseTensorPayload(const SparseTensor& sparse_tensor, MemoryPool* pool,
                              IpcPayload* out) {
  internal::SparseTensorSerializer writer(0, out);
  return writer.Assemble(sparse_tensor);
}

Result<std::unique_ptr<Message>> GetSparseTensorMessage(const SparseTensor& sparse_tensor,
                                                        MemoryPool* pool) {
  IpcPayload payload;
  RETURN_NOT_OK(GetSparseTensorPayload(sparse_tensor, pool, &payload));
  return std::unique_ptr<Message>(
      new Message(std::move(payload.metadata), std::move(payload.body_buffers[0])));
}

int64_t GetPayloadSize(const IpcPayload& payload, const IpcWriteOptions& options) {
  const int32_t prefix_size = options.write_legacy_ipc_format ? 4 : 8;
  const int32_t flatbuffer_size = static_cast<int32_t>(payload.metadata->size());
  const int32_t padded_message_length = static_cast<int32_t>(
      PaddedLength(flatbuffer_size + prefix_size, options.alignment));
  // body_length already accounts for padding
  return payload.body_length + padded_message_length;
}

Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size) {
  return GetRecordBatchSize(batch, IpcWriteOptions::Defaults(), size);
}

Status GetRecordBatchSize(const RecordBatch& batch, const IpcWriteOptions& options,
                          int64_t* size) {
  // emulates the behavior of Write without actually writing
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  io::MockOutputStream dst;
  RETURN_NOT_OK(
      WriteRecordBatch(batch, 0, &dst, &metadata_length, &body_length, options));
  *size = dst.GetExtentBytesWritten();
  return Status::OK();
}

Status GetTensorSize(const Tensor& tensor, int64_t* size) {
  // emulates the behavior of Write without actually writing
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  io::MockOutputStream dst;
  RETURN_NOT_OK(WriteTensor(tensor, &dst, &metadata_length, &body_length));
  *size = dst.GetExtentBytesWritten();
  return Status::OK();
}

// ----------------------------------------------------------------------

RecordBatchWriter::~RecordBatchWriter() {}

Status RecordBatchWriter::WriteTable(const Table& table, int64_t max_chunksize) {
  TableBatchReader reader(table);

  if (max_chunksize > 0) {
    reader.set_chunksize(max_chunksize);
  }

  std::shared_ptr<RecordBatch> batch;
  while (true) {
    RETURN_NOT_OK(reader.ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    RETURN_NOT_OK(WriteRecordBatch(*batch));
  }

  return Status::OK();
}

Status RecordBatchWriter::WriteTable(const Table& table) { return WriteTable(table, -1); }

// ----------------------------------------------------------------------
// Payload writer implementation

namespace internal {

IpcPayloadWriter::~IpcPayloadWriter() {}

Status IpcPayloadWriter::Start() { return Status::OK(); }

class ARROW_EXPORT IpcFormatWriter : public RecordBatchWriter {
 public:
  // A RecordBatchWriter implementation that writes to a IpcPayloadWriter.
  IpcFormatWriter(std::unique_ptr<internal::IpcPayloadWriter> payload_writer,
                  const Schema& schema, const IpcWriteOptions& options,
                  bool is_file_format)
      : payload_writer_(std::move(payload_writer)),
        schema_(schema),
        mapper_(schema),
        is_file_format_(is_file_format),
        options_(options) {}

  // A Schema-owning constructor variant
  IpcFormatWriter(std::unique_ptr<internal::IpcPayloadWriter> payload_writer,
                  const std::shared_ptr<Schema>& schema, const IpcWriteOptions& options,
                  bool is_file_format)
      : IpcFormatWriter(std::move(payload_writer), *schema, options, is_file_format) {
    shared_schema_ = schema;
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    return WriteRecordBatch(batch, NULLPTR);
  }

  Status WriteRecordBatch(
      const RecordBatch& batch,
      const std::shared_ptr<const KeyValueMetadata>& custom_metadata) override {
    if (!batch.schema()->Equals(schema_, false /* check_metadata */)) {
      return Status::Invalid("Tried to write record batch with different schema");
    }

    RETURN_NOT_OK(CheckStarted());

    RETURN_NOT_OK(WriteDictionaries(batch));

    IpcPayload payload;
    RETURN_NOT_OK(GetRecordBatchPayload(batch, custom_metadata, options_, &payload));
    RETURN_NOT_OK(WritePayload(payload));
    ++stats_.num_record_batches;

    stats_.total_raw_body_size += payload.raw_body_length;
    stats_.total_serialized_body_size += payload.body_length;

    return Status::OK();
  }

  Status WriteTable(const Table& table, int64_t max_chunksize) override {
    if (is_file_format_ && options_.unify_dictionaries) {
      ARROW_ASSIGN_OR_RAISE(auto unified_table,
                            DictionaryUnifier::UnifyTable(table, options_.memory_pool));
      return RecordBatchWriter::WriteTable(*unified_table, max_chunksize);
    } else {
      return RecordBatchWriter::WriteTable(table, max_chunksize);
    }
  }

  Status Close() override {
    RETURN_NOT_OK(CheckStarted());
    return payload_writer_->Close();
  }

  Status Start() {
    started_ = true;
    RETURN_NOT_OK(payload_writer_->Start());

    IpcPayload payload;
    RETURN_NOT_OK(GetSchemaPayload(schema_, options_, mapper_, &payload));
    return WritePayload(payload);
  }

  WriteStats stats() const override { return stats_; }

 protected:
  Status CheckStarted() {
    if (!started_) {
      return Start();
    }
    return Status::OK();
  }

  Status WriteDictionaries(const RecordBatch& batch) {
    ARROW_ASSIGN_OR_RAISE(const auto dictionaries, CollectDictionaries(batch, mapper_));
    const auto equal_options = EqualOptions().nans_equal(true);

    for (const auto& pair : dictionaries) {
      int64_t dictionary_id = pair.first;
      const auto& dictionary = pair.second;

      // If a dictionary with this id was already emitted, check if it was the same.
      auto* last_dictionary = &last_dictionaries_[dictionary_id];
      const bool dictionary_exists = (*last_dictionary != nullptr);
      int64_t delta_start = 0;
      if (dictionary_exists) {
        if ((*last_dictionary)->data() == dictionary->data()) {
          // Fast shortcut for a common case.
          // Same dictionary data by pointer => no need to emit it again
          continue;
        }
        const int64_t last_length = (*last_dictionary)->length();
        const int64_t new_length = dictionary->length();
        if (new_length == last_length &&
            ((*last_dictionary)->Equals(dictionary, equal_options))) {
          // Same dictionary by value => no need to emit it again
          // (while this can have a CPU cost, this code path is required
          //  for the IPC file format)
          continue;
        }

        // (the read path doesn't support outer dictionary deltas, don't emit them)
        if (new_length > last_length && options_.emit_dictionary_deltas &&
            !HasNestedDict(*dictionary->data()) &&
            ((*last_dictionary)
                 ->RangeEquals(dictionary, 0, last_length, 0, equal_options))) {
          // New dictionary starts with the current dictionary
          delta_start = last_length;
        }

        if (is_file_format_ && !delta_start) {
          return Status::Invalid(
              "Dictionary replacement detected when writing IPC file format. "
              "Arrow IPC files only support a single non-delta dictionary for "
              "a given field across all batches.");
        }
      }

      IpcPayload payload;
      if (delta_start) {
        RETURN_NOT_OK(GetDictionaryPayload(dictionary_id, /*is_delta=*/true,
                                           dictionary->Slice(delta_start), options_,
                                           &payload));
      } else {
        RETURN_NOT_OK(
            GetDictionaryPayload(dictionary_id, dictionary, options_, &payload));
      }
      RETURN_NOT_OK(WritePayload(payload));
      ++stats_.num_dictionary_batches;
      if (dictionary_exists) {
        if (delta_start) {
          ++stats_.num_dictionary_deltas;
        } else {
          ++stats_.num_replaced_dictionaries;
        }
      }

      // Remember dictionary for next batches
      *last_dictionary = dictionary;
    }
    return Status::OK();
  }

  Status WritePayload(const IpcPayload& payload) {
    RETURN_NOT_OK(payload_writer_->WritePayload(payload));
    ++stats_.num_messages;
    return Status::OK();
  }

  std::unique_ptr<IpcPayloadWriter> payload_writer_;
  std::shared_ptr<Schema> shared_schema_;
  const Schema& schema_;
  const DictionaryFieldMapper mapper_;
  const bool is_file_format_;

  // A map of last-written dictionaries by id.
  // This is required to avoid the same dictionary again and again,
  // and also for correctness when writing the IPC file format
  // (where replacements are unsupported).
  // The latter is also why we can't use weak_ptr.
  std::unordered_map<int64_t, std::shared_ptr<Array>> last_dictionaries_;

  bool started_ = false;
  IpcWriteOptions options_;
  WriteStats stats_;
};

class StreamBookKeeper {
 public:
  StreamBookKeeper(const IpcWriteOptions& options, io::OutputStream* sink)
      : options_(options), sink_(sink), position_(-1) {}
  StreamBookKeeper(const IpcWriteOptions& options, std::shared_ptr<io::OutputStream> sink)
      : options_(options),
        sink_(sink.get()),
        owned_sink_(std::move(sink)),
        position_(-1) {}

  Status UpdatePosition() { return sink_->Tell().Value(&position_); }

  Status UpdatePositionCheckAligned() {
    RETURN_NOT_OK(UpdatePosition());
    DCHECK_EQ(0, position_ % 8) << "Stream is not aligned";
    return Status::OK();
  }

  Status Align(int32_t alignment = kArrowIpcAlignment) {
    // Adds padding bytes if necessary to ensure all memory blocks are written on
    // 8-byte (or other alignment) boundaries.
    int64_t remainder = PaddedLength(position_, alignment) - position_;
    if (remainder > 0) {
      return Write(kPaddingBytes, remainder);
    }
    return Status::OK();
  }

  // Write data and update position
  Status Write(const void* data, int64_t nbytes) {
    RETURN_NOT_OK(sink_->Write(data, nbytes));
    position_ += nbytes;
    return Status::OK();
  }

  Status WriteEOS() {
    // End of stream marker
    constexpr int32_t kZeroLength = 0;
    if (!options_.write_legacy_ipc_format) {
      RETURN_NOT_OK(Write(&kIpcContinuationToken, sizeof(int32_t)));
    }
    return Write(&kZeroLength, sizeof(int32_t));
  }

 protected:
  IpcWriteOptions options_;
  io::OutputStream* sink_;
  std::shared_ptr<io::OutputStream> owned_sink_;
  int64_t position_;
};

/// A IpcPayloadWriter implementation that writes to an IPC stream
/// (with an end-of-stream marker)
class PayloadStreamWriter : public IpcPayloadWriter, protected StreamBookKeeper {
 public:
  PayloadStreamWriter(io::OutputStream* sink,
                      const IpcWriteOptions& options = IpcWriteOptions::Defaults())
      : StreamBookKeeper(options, sink) {}
  PayloadStreamWriter(std::shared_ptr<io::OutputStream> sink,
                      const IpcWriteOptions& options = IpcWriteOptions::Defaults())
      : StreamBookKeeper(options, std::move(sink)) {}

  ~PayloadStreamWriter() override = default;

  Status WritePayload(const IpcPayload& payload) override {
#ifndef NDEBUG
    // Catch bug fixed in ARROW-3236
    RETURN_NOT_OK(UpdatePositionCheckAligned());
#endif

    int32_t metadata_length = 0;  // unused
    RETURN_NOT_OK(WriteIpcPayload(payload, options_, sink_, &metadata_length));
    RETURN_NOT_OK(UpdatePositionCheckAligned());
    return Status::OK();
  }

  Status Close() override { return WriteEOS(); }
};

/// A IpcPayloadWriter implementation that writes to a IPC file
/// (with a footer as defined in File.fbs)
class PayloadFileWriter : public internal::IpcPayloadWriter, protected StreamBookKeeper {
 public:
  PayloadFileWriter(const IpcWriteOptions& options, const std::shared_ptr<Schema>& schema,
                    const std::shared_ptr<const KeyValueMetadata>& metadata,
                    io::OutputStream* sink)
      : StreamBookKeeper(options, sink), schema_(schema), metadata_(metadata) {}
  PayloadFileWriter(const IpcWriteOptions& options, const std::shared_ptr<Schema>& schema,
                    const std::shared_ptr<const KeyValueMetadata>& metadata,
                    std::shared_ptr<io::OutputStream> sink)
      : StreamBookKeeper(options, std::move(sink)),
        schema_(schema),
        metadata_(metadata) {}

  ~PayloadFileWriter() override = default;

  Status WritePayload(const IpcPayload& payload) override {
#ifndef NDEBUG
    // Catch bug fixed in ARROW-3236
    RETURN_NOT_OK(UpdatePositionCheckAligned());
#endif

    // Metadata length must include padding, it's computed by WriteIpcPayload()
    FileBlock block = {position_, 0, payload.body_length};
    RETURN_NOT_OK(WriteIpcPayload(payload, options_, sink_, &block.metadata_length));
    RETURN_NOT_OK(UpdatePositionCheckAligned());

    // Record position and size of some message types, to list them in the footer
    switch (payload.type) {
      case MessageType::DICTIONARY_BATCH:
        dictionaries_.push_back(block);
        break;
      case MessageType::RECORD_BATCH:
        record_batches_.push_back(block);
        break;
      default:
        break;
    }

    return Status::OK();
  }

  Status Start() override {
    // ARROW-3236: The initial position -1 needs to be updated to the stream's
    // current position otherwise an incorrect amount of padding will be
    // written to new files.
    RETURN_NOT_OK(UpdatePosition());

    // It is only necessary to align to 8-byte boundary at the start of the file
    RETURN_NOT_OK(Write(kArrowMagicBytes, strlen(kArrowMagicBytes)));
    RETURN_NOT_OK(Align());

    return Status::OK();
  }

  Status Close() override {
    // Write 0 EOS message for compatibility with sequential readers
    RETURN_NOT_OK(WriteEOS());

    // Write file footer
    RETURN_NOT_OK(UpdatePosition());
    int64_t initial_position = position_;
    RETURN_NOT_OK(
        WriteFileFooter(*schema_, dictionaries_, record_batches_, metadata_, sink_));

    // Write footer length
    RETURN_NOT_OK(UpdatePosition());
    int32_t footer_length = static_cast<int32_t>(position_ - initial_position);
    if (footer_length <= 0) {
      return Status::Invalid("Invalid file footer");
    }

    // write footer length in little endian
    footer_length = bit_util::ToLittleEndian(footer_length);
    RETURN_NOT_OK(Write(&footer_length, sizeof(int32_t)));

    // Write magic bytes to end file
    return Write(kArrowMagicBytes, strlen(kArrowMagicBytes));
  }

 protected:
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<const KeyValueMetadata> metadata_;
  std::vector<FileBlock> dictionaries_;
  std::vector<FileBlock> record_batches_;
};

}  // namespace internal

Result<std::shared_ptr<RecordBatchWriter>> MakeStreamWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options) {
  return std::make_shared<internal::IpcFormatWriter>(
      std::make_unique<internal::PayloadStreamWriter>(sink, options), schema, options,
      /*is_file_format=*/false);
}

Result<std::shared_ptr<RecordBatchWriter>> MakeStreamWriter(
    std::shared_ptr<io::OutputStream> sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options) {
  return std::make_shared<internal::IpcFormatWriter>(
      std::make_unique<internal::PayloadStreamWriter>(std::move(sink), options), schema,
      options, /*is_file_format=*/false);
}

Result<std::shared_ptr<RecordBatchWriter>> NewStreamWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options) {
  return MakeStreamWriter(sink, schema, options);
}

Result<std::shared_ptr<RecordBatchWriter>> MakeFileWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options,
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return std::make_shared<internal::IpcFormatWriter>(
      std::make_unique<internal::PayloadFileWriter>(options, schema, metadata, sink),
      schema, options, /*is_file_format=*/true);
}

Result<std::shared_ptr<RecordBatchWriter>> MakeFileWriter(
    std::shared_ptr<io::OutputStream> sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options,
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return std::make_shared<internal::IpcFormatWriter>(
      std::make_unique<internal::PayloadFileWriter>(options, schema, metadata,
                                                    std::move(sink)),
      schema, options, /*is_file_format=*/true);
}

Result<std::shared_ptr<RecordBatchWriter>> NewFileWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options,
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return MakeFileWriter(sink, schema, options, metadata);
}

namespace internal {

Result<std::unique_ptr<RecordBatchWriter>> OpenRecordBatchWriter(
    std::unique_ptr<IpcPayloadWriter> sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options) {
  auto writer = std::make_unique<internal::IpcFormatWriter>(
      std::move(sink), schema, options, /*is_file_format=*/false);
  RETURN_NOT_OK(writer->Start());
  return std::move(writer);
}

Result<std::unique_ptr<IpcPayloadWriter>> MakePayloadStreamWriter(
    io::OutputStream* sink, const IpcWriteOptions& options) {
  return std::make_unique<internal::PayloadStreamWriter>(sink, options);
}

Result<std::unique_ptr<IpcPayloadWriter>> MakePayloadFileWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options,
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return std::make_unique<internal::PayloadFileWriter>(options, schema, metadata, sink);
}

}  // namespace internal

// ----------------------------------------------------------------------
// Serialization public APIs

Result<std::shared_ptr<Buffer>> SerializeRecordBatch(const RecordBatch& batch,
                                                     std::shared_ptr<MemoryManager> mm) {
  auto options = IpcWriteOptions::Defaults();
  int64_t size = 0;
  RETURN_NOT_OK(GetRecordBatchSize(batch, options, &size));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, mm->AllocateBuffer(size));
  ARROW_ASSIGN_OR_RAISE(auto writer, Buffer::GetWriter(buffer));

  // XXX Should we have a helper function for getting a MemoryPool
  // for any MemoryManager (not only CPU)?
  if (mm->is_cpu()) {
    options.memory_pool = checked_pointer_cast<CPUMemoryManager>(mm)->pool();
  }
  RETURN_NOT_OK(SerializeRecordBatch(batch, options, writer.get()));
  RETURN_NOT_OK(writer->Close());
  return buffer;
}

Result<std::shared_ptr<Buffer>> SerializeRecordBatch(const RecordBatch& batch,
                                                     const IpcWriteOptions& options) {
  int64_t size = 0;
  RETURN_NOT_OK(GetRecordBatchSize(batch, options, &size));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer,
                        AllocateBuffer(size, options.memory_pool));

  io::FixedSizeBufferWriter stream(buffer);
  RETURN_NOT_OK(SerializeRecordBatch(batch, options, &stream));
  return buffer;
}

Status SerializeRecordBatch(const RecordBatch& batch, const IpcWriteOptions& options,
                            io::OutputStream* out) {
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  return WriteRecordBatch(batch, 0, out, &metadata_length, &body_length, options);
}

Result<std::shared_ptr<Buffer>> SerializeSchema(const Schema& schema, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto stream, io::BufferOutputStream::Create(1024, pool));

  auto options = IpcWriteOptions::Defaults();
  const bool is_file_format = false;  // indifferent as we don't write dictionaries
  internal::IpcFormatWriter writer(
      std::make_unique<internal::PayloadStreamWriter>(stream.get()), schema, options,
      is_file_format);
  RETURN_NOT_OK(writer.Start());
  return stream->Finish();
}

}  // namespace ipc
}  // namespace arrow
