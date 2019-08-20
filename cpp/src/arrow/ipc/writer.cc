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
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/util.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/result_internal.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/visitor.h"

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;
using internal::make_unique;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

// ----------------------------------------------------------------------
// Record batch write path

static inline Status GetTruncatedBitmap(int64_t offset, int64_t length,
                                        const std::shared_ptr<Buffer> input,
                                        MemoryPool* pool,
                                        std::shared_ptr<Buffer>* buffer) {
  if (!input) {
    *buffer = input;
    return Status::OK();
  }
  int64_t min_length = PaddedLength(BitUtil::BytesForBits(length));
  if (offset != 0 || min_length < input->size()) {
    // With a sliced array / non-zero offset, we must copy the bitmap
    RETURN_NOT_OK(CopyBitmap(pool, input->data(), offset, length, buffer));
  } else {
    *buffer = input;
  }
  return Status::OK();
}

template <typename T>
inline Status GetTruncatedBuffer(int64_t offset, int64_t length,
                                 const std::shared_ptr<Buffer> input, MemoryPool* pool,
                                 std::shared_ptr<Buffer>* buffer) {
  if (!input) {
    *buffer = input;
    return Status::OK();
  }
  int32_t byte_width = static_cast<int32_t>(sizeof(T));
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

namespace internal {

class RecordBatchSerializer : public ArrayVisitor {
 public:
  RecordBatchSerializer(MemoryPool* pool, int64_t buffer_start_offset,
                        const IpcOptions& options, IpcPayload* out)
      : out_(out),
        pool_(pool),
        options_(options),
        max_recursion_depth_(options.max_recursion_depth),
        buffer_start_offset_(buffer_start_offset) {
    DCHECK_GT(max_recursion_depth_, 0);
  }

  ~RecordBatchSerializer() override = default;

  Status VisitArray(const Array& arr) {
    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    if (!options_.allow_64bit && arr.length() > std::numeric_limits<int32_t>::max()) {
      return Status::CapacityError("Cannot write arrays larger than 2^31 - 1 in length");
    }

    // push back all common elements
    field_nodes_.push_back({arr.length(), arr.null_count(), 0});

    if (arr.null_count() > 0) {
      std::shared_ptr<Buffer> bitmap;
      RETURN_NOT_OK(GetTruncatedBitmap(arr.offset(), arr.length(), arr.null_bitmap(),
                                       pool_, &bitmap));
      out_->body_buffers.emplace_back(bitmap);
    } else {
      // Push a dummy zero-length buffer, not to be copied
      out_->body_buffers.emplace_back(std::make_shared<Buffer>(nullptr, 0));
    }
    return arr.Accept(this);
  }

  // Override this for writing dictionary metadata
  virtual Status SerializeMetadata(int64_t num_rows) {
    return WriteRecordBatchMessage(num_rows, out_->body_length, field_nodes_,
                                   buffer_meta_, &out_->metadata);
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

    // The position for the start of a buffer relative to the passed frame of
    // reference. May be 0 or some other position in an address space
    int64_t offset = buffer_start_offset_;

    buffer_meta_.reserve(out_->body_buffers.size());

    // Construct the buffer metadata for the record batch header
    for (size_t i = 0; i < out_->body_buffers.size(); ++i) {
      const Buffer* buffer = out_->body_buffers[i].get();
      int64_t size = 0;
      int64_t padding = 0;

      // The buffer might be null if we are handling zero row lengths.
      if (buffer) {
        size = buffer->size();
        padding = BitUtil::RoundUpToMultipleOf8(size) - size;
      }

      buffer_meta_.push_back({offset, size + padding});
      offset += size + padding;
    }

    out_->body_length = offset - buffer_start_offset_;
    DCHECK(BitUtil::IsMultipleOf8(out_->body_length));

    // Now that we have computed the locations of all of the buffers in shared
    // memory, the data header can be converted to a flatbuffer and written out
    //
    // Note: The memory written here is prefixed by the size of the flatbuffer
    // itself as an int32_t.
    return SerializeMetadata(batch.num_rows());
  }

 protected:
  template <typename ArrayType>
  Status VisitFixedWidth(const ArrayType& array) {
    std::shared_ptr<Buffer> data = array.values();

    const auto& fw_type = checked_cast<const FixedWidthType&>(*array.type());
    const int64_t type_width = fw_type.bit_width() / 8;
    int64_t min_length = PaddedLength(array.length() * type_width);

    if (NeedTruncate(array.offset(), data.get(), min_length)) {
      // Non-zero offset, slice the buffer
      const int64_t byte_offset = array.offset() * type_width;

      // Send padding if it's available
      const int64_t buffer_length =
          std::min(BitUtil::RoundUpToMultipleOf8(array.length() * type_width),
                   data->size() - byte_offset);
      data = SliceBuffer(data, byte_offset, buffer_length);
    }
    out_->body_buffers.emplace_back(data);
    return Status::OK();
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

      std::shared_ptr<Buffer> shifted_offsets;
      RETURN_NOT_OK(AllocateBuffer(pool_, required_bytes, &shifted_offsets));

      offset_type* dest_offsets =
          reinterpret_cast<offset_type*>(shifted_offsets->mutable_data());
      const offset_type start_offset = array.value_offset(0);

      for (int i = 0; i < array.length(); ++i) {
        dest_offsets[i] = array.value_offset(i) - start_offset;
      }
      // Final offset
      dest_offsets[array.length()] = array.value_offset(array.length()) - start_offset;
      offsets = shifted_offsets;
    } else {
      // ARROW-6046: Slice offsets to used extent, in case we have a truncated
      // slice
      if (offsets != nullptr && offsets->size() > required_bytes) {
        offsets = SliceBuffer(offsets, 0, required_bytes);
      }
    }
    *value_offsets = offsets;
    return Status::OK();
  }

  template <typename ArrayType>
  Status VisitBinary(const ArrayType& array) {
    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<ArrayType>(array, &value_offsets));
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

  template <typename ArrayType>
  Status VisitList(const ArrayType& array) {
    using offset_type = typename ArrayType::offset_type;

    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<ArrayType>(array, &value_offsets));
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

  Status Visit(const BooleanArray& array) override {
    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(
        GetTruncatedBitmap(array.offset(), array.length(), array.values(), pool_, &data));
    out_->body_buffers.emplace_back(data);
    return Status::OK();
  }

  Status Visit(const NullArray& array) override {
    out_->body_buffers.emplace_back(nullptr);
    return Status::OK();
  }

#define VISIT_FIXED_WIDTH(TYPE) \
  Status Visit(const TYPE& array) override { return VisitFixedWidth<TYPE>(array); }

  VISIT_FIXED_WIDTH(Int8Array)
  VISIT_FIXED_WIDTH(Int16Array)
  VISIT_FIXED_WIDTH(Int32Array)
  VISIT_FIXED_WIDTH(Int64Array)
  VISIT_FIXED_WIDTH(UInt8Array)
  VISIT_FIXED_WIDTH(UInt16Array)
  VISIT_FIXED_WIDTH(UInt32Array)
  VISIT_FIXED_WIDTH(UInt64Array)
  VISIT_FIXED_WIDTH(HalfFloatArray)
  VISIT_FIXED_WIDTH(FloatArray)
  VISIT_FIXED_WIDTH(DoubleArray)
  VISIT_FIXED_WIDTH(Date32Array)
  VISIT_FIXED_WIDTH(Date64Array)
  VISIT_FIXED_WIDTH(TimestampArray)
  VISIT_FIXED_WIDTH(DurationArray)
  VISIT_FIXED_WIDTH(MonthIntervalArray)
  VISIT_FIXED_WIDTH(DayTimeIntervalArray)
  VISIT_FIXED_WIDTH(Time32Array)
  VISIT_FIXED_WIDTH(Time64Array)
  VISIT_FIXED_WIDTH(FixedSizeBinaryArray)
  VISIT_FIXED_WIDTH(Decimal128Array)

#undef VISIT_FIXED_WIDTH

  Status Visit(const StringArray& array) override { return VisitBinary(array); }

  Status Visit(const BinaryArray& array) override { return VisitBinary(array); }

  Status Visit(const LargeStringArray& array) override { return VisitBinary(array); }

  Status Visit(const LargeBinaryArray& array) override { return VisitBinary(array); }

  Status Visit(const ListArray& array) override { return VisitList(array); }

  Status Visit(const LargeListArray& array) override { return VisitList(array); }

  Status Visit(const MapArray& array) override { return VisitList(array); }

  Status Visit(const FixedSizeListArray& array) override {
    --max_recursion_depth_;
    auto size = array.list_type()->list_size();
    auto values = array.values()->Slice(array.offset() * size, array.length() * size);

    RETURN_NOT_OK(VisitArray(*values));
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const StructArray& array) override {
    --max_recursion_depth_;
    for (int i = 0; i < array.num_fields(); ++i) {
      std::shared_ptr<Array> field = array.field(i);
      RETURN_NOT_OK(VisitArray(*field));
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const UnionArray& array) override {
    const int64_t offset = array.offset();
    const int64_t length = array.length();

    std::shared_ptr<Buffer> type_ids;
    RETURN_NOT_OK(GetTruncatedBuffer<UnionArray::type_id_t>(
        offset, length, array.type_ids(), pool_, &type_ids));
    out_->body_buffers.emplace_back(type_ids);

    --max_recursion_depth_;
    if (array.mode() == UnionMode::DENSE) {
      const auto& type = checked_cast<const UnionType&>(*array.type());

      std::shared_ptr<Buffer> value_offsets;
      RETURN_NOT_OK(GetTruncatedBuffer<int32_t>(offset, length, array.value_offsets(),
                                                pool_, &value_offsets));

      // The Union type codes are not necessary 0-indexed
      uint8_t max_code = 0;
      for (uint8_t code : type.type_codes()) {
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
        const uint8_t* type_ids = array.raw_type_ids();

        // Allocate the shifted offsets
        std::shared_ptr<Buffer> shifted_offsets_buffer;
        RETURN_NOT_OK(
            AllocateBuffer(pool_, length * sizeof(int32_t), &shifted_offsets_buffer));
        int32_t* shifted_offsets =
            reinterpret_cast<int32_t*>(shifted_offsets_buffer->mutable_data());

        // Offsets may not be ascending, so we need to find out the start offset
        // for each child
        for (int64_t i = 0; i < length; ++i) {
          const uint8_t code = type_ids[i];
          if (child_offsets[code] == -1) {
            child_offsets[code] = unshifted_offsets[i];
          } else {
            child_offsets[code] = std::min(child_offsets[code], unshifted_offsets[i]);
          }
        }

        // Now compute shifted offsets by subtracting child offset
        for (int64_t i = 0; i < length; ++i) {
          const uint8_t code = type_ids[i];
          shifted_offsets[i] = unshifted_offsets[i] - child_offsets[code];
          // Update the child length to account for observed value
          child_lengths[code] = std::max(child_lengths[code], shifted_offsets[i] + 1);
        }

        value_offsets = shifted_offsets_buffer;
      }
      out_->body_buffers.emplace_back(value_offsets);

      // Visit children and slice accordingly
      for (int i = 0; i < type.num_children(); ++i) {
        std::shared_ptr<Array> child = array.child(i);

        // TODO: ARROW-809, for sliced unions, tricky to know how much to
        // truncate the children. For now, we are truncating the children to be
        // no longer than the parent union.
        if (offset != 0) {
          const uint8_t code = type.type_codes()[i];
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
    } else {
      for (int i = 0; i < array.num_fields(); ++i) {
        // Sparse union, slicing is done for us by child()
        RETURN_NOT_OK(VisitArray(*array.child(i)));
      }
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) override {
    // Dictionary written out separately. Slice offset contained in the indices
    return array.indices()->Accept(this);
  }

  Status Visit(const ExtensionArray& array) override {
    return array.storage()->Accept(this);
  }

  // Destination for output buffers
  IpcPayload* out_;

  // In some cases, intermediate buffers may need to be allocated (with sliced arrays)
  MemoryPool* pool_;

  std::vector<internal::FieldMetadata> field_nodes_;
  std::vector<internal::BufferMetadata> buffer_meta_;

  const IpcOptions& options_;
  int64_t max_recursion_depth_;
  int64_t buffer_start_offset_;
};

class DictionaryWriter : public RecordBatchSerializer {
 public:
  DictionaryWriter(int64_t dictionary_id, MemoryPool* pool, int64_t buffer_start_offset,
                   const IpcOptions& options, IpcPayload* out)
      : RecordBatchSerializer(pool, buffer_start_offset, options, out),
        dictionary_id_(dictionary_id) {}

  Status SerializeMetadata(int64_t num_rows) override {
    return WriteDictionaryMessage(dictionary_id_, num_rows, out_->body_length,
                                  field_nodes_, buffer_meta_, &out_->metadata);
  }

  Status Assemble(const std::shared_ptr<Array>& dictionary) {
    // Make a dummy record batch. A bit tedious as we have to make a schema
    auto schema = arrow::schema({arrow::field("dictionary", dictionary->type())});
    auto batch = RecordBatch::Make(schema, dictionary->length(), {dictionary});
    return RecordBatchSerializer::Assemble(*batch);
  }

 private:
  int64_t dictionary_id_;
};

Status WriteIpcPayload(const IpcPayload& payload, io::OutputStream* dst,
                       int32_t* metadata_length) {
  RETURN_NOT_OK(internal::WriteMessage(*payload.metadata, kArrowIpcAlignment, dst,
                                       metadata_length));

#ifndef NDEBUG
  RETURN_NOT_OK(CheckAligned(dst));
#endif

  // Now write the buffers
  for (size_t i = 0; i < payload.body_buffers.size(); ++i) {
    const Buffer* buffer = payload.body_buffers[i].get();
    int64_t size = 0;
    int64_t padding = 0;

    // The buffer might be null if we are handling zero row lengths.
    if (buffer) {
      size = buffer->size();
      padding = BitUtil::RoundUpToMultipleOf8(size) - size;
    }

    if (size > 0) {
      RETURN_NOT_OK(dst->Write(buffer->data(), size));
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

Status GetSchemaPayload(const Schema& schema, const IpcOptions& options,
                        DictionaryMemo* dictionary_memo, IpcPayload* out) {
  out->type = Message::SCHEMA;
  return WriteSchemaMessage(schema, dictionary_memo, &out->metadata);
}

Status GetDictionaryPayload(int64_t id, const std::shared_ptr<Array>& dictionary,
                            const IpcOptions& options, MemoryPool* pool,
                            IpcPayload* out) {
  out->type = Message::DICTIONARY_BATCH;
  // Frame of reference is 0, see ARROW-384
  DictionaryWriter writer(id, pool, /*buffer_start_offset=*/0, options, out);
  return writer.Assemble(dictionary);
}

Status GetRecordBatchPayload(const RecordBatch& batch, const IpcOptions& options,
                             MemoryPool* pool, IpcPayload* out) {
  out->type = Message::RECORD_BATCH;
  RecordBatchSerializer writer(pool, /*buffer_start_offset=*/0, options, out);
  return writer.Assemble(batch);
}

}  // namespace internal

Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
                        io::OutputStream* dst, int32_t* metadata_length,
                        int64_t* body_length, const IpcOptions& options,
                        MemoryPool* pool) {
  internal::IpcPayload payload;
  internal::RecordBatchSerializer writer(pool, buffer_start_offset, options, &payload);
  RETURN_NOT_OK(writer.Assemble(batch));

  // TODO(wesm): it's a rough edge that the metadata and body length here are
  // computed separately

  // The body size is computed in the payload
  *body_length = payload.body_length;

  return internal::WriteIpcPayload(payload, dst, metadata_length);
}

Status WriteRecordBatchStream(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                              const IpcOptions& options, io::OutputStream* dst) {
  ASSIGN_OR_RAISE(std::shared_ptr<RecordBatchWriter> writer,
                  RecordBatchStreamWriter::Open(dst, batches[0]->schema(), options));
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
  std::shared_ptr<Buffer> metadata;
  RETURN_NOT_OK(internal::WriteTensorMessage(tensor, 0, &metadata));
  return internal::WriteMessage(*metadata, kTensorAlignment, dst, metadata_length);
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
  const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
  const int elem_size = type.bit_width() / 8;

  std::shared_ptr<Buffer> scratch_space;
  RETURN_NOT_OK(AllocateBuffer(pool, tensor.shape()[tensor.ndim() - 1] * elem_size,
                               &scratch_space));

  std::shared_ptr<ResizableBuffer> contiguous_data;
  RETURN_NOT_OK(
      AllocateResizableBuffer(pool, tensor.size() * elem_size, &contiguous_data));

  io::BufferOutputStream stream(contiguous_data);
  RETURN_NOT_OK(WriteStridedTensorData(0, 0, elem_size, tensor,
                                       scratch_space->mutable_data(), &stream));

  out->reset(new Tensor(tensor.type(), contiguous_data, tensor.shape()));

  return Status::OK();
}

}  // namespace

Status WriteTensor(const Tensor& tensor, io::OutputStream* dst, int32_t* metadata_length,
                   int64_t* body_length) {
  const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
  const int elem_size = type.bit_width() / 8;

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

    // TODO(wesm): Do we care enough about this temporary allocation to pass in
    // a MemoryPool to this function?
    std::shared_ptr<Buffer> scratch_space;
    RETURN_NOT_OK(
        AllocateBuffer(tensor.shape()[tensor.ndim() - 1] * elem_size, &scratch_space));

    RETURN_NOT_OK(WriteStridedTensorData(0, 0, elem_size, tensor,
                                         scratch_space->mutable_data(), dst));
  }

  return Status::OK();
}

Status GetTensorMessage(const Tensor& tensor, MemoryPool* pool,
                        std::unique_ptr<Message>* out) {
  const Tensor* tensor_to_write = &tensor;
  std::unique_ptr<Tensor> temp_tensor;

  if (!tensor.is_contiguous()) {
    RETURN_NOT_OK(GetContiguousTensor(tensor, pool, &temp_tensor));
    tensor_to_write = temp_tensor.get();
  }

  std::shared_ptr<Buffer> metadata;
  RETURN_NOT_OK(internal::WriteTensorMessage(*tensor_to_write, 0, &metadata));
  out->reset(new Message(metadata, tensor_to_write->data()));
  return Status::OK();
}

namespace internal {

class SparseTensorSerializer {
 public:
  SparseTensorSerializer(int64_t buffer_start_offset, IpcPayload* out)
      : out_(out), buffer_start_offset_(buffer_start_offset) {}

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

      default:
        std::stringstream ss;
        ss << "Unable to convert type: " << sparse_index.ToString() << std::endl;
        return Status::NotImplemented(ss.str());
    }

    return Status::OK();
  }

  Status SerializeMetadata(const SparseTensor& sparse_tensor) {
    return WriteSparseTensorMessage(sparse_tensor, out_->body_length, buffer_meta_,
                                    &out_->metadata);
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

    for (size_t i = 0; i < out_->body_buffers.size(); ++i) {
      const Buffer* buffer = out_->body_buffers[i].get();
      int64_t size = buffer->size();
      int64_t padding = BitUtil::RoundUpToMultipleOf8(size) - size;
      buffer_meta_.push_back({offset, size + padding});
      offset += size + padding;
    }

    out_->body_length = offset - buffer_start_offset_;
    DCHECK(BitUtil::IsMultipleOf8(out_->body_length));

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

  IpcPayload* out_;

  std::vector<internal::BufferMetadata> buffer_meta_;

  int64_t buffer_start_offset_;
};

Status GetSparseTensorPayload(const SparseTensor& sparse_tensor, MemoryPool* pool,
                              IpcPayload* out) {
  SparseTensorSerializer writer(0, out);
  return writer.Assemble(sparse_tensor);
}

}  // namespace internal

Status WriteSparseTensor(const SparseTensor& sparse_tensor, io::OutputStream* dst,
                         int32_t* metadata_length, int64_t* body_length,
                         MemoryPool* pool) {
  internal::IpcPayload payload;
  internal::SparseTensorSerializer writer(0, &payload);
  RETURN_NOT_OK(writer.Assemble(sparse_tensor));

  *body_length = payload.body_length;
  return internal::WriteIpcPayload(payload, dst, metadata_length);
}

Status WriteDictionary(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
                       int64_t buffer_start_offset, io::OutputStream* dst,
                       int32_t* metadata_length, int64_t* body_length, MemoryPool* pool) {
  auto options = IpcOptions::Defaults();
  internal::IpcPayload payload;
  RETURN_NOT_OK(GetDictionaryPayload(dictionary_id, dictionary, options, pool, &payload));

  // The body size is computed in the payload
  *body_length = payload.body_length;
  return internal::WriteIpcPayload(payload, dst, metadata_length);
}

Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size) {
  // emulates the behavior of Write without actually writing
  auto options = IpcOptions::Defaults();
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  io::MockOutputStream dst;
  RETURN_NOT_OK(WriteRecordBatch(batch, 0, &dst, &metadata_length, &body_length, options,
                                 default_memory_pool()));
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

}  // namespace internal

namespace {

/// A RecordBatchWriter implementation that writes to a IpcPayloadWriter.
class RecordBatchPayloadWriter : public RecordBatchWriter {
 public:
  ~RecordBatchPayloadWriter() override = default;

  RecordBatchPayloadWriter(std::unique_ptr<internal::IpcPayloadWriter> payload_writer,
                           const Schema& schema, const IpcOptions& options,
                           DictionaryMemo* out_memo = nullptr)
      : payload_writer_(std::move(payload_writer)),
        schema_(schema),
        pool_(default_memory_pool()),
        dictionary_memo_(out_memo),
        options_(options) {
    if (out_memo == nullptr) {
      dictionary_memo_ = &internal_dict_memo_;
    }
  }

  // A Schema-owning constructor variant
  RecordBatchPayloadWriter(std::unique_ptr<internal::IpcPayloadWriter> payload_writer,
                           const std::shared_ptr<Schema>& schema,
                           const IpcOptions& options, DictionaryMemo* out_memo = nullptr)
      : RecordBatchPayloadWriter(std::move(payload_writer), *schema, options, out_memo) {
    shared_schema_ = schema;
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    if (!batch.schema()->Equals(schema_, false /* check_metadata */)) {
      return Status::Invalid("Tried to write record batch with different schema");
    }

    RETURN_NOT_OK(CheckStarted());

    if (!wrote_dictionaries_) {
      RETURN_NOT_OK(WriteDictionaries(batch));
      wrote_dictionaries_ = true;
    }

    // TODO(wesm): Check for delta dictionaries. Can we scan for
    // deltas while computing the RecordBatch payload to save time?

    internal::IpcPayload payload;
    RETURN_NOT_OK(GetRecordBatchPayload(batch, options_, pool_, &payload));
    return payload_writer_->WritePayload(payload);
  }

  Status Close() override {
    RETURN_NOT_OK(CheckStarted());
    return payload_writer_->Close();
  }

  void set_memory_pool(MemoryPool* pool) override { pool_ = pool; }

  Status Start() {
    started_ = true;
    RETURN_NOT_OK(payload_writer_->Start());

    internal::IpcPayload payload;
    RETURN_NOT_OK(GetSchemaPayload(schema_, options_, dictionary_memo_, &payload));
    return payload_writer_->WritePayload(payload);
  }

 protected:
  Status CheckStarted() {
    if (!started_) {
      return Start();
    }
    return Status::OK();
  }

  Status WriteDictionaries(const RecordBatch& batch) {
    RETURN_NOT_OK(CollectDictionaries(batch, dictionary_memo_));

    for (auto& pair : dictionary_memo_->id_to_dictionary()) {
      internal::IpcPayload payload;
      int64_t dictionary_id = pair.first;
      const auto& dictionary = pair.second;

      RETURN_NOT_OK(
          GetDictionaryPayload(dictionary_id, dictionary, options_, pool_, &payload));
      RETURN_NOT_OK(payload_writer_->WritePayload(payload));
    }
    return Status::OK();
  }

 protected:
  std::unique_ptr<internal::IpcPayloadWriter> payload_writer_;
  std::shared_ptr<Schema> shared_schema_;
  const Schema& schema_;
  MemoryPool* pool_;
  DictionaryMemo* dictionary_memo_;
  DictionaryMemo internal_dict_memo_;
  bool started_ = false;
  bool wrote_dictionaries_ = false;
  IpcOptions options_;
};

// ----------------------------------------------------------------------
// Stream and file writer implementation

class StreamBookKeeper {
 public:
  explicit StreamBookKeeper(io::OutputStream* sink) : sink_(sink), position_(-1) {}

  Status UpdatePosition() { return sink_->Tell(&position_); }

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

 protected:
  io::OutputStream* sink_;
  int64_t position_;
};

// End of stream marker
constexpr int32_t kEos = 0;

/// A IpcPayloadWriter implementation that writes to a IPC stream
/// (with an end-of-stream marker)
class PayloadStreamWriter : public internal::IpcPayloadWriter,
                            protected StreamBookKeeper {
 public:
  explicit PayloadStreamWriter(io::OutputStream* sink) : StreamBookKeeper(sink) {}

  ~PayloadStreamWriter() override = default;

  Status WritePayload(const internal::IpcPayload& payload) override {
#ifndef NDEBUG
    // Catch bug fixed in ARROW-3236
    RETURN_NOT_OK(UpdatePositionCheckAligned());
#endif

    int32_t metadata_length = 0;  // unused
    RETURN_NOT_OK(WriteIpcPayload(payload, sink_, &metadata_length));
    RETURN_NOT_OK(UpdatePositionCheckAligned());
    return Status::OK();
  }

  Status Close() override {
    // Write 0 EOS message
    return Write(&kEos, sizeof(int32_t));
  }
};

/// A IpcPayloadWriter implementation that writes to a IPC file
/// (with a footer as defined in File.fbs)
class PayloadFileWriter : public internal::IpcPayloadWriter, protected StreamBookKeeper {
 public:
  PayloadFileWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
      : StreamBookKeeper(sink), schema_(schema) {}

  ~PayloadFileWriter() override = default;

  Status WritePayload(const internal::IpcPayload& payload) override {
#ifndef NDEBUG
    // Catch bug fixed in ARROW-3236
    RETURN_NOT_OK(UpdatePositionCheckAligned());
#endif

    // Metadata length must include padding, it's computed by WriteIpcPayload()
    FileBlock block = {position_, 0, payload.body_length};
    RETURN_NOT_OK(WriteIpcPayload(payload, sink_, &block.metadata_length));
    RETURN_NOT_OK(UpdatePositionCheckAligned());

    // Record position and size of some message types, to list them in the footer
    switch (payload.type) {
      case Message::DICTIONARY_BATCH:
        dictionaries_.push_back(block);
        break;
      case Message::RECORD_BATCH:
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
    RETURN_NOT_OK(Write(&kEos, sizeof(int32_t)));

    // Write file footer
    RETURN_NOT_OK(UpdatePosition());
    int64_t initial_position = position_;
    RETURN_NOT_OK(WriteFileFooter(*schema_, dictionaries_, record_batches_, sink_));

    // Write footer length
    RETURN_NOT_OK(UpdatePosition());
    int32_t footer_length = static_cast<int32_t>(position_ - initial_position);
    if (footer_length <= 0) {
      return Status::Invalid("Invalid file footer");
    }

    RETURN_NOT_OK(Write(&footer_length, sizeof(int32_t)));

    // Write magic bytes to end file
    return Write(kArrowMagicBytes, strlen(kArrowMagicBytes));
  }

 protected:
  std::shared_ptr<Schema> schema_;
  std::vector<FileBlock> dictionaries_;
  std::vector<FileBlock> record_batches_;
};

}  // namespace

class RecordBatchStreamWriter::RecordBatchStreamWriterImpl
    : public RecordBatchPayloadWriter {
 public:
  RecordBatchStreamWriterImpl(io::OutputStream* sink,
                              const std::shared_ptr<Schema>& schema,
                              const IpcOptions& options)
      : RecordBatchPayloadWriter(
            std::unique_ptr<internal::IpcPayloadWriter>(new PayloadStreamWriter(sink)),
            schema, options) {}

  ~RecordBatchStreamWriterImpl() = default;
};

class RecordBatchFileWriter::RecordBatchFileWriterImpl : public RecordBatchPayloadWriter {
 public:
  RecordBatchFileWriterImpl(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
                            const IpcOptions& options)
      : RecordBatchPayloadWriter(std::unique_ptr<internal::IpcPayloadWriter>(
                                     new PayloadFileWriter(sink, schema)),
                                 schema, options) {}

  ~RecordBatchFileWriterImpl() = default;
};

RecordBatchStreamWriter::RecordBatchStreamWriter() {}

RecordBatchStreamWriter::~RecordBatchStreamWriter() {}

Status RecordBatchStreamWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

void RecordBatchStreamWriter::set_memory_pool(MemoryPool* pool) {
  impl_->set_memory_pool(pool);
}

Status RecordBatchStreamWriter::Open(io::OutputStream* sink,
                                     const std::shared_ptr<Schema>& schema,
                                     std::shared_ptr<RecordBatchWriter>* out) {
  ASSIGN_OR_RAISE(*out, Open(sink, schema));
  return Status::OK();
}

Result<std::shared_ptr<RecordBatchWriter>> RecordBatchStreamWriter::Open(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcOptions& options) {
  // ctor is private
  auto result = std::shared_ptr<RecordBatchStreamWriter>(new RecordBatchStreamWriter());
  result->impl_.reset(new RecordBatchStreamWriterImpl(sink, schema, options));
  return std::move(result);
}

Result<std::shared_ptr<RecordBatchWriter>> RecordBatchStreamWriter::Open(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema) {
  auto options = IpcOptions::Defaults();
  return Open(sink, schema, options);
}

Status RecordBatchStreamWriter::Close() { return impl_->Close(); }

RecordBatchFileWriter::RecordBatchFileWriter() {}

RecordBatchFileWriter::~RecordBatchFileWriter() {}

Status RecordBatchFileWriter::Open(io::OutputStream* sink,
                                   const std::shared_ptr<Schema>& schema,
                                   std::shared_ptr<RecordBatchWriter>* out) {
  ASSIGN_OR_RAISE(*out, Open(sink, schema));
  return Status::OK();
}

Result<std::shared_ptr<RecordBatchWriter>> RecordBatchFileWriter::Open(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcOptions& options) {
  // ctor is private
  auto result = std::shared_ptr<RecordBatchFileWriter>(new RecordBatchFileWriter());
  result->file_impl_.reset(new RecordBatchFileWriterImpl(sink, schema, options));
  return std::move(result);
}

Result<std::shared_ptr<RecordBatchWriter>> RecordBatchFileWriter::Open(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema) {
  auto options = IpcOptions::Defaults();
  return Open(sink, schema, options);
}

Status RecordBatchFileWriter::WriteRecordBatch(const RecordBatch& batch) {
  return file_impl_->WriteRecordBatch(batch);
}

Status RecordBatchFileWriter::Close() { return file_impl_->Close(); }

namespace internal {

Status OpenRecordBatchWriter(std::unique_ptr<IpcPayloadWriter> sink,
                             const std::shared_ptr<Schema>& schema,
                             std::unique_ptr<RecordBatchWriter>* out) {
  auto options = IpcOptions::Defaults();
  ASSIGN_OR_RAISE(*out, OpenRecordBatchWriter(std::move(sink), schema, options));
  return Status::OK();
}

Result<std::unique_ptr<RecordBatchWriter>> OpenRecordBatchWriter(
    std::unique_ptr<IpcPayloadWriter> sink, const std::shared_ptr<Schema>& schema,
    const IpcOptions& options) {
  // XXX should we call Start()?
  return std::unique_ptr<RecordBatchWriter>(
      new RecordBatchPayloadWriter(std::move(sink), schema, options));
}

}  // namespace internal

// ----------------------------------------------------------------------
// Serialization public APIs

Status SerializeRecordBatch(const RecordBatch& batch, MemoryPool* pool,
                            std::shared_ptr<Buffer>* out) {
  int64_t size = 0;
  RETURN_NOT_OK(GetRecordBatchSize(batch, &size));
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(AllocateBuffer(pool, size, &buffer));

  io::FixedSizeBufferWriter stream(buffer);
  RETURN_NOT_OK(SerializeRecordBatch(batch, pool, &stream));
  *out = buffer;
  return Status::OK();
}

Status SerializeRecordBatch(const RecordBatch& batch, MemoryPool* pool,
                            io::OutputStream* out) {
  auto options = IpcOptions::Defaults();
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  return WriteRecordBatch(batch, 0, out, &metadata_length, &body_length, options, pool);
}

Status SerializeSchema(const Schema& schema, DictionaryMemo* dictionary_memo,
                       MemoryPool* pool, std::shared_ptr<Buffer>* out) {
  std::shared_ptr<io::BufferOutputStream> stream;
  RETURN_NOT_OK(io::BufferOutputStream::Create(1024, pool, &stream));

  auto options = IpcOptions::Defaults();
  auto payload_writer = make_unique<PayloadStreamWriter>(stream.get());
  RecordBatchPayloadWriter writer(std::move(payload_writer), schema, options,
                                  dictionary_memo);
  // Write schema and populate fields (but not dictionaries) in dictionary_memo
  RETURN_NOT_OK(writer.Start());
  return stream->Finish(out);
}

}  // namespace ipc
}  // namespace arrow
