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
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/util.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

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

class RecordBatchSerializer : public ArrayVisitor {
 public:
  RecordBatchSerializer(MemoryPool* pool, int64_t buffer_start_offset,
                        int max_recursion_depth, bool allow_64bit)
      : pool_(pool),
        max_recursion_depth_(max_recursion_depth),
        buffer_start_offset_(buffer_start_offset),
        allow_64bit_(allow_64bit) {
    DCHECK_GT(max_recursion_depth, 0);
  }

  virtual ~RecordBatchSerializer() = default;

  Status VisitArray(const Array& arr) {
    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    if (!allow_64bit_ && arr.length() > std::numeric_limits<int32_t>::max()) {
      return Status::Invalid("Cannot write arrays larger than 2^31 - 1 in length");
    }

    // push back all common elements
    field_nodes_.push_back({arr.length(), arr.null_count(), 0});

    if (arr.null_count() > 0) {
      std::shared_ptr<Buffer> bitmap;
      RETURN_NOT_OK(GetTruncatedBitmap(arr.offset(), arr.length(), arr.null_bitmap(),
                                       pool_, &bitmap));
      buffers_.push_back(bitmap);
    } else {
      // Push a dummy zero-length buffer, not to be copied
      buffers_.push_back(std::make_shared<Buffer>(nullptr, 0));
    }
    return arr.Accept(this);
  }

  Status Assemble(const RecordBatch& batch, int64_t* body_length) {
    if (field_nodes_.size() > 0) {
      field_nodes_.clear();
      buffer_meta_.clear();
      buffers_.clear();
    }

    // Perform depth-first traversal of the row-batch
    for (int i = 0; i < batch.num_columns(); ++i) {
      RETURN_NOT_OK(VisitArray(*batch.column(i)));
    }

    // The position for the start of a buffer relative to the passed frame of
    // reference. May be 0 or some other position in an address space
    int64_t offset = buffer_start_offset_;

    buffer_meta_.reserve(buffers_.size());

    const int32_t kNoPageId = -1;

    // Construct the buffer metadata for the record batch header
    for (size_t i = 0; i < buffers_.size(); ++i) {
      const Buffer* buffer = buffers_[i].get();
      int64_t size = 0;
      int64_t padding = 0;

      // The buffer might be null if we are handling zero row lengths.
      if (buffer) {
        size = buffer->size();
        padding = BitUtil::RoundUpToMultipleOf8(size) - size;
      }

      // TODO(wesm): We currently have no notion of shared memory page id's,
      // but we've included it in the metadata IDL for when we have it in the
      // future. Use page = -1 for now
      //
      // Note that page ids are a bespoke notion for Arrow and not a feature we
      // are using from any OS-level shared memory. The thought is that systems
      // may (in the future) associate integer page id's with physical memory
      // pages (according to whatever is the desired shared memory mechanism)
      buffer_meta_.push_back({kNoPageId, offset, size + padding});
      offset += size + padding;
    }

    *body_length = offset - buffer_start_offset_;
    DCHECK(BitUtil::IsMultipleOf8(*body_length));

    return Status::OK();
  }

  // Override this for writing dictionary metadata
  virtual Status WriteMetadataMessage(int64_t num_rows, int64_t body_length,
                                      std::shared_ptr<Buffer>* out) {
    return WriteRecordBatchMessage(num_rows, body_length, field_nodes_, buffer_meta_,
                                   out);
  }

  Status Write(const RecordBatch& batch, io::OutputStream* dst, int32_t* metadata_length,
               int64_t* body_length) {
    RETURN_NOT_OK(Assemble(batch, body_length));

#ifndef NDEBUG
    int64_t start_position, current_position;
    RETURN_NOT_OK(dst->Tell(&start_position));
#endif

    // Now that we have computed the locations of all of the buffers in shared
    // memory, the data header can be converted to a flatbuffer and written out
    //
    // Note: The memory written here is prefixed by the size of the flatbuffer
    // itself as an int32_t.
    std::shared_ptr<Buffer> metadata_fb;
    RETURN_NOT_OK(WriteMetadataMessage(batch.num_rows(), *body_length, &metadata_fb));
    RETURN_NOT_OK(WriteMessage(*metadata_fb, dst, metadata_length));

#ifndef NDEBUG
    RETURN_NOT_OK(dst->Tell(&current_position));
    DCHECK(BitUtil::IsMultipleOf8(current_position));
#endif

    // Now write the buffers
    for (size_t i = 0; i < buffers_.size(); ++i) {
      const Buffer* buffer = buffers_[i].get();
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
    RETURN_NOT_OK(dst->Tell(&current_position));
    DCHECK(BitUtil::IsMultipleOf8(current_position));
#endif

    return Status::OK();
  }

 protected:
  template <typename ArrayType>
  Status VisitFixedWidth(const ArrayType& array) {
    std::shared_ptr<Buffer> data = array.values();

    const auto& fw_type = static_cast<const FixedWidthType&>(*array.type());
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
    buffers_.push_back(data);
    return Status::OK();
  }

  template <typename ArrayType>
  Status GetZeroBasedValueOffsets(const ArrayType& array,
                                  std::shared_ptr<Buffer>* value_offsets) {
    // Share slicing logic between ListArray and BinaryArray

    auto offsets = array.value_offsets();

    if (array.offset() != 0) {
      // If we have a non-zero offset, then the value offsets do not start at
      // zero. We must a) create a new offsets array with shifted offsets and
      // b) slice the values array accordingly

      std::shared_ptr<Buffer> shifted_offsets;
      RETURN_NOT_OK(AllocateBuffer(pool_, sizeof(int32_t) * (array.length() + 1),
                                   &shifted_offsets));

      int32_t* dest_offsets = reinterpret_cast<int32_t*>(shifted_offsets->mutable_data());
      const int32_t start_offset = array.value_offset(0);

      for (int i = 0; i < array.length(); ++i) {
        dest_offsets[i] = array.value_offset(i) - start_offset;
      }
      // Final offset
      dest_offsets[array.length()] = array.value_offset(array.length()) - start_offset;
      offsets = shifted_offsets;
    }

    *value_offsets = offsets;
    return Status::OK();
  }

  Status VisitBinary(const BinaryArray& array) {
    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<BinaryArray>(array, &value_offsets));
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

    buffers_.push_back(value_offsets);
    buffers_.push_back(data);
    return Status::OK();
  }

  Status Visit(const BooleanArray& array) override {
    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(
        GetTruncatedBitmap(array.offset(), array.length(), array.values(), pool_, &data));
    buffers_.push_back(data);
    return Status::OK();
  }

#define VISIT_FIXED_WIDTH(TYPE) \
  Status Visit(const TYPE& array) override { return VisitFixedWidth<TYPE>(array); }

  VISIT_FIXED_WIDTH(Int8Array);
  VISIT_FIXED_WIDTH(Int16Array);
  VISIT_FIXED_WIDTH(Int32Array);
  VISIT_FIXED_WIDTH(Int64Array);
  VISIT_FIXED_WIDTH(UInt8Array);
  VISIT_FIXED_WIDTH(UInt16Array);
  VISIT_FIXED_WIDTH(UInt32Array);
  VISIT_FIXED_WIDTH(UInt64Array);
  VISIT_FIXED_WIDTH(HalfFloatArray);
  VISIT_FIXED_WIDTH(FloatArray);
  VISIT_FIXED_WIDTH(DoubleArray);
  VISIT_FIXED_WIDTH(Date32Array);
  VISIT_FIXED_WIDTH(Date64Array);
  VISIT_FIXED_WIDTH(TimestampArray);
  VISIT_FIXED_WIDTH(Time32Array);
  VISIT_FIXED_WIDTH(Time64Array);
  VISIT_FIXED_WIDTH(FixedSizeBinaryArray);

#undef VISIT_FIXED_WIDTH

  Status Visit(const StringArray& array) override { return VisitBinary(array); }

  Status Visit(const BinaryArray& array) override { return VisitBinary(array); }

  Status Visit(const ListArray& array) override {
    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<ListArray>(array, &value_offsets));
    buffers_.push_back(value_offsets);

    --max_recursion_depth_;
    std::shared_ptr<Array> values = array.values();

    int32_t values_offset = 0;
    int32_t values_length = 0;
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

  Status Visit(const StructArray& array) override {
    --max_recursion_depth_;
    for (int i = 0; i < array.num_fields(); ++i) {
      std::shared_ptr<Array> field = array.field(i);
      if (array.offset() != 0 || array.length() < field->length()) {
        // If offset is non-zero, slice the child array
        field = field->Slice(array.offset(), array.length());
      }
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
    buffers_.push_back(type_ids);

    --max_recursion_depth_;
    if (array.mode() == UnionMode::DENSE) {
      const auto& type = static_cast<const UnionType&>(*array.type());

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
      std::vector<int32_t> child_offsets(max_code + 1);
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

        for (int64_t i = 0; i < length; ++i) {
          const uint8_t code = type_ids[i];
          int32_t shift = child_offsets[code];
          if (shift == -1) {
            child_offsets[code] = shift = unshifted_offsets[i];
          }
          shifted_offsets[i] = unshifted_offsets[i] - shift;

          // Update the child length to account for observed value
          ++child_lengths[code];
        }

        value_offsets = shifted_offsets_buffer;
      }
      buffers_.push_back(value_offsets);

      // Visit children and slice accordingly
      for (int i = 0; i < type.num_children(); ++i) {
        std::shared_ptr<Array> child = array.child(i);

        // TODO: ARROW-809, for sliced unions, tricky to know how much to
        // truncate the children. For now, we are truncating the children to be
        // no longer than the parent union
        const uint8_t code = type.type_codes()[i];
        const int64_t child_length = child_lengths[code];
        if (offset != 0 || length < child_length) {
          child = child->Slice(child_offsets[code], std::min(length, child_length));
        }
        RETURN_NOT_OK(VisitArray(*child));
      }
    } else {
      for (int i = 0; i < array.num_fields(); ++i) {
        std::shared_ptr<Array> child = array.child(i);

        // Sparse union, slicing is simpler
        if (offset != 0 || length < child->length()) {
          // If offset is non-zero, slice the child array
          child = child->Slice(offset, length);
        }
        RETURN_NOT_OK(VisitArray(*child));
      }
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) override {
    // Dictionary written out separately. Slice offset contained in the indices
    return array.indices()->Accept(this);
  }

  // In some cases, intermediate buffers may need to be allocated (with sliced arrays)
  MemoryPool* pool_;

  std::vector<FieldMetadata> field_nodes_;
  std::vector<BufferMetadata> buffer_meta_;
  std::vector<std::shared_ptr<Buffer>> buffers_;

  int64_t max_recursion_depth_;
  int64_t buffer_start_offset_;
  bool allow_64bit_;
};

class DictionaryWriter : public RecordBatchSerializer {
 public:
  using RecordBatchSerializer::RecordBatchSerializer;

  Status WriteMetadataMessage(int64_t num_rows, int64_t body_length,
                              std::shared_ptr<Buffer>* out) override {
    return WriteDictionaryMessage(dictionary_id_, num_rows, body_length, field_nodes_,
                                  buffer_meta_, out);
  }

  Status Write(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
               io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length) {
    dictionary_id_ = dictionary_id;

    // Make a dummy record batch. A bit tedious as we have to make a schema
    std::vector<std::shared_ptr<Field>> fields = {
        arrow::field("dictionary", dictionary->type())};
    auto schema = std::make_shared<Schema>(fields);
    RecordBatch batch(schema, dictionary->length(), {dictionary});

    return RecordBatchSerializer::Write(batch, dst, metadata_length, body_length);
  }

 private:
  // TODO(wesm): Setting this in Write is a bit unclean, but it works
  int64_t dictionary_id_;
};

// Adds padding bytes if necessary to ensure all memory blocks are written on
// 64-byte boundaries.
Status AlignStreamPosition(io::OutputStream* stream) {
  int64_t position;
  RETURN_NOT_OK(stream->Tell(&position));
  int64_t remainder = PaddedLength(position) - position;
  if (remainder > 0) {
    return stream->Write(kPaddingBytes, remainder);
  }
  return Status::OK();
}

Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
                        io::OutputStream* dst, int32_t* metadata_length,
                        int64_t* body_length, MemoryPool* pool, int max_recursion_depth,
                        bool allow_64bit) {
  RecordBatchSerializer writer(pool, buffer_start_offset, max_recursion_depth,
                               allow_64bit);
  return writer.Write(batch, dst, metadata_length, body_length);
}

Status WriteRecordBatchStream(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                              io::OutputStream* dst) {
  std::shared_ptr<RecordBatchWriter> writer;
  RETURN_NOT_OK(RecordBatchStreamWriter::Open(dst, batches[0]->schema(), &writer));
  for (const auto& batch : batches) {
    // allow sizes > INT32_MAX
    DCHECK(batch->schema()->Equals(*batches[0]->schema())) << "Schemas unequal";
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch, true));
  }
  RETURN_NOT_OK(writer->Close());
  return Status::OK();
}

Status WriteLargeRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
                             io::OutputStream* dst, int32_t* metadata_length,
                             int64_t* body_length, MemoryPool* pool) {
  return WriteRecordBatch(batch, buffer_start_offset, dst, metadata_length, body_length,
                          pool, kMaxNestingDepth, true);
}

static Status WriteStridedTensorData(int dim_index, int64_t offset, int elem_size,
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

Status WriteTensorHeader(const Tensor& tensor, io::OutputStream* dst,
                         int32_t* metadata_length, int64_t* body_length) {
  RETURN_NOT_OK(AlignStreamPosition(dst));
  std::shared_ptr<Buffer> metadata;
  RETURN_NOT_OK(WriteTensorMessage(tensor, 0, &metadata));
  return WriteMessage(*metadata, dst, metadata_length);
}

Status WriteTensor(const Tensor& tensor, io::OutputStream* dst, int32_t* metadata_length,
                   int64_t* body_length) {
  if (tensor.is_contiguous()) {
    RETURN_NOT_OK(WriteTensorHeader(tensor, dst, metadata_length, body_length));
    auto data = tensor.data();
    if (data) {
      *body_length = data->size();
      return dst->Write(data->data(), *body_length);
    } else {
      *body_length = 0;
      return Status::OK();
    }
  } else {
    Tensor dummy(tensor.type(), tensor.data(), tensor.shape());
    const auto& type = static_cast<const FixedWidthType&>(*tensor.type());
    RETURN_NOT_OK(WriteTensorHeader(dummy, dst, metadata_length, body_length));

    const int elem_size = type.bit_width() / 8;

    // TODO(wesm): Do we care enough about this temporary allocation to pass in
    // a MemoryPool to this function?
    std::shared_ptr<Buffer> scratch_space;
    RETURN_NOT_OK(AllocateBuffer(default_memory_pool(),
                                 tensor.shape()[tensor.ndim() - 1] * elem_size,
                                 &scratch_space));

    return WriteStridedTensorData(0, 0, elem_size, tensor, scratch_space->mutable_data(),
                                  dst);
  }
}

Status WriteDictionary(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
                       int64_t buffer_start_offset, io::OutputStream* dst,
                       int32_t* metadata_length, int64_t* body_length, MemoryPool* pool) {
  DictionaryWriter writer(pool, buffer_start_offset, kMaxNestingDepth, false);
  return writer.Write(dictionary_id, dictionary, dst, metadata_length, body_length);
}

Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size) {
  // emulates the behavior of Write without actually writing
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  io::MockOutputStream dst;
  RETURN_NOT_OK(WriteRecordBatch(batch, 0, &dst, &metadata_length, &body_length,
                                 default_memory_pool(), kMaxNestingDepth, true));
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

// ----------------------------------------------------------------------
// Stream writer implementation

class StreamBookKeeper {
 public:
  StreamBookKeeper() : sink_(nullptr), position_(-1) {}
  explicit StreamBookKeeper(io::OutputStream* sink) : sink_(sink), position_(-1) {}

  Status UpdatePosition() { return sink_->Tell(&position_); }

  Status Align(int64_t alignment = kArrowIpcAlignment) {
    // Adds padding bytes if necessary to ensure all memory blocks are written on
    // 8-byte (or other alignment) boundaries.
    int64_t remainder = PaddedLength(position_, alignment) - position_;
    if (remainder > 0) {
      return Write(kPaddingBytes, remainder);
    }
    return Status::OK();
  }

  // Write data and update position
  Status Write(const uint8_t* data, int64_t nbytes) {
    RETURN_NOT_OK(sink_->Write(data, nbytes));
    position_ += nbytes;
    return Status::OK();
  }

 protected:
  io::OutputStream* sink_;
  int64_t position_;
};

class SchemaWriter : public StreamBookKeeper {
 public:
  SchemaWriter(const Schema& schema, DictionaryMemo* dictionary_memo, MemoryPool* pool,
               io::OutputStream* sink)
      : StreamBookKeeper(sink), schema_(schema), dictionary_memo_(dictionary_memo) {}

  Status WriteSchema() {
    std::shared_ptr<Buffer> schema_fb;
    RETURN_NOT_OK(WriteSchemaMessage(schema_, dictionary_memo_, &schema_fb));

    int32_t metadata_length = 0;
    RETURN_NOT_OK(WriteMessage(*schema_fb, sink_, &metadata_length));
    RETURN_NOT_OK(UpdatePosition());
    DCHECK_EQ(0, position_ % 8) << "WriteSchema did not perform an aligned write";
    return Status::OK();
  }

  Status WriteDictionaries(std::vector<FileBlock>* dictionaries) {
    const DictionaryMap& id_to_dictionary = dictionary_memo_->id_to_dictionary();

    dictionaries->resize(id_to_dictionary.size());

    // TODO(wesm): does sorting by id yield any benefit?
    int dict_index = 0;
    for (const auto& entry : id_to_dictionary) {
      FileBlock* block = &(*dictionaries)[dict_index++];

      block->offset = position_;

      // Frame of reference in file format is 0, see ARROW-384
      const int64_t buffer_start_offset = 0;
      RETURN_NOT_OK(WriteDictionary(entry.first, entry.second, buffer_start_offset, sink_,
                                    &block->metadata_length, &block->body_length, pool_));
      RETURN_NOT_OK(UpdatePosition());
      DCHECK(position_ % 8 == 0) << "WriteDictionary did not perform aligned writes";
    }

    return Status::OK();
  }

  Status Write(std::vector<FileBlock>* dictionaries) {
    RETURN_NOT_OK(WriteSchema());

    // If there are any dictionaries, write them as the next messages
    return WriteDictionaries(dictionaries);
  }

 private:
  MemoryPool* pool_;
  const Schema& schema_;
  DictionaryMemo* dictionary_memo_;
};

class RecordBatchStreamWriter::RecordBatchStreamWriterImpl : public StreamBookKeeper {
 public:
  RecordBatchStreamWriterImpl(io::OutputStream* sink,
                              const std::shared_ptr<Schema>& schema)
      : StreamBookKeeper(sink),
        schema_(schema),
        pool_(default_memory_pool()),
        started_(false) {}

  virtual ~RecordBatchStreamWriterImpl() = default;

  virtual Status Start() {
    SchemaWriter schema_writer(*schema_, &dictionary_memo_, pool_, sink_);
    RETURN_NOT_OK(schema_writer.Write(&dictionaries_));
    started_ = true;
    return Status::OK();
  }

  virtual Status Close() {
    // Write the schema if not already written
    // User is responsible for closing the OutputStream
    RETURN_NOT_OK(CheckStarted());

    // Write 0 EOS message
    const int32_t kEos = 0;
    return Write(reinterpret_cast<const uint8_t*>(&kEos), sizeof(int32_t));
  }

  Status CheckStarted() {
    if (!started_) {
      return Start();
    }
    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit, FileBlock* block) {
    RETURN_NOT_OK(CheckStarted());
    RETURN_NOT_OK(UpdatePosition());

    block->offset = position_;

    // Frame of reference in file format is 0, see ARROW-384
    const int64_t buffer_start_offset = 0;
    RETURN_NOT_OK(arrow::ipc::WriteRecordBatch(
        batch, buffer_start_offset, sink_, &block->metadata_length, &block->body_length,
        pool_, kMaxNestingDepth, allow_64bit));
    RETURN_NOT_OK(UpdatePosition());

    DCHECK(position_ % 8 == 0) << "WriteRecordBatch did not perform aligned writes";

    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit) {
    // Push an empty FileBlock. Can be written in the footer later
    record_batches_.push_back({0, 0, 0});
    return WriteRecordBatch(batch, allow_64bit,
                            &record_batches_[record_batches_.size() - 1]);
  }

  void set_memory_pool(MemoryPool* pool) { pool_ = pool; }

 protected:
  std::shared_ptr<Schema> schema_;
  MemoryPool* pool_;
  bool started_;

  // When writing out the schema, we keep track of all the dictionaries we
  // encounter, as they must be written out first in the stream
  DictionaryMemo dictionary_memo_;

  std::vector<FileBlock> dictionaries_;
  std::vector<FileBlock> record_batches_;
};

RecordBatchStreamWriter::RecordBatchStreamWriter() {}

RecordBatchStreamWriter::~RecordBatchStreamWriter() {}

Status RecordBatchStreamWriter::WriteRecordBatch(const RecordBatch& batch,
                                                 bool allow_64bit) {
  return impl_->WriteRecordBatch(batch, allow_64bit);
}

void RecordBatchStreamWriter::set_memory_pool(MemoryPool* pool) {
  impl_->set_memory_pool(pool);
}

Status RecordBatchStreamWriter::Open(io::OutputStream* sink,
                                     const std::shared_ptr<Schema>& schema,
                                     std::shared_ptr<RecordBatchWriter>* out) {
  // ctor is private
  auto result = std::shared_ptr<RecordBatchStreamWriter>(new RecordBatchStreamWriter());
  result->impl_.reset(new RecordBatchStreamWriterImpl(sink, schema));
  *out = result;
  return Status::OK();
}

#ifndef ARROW_NO_DEPRECATED_API
Status RecordBatchStreamWriter::Open(io::OutputStream* sink,
                                     const std::shared_ptr<Schema>& schema,
                                     std::shared_ptr<RecordBatchStreamWriter>* out) {
  // ctor is private
  *out = std::shared_ptr<RecordBatchStreamWriter>(new RecordBatchStreamWriter());
  (*out)->impl_.reset(new RecordBatchStreamWriterImpl(sink, schema));
  return Status::OK();
}
#endif

Status RecordBatchStreamWriter::Close() { return impl_->Close(); }

// ----------------------------------------------------------------------
// File writer implementation

class RecordBatchFileWriter::RecordBatchFileWriterImpl
    : public RecordBatchStreamWriter::RecordBatchStreamWriterImpl {
 public:
  using BASE = RecordBatchStreamWriter::RecordBatchStreamWriterImpl;

  RecordBatchFileWriterImpl(io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
      : BASE(sink, schema) {}

  Status Start() override {
    // It is only necessary to align to 8-byte boundary at the start of the file
    RETURN_NOT_OK(Write(reinterpret_cast<const uint8_t*>(kArrowMagicBytes),
                        strlen(kArrowMagicBytes)));
    RETURN_NOT_OK(Align());

    // We write the schema at the start of the file (and the end). This also
    // writes all the dictionaries at the beginning of the file
    return BASE::Start();
  }

  Status Close() override {
    // Write metadata
    RETURN_NOT_OK(UpdatePosition());

    int64_t initial_position = position_;
    RETURN_NOT_OK(WriteFileFooter(*schema_, dictionaries_, record_batches_,
                                  &dictionary_memo_, sink_));
    RETURN_NOT_OK(UpdatePosition());

    // Write footer length
    int32_t footer_length = static_cast<int32_t>(position_ - initial_position);

    if (footer_length <= 0) {
      return Status::Invalid("Invalid file footer");
    }

    RETURN_NOT_OK(
        Write(reinterpret_cast<const uint8_t*>(&footer_length), sizeof(int32_t)));

    // Write magic bytes to end file
    return Write(reinterpret_cast<const uint8_t*>(kArrowMagicBytes),
                 strlen(kArrowMagicBytes));
  }
};

RecordBatchFileWriter::RecordBatchFileWriter() {}

RecordBatchFileWriter::~RecordBatchFileWriter() {}

Status RecordBatchFileWriter::Open(io::OutputStream* sink,
                                   const std::shared_ptr<Schema>& schema,
                                   std::shared_ptr<RecordBatchWriter>* out) {
  // ctor is private
  auto result = std::shared_ptr<RecordBatchFileWriter>(new RecordBatchFileWriter());
  result->impl_.reset(new RecordBatchFileWriterImpl(sink, schema));
  *out = result;
  return Status::OK();
}

#ifndef ARROW_NO_DEPRECATED_API
Status RecordBatchFileWriter::Open(io::OutputStream* sink,
                                   const std::shared_ptr<Schema>& schema,
                                   std::shared_ptr<RecordBatchFileWriter>* out) {
  // ctor is private
  *out = std::shared_ptr<RecordBatchFileWriter>(new RecordBatchFileWriter());
  (*out)->impl_.reset(new RecordBatchFileWriterImpl(sink, schema));
  return Status::OK();
}
#endif

Status RecordBatchFileWriter::WriteRecordBatch(const RecordBatch& batch,
                                               bool allow_64bit) {
  return impl_->WriteRecordBatch(batch, allow_64bit);
}

Status RecordBatchFileWriter::Close() { return impl_->Close(); }

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
  int32_t metadata_length = 0;
  int64_t body_length = 0;
  return WriteRecordBatch(batch, 0, out, &metadata_length, &body_length, pool,
                          kMaxNestingDepth, true);
}

Status SerializeSchema(const Schema& schema, MemoryPool* pool,
                       std::shared_ptr<Buffer>* out) {
  std::shared_ptr<io::BufferOutputStream> stream;
  RETURN_NOT_OK(io::BufferOutputStream::Create(1024, pool, &stream));

  DictionaryMemo memo;
  SchemaWriter schema_writer(schema, &memo, pool, stream.get());

  // Unused
  std::vector<FileBlock> dictionary_blocks;

  RETURN_NOT_OK(schema_writer.Write(&dictionary_blocks));
  return stream->Finish(out);
}

}  // namespace ipc
}  // namespace arrow
