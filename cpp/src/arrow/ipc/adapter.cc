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

#include "arrow/ipc/adapter.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

// ----------------------------------------------------------------------
// Record batch write path

class RecordBatchWriter : public ArrayVisitor {
 public:
  RecordBatchWriter(MemoryPool* pool, const RecordBatch& batch,
      int64_t buffer_start_offset, int max_recursion_depth)
      : pool_(pool),
        batch_(batch),
        max_recursion_depth_(max_recursion_depth),
        buffer_start_offset_(buffer_start_offset) {}

  Status VisitArray(const Array& arr) {
    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }
    // push back all common elements
    field_nodes_.push_back(flatbuf::FieldNode(arr.length(), arr.null_count()));
    if (arr.null_count() > 0) {
      std::shared_ptr<Buffer> bitmap = arr.null_bitmap();

      if (arr.offset() != 0) {
        // With a sliced array / non-zero offset, we must copy the bitmap
        RETURN_NOT_OK(
            CopyBitmap(pool_, bitmap->data(), arr.offset(), arr.length(), &bitmap));
      }

      buffers_.push_back(bitmap);
    } else {
      // Push a dummy zero-length buffer, not to be copied
      buffers_.push_back(std::make_shared<Buffer>(nullptr, 0));
    }
    return arr.Accept(this);
  }

  Status Assemble(int64_t* body_length) {
    if (field_nodes_.size() > 0) {
      field_nodes_.clear();
      buffer_meta_.clear();
      buffers_.clear();
    }

    // Perform depth-first traversal of the row-batch
    for (int i = 0; i < batch_.num_columns(); ++i) {
      RETURN_NOT_OK(VisitArray(*batch_.column(i)));
    }

    // The position for the start of a buffer relative to the passed frame of
    // reference. May be 0 or some other position in an address space
    int64_t offset = buffer_start_offset_;

    // Construct the buffer metadata for the record batch header
    for (size_t i = 0; i < buffers_.size(); ++i) {
      const Buffer* buffer = buffers_[i].get();
      int64_t size = 0;
      int64_t padding = 0;

      // The buffer might be null if we are handling zero row lengths.
      if (buffer) {
        size = buffer->size();
        padding = BitUtil::RoundUpToMultipleOf64(size) - size;
      }

      // TODO(wesm): We currently have no notion of shared memory page id's,
      // but we've included it in the metadata IDL for when we have it in the
      // future. Use page = -1 for now
      //
      // Note that page ids are a bespoke notion for Arrow and not a feature we
      // are using from any OS-level shared memory. The thought is that systems
      // may (in the future) associate integer page id's with physical memory
      // pages (according to whatever is the desired shared memory mechanism)
      buffer_meta_.push_back(flatbuf::Buffer(-1, offset, size + padding));
      offset += size + padding;
    }

    *body_length = offset - buffer_start_offset_;
    DCHECK(BitUtil::IsMultipleOf64(*body_length));

    return Status::OK();
  }

  Status WriteMetadata(
      int64_t body_length, io::OutputStream* dst, int32_t* metadata_length) {
    // Now that we have computed the locations of all of the buffers in shared
    // memory, the data header can be converted to a flatbuffer and written out
    //
    // Note: The memory written here is prefixed by the size of the flatbuffer
    // itself as an int32_t.
    std::shared_ptr<Buffer> metadata_fb;
    RETURN_NOT_OK(WriteRecordBatchMetadata(
        batch_.num_rows(), body_length, field_nodes_, buffer_meta_, &metadata_fb));

    // Need to write 4 bytes (metadata size), the metadata, plus padding to
    // end on an 8-byte offset
    int64_t start_offset;
    RETURN_NOT_OK(dst->Tell(&start_offset));

    int64_t padded_metadata_length = metadata_fb->size() + 4;
    const int remainder = (padded_metadata_length + start_offset) % 8;
    if (remainder != 0) { padded_metadata_length += 8 - remainder; }

    // The returned metadata size includes the length prefix, the flatbuffer,
    // plus padding
    *metadata_length = static_cast<int32_t>(padded_metadata_length);

    // Write the flatbuffer size prefix including padding
    int32_t flatbuffer_size = padded_metadata_length - 4;
    RETURN_NOT_OK(
        dst->Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

    // Write the flatbuffer
    RETURN_NOT_OK(dst->Write(metadata_fb->data(), metadata_fb->size()));

    // Write any padding
    int64_t padding = padded_metadata_length - metadata_fb->size() - 4;
    if (padding > 0) { RETURN_NOT_OK(dst->Write(kPaddingBytes, padding)); }

    return Status::OK();
  }

  Status Write(io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length) {
    RETURN_NOT_OK(Assemble(body_length));

#ifndef NDEBUG
    int64_t start_position, current_position;
    RETURN_NOT_OK(dst->Tell(&start_position));
#endif

    RETURN_NOT_OK(WriteMetadata(*body_length, dst, metadata_length));

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
        padding = BitUtil::RoundUpToMultipleOf64(size) - size;
      }

      if (size > 0) { RETURN_NOT_OK(dst->Write(buffer->data(), size)); }

      if (padding > 0) { RETURN_NOT_OK(dst->Write(kPaddingBytes, padding)); }
    }

#ifndef NDEBUG
    RETURN_NOT_OK(dst->Tell(&current_position));
    DCHECK(BitUtil::IsMultipleOf8(current_position));
#endif

    return Status::OK();
  }

  Status GetTotalSize(int64_t* size) {
    // emulates the behavior of Write without actually writing
    int32_t metadata_length = 0;
    int64_t body_length = 0;
    MockOutputStream dst;
    RETURN_NOT_OK(Write(&dst, &metadata_length, &body_length));
    *size = dst.GetExtentBytesWritten();
    return Status::OK();
  }

 private:
  Status Visit(const NullArray& array) override { return Status::NotImplemented("null"); }

  template <typename ArrayType>
  Status VisitFixedWidth(const ArrayType& array) {
    std::shared_ptr<Buffer> data_buffer = array.data();

    if (array.offset() != 0) {
      // Non-zero offset, slice the buffer
      const auto& fw_type = static_cast<const FixedWidthType&>(*array.type());
      const int type_width = fw_type.bit_width() / 8;
      const int64_t byte_offset = array.offset() * type_width;

      // Send padding if it's available
      const int64_t buffer_length =
          std::min(BitUtil::RoundUpToMultipleOf64(array.length() * type_width),
              data_buffer->size() - byte_offset);
      data_buffer = SliceBuffer(data_buffer, byte_offset, buffer_length);
    }
    buffers_.push_back(data_buffer);
    return Status::OK();
  }

  template <typename ArrayType>
  Status GetZeroBasedValueOffsets(
      const ArrayType& array, std::shared_ptr<Buffer>* value_offsets) {
    // Share slicing logic between ListArray and BinaryArray

    auto offsets = array.value_offsets();

    if (array.offset() != 0) {
      // If we have a non-zero offset, then the value offsets do not start at
      // zero. We must a) create a new offsets array with shifted offsets and
      // b) slice the values array accordingly

      std::shared_ptr<MutableBuffer> shifted_offsets;
      RETURN_NOT_OK(AllocateBuffer(
          pool_, sizeof(int32_t) * (array.length() + 1), &shifted_offsets));

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
    auto data = array.data();

    if (array.offset() != 0) {
      // Slice the data buffer to include only the range we need now
      data = SliceBuffer(data, array.value_offset(0), array.value_offset(array.length()));
    }

    buffers_.push_back(value_offsets);
    buffers_.push_back(data);
    return Status::OK();
  }

  Status Visit(const BooleanArray& array) override {
    buffers_.push_back(array.data());
    return Status::OK();
  }

  Status Visit(const Int8Array& array) override {
    return VisitFixedWidth<Int8Array>(array);
  }

  Status Visit(const Int16Array& array) override {
    return VisitFixedWidth<Int16Array>(array);
  }

  Status Visit(const Int32Array& array) override {
    return VisitFixedWidth<Int32Array>(array);
  }

  Status Visit(const Int64Array& array) override {
    return VisitFixedWidth<Int64Array>(array);
  }

  Status Visit(const UInt8Array& array) override {
    return VisitFixedWidth<UInt8Array>(array);
  }

  Status Visit(const UInt16Array& array) override {
    return VisitFixedWidth<UInt16Array>(array);
  }

  Status Visit(const UInt32Array& array) override {
    return VisitFixedWidth<UInt32Array>(array);
  }

  Status Visit(const UInt64Array& array) override {
    return VisitFixedWidth<UInt64Array>(array);
  }

  Status Visit(const HalfFloatArray& array) override {
    return VisitFixedWidth<HalfFloatArray>(array);
  }

  Status Visit(const FloatArray& array) override {
    return VisitFixedWidth<FloatArray>(array);
  }

  Status Visit(const DoubleArray& array) override {
    return VisitFixedWidth<DoubleArray>(array);
  }

  Status Visit(const StringArray& array) override { return VisitBinary(array); }

  Status Visit(const BinaryArray& array) override { return VisitBinary(array); }

  Status Visit(const DateArray& array) override {
    return VisitFixedWidth<DateArray>(array);
  }

  Status Visit(const TimeArray& array) override {
    return VisitFixedWidth<TimeArray>(array);
  }

  Status Visit(const TimestampArray& array) override {
    return VisitFixedWidth<TimestampArray>(array);
  }

  Status Visit(const IntervalArray& array) override {
    return Status::NotImplemented("interval");
  }

  Status Visit(const DecimalArray& array) override {
    return Status::NotImplemented("decimal");
  }

  Status Visit(const ListArray& array) override {
    std::shared_ptr<Buffer> value_offsets;
    RETURN_NOT_OK(GetZeroBasedValueOffsets<ListArray>(array, &value_offsets));
    buffers_.push_back(value_offsets);

    --max_recursion_depth_;
    std::shared_ptr<Array> values = array.values();

    if (array.offset() != 0) {
      // For non-zero offset, we slice the values array accordingly
      const int32_t offset = array.value_offset(0);
      const int32_t length = array.value_offset(array.length()) - offset;
      values = values->Slice(offset, length);
    }
    RETURN_NOT_OK(VisitArray(*values));
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const StructArray& array) override {
    --max_recursion_depth_;
    for (std::shared_ptr<Array> field : array.fields()) {
      if (array.offset() != 0) {
        // If offset is non-zero, slice the child array
        field = field->Slice(array.offset(), array.length());
      }
      RETURN_NOT_OK(VisitArray(*field));
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const UnionArray& array) override {
    auto type_ids = array.type_ids();
    if (array.offset() != 0) {
      type_ids = SliceBuffer(type_ids, array.offset() * sizeof(UnionArray::type_id_t),
          array.length() * sizeof(UnionArray::type_id_t));
    }

    buffers_.push_back(type_ids);

    --max_recursion_depth_;
    if (array.mode() == UnionMode::DENSE) {
      const auto& type = static_cast<const UnionType&>(*array.type());
      auto value_offsets = array.value_offsets();

      // The Union type codes are not necessary 0-indexed
      uint8_t max_code = 0;
      for (uint8_t code : type.type_codes) {
        if (code > max_code) { max_code = code; }
      }

      // Allocate an array of child offsets. Set all to -1 to indicate that we
      // haven't observed a first occurrence of a particular child yet
      std::vector<int32_t> child_offsets(max_code + 1);
      std::vector<int32_t> child_lengths(max_code + 1, 0);

      if (array.offset() != 0) {
        // This is an unpleasant case. Because the offsets are different for
        // each child array, when we have a sliced array, we need to "rebase"
        // the value_offsets for each array

        const int32_t* unshifted_offsets = array.raw_value_offsets();
        const uint8_t* type_ids = array.raw_type_ids();

        // Allocate the shifted offsets
        std::shared_ptr<MutableBuffer> shifted_offsets_buffer;
        RETURN_NOT_OK(AllocateBuffer(
            pool_, array.length() * sizeof(int32_t), &shifted_offsets_buffer));
        int32_t* shifted_offsets =
            reinterpret_cast<int32_t*>(shifted_offsets_buffer->mutable_data());

        for (int32_t i = 0; i < array.length(); ++i) {
          const uint8_t code = type_ids[i];
          int32_t shift = child_offsets[code];
          if (shift == -1) { child_offsets[code] = shift = unshifted_offsets[i]; }
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
        if (array.offset() != 0) {
          const uint8_t code = type.type_codes[i];
          child = child->Slice(child_offsets[code], child_lengths[code]);
        }
        RETURN_NOT_OK(VisitArray(*child));
      }
    } else {
      for (std::shared_ptr<Array> child : array.children()) {
        // Sparse union, slicing is simpler
        if (array.offset() != 0) {
          // If offset is non-zero, slice the child array
          child = child->Slice(array.offset(), array.length());
        }
        RETURN_NOT_OK(VisitArray(*child));
      }
    }
    ++max_recursion_depth_;
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) override {
    // Dictionary written out separately
    const auto& indices = static_cast<const PrimitiveArray&>(*array.indices().get());
    buffers_.push_back(indices.data());
    return Status::OK();
  }

  // In some cases, intermediate buffers may need to be allocated (with sliced arrays)
  MemoryPool* pool_;
  const RecordBatch& batch_;

  std::vector<flatbuf::FieldNode> field_nodes_;
  std::vector<flatbuf::Buffer> buffer_meta_;
  std::vector<std::shared_ptr<Buffer>> buffers_;

  int64_t max_recursion_depth_;
  int64_t buffer_start_offset_;
};

Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
    io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length,
    MemoryPool* pool, int max_recursion_depth) {
  DCHECK_GT(max_recursion_depth, 0);
  RecordBatchWriter serializer(pool, batch, buffer_start_offset, max_recursion_depth);
  return serializer.Write(dst, metadata_length, body_length);
}

Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size) {
  RecordBatchWriter serializer(default_memory_pool(), batch, 0, kMaxIpcRecursionDepth);
  RETURN_NOT_OK(serializer.GetTotalSize(size));
  return Status::OK();
}

// ----------------------------------------------------------------------
// Record batch read path

struct RecordBatchContext {
  const RecordBatchMetadata* metadata;
  int buffer_index;
  int field_index;
  int max_recursion_depth;
};

// Traverse the flattened record batch metadata and reassemble the
// corresponding array containers
class ArrayLoader : public TypeVisitor {
 public:
  ArrayLoader(
      const Field& field, RecordBatchContext* context, io::ReadableFileInterface* file)
      : field_(field), context_(context), file_(file) {}

  Status Load(std::shared_ptr<Array>* out) {
    if (context_->max_recursion_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    // Load the array
    RETURN_NOT_OK(field_.type->Accept(this));

    *out = std::move(result_);
    return Status::OK();
  }

 private:
  const Field& field_;
  RecordBatchContext* context_;
  io::ReadableFileInterface* file_;

  // Used in visitor pattern
  std::shared_ptr<Array> result_;

  Status LoadChild(const Field& field, std::shared_ptr<Array>* out) {
    ArrayLoader loader(field, context_, file_);
    --context_->max_recursion_depth;
    RETURN_NOT_OK(loader.Load(out));
    ++context_->max_recursion_depth;
    return Status::OK();
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    BufferMetadata metadata = context_->metadata->buffer(buffer_index);

    if (metadata.length == 0) {
      *out = nullptr;
      return Status::OK();
    } else {
      return file_->ReadAt(metadata.offset, metadata.length, out);
    }
  }

  Status LoadCommon(FieldMetadata* field_meta, std::shared_ptr<Buffer>* null_bitmap) {
    // pop off a field
    if (context_->field_index >= context_->metadata->num_fields()) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }

    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    *field_meta = context_->metadata->field(context_->field_index++);

    // extract null_bitmap which is common to all arrays
    if (field_meta->null_count == 0) {
      *null_bitmap = nullptr;
    } else {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, null_bitmap));
    }
    context_->buffer_index++;
    return Status::OK();
  }

  Status LoadPrimitive(const DataType& type) {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));

    std::shared_ptr<Buffer> data;
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &data));
    } else {
      context_->buffer_index++;
      data.reset(new Buffer(nullptr, 0));
    }
    return MakePrimitiveArray(field_.type, field_meta.length, data, null_bitmap,
        field_meta.null_count, 0, &result_);
  }

  template <typename CONTAINER>
  Status LoadBinary() {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));

    std::shared_ptr<Buffer> offsets;
    std::shared_ptr<Buffer> values;
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &offsets));
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &values));
    } else {
      context_->buffer_index += 2;
      offsets = values = nullptr;
    }

    result_ = std::make_shared<CONTAINER>(
        field_meta.length, offsets, values, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const NullType& type) override { return Status::NotImplemented("null"); }

  Status Visit(const BooleanType& type) override { return LoadPrimitive(type); }

  Status Visit(const Int8Type& type) override { return LoadPrimitive(type); }

  Status Visit(const Int16Type& type) override { return LoadPrimitive(type); }

  Status Visit(const Int32Type& type) override { return LoadPrimitive(type); }

  Status Visit(const Int64Type& type) override { return LoadPrimitive(type); }

  Status Visit(const UInt8Type& type) override { return LoadPrimitive(type); }

  Status Visit(const UInt16Type& type) override { return LoadPrimitive(type); }

  Status Visit(const UInt32Type& type) override { return LoadPrimitive(type); }

  Status Visit(const UInt64Type& type) override { return LoadPrimitive(type); }

  Status Visit(const HalfFloatType& type) override { return LoadPrimitive(type); }

  Status Visit(const FloatType& type) override { return LoadPrimitive(type); }

  Status Visit(const DoubleType& type) override { return LoadPrimitive(type); }

  Status Visit(const StringType& type) override { return LoadBinary<StringArray>(); }

  Status Visit(const BinaryType& type) override { return LoadBinary<BinaryArray>(); }

  Status Visit(const DateType& type) override { return LoadPrimitive(type); }

  Status Visit(const TimeType& type) override { return LoadPrimitive(type); }

  Status Visit(const TimestampType& type) override { return LoadPrimitive(type); }

  Status Visit(const IntervalType& type) override {
    return Status::NotImplemented(type.ToString());
  }

  Status Visit(const DecimalType& type) override {
    return Status::NotImplemented(type.ToString());
  }

  Status Visit(const ListType& type) override {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap;

    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));

    std::shared_ptr<Buffer> offsets;
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &offsets));
    } else {
      offsets = nullptr;
    }
    ++context_->buffer_index;

    const int num_children = type.num_children();
    if (num_children != 1) {
      std::stringstream ss;
      ss << "Wrong number of children: " << num_children;
      return Status::Invalid(ss.str());
    }
    std::shared_ptr<Array> values_array;

    RETURN_NOT_OK(LoadChild(*type.child(0).get(), &values_array));

    result_ = std::make_shared<ListArray>(field_.type, field_meta.length, offsets,
        values_array, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status LoadChildren(std::vector<std::shared_ptr<Field>> child_fields,
      std::vector<std::shared_ptr<Array>>* arrays) {
    arrays->reserve(static_cast<int>(child_fields.size()));

    for (const auto& child_field : child_fields) {
      std::shared_ptr<Array> field_array;
      RETURN_NOT_OK(LoadChild(*child_field.get(), &field_array));
      arrays->emplace_back(field_array);
    }
    return Status::OK();
  }

  Status Visit(const StructType& type) override {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(LoadChildren(type.children(), &fields));

    result_ = std::make_shared<StructArray>(
        field_.type, field_meta.length, fields, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const UnionType& type) override {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));

    std::shared_ptr<Buffer> type_ids = nullptr;
    std::shared_ptr<Buffer> offsets = nullptr;

    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &type_ids));
      if (type.mode == UnionMode::DENSE) {
        RETURN_NOT_OK(GetBuffer(context_->buffer_index + 1, &offsets));
      }
    }
    context_->buffer_index += type.mode == UnionMode::DENSE? 2 : 1;

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(LoadChildren(type.children(), &fields));

    result_ = std::make_shared<UnionArray>(field_.type, field_meta.length, fields,
        type_ids, offsets, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) override {
    return Status::NotImplemented("dictionary");
  };
};

class RecordBatchReader {
 public:
  RecordBatchReader(const std::shared_ptr<RecordBatchMetadata>& metadata,
      const std::shared_ptr<Schema>& schema, int max_recursion_depth,
      io::ReadableFileInterface* file)
      : metadata_(metadata),
        schema_(schema),
        max_recursion_depth_(max_recursion_depth),
        file_(file) {}

  Status Read(std::shared_ptr<RecordBatch>* out) {
    std::vector<std::shared_ptr<Array>> arrays(schema_->num_fields());

    // The field_index and buffer_index are incremented in the ArrayLoader
    // based on how much of the batch is "consumed" (through nested data
    // reconstruction, for example)
    context_.metadata = metadata_.get();
    context_.field_index = 0;
    context_.buffer_index = 0;
    context_.max_recursion_depth = max_recursion_depth_;

    for (int i = 0; i < schema_->num_fields(); ++i) {
      ArrayLoader loader(*schema_->field(i).get(), &context_, file_);
      RETURN_NOT_OK(loader.Load(&arrays[i]));
    }

    *out = std::make_shared<RecordBatch>(schema_, metadata_->length(), arrays);
    return Status::OK();
  }

 private:
  RecordBatchContext context_;
  std::shared_ptr<RecordBatchMetadata> metadata_;
  std::shared_ptr<Schema> schema_;
  int max_recursion_depth_;
  io::ReadableFileInterface* file_;
};

Status ReadRecordBatchMetadata(int64_t offset, int32_t metadata_length,
    io::ReadableFileInterface* file, std::shared_ptr<RecordBatchMetadata>* metadata) {
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(file->ReadAt(offset, metadata_length, &buffer));

  int32_t flatbuffer_size = *reinterpret_cast<const int32_t*>(buffer->data());

  if (flatbuffer_size + static_cast<int>(sizeof(int32_t)) > metadata_length) {
    std::stringstream ss;
    ss << "flatbuffer size " << metadata_length << " invalid. File offset: " << offset
       << ", metadata length: " << metadata_length;
    return Status::Invalid(ss.str());
  }

  std::shared_ptr<Message> message;
  RETURN_NOT_OK(Message::Open(buffer, 4, &message));
  *metadata = std::make_shared<RecordBatchMetadata>(message);
  return Status::OK();
}

Status ReadRecordBatch(const std::shared_ptr<RecordBatchMetadata>& metadata,
    const std::shared_ptr<Schema>& schema, io::ReadableFileInterface* file,
    std::shared_ptr<RecordBatch>* out) {
  return ReadRecordBatch(metadata, schema, kMaxIpcRecursionDepth, file, out);
}

Status ReadRecordBatch(const std::shared_ptr<RecordBatchMetadata>& metadata,
    const std::shared_ptr<Schema>& schema, int max_recursion_depth,
    io::ReadableFileInterface* file, std::shared_ptr<RecordBatch>* out) {
  RecordBatchReader reader(metadata, schema, max_recursion_depth, file);
  return reader.Read(out);
}

}  // namespace ipc
}  // namespace arrow
