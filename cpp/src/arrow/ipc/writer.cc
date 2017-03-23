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
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/loader.h"
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// Record batch write path

class RecordBatchWriter : public ArrayVisitor {
 public:
  RecordBatchWriter(MemoryPool* pool, int64_t buffer_start_offset,
      int max_recursion_depth, bool allow_64bit)
      : pool_(pool),
        max_recursion_depth_(max_recursion_depth),
        buffer_start_offset_(buffer_start_offset),
        allow_64bit_(allow_64bit) {
    DCHECK_GT(max_recursion_depth, 0);
  }

  virtual ~RecordBatchWriter() = default;

  Status VisitArray(const Array& arr) {
    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    if (!allow_64bit_ && arr.length() > std::numeric_limits<int32_t>::max()) {
      return Status::Invalid("Cannot write arrays larger than 2^31 - 1 in length");
    }

    // push back all common elements
    field_nodes_.emplace_back(arr.length(), arr.null_count(), 0);

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
      buffer_meta_.push_back({kNoPageId, offset, size + padding});
      offset += size + padding;
    }

    *body_length = offset - buffer_start_offset_;
    DCHECK(BitUtil::IsMultipleOf64(*body_length));

    return Status::OK();
  }

  // Override this for writing dictionary metadata
  virtual Status WriteMetadataMessage(
      int64_t num_rows, int64_t body_length, std::shared_ptr<Buffer>* out) {
    return WriteRecordBatchMessage(
        static_cast<int32_t>(num_rows), body_length, field_nodes_, buffer_meta_, out);
  }

  Status WriteMetadata(int64_t num_rows, int64_t body_length, io::OutputStream* dst,
      int32_t* metadata_length) {
    // Now that we have computed the locations of all of the buffers in shared
    // memory, the data header can be converted to a flatbuffer and written out
    //
    // Note: The memory written here is prefixed by the size of the flatbuffer
    // itself as an int32_t.
    std::shared_ptr<Buffer> metadata_fb;
    RETURN_NOT_OK(WriteMetadataMessage(num_rows, body_length, &metadata_fb));

    // Need to write 4 bytes (metadata size), the metadata, plus padding to
    // end on an 8-byte offset
    int64_t start_offset;
    RETURN_NOT_OK(dst->Tell(&start_offset));

    int32_t padded_metadata_length = static_cast<int32_t>(metadata_fb->size()) + 4;
    const int32_t remainder =
        (padded_metadata_length + static_cast<int32_t>(start_offset)) % 8;
    if (remainder != 0) { padded_metadata_length += 8 - remainder; }

    // The returned metadata size includes the length prefix, the flatbuffer,
    // plus padding
    *metadata_length = padded_metadata_length;

    // Write the flatbuffer size prefix including padding
    int32_t flatbuffer_size = padded_metadata_length - 4;
    RETURN_NOT_OK(
        dst->Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

    // Write the flatbuffer
    RETURN_NOT_OK(dst->Write(metadata_fb->data(), metadata_fb->size()));

    // Write any padding
    int32_t padding =
        padded_metadata_length - static_cast<int32_t>(metadata_fb->size()) - 4;
    if (padding > 0) { RETURN_NOT_OK(dst->Write(kPaddingBytes, padding)); }

    return Status::OK();
  }

  Status Write(const RecordBatch& batch, io::OutputStream* dst, int32_t* metadata_length,
      int64_t* body_length) {
    RETURN_NOT_OK(Assemble(batch, body_length));

#ifndef NDEBUG
    int64_t start_position, current_position;
    RETURN_NOT_OK(dst->Tell(&start_position));
#endif

    RETURN_NOT_OK(WriteMetadata(batch.num_rows(), *body_length, dst, metadata_length));

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

  Status GetTotalSize(const RecordBatch& batch, int64_t* size) {
    // emulates the behavior of Write without actually writing
    int32_t metadata_length = 0;
    int64_t body_length = 0;
    MockOutputStream dst;
    RETURN_NOT_OK(Write(batch, &dst, &metadata_length, &body_length));
    *size = dst.GetExtentBytesWritten();
    return Status::OK();
  }

 protected:
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

  Status Visit(const FixedWidthBinaryArray& array) override {
    auto data = array.data();
    int32_t width = array.byte_width();

    if (array.offset() != 0) {
      data = SliceBuffer(data, array.offset() * width, width * array.length());
    }
    buffers_.push_back(data);
    return Status::OK();
  }

  Status Visit(const BooleanArray& array) override {
    buffers_.push_back(array.data());
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
  VISIT_FIXED_WIDTH(TimeArray);
  VISIT_FIXED_WIDTH(TimestampArray);

#undef VISIT_FIXED_WIDTH

  Status Visit(const StringArray& array) override { return VisitBinary(array); }

  Status Visit(const BinaryArray& array) override { return VisitBinary(array); }

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

        for (int64_t i = 0; i < array.length(); ++i) {
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

class DictionaryWriter : public RecordBatchWriter {
 public:
  using RecordBatchWriter::RecordBatchWriter;

  Status WriteMetadataMessage(
      int64_t num_rows, int64_t body_length, std::shared_ptr<Buffer>* out) override {
    return WriteDictionaryMessage(dictionary_id_, static_cast<int32_t>(num_rows),
        body_length, field_nodes_, buffer_meta_, out);
  }

  Status Write(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
      io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length) {
    dictionary_id_ = dictionary_id;

    // Make a dummy record batch. A bit tedious as we have to make a schema
    std::vector<std::shared_ptr<Field>> fields = {
        arrow::field("dictionary", dictionary->type())};
    auto schema = std::make_shared<Schema>(fields);
    RecordBatch batch(schema, dictionary->length(), {dictionary});

    return RecordBatchWriter::Write(batch, dst, metadata_length, body_length);
  }

 private:
  // TODO(wesm): Setting this in Write is a bit unclean, but it works
  int64_t dictionary_id_;
};

Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
    io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length,
    MemoryPool* pool, int max_recursion_depth, bool allow_64bit) {
  RecordBatchWriter writer(pool, buffer_start_offset, max_recursion_depth, allow_64bit);
  return writer.Write(batch, dst, metadata_length, body_length);
}

Status WriteDictionary(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
    int64_t buffer_start_offset, io::OutputStream* dst, int32_t* metadata_length,
    int64_t* body_length, MemoryPool* pool) {
  DictionaryWriter writer(pool, buffer_start_offset, kMaxNestingDepth, false);
  return writer.Write(dictionary_id, dictionary, dst, metadata_length, body_length);
}

Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size) {
  RecordBatchWriter writer(default_memory_pool(), 0, kMaxNestingDepth, true);
  RETURN_NOT_OK(writer.GetTotalSize(batch, size));
  return Status::OK();
}

// ----------------------------------------------------------------------
// Stream writer implementation

class StreamWriter::StreamWriterImpl {
 public:
  StreamWriterImpl()
      : dictionary_memo_(std::make_shared<DictionaryMemo>()),
        pool_(default_memory_pool()),
        position_(-1),
        started_(false) {}

  virtual ~StreamWriterImpl() = default;

  Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema) {
    sink_ = sink;
    schema_ = schema;
    return UpdatePosition();
  }

  virtual Status Start() {
    std::shared_ptr<Buffer> schema_fb;
    RETURN_NOT_OK(WriteSchemaMessage(*schema_, dictionary_memo_.get(), &schema_fb));

    int32_t flatbuffer_size = static_cast<int32_t>(schema_fb->size());
    RETURN_NOT_OK(
        Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

    // Write the flatbuffer
    RETURN_NOT_OK(Write(schema_fb->data(), flatbuffer_size));

    // If there are any dictionaries, write them as the next messages
    RETURN_NOT_OK(WriteDictionaries());

    started_ = true;
    return Status::OK();
  }

  virtual Status Close() {
    // Write the schema if not already written
    // User is responsible for closing the OutputStream
    return CheckStarted();
  }

  Status CheckStarted() {
    if (!started_) { return Start(); }
    return Status::OK();
  }

  Status UpdatePosition() { return sink_->Tell(&position_); }

  Status WriteDictionaries() {
    const DictionaryMap& id_to_dictionary = dictionary_memo_->id_to_dictionary();

    dictionaries_.resize(id_to_dictionary.size());

    // TODO(wesm): does sorting by id yield any benefit?
    int dict_index = 0;
    for (const auto& entry : id_to_dictionary) {
      FileBlock* block = &dictionaries_[dict_index++];

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

  Status WriteRecordBatch(const RecordBatch& batch, FileBlock* block) {
    RETURN_NOT_OK(CheckStarted());

    block->offset = position_;

    // Frame of reference in file format is 0, see ARROW-384
    const int64_t buffer_start_offset = 0;
    RETURN_NOT_OK(arrow::ipc::WriteRecordBatch(batch, buffer_start_offset, sink_,
        &block->metadata_length, &block->body_length, pool_));
    RETURN_NOT_OK(UpdatePosition());

    DCHECK(position_ % 8 == 0) << "WriteRecordBatch did not perform aligned writes";

    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch) {
    // Push an empty FileBlock. Can be written in the footer later
    record_batches_.emplace_back(0, 0, 0);
    return WriteRecordBatch(batch, &record_batches_[record_batches_.size() - 1]);
  }

  // Adds padding bytes if necessary to ensure all memory blocks are written on
  // 8-byte boundaries.
  Status Align() {
    int64_t remainder = PaddedLength(position_) - position_;
    if (remainder > 0) { return Write(kPaddingBytes, remainder); }
    return Status::OK();
  }

  // Write data and update position
  Status Write(const uint8_t* data, int64_t nbytes) {
    RETURN_NOT_OK(sink_->Write(data, nbytes));
    position_ += nbytes;
    return Status::OK();
  }

  // Write and align
  Status WriteAligned(const uint8_t* data, int64_t nbytes) {
    RETURN_NOT_OK(Write(data, nbytes));
    return Align();
  }

  void set_memory_pool(MemoryPool* pool) { pool_ = pool; }

 protected:
  io::OutputStream* sink_;
  std::shared_ptr<Schema> schema_;

  // When writing out the schema, we keep track of all the dictionaries we
  // encounter, as they must be written out first in the stream
  std::shared_ptr<DictionaryMemo> dictionary_memo_;

  MemoryPool* pool_;

  int64_t position_;
  bool started_;

  std::vector<FileBlock> dictionaries_;
  std::vector<FileBlock> record_batches_;
};

StreamWriter::StreamWriter() {
  impl_.reset(new StreamWriterImpl());
}

Status StreamWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

void StreamWriter::set_memory_pool(MemoryPool* pool) {
  impl_->set_memory_pool(pool);
}

Status StreamWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<StreamWriter>* out) {
  // ctor is private
  *out = std::shared_ptr<StreamWriter>(new StreamWriter());
  return (*out)->impl_->Open(sink, schema);
}

Status StreamWriter::Close() {
  return impl_->Close();
}

// ----------------------------------------------------------------------
// File writer implementation

class FileWriter::FileWriterImpl : public StreamWriter::StreamWriterImpl {
 public:
  using BASE = StreamWriter::StreamWriterImpl;

  Status Start() override {
    RETURN_NOT_OK(WriteAligned(
        reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes)));

    // We write the schema at the start of the file (and the end). This also
    // writes all the dictionaries at the beginning of the file
    return BASE::Start();
  }

  Status Close() override {
    // Write metadata
    int64_t initial_position = position_;
    RETURN_NOT_OK(WriteFileFooter(
        *schema_, dictionaries_, record_batches_, dictionary_memo_.get(), sink_));
    RETURN_NOT_OK(UpdatePosition());

    // Write footer length
    int32_t footer_length = static_cast<int32_t>(position_ - initial_position);

    if (footer_length <= 0) { return Status::Invalid("Invalid file footer"); }

    RETURN_NOT_OK(
        Write(reinterpret_cast<const uint8_t*>(&footer_length), sizeof(int32_t)));

    // Write magic bytes to end file
    return Write(
        reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes));
  }
};

FileWriter::FileWriter() {
  impl_.reset(new FileWriterImpl());
}

Status FileWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<FileWriter>* out) {
  *out = std::shared_ptr<FileWriter>(new FileWriter());  // ctor is private
  return (*out)->impl_->Open(sink, schema);
}

Status FileWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

Status FileWriter::Close() {
  return impl_->Close();
}

Status WriteLargeRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
    io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length,
    MemoryPool* pool) {
  return WriteRecordBatch(batch, buffer_start_offset, dst, metadata_length, body_length,
      pool, kMaxNestingDepth, true);
}

}  // namespace ipc
}  // namespace arrow
