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

#include <cstdint>
#include <cstring>
#include <sstream>
#include <vector>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/schema.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/types/construct.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/util/status.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

static bool IsPrimitive(const DataType* type) {
  DCHECK(type != nullptr);
  switch (type->type) {
    // NA is null type or "no type", considered primitive for now
    case Type::NA:
    case Type::BOOL:
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
    case Type::FLOAT:
    case Type::DOUBLE:
      return true;
    default:
      return false;
  }
}

// ----------------------------------------------------------------------
// Record batch write path

Status VisitArray(const Array* arr, std::vector<flatbuf::FieldNode>* field_nodes,
    std::vector<std::shared_ptr<Buffer>>* buffers, int max_recursion_depth) {
  if (max_recursion_depth <= 0) { return Status::Invalid("Max recursion depth reached"); }
  DCHECK(arr);
  DCHECK(field_nodes);
  // push back all common elements
  field_nodes->push_back(flatbuf::FieldNode(arr->length(), arr->null_count()));
  if (arr->null_count() > 0) {
    buffers->push_back(arr->null_bitmap());
  } else {
    // Push a dummy zero-length buffer, not to be copied
    buffers->push_back(std::make_shared<Buffer>(nullptr, 0));
  }

  const DataType* arr_type = arr->type().get();
  if (IsPrimitive(arr_type)) {
    const auto prim_arr = static_cast<const PrimitiveArray*>(arr);
    buffers->push_back(prim_arr->data());
  } else if (arr->type_enum() == Type::STRING || arr->type_enum() == Type::BINARY) {
    const auto binary_arr = static_cast<const BinaryArray*>(arr);
    buffers->push_back(binary_arr->offsets());
    buffers->push_back(binary_arr->data());
  } else if (arr->type_enum() == Type::LIST) {
    const auto list_arr = static_cast<const ListArray*>(arr);
    buffers->push_back(list_arr->offsets());
    RETURN_NOT_OK(VisitArray(
        list_arr->values().get(), field_nodes, buffers, max_recursion_depth - 1));
  } else if (arr->type_enum() == Type::STRUCT) {
    const auto struct_arr = static_cast<const StructArray*>(arr);
    for (auto& field : struct_arr->fields()) {
      RETURN_NOT_OK(
          VisitArray(field.get(), field_nodes, buffers, max_recursion_depth - 1));
    }
  } else {
    return Status::NotImplemented("Unrecognized type");
  }
  return Status::OK();
}

class RecordBatchWriter {
 public:
  RecordBatchWriter(const std::vector<std::shared_ptr<Array>>& columns, int32_t num_rows,
      int64_t buffer_start_offset, int max_recursion_depth)
      : columns_(&columns),
        num_rows_(num_rows),
        buffer_start_offset_(buffer_start_offset),
        max_recursion_depth_(max_recursion_depth) {}

  Status AssemblePayload(int64_t* body_length) {
    if (field_nodes_.size() > 0) {
      field_nodes_.clear();
      buffer_meta_.clear();
      buffers_.clear();
    }

    // Perform depth-first traversal of the row-batch
    for (size_t i = 0; i < columns_->size(); ++i) {
      const Array* arr = (*columns_)[i].get();
      RETURN_NOT_OK(VisitArray(arr, &field_nodes_, &buffers_, max_recursion_depth_));
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
        num_rows_, body_length, field_nodes_, buffer_meta_, &metadata_fb));

    // Need to write 4 bytes (metadata size), the metadata, plus padding to
    // fall on a 64-byte offset
    int64_t padded_metadata_length =
        BitUtil::RoundUpToMultipleOf64(metadata_fb->size() + 4);

    // The returned metadata size includes the length prefix, the flatbuffer,
    // plus padding
    *metadata_length = padded_metadata_length;

    // Write the flatbuffer size prefix
    int32_t flatbuffer_size = metadata_fb->size();
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
    RETURN_NOT_OK(AssemblePayload(body_length));

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
    int32_t metadata_length;
    int64_t body_length;
    MockOutputStream dst;
    RETURN_NOT_OK(Write(&dst, &metadata_length, &body_length));
    *size = dst.GetExtentBytesWritten();
    return Status::OK();
  }

 private:
  // Do not copy this vector. Ownership must be retained elsewhere
  const std::vector<std::shared_ptr<Array>>* columns_;
  int32_t num_rows_;
  int64_t buffer_start_offset_;

  std::vector<flatbuf::FieldNode> field_nodes_;
  std::vector<flatbuf::Buffer> buffer_meta_;
  std::vector<std::shared_ptr<Buffer>> buffers_;
  int max_recursion_depth_;
};

Status WriteRecordBatch(const std::vector<std::shared_ptr<Array>>& columns,
    int32_t num_rows, int64_t buffer_start_offset, io::OutputStream* dst,
    int32_t* metadata_length, int64_t* body_length, int max_recursion_depth) {
  DCHECK_GT(max_recursion_depth, 0);
  RecordBatchWriter serializer(
      columns, num_rows, buffer_start_offset, max_recursion_depth);
  return serializer.Write(dst, metadata_length, body_length);
}

Status GetRecordBatchSize(const RecordBatch* batch, int64_t* size) {
  RecordBatchWriter serializer(
      batch->columns(), batch->num_rows(), 0, kMaxIpcRecursionDepth);
  RETURN_NOT_OK(serializer.GetTotalSize(size));
  return Status::OK();
}

// ----------------------------------------------------------------------
// Record batch read path

class RecordBatchReader {
 public:
  RecordBatchReader(const std::shared_ptr<RecordBatchMetadata>& metadata,
      const std::shared_ptr<Schema>& schema, int max_recursion_depth,
      io::ReadableFileInterface* file)
      : metadata_(metadata),
        schema_(schema),
        max_recursion_depth_(max_recursion_depth),
        file_(file) {
    num_buffers_ = metadata->num_buffers();
    num_flattened_fields_ = metadata->num_fields();
  }

  Status Read(std::shared_ptr<RecordBatch>* out) {
    std::vector<std::shared_ptr<Array>> arrays(schema_->num_fields());

    // The field_index and buffer_index are incremented in NextArray based on
    // how much of the batch is "consumed" (through nested data reconstruction,
    // for example)
    field_index_ = 0;
    buffer_index_ = 0;
    for (int i = 0; i < schema_->num_fields(); ++i) {
      const Field* field = schema_->field(i).get();
      RETURN_NOT_OK(NextArray(field, max_recursion_depth_, &arrays[i]));
    }

    *out = std::make_shared<RecordBatch>(schema_, metadata_->length(), arrays);
    return Status::OK();
  }

 private:
  // Traverse the flattened record batch metadata and reassemble the
  // corresponding array containers
  Status NextArray(
      const Field* field, int max_recursion_depth, std::shared_ptr<Array>* out) {
    const TypePtr& type = field->type;
    if (max_recursion_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    // pop off a field
    if (field_index_ >= num_flattened_fields_) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }

    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    FieldMetadata field_meta = metadata_->field(field_index_++);

    // extract null_bitmap which is common to all arrays
    std::shared_ptr<Buffer> null_bitmap;
    if (field_meta.null_count == 0) {
      ++buffer_index_;
    } else {
      RETURN_NOT_OK(GetBuffer(buffer_index_++, &null_bitmap));
    }

    if (IsPrimitive(type.get())) {
      std::shared_ptr<Buffer> data;
      if (field_meta.length > 0) {
        RETURN_NOT_OK(GetBuffer(buffer_index_++, &data));
      } else {
        buffer_index_++;
        data.reset(new Buffer(nullptr, 0));
      }
      return MakePrimitiveArray(
          type, field_meta.length, data, field_meta.null_count, null_bitmap, out);
    } else if (type->type == Type::STRING || type->type == Type::BINARY) {
      std::shared_ptr<Buffer> offsets;
      std::shared_ptr<Buffer> values;
      RETURN_NOT_OK(GetBuffer(buffer_index_++, &offsets));
      RETURN_NOT_OK(GetBuffer(buffer_index_++, &values));

      if (type->type == Type::STRING) {
        *out = std::make_shared<StringArray>(
            field_meta.length, offsets, values, field_meta.null_count, null_bitmap);
      } else {
        *out = std::make_shared<BinaryArray>(
            field_meta.length, offsets, values, field_meta.null_count, null_bitmap);
      }
      return Status::OK();
    } else if (type->type == Type::LIST) {
      std::shared_ptr<Buffer> offsets;
      RETURN_NOT_OK(GetBuffer(buffer_index_++, &offsets));
      const int num_children = type->num_children();
      if (num_children != 1) {
        std::stringstream ss;
        ss << "Field: " << field->ToString()
           << " has wrong number of children:" << num_children;
        return Status::Invalid(ss.str());
      }
      std::shared_ptr<Array> values_array;
      RETURN_NOT_OK(
          NextArray(type->child(0).get(), max_recursion_depth - 1, &values_array));
      *out = std::make_shared<ListArray>(type, field_meta.length, offsets, values_array,
          field_meta.null_count, null_bitmap);
      return Status::OK();
    } else if (type->type == Type::STRUCT) {
      const int num_children = type->num_children();
      std::vector<ArrayPtr> fields;
      fields.reserve(num_children);
      for (int child_idx = 0; child_idx < num_children; ++child_idx) {
        std::shared_ptr<Array> field_array;
        RETURN_NOT_OK(NextArray(
            type->child(child_idx).get(), max_recursion_depth - 1, &field_array));
        fields.push_back(field_array);
      }
      out->reset(new StructArray(
          type, field_meta.length, fields, field_meta.null_count, null_bitmap));
      return Status::OK();
    }

    return Status::NotImplemented("Non-primitive types not complete yet");
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    BufferMetadata metadata = metadata_->buffer(buffer_index);

    if (metadata.length == 0) {
      *out = std::make_shared<Buffer>(nullptr, 0);
      return Status::OK();
    } else {
      return file_->ReadAt(metadata.offset, metadata.length, out);
    }
  }

 private:
  std::shared_ptr<RecordBatchMetadata> metadata_;
  std::shared_ptr<Schema> schema_;
  int max_recursion_depth_;
  io::ReadableFileInterface* file_;

  int field_index_;
  int buffer_index_;
  int num_buffers_;
  int num_flattened_fields_;
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

  *metadata = std::make_shared<RecordBatchMetadata>(buffer, sizeof(int32_t));
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
