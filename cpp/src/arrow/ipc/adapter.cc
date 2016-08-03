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
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/memory.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/metadata.h"
#include "arrow/schema.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/types/construct.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/util/status.h"

namespace arrow {

namespace flatbuf = apache::arrow::flatbuf;

namespace ipc {

namespace {
Status CheckMultipleOf64(int64_t size) {
  if (util::is_multiple_of_64(size)) { return Status::OK(); }
  return Status::Invalid(
      "Attempted to write a buffer that "
      "wasn't a multiple of 64 bytes");
}
}

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

static bool IsListType(const DataType* type) {
  DCHECK(type != nullptr);
  switch (type->type) {
    // TODO(emkornfield) grouping like this are used in a few places in the
    // code consider using pattern like:
    // http://stackoverflow.com/questions/26784685/c-macro-for-calling-function-based-on-enum-type
    //
    case Type::BINARY:
    case Type::LIST:
    case Type::STRING:
      return true;
    default:
      return false;
  }
}

// ----------------------------------------------------------------------
// Row batch write path

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
  } else if (IsListType(arr_type)) {
    const auto list_arr = static_cast<const ListArray*>(arr);
    buffers->push_back(list_arr->offset_buffer());
    RETURN_NOT_OK(VisitArray(
        list_arr->values().get(), field_nodes, buffers, max_recursion_depth - 1));
  } else if (arr->type_enum() == Type::STRUCT) {
    // TODO(wesm)
    return Status::NotImplemented("Struct type");
  } else {
    return Status::NotImplemented("Unrecognized type");
  }
  return Status::OK();
}

class RowBatchWriter {
 public:
  RowBatchWriter(const RowBatch* batch, int max_recursion_depth)
      : batch_(batch), max_recursion_depth_(max_recursion_depth) {}

  Status AssemblePayload() {
    // Perform depth-first traversal of the row-batch
    for (int i = 0; i < batch_->num_columns(); ++i) {
      const Array* arr = batch_->column(i).get();
      RETURN_NOT_OK(VisitArray(arr, &field_nodes_, &buffers_, max_recursion_depth_));
    }
    return Status::OK();
  }

  Status Write(MemorySource* dst, int64_t position, int64_t* data_header_offset) {
    // Write out all the buffers contiguously and compute the total size of the
    // memory payload
    int64_t offset = 0;
    for (size_t i = 0; i < buffers_.size(); ++i) {
      const Buffer* buffer = buffers_[i].get();
      int64_t size = 0;

      // The buffer might be null if we are handling zero row lengths.
      if (buffer) {
        // We use capacity here, because size might not reflect the padding
        // requirements of buffers but capacity always should.
        size = buffer->capacity();
        // check that padding is appropriate
        RETURN_NOT_OK(CheckMultipleOf64(size));
      }
      // TODO(wesm): We currently have no notion of shared memory page id's,
      // but we've included it in the metadata IDL for when we have it in the
      // future. Use page=0 for now
      //
      // Note that page ids are a bespoke notion for Arrow and not a feature we
      // are using from any OS-level shared memory. The thought is that systems
      // may (in the future) associate integer page id's with physical memory
      // pages (according to whatever is the desired shared memory mechanism)
      buffer_meta_.push_back(flatbuf::Buffer(0, position + offset, size));

      if (size > 0) {
        RETURN_NOT_OK(dst->Write(position + offset, buffer->data(), size));
        offset += size;
      }
    }

    // Now that we have computed the locations of all of the buffers in shared
    // memory, the data header can be converted to a flatbuffer and written out
    //
    // Note: The memory written here is prefixed by the size of the flatbuffer
    // itself as an int32_t. On reading from a MemorySource, you will have to
    // determine the data header size then request a buffer such that you can
    // construct the flatbuffer data accessor object (see arrow::ipc::Message)
    std::shared_ptr<Buffer> data_header;
    RETURN_NOT_OK(WriteDataHeader(
        batch_->num_rows(), offset, field_nodes_, buffer_meta_, &data_header));

    // Write the data header at the end
    RETURN_NOT_OK(
        dst->Write(position + offset, data_header->data(), data_header->size()));

    *data_header_offset = position + offset;
    return Status::OK();
  }

  // This must be called after invoking AssemblePayload
  Status GetTotalSize(int64_t* size) {
    // emulates the behavior of Write without actually writing
    int64_t data_header_offset;
    MockMemorySource source(0);
    RETURN_NOT_OK(Write(&source, 0, &data_header_offset));
    *size = source.GetExtentBytesWritten();
    return Status::OK();
  }

 private:
  const RowBatch* batch_;

  std::vector<flatbuf::FieldNode> field_nodes_;
  std::vector<flatbuf::Buffer> buffer_meta_;
  std::vector<std::shared_ptr<Buffer>> buffers_;
  int max_recursion_depth_;
};

Status WriteRowBatch(MemorySource* dst, const RowBatch* batch, int64_t position,
    int64_t* header_offset, int max_recursion_depth) {
  DCHECK_GT(max_recursion_depth, 0);
  RowBatchWriter serializer(batch, max_recursion_depth);
  RETURN_NOT_OK(serializer.AssemblePayload());
  return serializer.Write(dst, position, header_offset);
}

Status GetRowBatchSize(const RowBatch* batch, int64_t* size) {
  RowBatchWriter serializer(batch, kMaxIpcRecursionDepth);
  RETURN_NOT_OK(serializer.AssemblePayload());
  RETURN_NOT_OK(serializer.GetTotalSize(size));
  return Status::OK();
}

// ----------------------------------------------------------------------
// Row batch read path

static constexpr int64_t INIT_METADATA_SIZE = 4096;

class RowBatchReader::Impl {
 public:
  Impl(MemorySource* source, const std::shared_ptr<RecordBatchMessage>& metadata,
      int max_recursion_depth)
      : source_(source), metadata_(metadata), max_recursion_depth_(max_recursion_depth) {
    num_buffers_ = metadata->num_buffers();
    num_flattened_fields_ = metadata->num_fields();
  }

  Status AssembleBatch(
      const std::shared_ptr<Schema>& schema, std::shared_ptr<RowBatch>* out) {
    std::vector<std::shared_ptr<Array>> arrays(schema->num_fields());

    // The field_index and buffer_index are incremented in NextArray based on
    // how much of the batch is "consumed" (through nested data reconstruction,
    // for example)
    field_index_ = 0;
    buffer_index_ = 0;
    for (int i = 0; i < schema->num_fields(); ++i) {
      const Field* field = schema->field(i).get();
      RETURN_NOT_OK(NextArray(field, max_recursion_depth_, &arrays[i]));
    }

    *out = std::make_shared<RowBatch>(schema, metadata_->length(), arrays);
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
    }

    if (IsListType(type.get())) {
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
      return MakeListArray(type, field_meta.length, offsets, values_array,
          field_meta.null_count, null_bitmap, out);
    }
    return Status::NotImplemented("Non-primitive types not complete yet");
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    BufferMetadata metadata = metadata_->buffer(buffer_index);
    RETURN_NOT_OK(CheckMultipleOf64(metadata.length));
    return source_->ReadAt(metadata.offset, metadata.length, out);
  }

  MemorySource* source_;
  std::shared_ptr<RecordBatchMessage> metadata_;

  int field_index_;
  int buffer_index_;
  int max_recursion_depth_;
  int num_buffers_;
  int num_flattened_fields_;
};

Status RowBatchReader::Open(
    MemorySource* source, int64_t position, std::shared_ptr<RowBatchReader>* out) {
  return Open(source, position, kMaxIpcRecursionDepth, out);
}

Status RowBatchReader::Open(MemorySource* source, int64_t position,
    int max_recursion_depth, std::shared_ptr<RowBatchReader>* out) {
  std::shared_ptr<Buffer> metadata;
  RETURN_NOT_OK(source->ReadAt(position, INIT_METADATA_SIZE, &metadata));

  int32_t metadata_size = *reinterpret_cast<const int32_t*>(metadata->data());

  // We may not need to call source->ReadAt again
  if (metadata_size > static_cast<int>(INIT_METADATA_SIZE - sizeof(int32_t))) {
    // We don't have enough data, read the indicated metadata size.
    RETURN_NOT_OK(source->ReadAt(position + sizeof(int32_t), metadata_size, &metadata));
  }

  // TODO(wesm): buffer slicing here would be better in case ReadAt returns
  // allocated memory

  std::shared_ptr<Message> message;
  RETURN_NOT_OK(Message::Open(metadata, &message));

  if (message->type() != Message::RECORD_BATCH) {
    return Status::Invalid("Metadata message is not a record batch");
  }

  std::shared_ptr<RecordBatchMessage> batch_meta = message->GetRecordBatch();

  std::shared_ptr<RowBatchReader> result(new RowBatchReader());
  result->impl_.reset(new Impl(source, batch_meta, max_recursion_depth));
  *out = result;

  return Status::OK();
}

// Here the explicit destructor is required for compilers to be aware of
// the complete information of RowBatchReader::Impl class
RowBatchReader::~RowBatchReader() {}

Status RowBatchReader::GetRowBatch(
    const std::shared_ptr<Schema>& schema, std::shared_ptr<RowBatch>* out) {
  return impl_->AssembleBatch(schema, out);
}

}  // namespace ipc
}  // namespace arrow
