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
#include <vector>

#include "arrow/array.h"
#include "arrow/ipc/memory.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/schema.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/types/construct.h"
#include "arrow/types/primitive.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

namespace flatbuf = apache::arrow::flatbuf;

namespace ipc {

static bool IsPrimitive(const DataType* type) {
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
// Row batch write path

Status VisitArray(const Array* arr, std::vector<flatbuf::FieldNode>* field_nodes,
    std::vector<std::shared_ptr<Buffer>>* buffers) {
  if (IsPrimitive(arr->type().get())) {
    const PrimitiveArray* prim_arr = static_cast<const PrimitiveArray*>(arr);

    field_nodes->push_back(
        flatbuf::FieldNode(prim_arr->length(), prim_arr->null_count()));

    if (prim_arr->null_count() > 0) {
      buffers->push_back(prim_arr->nulls());
    } else {
      // Push a dummy zero-length buffer, not to be copied
      buffers->push_back(std::make_shared<Buffer>(nullptr, 0));
    }
    buffers->push_back(prim_arr->data());
  } else if (arr->type_enum() == Type::LIST) {
    // TODO(wesm)
    return Status::NotImplemented("List type");
  } else if (arr->type_enum() == Type::STRUCT) {
    // TODO(wesm)
    return Status::NotImplemented("Struct type");
  }

  return Status::OK();
}

class RowBatchWriter {
 public:
  explicit RowBatchWriter(const RowBatch* batch) :
      batch_(batch) {}

  Status AssemblePayload() {
    // Perform depth-first traversal of the row-batch
    for (int i = 0; i < batch_->num_columns(); ++i) {
      const Array* arr = batch_->column(i).get();
      RETURN_NOT_OK(VisitArray(arr, &field_nodes_, &buffers_));
    }
    return Status::OK();
  }

  Status Write(MemorySource* dst, int64_t position, int64_t* data_header_offset) {
    // Write out all the buffers contiguously and compute the total size of the
    // memory payload
    int64_t offset = 0;
    for (size_t i = 0; i < buffers_.size(); ++i) {
      const Buffer* buffer = buffers_[i].get();
      int64_t size = buffer->size();

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
    RETURN_NOT_OK(WriteDataHeader(batch_->num_rows(), offset,
            field_nodes_, buffer_meta_, &data_header));

    // Write the data header at the end
    RETURN_NOT_OK(dst->Write(position + offset, data_header->data(),
            data_header->size()));

    *data_header_offset = position + offset;
    return Status::OK();
  }

  // This must be called after invoking AssemblePayload
  int64_t DataHeaderSize() {
    // TODO(wesm): In case it is needed, compute the upper bound for the size
    // of the buffer containing the flatbuffer data header.
    return 0;
  }

  // Total footprint of buffers. This must be called after invoking
  // AssemblePayload
  int64_t TotalBytes() {
    int64_t total = 0;
    for (const std::shared_ptr<Buffer>& buffer : buffers_) {
      total += buffer->size();
    }
    return total;
  }

 private:
  const RowBatch* batch_;

  std::vector<flatbuf::FieldNode> field_nodes_;
  std::vector<flatbuf::Buffer> buffer_meta_;
  std::vector<std::shared_ptr<Buffer>> buffers_;
};

Status WriteRowBatch(MemorySource* dst, const RowBatch* batch, int64_t position,
    int64_t* header_offset) {
  RowBatchWriter serializer(batch);
  RETURN_NOT_OK(serializer.AssemblePayload());
  return serializer.Write(dst, position, header_offset);
}
// ----------------------------------------------------------------------
// Row batch read path

static constexpr int64_t INIT_METADATA_SIZE = 4096;

class RowBatchReader::Impl {
 public:
  Impl(MemorySource* source, const std::shared_ptr<RecordBatchMessage>& metadata) :
      source_(source),
      metadata_(metadata) {
    num_buffers_ = metadata->num_buffers();
    num_flattened_fields_ = metadata->num_fields();
  }

  Status AssembleBatch(const std::shared_ptr<Schema>& schema,
      std::shared_ptr<RowBatch>* out) {
    std::vector<std::shared_ptr<Array>> arrays(schema->num_fields());

    // The field_index and buffer_index are incremented in NextArray based on
    // how much of the batch is "consumed" (through nested data reconstruction,
    // for example)
    field_index_ = 0;
    buffer_index_ = 0;
    for (int i = 0; i < schema->num_fields(); ++i) {
      const Field* field = schema->field(i).get();
      RETURN_NOT_OK(NextArray(field, &arrays[i]));
    }

    *out = std::make_shared<RowBatch>(schema, metadata_->length(),
        arrays);
    return Status::OK();
  }

 private:
  // Traverse the flattened record batch metadata and reassemble the
  // corresponding array containers
  Status NextArray(const Field* field, std::shared_ptr<Array>* out) {
    const std::shared_ptr<DataType>& type = field->type;

    // pop off a field
    if (field_index_ >= num_flattened_fields_) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }

    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    FieldMetadata field_meta = metadata_->field(field_index_++);

    if (IsPrimitive(type.get())) {
      std::shared_ptr<Buffer> nulls;
      std::shared_ptr<Buffer> data;
      if (field_meta.null_count == 0) {
        nulls = nullptr;
        ++buffer_index_;
      } else {
        RETURN_NOT_OK(GetBuffer(buffer_index_++, &nulls));
      }
      if (field_meta.length > 0) {
        RETURN_NOT_OK(GetBuffer(buffer_index_++, &data));
      } else {
        data.reset(new Buffer(nullptr, 0));
      }
      return MakePrimitiveArray(type, field_meta.length, data,
          field_meta.null_count, nulls, out);
    } else {
      return Status::NotImplemented("Non-primitive types not complete yet");
    }
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    BufferMetadata metadata = metadata_->buffer(buffer_index);
    return source_->ReadAt(metadata.offset, metadata.length, out);
  }

  MemorySource* source_;
  std::shared_ptr<RecordBatchMessage> metadata_;

  int field_index_;
  int buffer_index_;
  int num_buffers_;
  int num_flattened_fields_;
};

Status RowBatchReader::Open(MemorySource* source, int64_t position,
    std::shared_ptr<RowBatchReader>* out) {
  std::shared_ptr<Buffer> metadata;
  RETURN_NOT_OK(source->ReadAt(position, INIT_METADATA_SIZE, &metadata));

  int32_t metadata_size = *reinterpret_cast<const int32_t*>(metadata->data());

  // We may not need to call source->ReadAt again
  if (metadata_size > static_cast<int>(INIT_METADATA_SIZE - sizeof(int32_t))) {
    // We don't have enough data, read the indicated metadata size.
    RETURN_NOT_OK(source->ReadAt(position + sizeof(int32_t),
            metadata_size, &metadata));
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
  result->impl_.reset(new Impl(source, batch_meta));
  *out = result;

  return Status::OK();
}

Status RowBatchReader::GetRowBatch(const std::shared_ptr<Schema>& schema,
    std::shared_ptr<RowBatch>* out) {
  return impl_->AssembleBatch(schema, out);
}


} // namespace ipc
} // namespace arrow
