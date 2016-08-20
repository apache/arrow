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

#include "arrow/ipc/metadata.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "flatbuffers/flatbuffers.h"

// Generated C++ flatbuffer IDL
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata-internal.h"

#include "arrow/schema.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

Status WriteSchema(const Schema* schema, std::shared_ptr<Buffer>* out) {
  MessageBuilder message;
  RETURN_NOT_OK(message.SetSchema(schema));
  RETURN_NOT_OK(message.Finish());
  return message.GetBuffer(out);
}

//----------------------------------------------------------------------
// Message reader

class Message::Impl {
 public:
  explicit Impl(const std::shared_ptr<Buffer>& buffer, const flatbuf::Message* message)
      : buffer_(buffer), message_(message) {}

  Message::Type type() const {
    switch (message_->header_type()) {
      case flatbuf::MessageHeader_Schema:
        return Message::SCHEMA;
      case flatbuf::MessageHeader_DictionaryBatch:
        return Message::DICTIONARY_BATCH;
      case flatbuf::MessageHeader_RecordBatch:
        return Message::RECORD_BATCH;
      default:
        return Message::NONE;
    }
  }

  const void* header() const { return message_->header(); }

  int64_t body_length() const { return message_->bodyLength(); }

 private:
  // Owns the memory this message accesses
  std::shared_ptr<Buffer> buffer_;

  const flatbuf::Message* message_;
};

class SchemaMessage::Impl {
 public:
  explicit Impl(const void* schema)
      : schema_(static_cast<const flatbuf::Schema*>(schema)) {}

  const flatbuf::Field* field(int i) const { return schema_->fields()->Get(i); }

  int num_fields() const { return schema_->fields()->size(); }

 private:
  const flatbuf::Schema* schema_;
};

Message::Message() {}

Status Message::Open(
    const std::shared_ptr<Buffer>& buffer, std::shared_ptr<Message>* out) {
  std::shared_ptr<Message> result(new Message());

  // The buffer is prefixed by its size as int32_t
  const uint8_t* fb_head = buffer->data() + sizeof(int32_t);
  const flatbuf::Message* message = flatbuf::GetMessage(fb_head);

  // TODO(wesm): verify message
  result->impl_.reset(new Impl(buffer, message));
  *out = result;

  return Status::OK();
}

Message::Type Message::type() const {
  return impl_->type();
}

int64_t Message::body_length() const {
  return impl_->body_length();
}

std::shared_ptr<Message> Message::get_shared_ptr() {
  return this->shared_from_this();
}

std::shared_ptr<SchemaMessage> Message::GetSchema() {
  return std::make_shared<SchemaMessage>(this->shared_from_this(), impl_->header());
}

SchemaMessage::SchemaMessage(
    const std::shared_ptr<Message>& message, const void* schema) {
  message_ = message;
  impl_.reset(new Impl(schema));
}

int SchemaMessage::num_fields() const {
  return impl_->num_fields();
}

Status SchemaMessage::GetField(int i, std::shared_ptr<Field>* out) const {
  const flatbuf::Field* field = impl_->field(i);
  return FieldFromFlatbuffer(field, out);
}

Status SchemaMessage::GetSchema(std::shared_ptr<Schema>* out) const {
  std::vector<std::shared_ptr<Field>> fields(num_fields());
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(GetField(i, &fields[i]));
  }
  *out = std::make_shared<Schema>(fields);
  return Status::OK();
}

class RecordBatchMessage::Impl {
 public:
  explicit Impl(const void* batch)
      : batch_(static_cast<const flatbuf::RecordBatch*>(batch)) {
    nodes_ = batch_->nodes();
    buffers_ = batch_->buffers();
  }

  const flatbuf::FieldNode* field(int i) const { return nodes_->Get(i); }

  const flatbuf::Buffer* buffer(int i) const { return buffers_->Get(i); }

  int32_t length() const { return batch_->length(); }

  int num_buffers() const { return batch_->buffers()->size(); }

  int num_fields() const { return batch_->nodes()->size(); }

 private:
  const flatbuf::RecordBatch* batch_;
  const flatbuffers::Vector<const flatbuf::FieldNode*>* nodes_;
  const flatbuffers::Vector<const flatbuf::Buffer*>* buffers_;
};

std::shared_ptr<RecordBatchMessage> Message::GetRecordBatch() {
  return std::make_shared<RecordBatchMessage>(this->shared_from_this(), impl_->header());
}

RecordBatchMessage::RecordBatchMessage(
    const std::shared_ptr<Message>& message, const void* batch) {
  message_ = message;
  impl_.reset(new Impl(batch));
}

// TODO(wesm): Copying the flatbuffer data isn't great, but this will do for
// now
FieldMetadata RecordBatchMessage::field(int i) const {
  const flatbuf::FieldNode* node = impl_->field(i);

  FieldMetadata result;
  result.length = node->length();
  result.null_count = node->null_count();
  return result;
}

BufferMetadata RecordBatchMessage::buffer(int i) const {
  const flatbuf::Buffer* buffer = impl_->buffer(i);

  BufferMetadata result;
  result.page = buffer->page();
  result.offset = buffer->offset();
  result.length = buffer->length();
  return result;
}

int32_t RecordBatchMessage::length() const {
  return impl_->length();
}

int RecordBatchMessage::num_buffers() const {
  return impl_->num_buffers();
}

int RecordBatchMessage::num_fields() const {
  return impl_->num_fields();
}

}  // namespace ipc
}  // namespace arrow
