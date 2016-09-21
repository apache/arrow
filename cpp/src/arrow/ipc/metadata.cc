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

#include "arrow/io/interfaces.h"
#include "arrow/ipc/File_generated.h"
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

class Message::MessageImpl {
 public:
  explicit MessageImpl(
      const std::shared_ptr<Buffer>& buffer, const flatbuf::Message* message)
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

Message::Message() {}

Status Message::Open(
    const std::shared_ptr<Buffer>& buffer, std::shared_ptr<Message>* out) {
  std::shared_ptr<Message> result(new Message());

  const flatbuf::Message* message = flatbuf::GetMessage(buffer->data());

  // TODO(wesm): verify message
  result->impl_.reset(new MessageImpl(buffer, message));
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

// ----------------------------------------------------------------------
// SchemaMessage

class SchemaMessage::SchemaMessageImpl {
 public:
  explicit SchemaMessageImpl(const void* schema)
      : schema_(static_cast<const flatbuf::Schema*>(schema)) {}

  const flatbuf::Field* field(int i) const { return schema_->fields()->Get(i); }

  int num_fields() const { return schema_->fields()->size(); }

 private:
  const flatbuf::Schema* schema_;
};

SchemaMessage::SchemaMessage(
    const std::shared_ptr<Message>& message, const void* schema) {
  message_ = message;
  impl_.reset(new SchemaMessageImpl(schema));
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

// ----------------------------------------------------------------------
// RecordBatchMessage

class RecordBatchMessage::RecordBatchMessageImpl {
 public:
  explicit RecordBatchMessageImpl(const void* batch)
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
  impl_.reset(new RecordBatchMessageImpl(batch));
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

// ----------------------------------------------------------------------
// File footer

static flatbuffers::Offset<flatbuffers::Vector<const flatbuf::Block*>>
FileBlocksToFlatbuffer(FBB& fbb, const std::vector<FileBlock>& blocks) {
  std::vector<flatbuf::Block> fb_blocks;

  for (const FileBlock& block : blocks) {
    fb_blocks.emplace_back(block.offset, block.metadata_length, block.body_length);
  }

  return fbb.CreateVectorOfStructs(fb_blocks);
}

Status WriteFileFooter(const Schema* schema, const std::vector<FileBlock>& dictionaries,
    const std::vector<FileBlock>& record_batches, io::OutputStream* out) {
  FBB fbb;

  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb, schema, &fb_schema));

  auto fb_dictionaries = FileBlocksToFlatbuffer(fbb, dictionaries);
  auto fb_record_batches = FileBlocksToFlatbuffer(fbb, record_batches);

  auto footer = flatbuf::CreateFooter(
      fbb, kMetadataVersion, fb_schema, fb_dictionaries, fb_record_batches);

  fbb.Finish(footer);

  int32_t size = fbb.GetSize();

  return out->Write(fbb.GetBufferPointer(), size);
}

static inline FileBlock FileBlockFromFlatbuffer(const flatbuf::Block* block) {
  return FileBlock(block->offset(), block->metaDataLength(), block->bodyLength());
}

class FileFooter::FileFooterImpl {
 public:
  FileFooterImpl(const std::shared_ptr<Buffer>& buffer, const flatbuf::Footer* footer)
      : buffer_(buffer), footer_(footer) {}

  int num_dictionaries() const { return footer_->dictionaries()->size(); }

  int num_record_batches() const { return footer_->recordBatches()->size(); }

  MetadataVersion::type version() const {
    switch (footer_->version()) {
      case flatbuf::MetadataVersion_V1_SNAPSHOT:
        return MetadataVersion::V1_SNAPSHOT;
      // Add cases as other versions become available
      default:
        return MetadataVersion::V1_SNAPSHOT;
    }
  }

  FileBlock record_batch(int i) const {
    return FileBlockFromFlatbuffer(footer_->recordBatches()->Get(i));
  }

  FileBlock dictionary(int i) const {
    return FileBlockFromFlatbuffer(footer_->dictionaries()->Get(i));
  }

  Status GetSchema(std::shared_ptr<Schema>* out) const {
    auto schema_msg = std::make_shared<SchemaMessage>(nullptr, footer_->schema());
    return schema_msg->GetSchema(out);
  }

 private:
  // Retain reference to memory
  std::shared_ptr<Buffer> buffer_;

  const flatbuf::Footer* footer_;
};

FileFooter::FileFooter() {}

FileFooter::~FileFooter() {}

Status FileFooter::Open(
    const std::shared_ptr<Buffer>& buffer, std::unique_ptr<FileFooter>* out) {
  const flatbuf::Footer* footer = flatbuf::GetFooter(buffer->data());

  *out = std::unique_ptr<FileFooter>(new FileFooter());

  // TODO(wesm): Verify the footer
  (*out)->impl_.reset(new FileFooterImpl(buffer, footer));

  return Status::OK();
}

int FileFooter::num_dictionaries() const {
  return impl_->num_dictionaries();
}

int FileFooter::num_record_batches() const {
  return impl_->num_record_batches();
}

MetadataVersion::type FileFooter::version() const {
  return impl_->version();
}

FileBlock FileFooter::record_batch(int i) const {
  return impl_->record_batch(i);
}

FileBlock FileFooter::dictionary(int i) const {
  return impl_->dictionary(i);
}

Status FileFooter::GetSchema(std::shared_ptr<Schema>* out) const {
  return impl_->GetSchema(out);
}

}  // namespace ipc
}  // namespace arrow
