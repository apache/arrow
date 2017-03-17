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
#include <sstream>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "arrow/io/interfaces.h"
#include "arrow/ipc/File_generated.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata-internal.h"

#include "arrow/buffer.h"
#include "arrow/schema.h"
#include "arrow/status.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

// ----------------------------------------------------------------------
// Memoization data structure for handling shared dictionaries

DictionaryMemo::DictionaryMemo() {}

// Returns KeyError if dictionary not found
Status DictionaryMemo::GetDictionary(
    int64_t id, std::shared_ptr<Array>* dictionary) const {
  auto it = id_to_dictionary_.find(id);
  if (it == id_to_dictionary_.end()) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " not found";
    return Status::KeyError(ss.str());
  }
  *dictionary = it->second;
  return Status::OK();
}

int64_t DictionaryMemo::GetId(const std::shared_ptr<Array>& dictionary) {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  if (it != dictionary_to_id_.end()) {
    // Dictionary already observed, return the id
    return it->second;
  } else {
    int64_t new_id = static_cast<int64_t>(dictionary_to_id_.size()) + 1;
    dictionary_to_id_[address] = new_id;
    id_to_dictionary_[new_id] = dictionary;
    return new_id;
  }
}

bool DictionaryMemo::HasDictionary(const std::shared_ptr<Array>& dictionary) const {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  return it != dictionary_to_id_.end();
}

bool DictionaryMemo::HasDictionaryId(int64_t id) const {
  auto it = id_to_dictionary_.find(id);
  return it != id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(
    int64_t id, const std::shared_ptr<Array>& dictionary) {
  if (HasDictionaryId(id)) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " already exists";
    return Status::KeyError(ss.str());
  }
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  id_to_dictionary_[id] = dictionary;
  dictionary_to_id_[address] = id;
  return Status::OK();
}

//----------------------------------------------------------------------
// Message reader

class Message::MessageImpl {
 public:
  explicit MessageImpl(const std::shared_ptr<Buffer>& buffer, int64_t offset)
      : buffer_(buffer), offset_(offset), message_(nullptr) {}

  Status Open() {
    message_ = flatbuf::GetMessage(buffer_->data() + offset_);

    // TODO(wesm): verify the message
    return Status::OK();
  }

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
  // Retain reference to memory
  std::shared_ptr<Buffer> buffer_;
  int64_t offset_;

  const flatbuf::Message* message_;
};

Message::Message(const std::shared_ptr<Buffer>& buffer, int64_t offset) {
  impl_.reset(new MessageImpl(buffer, offset));
}

Status Message::Open(const std::shared_ptr<Buffer>& buffer, int64_t offset,
    std::shared_ptr<Message>* out) {
  // ctor is private

  *out = std::shared_ptr<Message>(new Message(buffer, offset));
  return (*out)->impl_->Open();
}

Message::Type Message::type() const {
  return impl_->type();
}

int64_t Message::body_length() const {
  return impl_->body_length();
}

// ----------------------------------------------------------------------
// SchemaMetadata

class SchemaMetadata::SchemaMetadataImpl {
 public:
  explicit SchemaMetadataImpl(const void* schema)
      : schema_(static_cast<const flatbuf::Schema*>(schema)) {}

  const flatbuf::Field* get_field(int i) const { return schema_->fields()->Get(i); }

  int num_fields() const { return schema_->fields()->size(); }

  Status VisitField(const flatbuf::Field* field, DictionaryTypeMap* id_to_field) const {
    const flatbuf::DictionaryEncoding* dict_metadata = field->dictionary();
    if (dict_metadata == nullptr) {
      // Field is not dictionary encoded. Visit children
      auto children = field->children();
      for (flatbuffers::uoffset_t i = 0; i < children->size(); ++i) {
        RETURN_NOT_OK(VisitField(children->Get(i), id_to_field));
      }
    } else {
      // Field is dictionary encoded. Construct the data type for the
      // dictionary (no descendents can be dictionary encoded)
      std::shared_ptr<Field> dictionary_field;
      RETURN_NOT_OK(FieldFromFlatbufferDictionary(field, &dictionary_field));
      (*id_to_field)[dict_metadata->id()] = dictionary_field;
    }
    return Status::OK();
  }

  Status GetDictionaryTypes(DictionaryTypeMap* id_to_field) const {
    for (int i = 0; i < num_fields(); ++i) {
      RETURN_NOT_OK(VisitField(get_field(i), id_to_field));
    }
    return Status::OK();
  }

 private:
  const flatbuf::Schema* schema_;
};

SchemaMetadata::SchemaMetadata(
    const std::shared_ptr<Message>& message, const void* flatbuf) {
  message_ = message;
  impl_.reset(new SchemaMetadataImpl(flatbuf));
}

SchemaMetadata::SchemaMetadata(const std::shared_ptr<Message>& message) {
  message_ = message;
  impl_.reset(new SchemaMetadataImpl(message->impl_->header()));
}

SchemaMetadata::~SchemaMetadata() {}

int SchemaMetadata::num_fields() const {
  return impl_->num_fields();
}

Status SchemaMetadata::GetDictionaryTypes(DictionaryTypeMap* id_to_field) const {
  return impl_->GetDictionaryTypes(id_to_field);
}

Status SchemaMetadata::GetSchema(
    const DictionaryMemo& dictionary_memo, std::shared_ptr<Schema>* out) const {
  std::vector<std::shared_ptr<Field>> fields(num_fields());
  for (int i = 0; i < this->num_fields(); ++i) {
    const flatbuf::Field* field = impl_->get_field(i);
    RETURN_NOT_OK(FieldFromFlatbuffer(field, dictionary_memo, &fields[i]));
  }
  *out = std::make_shared<Schema>(fields);
  return Status::OK();
}

// ----------------------------------------------------------------------
// RecordBatchMetadata

class RecordBatchMetadata::RecordBatchMetadataImpl {
 public:
  explicit RecordBatchMetadataImpl(const void* batch)
      : batch_(static_cast<const flatbuf::RecordBatch*>(batch)) {
    nodes_ = batch_->nodes();
    buffers_ = batch_->buffers();
  }

  const flatbuf::FieldNode* field(int i) const { return nodes_->Get(i); }

  const flatbuf::Buffer* buffer(int i) const { return buffers_->Get(i); }

  int32_t length() const { return batch_->length(); }

  int num_buffers() const { return batch_->buffers()->size(); }

  int num_fields() const { return batch_->nodes()->size(); }

  void set_message(const std::shared_ptr<Message>& message) { message_ = message; }

  void set_buffer(const std::shared_ptr<Buffer>& buffer) { buffer_ = buffer; }

 private:
  const flatbuf::RecordBatch* batch_;
  const flatbuffers::Vector<const flatbuf::FieldNode*>* nodes_;
  const flatbuffers::Vector<const flatbuf::Buffer*>* buffers_;

  // Possible parents, owns the flatbuffer data
  std::shared_ptr<Message> message_;
  std::shared_ptr<Buffer> buffer_;
};

RecordBatchMetadata::RecordBatchMetadata(const std::shared_ptr<Message>& message) {
  impl_.reset(new RecordBatchMetadataImpl(message->impl_->header()));
  impl_->set_message(message);
}

RecordBatchMetadata::RecordBatchMetadata(const void* header) {
  impl_.reset(new RecordBatchMetadataImpl(header));
}

RecordBatchMetadata::RecordBatchMetadata(
    const std::shared_ptr<Buffer>& buffer, int64_t offset)
    : RecordBatchMetadata(buffer->data() + offset) {
  // Preserve ownership
  impl_->set_buffer(buffer);
}

RecordBatchMetadata::~RecordBatchMetadata() {}

// TODO(wesm): Copying the flatbuffer data isn't great, but this will do for
// now
FieldMetadata RecordBatchMetadata::field(int i) const {
  const flatbuf::FieldNode* node = impl_->field(i);

  FieldMetadata result;
  result.length = node->length();
  result.null_count = node->null_count();
  result.offset = 0;
  return result;
}

BufferMetadata RecordBatchMetadata::buffer(int i) const {
  const flatbuf::Buffer* buffer = impl_->buffer(i);

  BufferMetadata result;
  result.page = buffer->page();
  result.offset = buffer->offset();
  result.length = buffer->length();
  return result;
}

int32_t RecordBatchMetadata::length() const {
  return impl_->length();
}

int RecordBatchMetadata::num_buffers() const {
  return impl_->num_buffers();
}

int RecordBatchMetadata::num_fields() const {
  return impl_->num_fields();
}

// ----------------------------------------------------------------------
// DictionaryBatchMetadata

class DictionaryBatchMetadata::DictionaryBatchMetadataImpl {
 public:
  explicit DictionaryBatchMetadataImpl(const void* dictionary)
      : metadata_(static_cast<const flatbuf::DictionaryBatch*>(dictionary)) {
    record_batch_.reset(new RecordBatchMetadata(metadata_->data()));
  }

  int64_t id() const { return metadata_->id(); }
  const RecordBatchMetadata& record_batch() const { return *record_batch_; }

  void set_message(const std::shared_ptr<Message>& message) { message_ = message; }

 private:
  const flatbuf::DictionaryBatch* metadata_;

  std::unique_ptr<RecordBatchMetadata> record_batch_;

  // Parent, owns the flatbuffer data
  std::shared_ptr<Message> message_;
};

DictionaryBatchMetadata::DictionaryBatchMetadata(
    const std::shared_ptr<Message>& message) {
  impl_.reset(new DictionaryBatchMetadataImpl(message->impl_->header()));
  impl_->set_message(message);
}

DictionaryBatchMetadata::~DictionaryBatchMetadata() {}

int64_t DictionaryBatchMetadata::id() const {
  return impl_->id();
}

const RecordBatchMetadata& DictionaryBatchMetadata::record_batch() const {
  return impl_->record_batch();
}

// ----------------------------------------------------------------------
// Conveniences

Status ReadMessage(int64_t offset, int32_t metadata_length,
    io::RandomAccessFile* file, std::shared_ptr<Message>* message) {
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(file->ReadAt(offset, metadata_length, &buffer));

  int32_t flatbuffer_size = *reinterpret_cast<const int32_t*>(buffer->data());

  if (flatbuffer_size + static_cast<int>(sizeof(int32_t)) > metadata_length) {
    std::stringstream ss;
    ss << "flatbuffer size " << metadata_length << " invalid. File offset: " << offset
       << ", metadata length: " << metadata_length;
    return Status::Invalid(ss.str());
  }
  return Message::Open(buffer, 4, message);
}

}  // namespace ipc
}  // namespace arrow
