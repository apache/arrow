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

// C++ object model and user API for interprocess schema messaging

#ifndef ARROW_IPC_METADATA_H
#define ARROW_IPC_METADATA_H

#include <cstdint>
#include <memory>

namespace arrow {

class Buffer;
struct Field;
class Schema;
class Status;

namespace ipc {

//----------------------------------------------------------------------
// Message read/write APIs

// Serialize arrow::Schema as a Flatbuffer
Status WriteSchema(const Schema* schema, std::shared_ptr<Buffer>* out);

//----------------------------------------------------------------------

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

class Message;

// Container for serialized Schema metadata contained in an IPC message
class SchemaMessage {
 public:
  // Accepts an opaque flatbuffer pointer
  SchemaMessage(const std::shared_ptr<Message>& message, const void* schema);

  int num_fields() const;

  // Construct an arrow::Field for the i-th value in the metadata
  Status GetField(int i, std::shared_ptr<Field>* out) const;

  // Construct a complete Schema from the message. May be expensive for very
  // large schemas if you are only interested in a few fields
  Status GetSchema(std::shared_ptr<Schema>* out) const;

 private:
  // Parent, owns the flatbuffer data
  std::shared_ptr<Message> message_;

  class Impl;
  std::unique_ptr<Impl> impl_;
};

// Field metadata
struct FieldMetadata {
  int32_t length;
  int32_t null_count;
};

struct BufferMetadata {
  int32_t page;
  int64_t offset;
  int64_t length;
};

// Container for serialized record batch metadata contained in an IPC message
class RecordBatchMessage {
 public:
  // Accepts an opaque flatbuffer pointer
  RecordBatchMessage(const std::shared_ptr<Message>& message,
      const void* batch_meta);

  FieldMetadata field(int i) const;
  BufferMetadata buffer(int i) const;

  int32_t length() const;
  int num_buffers() const;
  int num_fields() const;

 private:
  // Parent, owns the flatbuffer data
  std::shared_ptr<Message> message_;

  class Impl;
  std::unique_ptr<Impl> impl_;
};

class DictionaryBatchMessage {
 public:
  int64_t id() const;
  std::unique_ptr<RecordBatchMessage> data() const;
};

class Message : public std::enable_shared_from_this<Message> {
 public:
  enum Type {
    NONE,
    SCHEMA,
    DICTIONARY_BATCH,
    RECORD_BATCH
  };

  static Status Open(const std::shared_ptr<Buffer>& buffer,
    std::shared_ptr<Message>* out);

  std::shared_ptr<Message> get_shared_ptr();

  int64_t body_length() const;

  Type type() const;

  // These methods only to be invoked if you have checked the message type
  std::shared_ptr<SchemaMessage> GetSchema();
  std::shared_ptr<RecordBatchMessage> GetRecordBatch();
  std::shared_ptr<DictionaryBatchMessage> GetDictionaryBatch();

 private:
  Message();

  // Hide serialization details from user API
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace ipc
} // namespace arrow

#endif // ARROW_IPC_METADATA_H
