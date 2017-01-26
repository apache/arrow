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
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
struct Field;
class Schema;
class Status;

namespace io {

class OutputStream;

}  // namespace io

namespace ipc {

struct MetadataVersion {
  enum type { V1, V2 };
};

//----------------------------------------------------------------------

// Serialize arrow::Schema as a Flatbuffer
ARROW_EXPORT
Status WriteSchema(const Schema& schema, std::shared_ptr<Buffer>* out);

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

class Message;

// Container for serialized Schema metadata contained in an IPC message
class ARROW_EXPORT SchemaMetadata {
 public:
  explicit SchemaMetadata(const std::shared_ptr<Message>& message);

  // Accepts an opaque flatbuffer pointer
  SchemaMetadata(const std::shared_ptr<Message>& message, const void* schema);

  ~SchemaMetadata();

  int num_fields() const;

  // Construct an arrow::Field for the i-th value in the metadata
  Status GetField(int i, std::shared_ptr<Field>* out) const;

  // Construct a complete Schema from the message. May be expensive for very
  // large schemas if you are only interested in a few fields
  Status GetSchema(std::shared_ptr<Schema>* out) const;

 private:
  // Parent, owns the flatbuffer data
  std::shared_ptr<Message> message_;

  class SchemaMetadataImpl;
  std::unique_ptr<SchemaMetadataImpl> impl_;
};

// Field metadata
struct ARROW_EXPORT FieldMetadata {
  int32_t length;
  int32_t null_count;
};

struct ARROW_EXPORT BufferMetadata {
  int32_t page;
  int64_t offset;
  int64_t length;
};

// Container for serialized record batch metadata contained in an IPC message
class ARROW_EXPORT RecordBatchMetadata {
 public:
  explicit RecordBatchMetadata(const std::shared_ptr<Message>& message);

  RecordBatchMetadata(const std::shared_ptr<Buffer>& message, int64_t offset);

  ~RecordBatchMetadata();

  FieldMetadata field(int i) const;
  BufferMetadata buffer(int i) const;

  int32_t length() const;
  int num_buffers() const;
  int num_fields() const;

 private:
  // Parent, owns the flatbuffer data
  std::shared_ptr<Message> message_;
  std::shared_ptr<Buffer> buffer_;

  class RecordBatchMetadataImpl;
  std::unique_ptr<RecordBatchMetadataImpl> impl_;
};

class ARROW_EXPORT DictionaryBatchMetadata {
 public:
  int64_t id() const;
  std::unique_ptr<RecordBatchMetadata> data() const;
};

class ARROW_EXPORT Message {
 public:
  enum Type { NONE, SCHEMA, DICTIONARY_BATCH, RECORD_BATCH };

  static Status Open(const std::shared_ptr<Buffer>& buffer, int64_t offset,
      std::shared_ptr<Message>* out);

  int64_t body_length() const;

  Type type() const;

 private:
  Message(const std::shared_ptr<Buffer>& buffer, int64_t offset);

  friend class RecordBatchMetadata;
  friend class SchemaMetadata;

  // Hide serialization details from user API
  class MessageImpl;
  std::unique_ptr<MessageImpl> impl_;
};

struct ARROW_EXPORT FileBlock {
  FileBlock() {}
  FileBlock(int64_t offset, int32_t metadata_length, int64_t body_length)
      : offset(offset), metadata_length(metadata_length), body_length(body_length) {}

  int64_t offset;
  int32_t metadata_length;
  int64_t body_length;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_METADATA_H
