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

#include "arrow/ipc/stream.h"

#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// Stream writer implementation

StreamWriter::StreamWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
    : sink_(sink),
      schema_(schema),
      dictionary_memo_(std::make_shared<DictionaryMemo>()),
      pool_(default_memory_pool()),
      position_(-1),
      started_(false) {}

Status StreamWriter::UpdatePosition() {
  return sink_->Tell(&position_);
}

Status StreamWriter::Write(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(sink_->Write(data, nbytes));
  position_ += nbytes;
  return Status::OK();
}

Status StreamWriter::Align() {
  int64_t remainder = PaddedLength(position_) - position_;
  if (remainder > 0) { return Write(kPaddingBytes, remainder); }
  return Status::OK();
}

Status StreamWriter::WriteAligned(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(Write(data, nbytes));
  return Align();
}

Status StreamWriter::CheckStarted() {
  if (!started_) { return Start(); }
  return Status::OK();
}

Status StreamWriter::WriteRecordBatch(const RecordBatch& batch, FileBlock* block) {
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

void StreamWriter::set_memory_pool(MemoryPool* pool) {
  pool_ = pool;
}

// ----------------------------------------------------------------------
// StreamWriter implementation

Status StreamWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<StreamWriter>* out) {
  // ctor is private
  *out = std::shared_ptr<StreamWriter>(new StreamWriter(sink, schema));
  RETURN_NOT_OK((*out)->UpdatePosition());
  return Status::OK();
}

Status StreamWriter::Start() {
  std::shared_ptr<Buffer> schema_fb;
  RETURN_NOT_OK(WriteSchemaMessage(*schema_, dictionary_memo_.get(), &schema_fb));

  int32_t flatbuffer_size = schema_fb->size();
  RETURN_NOT_OK(
      Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

  // Write the flatbuffer
  RETURN_NOT_OK(Write(schema_fb->data(), flatbuffer_size));

  // If there are any dictionaries, write them as the next messages
  RETURN_NOT_OK(WriteDictionaries());

  started_ = true;
  return Status::OK();
}

Status StreamWriter::WriteRecordBatch(const RecordBatch& batch) {
  // Push an empty FileBlock. Can be written in the footer later
  record_batches_.emplace_back(0, 0, 0);
  return WriteRecordBatch(batch, &record_batches_[record_batches_.size() - 1]);
}

Status StreamWriter::WriteDictionaries() {
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

Status StreamWriter::Close() {
  // Write the schema if not already written
  // User is responsible for closing the OutputStream
  return CheckStarted();
}

// ----------------------------------------------------------------------
// StreamReader implementation

static inline std::string message_type_name(Message::Type type) {
  switch (type) {
    case Message::SCHEMA:
      return "schema";
    case Message::RECORD_BATCH:
      return "record batch";
    case Message::DICTIONARY_BATCH:
      return "dictionary";
    default:
      break;
  }
  return "unknown";
}

class StreamReader::StreamReaderImpl {
 public:
  StreamReaderImpl() {}
  ~StreamReaderImpl() {}

  Status Open(const std::shared_ptr<io::InputStream>& stream) {
    stream_ = stream;
    return ReadSchema();
  }

  Status ReadNextMessage(Message::Type expected_type, std::shared_ptr<Message>* message) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(stream_->Read(sizeof(int32_t), &buffer));

    if (buffer->size() != sizeof(int32_t)) {
      *message = nullptr;
      return Status::OK();
    }

    int32_t message_length = *reinterpret_cast<const int32_t*>(buffer->data());

    RETURN_NOT_OK(stream_->Read(message_length, &buffer));
    if (buffer->size() != message_length) {
      return Status::IOError("Unexpected end of stream trying to read message");
    }

    RETURN_NOT_OK(Message::Open(buffer, 0, message));

    if ((*message)->type() != expected_type) {
      std::stringstream ss;
      ss << "Message not expected type: " << message_type_name(expected_type)
         << ", was: " << (*message)->type();
      return Status::IOError(ss.str());
    }
    return Status::OK();
  }

  Status ReadExact(int64_t size, std::shared_ptr<Buffer>* buffer) {
    RETURN_NOT_OK(stream_->Read(size, buffer));

    if ((*buffer)->size() < size) {
      return Status::IOError("Unexpected EOS when reading buffer");
    }
    return Status::OK();
  }

  Status ReadNextDictionary() {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::DICTIONARY_BATCH, &message));

    DictionaryBatchMetadata metadata(message);

    std::shared_ptr<Buffer> batch_body;
    RETURN_NOT_OK(ReadExact(message->body_length(), &batch_body))
    io::BufferReader reader(batch_body);

    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(ReadDictionary(metadata, dictionary_types_, &reader, &dictionary));
    return dictionary_memo_.AddDictionary(metadata.id(), dictionary);
  }

  Status ReadSchema() {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::SCHEMA, &message));

    SchemaMetadata schema_meta(message);
    RETURN_NOT_OK(schema_meta.GetDictionaryTypes(&dictionary_types_));

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    int num_dictionaries = static_cast<int>(dictionary_types_.size());
    for (int i = 0; i < num_dictionaries; ++i) {
      RETURN_NOT_OK(ReadNextDictionary());
    }

    return schema_meta.GetSchema(dictionary_memo_, &schema_);
  }

  Status GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::RECORD_BATCH, &message));

    if (message == nullptr) {
      // End of stream
      *batch = nullptr;
      return Status::OK();
    }

    RecordBatchMetadata batch_metadata(message);

    std::shared_ptr<Buffer> batch_body;
    RETURN_NOT_OK(ReadExact(message->body_length(), &batch_body));
    io::BufferReader reader(batch_body);
    return ReadRecordBatch(batch_metadata, schema_, &reader, batch);
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  // dictionary_id -> type
  DictionaryTypeMap dictionary_types_;

  DictionaryMemo dictionary_memo_;

  std::shared_ptr<io::InputStream> stream_;
  std::shared_ptr<Schema> schema_;
};

StreamReader::StreamReader() {
  impl_.reset(new StreamReaderImpl());
}

StreamReader::~StreamReader() {}

Status StreamReader::Open(const std::shared_ptr<io::InputStream>& stream,
    std::shared_ptr<StreamReader>* reader) {
  // Private ctor
  *reader = std::shared_ptr<StreamReader>(new StreamReader());
  return (*reader)->impl_->Open(stream);
}

std::shared_ptr<Schema> StreamReader::schema() const {
  return impl_->schema();
}

Status StreamReader::GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
  return impl_->GetNextRecordBatch(batch);
}

}  // namespace ipc
}  // namespace arrow
