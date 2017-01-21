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
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// Stream writer implementation

StreamWriter::~StreamWriter() {}

StreamWriter::StreamWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
    : sink_(sink), schema_(schema), position_(-1), started_(false) {}

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
  RETURN_NOT_OK(arrow::ipc::WriteRecordBatch(
      batch, buffer_start_offset, sink_, &block->metadata_length, &block->body_length));
  RETURN_NOT_OK(UpdatePosition());

  DCHECK(position_ % 8 == 0) << "WriteRecordBatch did not perform aligned writes";

  return Status::OK();
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
  RETURN_NOT_OK(WriteSchema(*schema_, &schema_fb));

  int32_t flatbuffer_size = schema_fb->size();
  RETURN_NOT_OK(
      Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

  // Write the flatbuffer
  RETURN_NOT_OK(Write(schema_fb->data(), flatbuffer_size));
  started_ = true;
  return Status::OK();
}

Status StreamWriter::WriteRecordBatch(const RecordBatch& batch) {
  // Pass FileBlock, but results not used
  FileBlock dummy_block;
  return WriteRecordBatch(batch, &dummy_block);
}

Status StreamWriter::Close() {
  // Close the stream
  RETURN_NOT_OK(CheckStarted());
  return sink_->Close();
}

// ----------------------------------------------------------------------
// StreamReader implementation

StreamReader::StreamReader(const std::shared_ptr<io::InputStream>& stream)
    : stream_(stream), schema_(nullptr) {}

StreamReader::~StreamReader() {}

Status StreamReader::Open(const std::shared_ptr<io::InputStream>& stream,
    std::shared_ptr<StreamReader>* reader) {
  // Private ctor
  *reader = std::shared_ptr<StreamReader>(new StreamReader(stream));
  return (*reader)->ReadSchema();
}

Status StreamReader::ReadSchema() {
  std::shared_ptr<Message> message;
  RETURN_NOT_OK(ReadNextMessage(&message));

  if (message->type() != Message::SCHEMA) {
    return Status::IOError("First message was not schema type");
  }

  SchemaMetadata schema_meta(message);

  // TODO(wesm): If the schema contains dictionaries, we must read all the
  // dictionaries from the stream before constructing the final Schema
  return schema_meta.GetSchema(&schema_);
}

Status StreamReader::ReadNextMessage(std::shared_ptr<Message>* message) {
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
  return Message::Open(buffer, 0, message);
}

std::shared_ptr<Schema> StreamReader::schema() const {
  return schema_;
}

Status StreamReader::GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
  std::shared_ptr<Message> message;
  RETURN_NOT_OK(ReadNextMessage(&message));

  if (message == nullptr) {
    // End of stream
    *batch = nullptr;
    return Status::OK();
  }

  if (message->type() != Message::RECORD_BATCH) {
    return Status::IOError("Metadata not record batch");
  }

  auto batch_metadata = std::make_shared<RecordBatchMetadata>(message);

  std::shared_ptr<Buffer> batch_body;
  RETURN_NOT_OK(stream_->Read(message->body_length(), &batch_body));

  if (batch_body->size() < message->body_length()) {
    return Status::IOError("Unexpected EOS when reading message body");
  }

  io::BufferReader reader(batch_body);

  return ReadRecordBatch(batch_metadata, schema_, &reader, batch);
}

}  // namespace ipc
}  // namespace arrow
