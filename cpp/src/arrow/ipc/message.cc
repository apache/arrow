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

#include "arrow/ipc/message.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/Schema_generated.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/status.h"

namespace arrow {
namespace ipc {

class Message::MessageImpl {
 public:
  explicit MessageImpl(const std::shared_ptr<Buffer>& metadata,
                       const std::shared_ptr<Buffer>& body)
      : metadata_(metadata), message_(nullptr), body_(body) {}

  Status Open() {
    message_ = flatbuf::GetMessage(metadata_->data());

    // Check that the metadata version is supported
    if (message_->version() < kMinMetadataVersion) {
      return Status::Invalid("Old metadata version not supported");
    }

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
      case flatbuf::MessageHeader_Tensor:
        return Message::TENSOR;
      default:
        return Message::NONE;
    }
  }

  MetadataVersion version() const {
    switch (message_->version()) {
      case flatbuf::MetadataVersion_V1:
        // Arrow 0.1
        return MetadataVersion::V1;
      case flatbuf::MetadataVersion_V2:
        // Arrow 0.2
        return MetadataVersion::V2;
      case flatbuf::MetadataVersion_V3:
        // Arrow >= 0.3
        return MetadataVersion::V3;
      // Add cases as other versions become available
      default:
        return MetadataVersion::V3;
    }
  }

  const void* header() const { return message_->header(); }

  std::shared_ptr<Buffer> body() const { return body_; }

  std::shared_ptr<Buffer> metadata() const { return metadata_; }

 private:
  // The Flatbuffer metadata
  std::shared_ptr<Buffer> metadata_;
  const flatbuf::Message* message_;

  // The message body, if any
  std::shared_ptr<Buffer> body_;
};

Message::Message(const std::shared_ptr<Buffer>& metadata,
                 const std::shared_ptr<Buffer>& body) {
  impl_.reset(new MessageImpl(metadata, body));
}

Status Message::Open(const std::shared_ptr<Buffer>& metadata,
                     const std::shared_ptr<Buffer>& body, std::unique_ptr<Message>* out) {
  out->reset(new Message(metadata, body));
  return (*out)->impl_->Open();
}

Message::~Message() {}

std::shared_ptr<Buffer> Message::body() const { return impl_->body(); }

std::shared_ptr<Buffer> Message::metadata() const { return impl_->metadata(); }

Message::Type Message::type() const { return impl_->type(); }

MetadataVersion Message::metadata_version() const { return impl_->version(); }

const void* Message::header() const { return impl_->header(); }

bool Message::Equals(const Message& other) const {
  int64_t metadata_bytes = std::min(metadata()->size(), other.metadata()->size());

  if (!metadata()->Equals(*other.metadata(), metadata_bytes)) {
    return false;
  }

  // Compare bodies, if they have them
  auto this_body = body();
  auto other_body = other.body();

  const bool this_has_body = (this_body != nullptr) && (this_body->size() > 0);
  const bool other_has_body = (other_body != nullptr) && (other_body->size() > 0);

  if (this_has_body && other_has_body) {
    return this_body->Equals(*other_body);
  } else if (this_has_body ^ other_has_body) {
    // One has a body but not the other
    return false;
  } else {
    // Neither has a body
    return true;
  }
}

Status Message::ReadFrom(const std::shared_ptr<Buffer>& metadata, io::InputStream* stream,
                         std::unique_ptr<Message>* out) {
  auto fb_message = flatbuf::GetMessage(metadata->data());

  int64_t body_length = fb_message->bodyLength();

  std::shared_ptr<Buffer> body;
  RETURN_NOT_OK(stream->Read(body_length, &body));
  if (body->size() < body_length) {
    std::stringstream ss;
    ss << "Expected to be able to read " << body_length << " bytes for message body, got "
       << body->size();
    return Status::IOError(ss.str());
  }

  return Message::Open(metadata, body, out);
}

Status Message::SerializeTo(io::OutputStream* file, int64_t* output_length) const {
  int32_t metadata_length = 0;
  RETURN_NOT_OK(WriteMessage(*metadata(), file, &metadata_length));

  *output_length = metadata_length;

  auto body_buffer = body();
  if (body_buffer) {
    RETURN_NOT_OK(file->Write(body_buffer->data(), body_buffer->size()));
    *output_length += body_buffer->size();
  }

  return Status::OK();
}

std::string FormatMessageType(Message::Type type) {
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

Status ReadMessage(int64_t offset, int32_t metadata_length, io::RandomAccessFile* file,
                   std::unique_ptr<Message>* message) {
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(file->ReadAt(offset, metadata_length, &buffer));

  int32_t flatbuffer_size = *reinterpret_cast<const int32_t*>(buffer->data());

  if (flatbuffer_size + static_cast<int>(sizeof(int32_t)) > metadata_length) {
    std::stringstream ss;
    ss << "flatbuffer size " << metadata_length << " invalid. File offset: " << offset
       << ", metadata length: " << metadata_length;
    return Status::Invalid(ss.str());
  }

  auto metadata = SliceBuffer(buffer, 4, buffer->size() - 4);
  return Message::ReadFrom(metadata, file, message);
}

Status ReadMessage(io::InputStream* file, std::unique_ptr<Message>* message) {
  int32_t message_length = 0;
  int64_t bytes_read = 0;
  RETURN_NOT_OK(file->Read(sizeof(int32_t), &bytes_read,
                           reinterpret_cast<uint8_t*>(&message_length)));

  if (bytes_read != sizeof(int32_t)) {
    *message = nullptr;
    return Status::OK();
  }

  if (message_length == 0) {
    // Optional 0 EOS control message
    *message = nullptr;
    return Status::OK();
  }

  std::shared_ptr<Buffer> metadata;
  RETURN_NOT_OK(file->Read(message_length, &metadata));
  if (metadata->size() != message_length) {
    return Status::IOError("Unexpected end of stream trying to read message");
  }

  return Message::ReadFrom(metadata, file, message);
}

// ----------------------------------------------------------------------
// Implement InputStream message reader

Status InputStreamMessageReader::ReadNextMessage(std::unique_ptr<Message>* message) {
  return ReadMessage(stream_, message);
}

InputStreamMessageReader::~InputStreamMessageReader() {}

}  // namespace ipc
}  // namespace arrow
