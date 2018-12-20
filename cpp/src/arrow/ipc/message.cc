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

#include <flatbuffers/flatbuffers.h>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

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
    if (message_->version() < internal::kMinMetadataVersion) {
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
    return internal::GetMetadataVersion(message_->version());
  }

  const void* header() const { return message_->header(); }

  int64_t body_length() const { return message_->bodyLength(); }

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

int64_t Message::body_length() const { return impl_->body_length(); }

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
  auto data = metadata->data();
  flatbuffers::Verifier verifier(data, metadata->size(), 128);
  if (!flatbuf::VerifyMessageBuffer(verifier)) {
    return Status::IOError("Invalid flatbuffers message.");
  }
  auto fb_message = flatbuf::GetMessage(data);

  int64_t body_length = fb_message->bodyLength();

  std::shared_ptr<Buffer> body;
  RETURN_NOT_OK(stream->Read(body_length, &body));
  if (body->size() < body_length) {
    return Status::IOError("Expected to be able to read ", body_length,
                           " bytes for message body, got ", body->size());
  }

  return Message::Open(metadata, body, out);
}

Status Message::ReadFrom(const int64_t offset, const std::shared_ptr<Buffer>& metadata,
                         io::RandomAccessFile* file, std::unique_ptr<Message>* out) {
  auto fb_message = flatbuf::GetMessage(metadata->data());

  int64_t body_length = fb_message->bodyLength();

  std::shared_ptr<Buffer> body;
  RETURN_NOT_OK(file->ReadAt(offset, body_length, &body));
  if (body->size() < body_length) {
    return Status::IOError("Expected to be able to read ", body_length,
                           " bytes for message body, got ", body->size());
  }

  return Message::Open(metadata, body, out);
}

Status WritePadding(io::OutputStream* stream, int64_t nbytes) {
  while (nbytes > 0) {
    const int64_t bytes_to_write = std::min<int64_t>(nbytes, kArrowAlignment);
    RETURN_NOT_OK(stream->Write(kPaddingBytes, bytes_to_write));
    nbytes -= bytes_to_write;
  }
  return Status::OK();
}

Status Message::SerializeTo(io::OutputStream* stream, int32_t alignment,
                            int64_t* output_length) const {
  int32_t metadata_length = 0;
  RETURN_NOT_OK(internal::WriteMessage(*metadata(), alignment, stream, &metadata_length));

  *output_length = metadata_length;

  auto body_buffer = body();
  if (body_buffer) {
    RETURN_NOT_OK(stream->Write(body_buffer->data(), body_buffer->size()));
    *output_length += body_buffer->size();

    DCHECK_GE(this->body_length(), body_buffer->size());

    int64_t remainder = this->body_length() - body_buffer->size();
    RETURN_NOT_OK(WritePadding(stream, remainder));
    *output_length += remainder;
  }
  return Status::OK();
}

bool Message::Verify() const {
  std::shared_ptr<Buffer> meta = this->metadata();
  flatbuffers::Verifier verifier(meta->data(), meta->size(), 128);
  return flatbuf::VerifyMessageBuffer(verifier);
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
  DCHECK_GT(static_cast<size_t>(metadata_length), sizeof(int32_t));

  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(file->ReadAt(offset, metadata_length, &buffer));

  if (buffer->size() < metadata_length) {
    return Status::Invalid("Expected to read ", metadata_length,
                           " metadata bytes but got ", buffer->size());
  }

  int32_t flatbuffer_size = *reinterpret_cast<const int32_t*>(buffer->data());

  if (flatbuffer_size + static_cast<int>(sizeof(int32_t)) > metadata_length) {
    return Status::Invalid("flatbuffer size ", metadata_length,
                           " invalid. File offset: ", offset,
                           ", metadata length: ", metadata_length);
  }

  auto metadata = SliceBuffer(buffer, 4, buffer->size() - 4);
  return Message::ReadFrom(offset + metadata_length, metadata, file, message);
}

Status AlignStream(io::InputStream* stream, int32_t alignment) {
  int64_t position = -1;
  RETURN_NOT_OK(stream->Tell(&position));
  return stream->Advance(PaddedLength(position, alignment) - position);
}

Status AlignStream(io::OutputStream* stream, int32_t alignment) {
  int64_t position = -1;
  RETURN_NOT_OK(stream->Tell(&position));
  int64_t remainder = PaddedLength(position, alignment) - position;
  if (remainder > 0) {
    return stream->Write(kPaddingBytes, remainder);
  }
  return Status::OK();
}

Status CheckAligned(io::FileInterface* stream, int32_t alignment) {
  int64_t current_position;
  ARROW_RETURN_NOT_OK(stream->Tell(&current_position));
  if (current_position % alignment != 0) {
    return Status::Invalid("Stream is not aligned");
  } else {
    return Status::OK();
  }
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
    return Status::Invalid("Expected to read ", message_length, " metadata bytes, but ",
                           "only read ", metadata->size());
  }

  return Message::ReadFrom(metadata, file, message);
}

// ----------------------------------------------------------------------
// Implement InputStream message reader

/// \brief Implementation of MessageReader that reads from InputStream
class InputStreamMessageReader : public MessageReader {
 public:
  explicit InputStreamMessageReader(io::InputStream* stream) : stream_(stream) {}

  explicit InputStreamMessageReader(const std::shared_ptr<io::InputStream>& owned_stream)
      : InputStreamMessageReader(owned_stream.get()) {
    owned_stream_ = owned_stream;
  }

  ~InputStreamMessageReader() {}

  Status ReadNextMessage(std::unique_ptr<Message>* message) {
    return ReadMessage(stream_, message);
  }

 private:
  io::InputStream* stream_;
  std::shared_ptr<io::InputStream> owned_stream_;
};

std::unique_ptr<MessageReader> MessageReader::Open(io::InputStream* stream) {
  return std::unique_ptr<MessageReader>(new InputStreamMessageReader(stream));
}

std::unique_ptr<MessageReader> MessageReader::Open(
    const std::shared_ptr<io::InputStream>& owned_stream) {
  return std::unique_ptr<MessageReader>(new InputStreamMessageReader(owned_stream));
}

}  // namespace ipc
}  // namespace arrow
