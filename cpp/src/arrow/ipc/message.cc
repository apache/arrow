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
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

#include "generated/Message_generated.h"

namespace arrow {
namespace ipc {

class Message::MessageImpl {
 public:
  explicit MessageImpl(const std::shared_ptr<Buffer>& metadata,
                       const std::shared_ptr<Buffer>& body)
      : metadata_(metadata), message_(nullptr), body_(body) {}

  Status Open() {
    RETURN_NOT_OK(
        internal::VerifyMessage(metadata_->data(), metadata_->size(), &message_));

    // Check that the metadata version is supported
    if (message_->version() < internal::kMinMetadataVersion) {
      return Status::Invalid("Old metadata version not supported");
    }

    return Status::OK();
  }

  Message::Type type() const {
    switch (message_->header_type()) {
      case flatbuf::MessageHeader::Schema:
        return Message::SCHEMA;
      case flatbuf::MessageHeader::DictionaryBatch:
        return Message::DICTIONARY_BATCH;
      case flatbuf::MessageHeader::RecordBatch:
        return Message::RECORD_BATCH;
      case flatbuf::MessageHeader::Tensor:
        return Message::TENSOR;
      case flatbuf::MessageHeader::SparseTensor:
        return Message::SPARSE_TENSOR;
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

Status MaybeAlignMetadata(std::shared_ptr<Buffer>* metadata) {
  if (reinterpret_cast<uintptr_t>((*metadata)->data()) % 8 != 0) {
    // If the metadata memory is not aligned, we copy it here to avoid
    // potential UBSAN issues from Flatbuffers
    RETURN_NOT_OK((*metadata)->Copy(0, (*metadata)->size(), metadata));
  }
  return Status::OK();
}

Status CheckMetadataAndGetBodyLength(const Buffer& metadata, int64_t* body_length) {
  const flatbuf::Message* fb_message;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &fb_message));
  *body_length = fb_message->bodyLength();
  return Status::OK();
}

Status Message::ReadFrom(std::shared_ptr<Buffer> metadata, io::InputStream* stream,
                         std::unique_ptr<Message>* out) {
  RETURN_NOT_OK(MaybeAlignMetadata(&metadata));
  int64_t body_length = -1;
  RETURN_NOT_OK(CheckMetadataAndGetBodyLength(*metadata, &body_length));

  ARROW_ASSIGN_OR_RAISE(auto body, stream->Read(body_length));
  if (body->size() < body_length) {
    return Status::IOError("Expected to be able to read ", body_length,
                           " bytes for message body, got ", body->size());
  }

  return Message::Open(metadata, body, out);
}

Status Message::ReadFrom(const int64_t offset, std::shared_ptr<Buffer> metadata,
                         io::RandomAccessFile* file, std::unique_ptr<Message>* out) {
  RETURN_NOT_OK(MaybeAlignMetadata(&metadata));
  int64_t body_length = -1;
  RETURN_NOT_OK(CheckMetadataAndGetBodyLength(*metadata, &body_length));

  ARROW_ASSIGN_OR_RAISE(auto body, file->ReadAt(offset, body_length));
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

Status Message::SerializeTo(io::OutputStream* stream, const IpcOptions& options,
                            int64_t* output_length) const {
  int32_t metadata_length = 0;
  RETURN_NOT_OK(WriteMessage(*metadata(), options, stream, &metadata_length));

  *output_length = metadata_length;

  auto body_buffer = body();
  if (body_buffer) {
    RETURN_NOT_OK(stream->Write(body_buffer));
    *output_length += body_buffer->size();

    DCHECK_GE(this->body_length(), body_buffer->size());

    int64_t remainder = this->body_length() - body_buffer->size();
    RETURN_NOT_OK(WritePadding(stream, remainder));
    *output_length += remainder;
  }
  return Status::OK();
}

bool Message::Verify() const {
  const flatbuf::Message* unused;
  return internal::VerifyMessage(metadata()->data(), metadata()->size(), &unused).ok();
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
  if (static_cast<size_t>(metadata_length) < sizeof(int32_t)) {
    return Status::Invalid("metadata_length should be at least 4");
  }

  ARROW_ASSIGN_OR_RAISE(auto buffer, file->ReadAt(offset, metadata_length));

  if (buffer->size() < metadata_length) {
    return Status::Invalid("Expected to read ", metadata_length,
                           " metadata bytes but got ", buffer->size());
  }

  const int32_t continuation = util::SafeLoadAs<int32_t>(buffer->data());

  // The size of the Flatbuffer including padding
  int32_t flatbuffer_length = -1;
  int32_t prefix_size = -1;
  if (continuation == internal::kIpcContinuationToken) {
    if (metadata_length < 8) {
      return Status::Invalid(
          "Corrupted IPC message, had continuation token "
          " but length ",
          metadata_length);
    }

    // Valid IPC message, parse the message length now
    flatbuffer_length = util::SafeLoadAs<int32_t>(buffer->data() + 4);
    prefix_size = 8;
  } else {
    // ARROW-6314: Backwards compatibility for reading old IPC
    // messages produced prior to version 0.15.0
    flatbuffer_length = continuation;
    prefix_size = 4;
  }

  if (flatbuffer_length == 0) {
    return Status::Invalid("Unexpected empty message in IPC file format");
  }

  if (flatbuffer_length != metadata_length - prefix_size) {
    return Status::Invalid("flatbuffer size ", flatbuffer_length,
                           " invalid. File offset: ", offset,
                           ", metadata length: ", metadata_length);
  }

  std::shared_ptr<Buffer> metadata =
      SliceBuffer(buffer, prefix_size, buffer->size() - prefix_size);
  return Message::ReadFrom(offset + metadata_length, metadata, file, message);
}

Status AlignStream(io::InputStream* stream, int32_t alignment) {
  ARROW_ASSIGN_OR_RAISE(int64_t position, stream->Tell());
  return stream->Advance(PaddedLength(position, alignment) - position);
}

Status AlignStream(io::OutputStream* stream, int32_t alignment) {
  ARROW_ASSIGN_OR_RAISE(int64_t position, stream->Tell());
  int64_t remainder = PaddedLength(position, alignment) - position;
  if (remainder > 0) {
    return stream->Write(kPaddingBytes, remainder);
  }
  return Status::OK();
}

Status CheckAligned(io::FileInterface* stream, int32_t alignment) {
  ARROW_ASSIGN_OR_RAISE(int64_t position, stream->Tell());
  if (position % alignment != 0) {
    return Status::Invalid("Stream is not aligned pos: ", position,
                           " alignment: ", alignment);
  } else {
    return Status::OK();
  }
}

namespace {

Status ReadMessage(io::InputStream* file, MemoryPool* pool, bool copy_metadata,
                   std::unique_ptr<Message>* message) {
  int32_t continuation = 0;
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, file->Read(sizeof(int32_t), &continuation));

  if (bytes_read == 0) {
    // EOS without indication
    *message = nullptr;
    return Status::OK();
  } else if (bytes_read != sizeof(int32_t)) {
    return Status::Invalid("Corrupted message, only ", bytes_read, " bytes available");
  }

  int32_t flatbuffer_length = -1;
  if (continuation == internal::kIpcContinuationToken) {
    // Valid IPC message, read the message length now
    ARROW_ASSIGN_OR_RAISE(bytes_read, file->Read(sizeof(int32_t), &flatbuffer_length));
  } else {
    // ARROW-6314: Backwards compatibility for reading old IPC
    // messages produced prior to version 0.15.0
    flatbuffer_length = continuation;
  }

  if (flatbuffer_length == 0) {
    // EOS
    *message = nullptr;
    return Status::OK();
  }

  std::shared_ptr<Buffer> metadata;
  if (copy_metadata) {
    DCHECK_NE(pool, nullptr);
    RETURN_NOT_OK(AllocateBuffer(pool, flatbuffer_length, &metadata));
    ARROW_ASSIGN_OR_RAISE(bytes_read,
                          file->Read(flatbuffer_length, metadata->mutable_data()));
  } else {
    ARROW_ASSIGN_OR_RAISE(metadata, file->Read(flatbuffer_length));
    bytes_read = metadata->size();
  }
  if (bytes_read != flatbuffer_length) {
    return Status::Invalid("Expected to read ", flatbuffer_length,
                           " metadata bytes, but ", "only read ", bytes_read);
  }

  return Message::ReadFrom(metadata, file, message);
}

}  // namespace

Status ReadMessage(io::InputStream* file, std::unique_ptr<Message>* out) {
  return ReadMessage(file, default_memory_pool(), /*copy_metadata=*/false, out);
}

Status ReadMessageCopy(io::InputStream* file, MemoryPool* pool,
                       std::unique_ptr<Message>* out) {
  return ReadMessage(file, pool, /*copy_metadata=*/true, out);
}

Status WriteMessage(const Buffer& message, const IpcOptions& options,
                    io::OutputStream* file, int32_t* message_length) {
  const int32_t prefix_size = options.write_legacy_ipc_format ? 4 : 8;
  const int32_t flatbuffer_size = static_cast<int32_t>(message.size());

  int32_t padded_message_length = static_cast<int32_t>(
      PaddedLength(flatbuffer_size + prefix_size, options.alignment));

  int32_t padding = padded_message_length - flatbuffer_size - prefix_size;

  // The returned message size includes the length prefix, the flatbuffer,
  // plus padding
  *message_length = padded_message_length;

  // ARROW-6314: Write continuation / padding token
  if (!options.write_legacy_ipc_format) {
    RETURN_NOT_OK(file->Write(&internal::kIpcContinuationToken, sizeof(int32_t)));
  }

  // Write the flatbuffer size prefix including padding
  int32_t padded_flatbuffer_size = padded_message_length - prefix_size;
  RETURN_NOT_OK(file->Write(&padded_flatbuffer_size, sizeof(int32_t)));

  // Write the flatbuffer
  RETURN_NOT_OK(file->Write(message.data(), flatbuffer_size));
  if (padding > 0) {
    RETURN_NOT_OK(file->Write(kPaddingBytes, padding));
  }

  return Status::OK();
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
