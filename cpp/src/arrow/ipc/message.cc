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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

#include "generated/Message_generated.h"

namespace arrow {

class KeyValueMetadata;
class MemoryPool;

namespace ipc {

class Message::MessageImpl {
 public:
  explicit MessageImpl(std::shared_ptr<Buffer> metadata, std::shared_ptr<Buffer> body)
      : metadata_(std::move(metadata)), message_(nullptr), body_(std::move(body)) {}

  Status Open() {
    RETURN_NOT_OK(
        internal::VerifyMessage(metadata_->data(), metadata_->size(), &message_));

    // Check that the metadata version is supported
    if (message_->version() < internal::kMinMetadataVersion) {
      return Status::Invalid("Old metadata version not supported");
    }

    if (message_->custom_metadata() != nullptr) {
      // Deserialize from Flatbuffers if first time called
      RETURN_NOT_OK(
          internal::GetKeyValueMetadata(message_->custom_metadata(), &custom_metadata_));
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

  const std::shared_ptr<const KeyValueMetadata>& custom_metadata() const {
    return custom_metadata_;
  }

 private:
  // The Flatbuffer metadata
  std::shared_ptr<Buffer> metadata_;
  const flatbuf::Message* message_;

  // The recontructed custom_metadata field from the Message Flatbuffer
  std::shared_ptr<const KeyValueMetadata> custom_metadata_;

  // The message body, if any
  std::shared_ptr<Buffer> body_;
};

Message::Message(std::shared_ptr<Buffer> metadata, std::shared_ptr<Buffer> body) {
  impl_.reset(new MessageImpl(std::move(metadata), std::move(body)));
}

Result<std::unique_ptr<Message>> Message::Open(std::shared_ptr<Buffer> metadata,
                                               std::shared_ptr<Buffer> body) {
  std::unique_ptr<Message> result(new Message(std::move(metadata), std::move(body)));
  RETURN_NOT_OK(result->impl_->Open());
  return std::move(result);
}

Message::~Message() {}

std::shared_ptr<Buffer> Message::body() const { return impl_->body(); }

int64_t Message::body_length() const { return impl_->body_length(); }

std::shared_ptr<Buffer> Message::metadata() const { return impl_->metadata(); }

Message::Type Message::type() const { return impl_->type(); }

MetadataVersion Message::metadata_version() const { return impl_->version(); }

const void* Message::header() const { return impl_->header(); }

const std::shared_ptr<const KeyValueMetadata>& Message::custom_metadata() const {
  return impl_->custom_metadata();
}

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
    ARROW_ASSIGN_OR_RAISE(*metadata, (*metadata)->CopySlice(0, (*metadata)->size()));
  }
  return Status::OK();
}

Status CheckMetadataAndGetBodyLength(const Buffer& metadata, int64_t* body_length) {
  const flatbuf::Message* fb_message;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &fb_message));
  *body_length = fb_message->bodyLength();
  return Status::OK();
}

Result<std::unique_ptr<Message>> Message::ReadFrom(std::shared_ptr<Buffer> metadata,
                                                   io::InputStream* stream) {
  std::unique_ptr<Message> result;
  auto receiver = std::make_shared<MessageReceiverAssign>(&result);
  MessageEmitter emitter(receiver, MessageEmitter::State::METADATA, metadata->size());
  ARROW_RETURN_NOT_OK(emitter.Consume(metadata));

  ARROW_ASSIGN_OR_RAISE(auto body, stream->Read(emitter.next_required_size()));
  if (body->size() < emitter.next_required_size()) {
    return Status::IOError("Expected to be able to read ", emitter.next_required_size(),
                           " bytes for message body, got ", body->size());
  }
  RETURN_NOT_OK(emitter.Consume(body));
  return result;
}

Result<std::unique_ptr<Message>> Message::ReadFrom(const int64_t offset,
                                                   std::shared_ptr<Buffer> metadata,
                                                   io::RandomAccessFile* file) {
  std::unique_ptr<Message> result;
  auto receiver = std::make_shared<MessageReceiverAssign>(&result);
  MessageEmitter emitter(receiver, MessageEmitter::State::METADATA, metadata->size());
  ARROW_RETURN_NOT_OK(emitter.Consume(metadata));

  ARROW_ASSIGN_OR_RAISE(auto body, file->ReadAt(offset, emitter.next_required_size()));
  if (body->size() < emitter.next_required_size()) {
    return Status::IOError("Expected to be able to read ", emitter.next_required_size(),
                           " bytes for message body, got ", body->size());
  }
  RETURN_NOT_OK(emitter.Consume(body));
  return result;
}

Status WritePadding(io::OutputStream* stream, int64_t nbytes) {
  while (nbytes > 0) {
    const int64_t bytes_to_write = std::min<int64_t>(nbytes, kArrowAlignment);
    RETURN_NOT_OK(stream->Write(kPaddingBytes, bytes_to_write));
    nbytes -= bytes_to_write;
  }
  return Status::OK();
}

Status Message::SerializeTo(io::OutputStream* stream, const IpcWriteOptions& options,
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

Result<std::unique_ptr<Message>> ReadMessage(int64_t offset, int32_t metadata_length,
                                             io::RandomAccessFile* file) {
  std::unique_ptr<Message> result;
  auto receiver = std::make_shared<MessageReceiverAssign>(&result);
  MessageEmitter emitter(receiver);

  if (metadata_length < emitter.next_required_size()) {
    return Status::Invalid("metadata_length should be at least ",
                           emitter.next_required_size());
  }

  ARROW_ASSIGN_OR_RAISE(auto metadata, file->ReadAt(offset, metadata_length));
  if (metadata->size() < metadata_length) {
    return Status::Invalid("Expected to read ", metadata_length,
                           " metadata bytes but got ", metadata->size());
  }
  ARROW_RETURN_NOT_OK(emitter.Consume(metadata));

  switch (emitter.state()) {
    case MessageEmitter::State::INITIAL:
      return result;
    case MessageEmitter::State::METADATA_LENGTH:
      return Status::Invalid("metadata length is missing. File offset: ", offset,
                             ", metadata length: ", metadata_length);
    case MessageEmitter::State::METADATA:
      return Status::Invalid("flatbuffer size ", emitter.next_required_size(),
                             " invalid. File offset: ", offset,
                             ", metadata length: ", metadata_length);
    case MessageEmitter::State::BODY: {
      ARROW_ASSIGN_OR_RAISE(auto body, file->ReadAt(offset + metadata_length,
                                                    emitter.next_required_size()));
      if (body->size() < emitter.next_required_size()) {
        return Status::IOError("Expected to be able to read ",
                               emitter.next_required_size(),
                               " bytes for message body, got ", body->size());
      }
      RETURN_NOT_OK(emitter.Consume(body));
      return result;
    }
    case MessageEmitter::State::EOS:
      return Status::Invalid("Unexpected empty message in IPC file format");
    default:
      return Status::Invalid("Unexpected state: ", emitter.state());
  }
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

Result<std::unique_ptr<Message>> ReadMessage(io::InputStream* file, MemoryPool* pool) {
  std::unique_ptr<Message> message;
  auto receiver = std::make_shared<MessageReceiverAssign>(&message);
  MessageEmitter emitter(receiver, pool);

  {
    uint8_t continuation[sizeof(int32_t)];
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, file->Read(sizeof(int32_t), &continuation));
    if (bytes_read == 0) {
      // EOS without indication
      return nullptr;
    } else if (bytes_read != emitter.next_required_size()) {
      return Status::Invalid("Corrupted message, only ", bytes_read, " bytes available");
    }
    ARROW_RETURN_NOT_OK(emitter.Consume(continuation, bytes_read));
  }

  if (emitter.state() == MessageEmitter::State::METADATA_LENGTH) {
    // Valid IPC message, read the message length now
    uint8_t metadata_length[sizeof(int32_t)];
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                          file->Read(sizeof(int32_t), &metadata_length));
    if (bytes_read != emitter.next_required_size()) {
      return Status::Invalid("Corrupted metadata length, only ", bytes_read,
                             " bytes available");
    }
    ARROW_RETURN_NOT_OK(emitter.Consume(metadata_length, bytes_read));
  }

  if (emitter.state() == MessageEmitter::State::EOS) {
    return nullptr;
  }

  auto metadata_length = emitter.next_required_size();
  ARROW_ASSIGN_OR_RAISE(auto metadata, file->Read(metadata_length));
  if (metadata->size() != metadata_length) {
    return Status::Invalid("Expected to read ", metadata_length, " metadata bytes, but ",
                           "only read ", metadata->size());
  }
  RETURN_NOT_OK(emitter.Consume(metadata));

  if (emitter.state() == MessageEmitter::State::BODY) {
    ARROW_ASSIGN_OR_RAISE(auto body, file->Read(emitter.next_required_size()));
    if (body->size() < emitter.next_required_size()) {
      return Status::IOError("Expected to be able to read ", emitter.next_required_size(),
                             " bytes for message body, got ", body->size());
    }
    ARROW_RETURN_NOT_OK(emitter.Consume(body));
  }

  if (!message) {
    return Status::Invalid("Failed to read message");
  }
  return message;
}

Status WriteMessage(const Buffer& message, const IpcWriteOptions& options,
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
// Implement MessageEmitter

static constexpr auto kMessageEmitterNextRequiredSizeInitial = sizeof(int32_t);
static constexpr auto kMessageEmitterNextRequiredSizeMetadataLength = sizeof(int32_t);

class MessageEmitter::MessageEmitterImpl {
 public:
  explicit MessageEmitterImpl(std::shared_ptr<MessageReceiver> receiver,
                              State initial_state, int64_t initial_next_required_size,
                              MemoryPool* pool)
      : receiver_(std::move(receiver)),
        pool_(pool),
        state_(initial_state),
        next_required_size_(initial_next_required_size),
        chunks_(),
        buffered_size_(0),
        metadata_(nullptr) {}

  Status ConsumeData(const uint8_t* data, int64_t size) {
    if (buffered_size_ == 0) {
      while (size > 0 && size >= next_required_size_) {
        auto used_size = next_required_size_;
        switch (state_) {
          case State::INITIAL:
            RETURN_NOT_OK(ConsumeInitialData(data, next_required_size_));
            break;
          case State::METADATA_LENGTH:
            RETURN_NOT_OK(ConsumeMetadataLengthData(data, next_required_size_));
            break;
          case State::METADATA: {
            auto buffer = std::make_shared<Buffer>(data, next_required_size_);
            RETURN_NOT_OK(ConsumeMetadataBuffer(&buffer));
          } break;
          case State::BODY: {
            auto buffer = std::make_shared<Buffer>(data, next_required_size_);
            RETURN_NOT_OK(ConsumeBodyBuffer(&buffer));
          } break;
          case State::EOS:
            return Status::OK();
        }
        data += used_size;
        size -= used_size;
      }
    }

    if (size == 0) {
      return Status::OK();
    }

    chunks_.push_back(std::make_shared<Buffer>(data, size));
    buffered_size_ += size;
    return ConsumeChunks();
  }

  Status ConsumeBuffer(std::shared_ptr<Buffer>* buffer) {
    if (buffered_size_ == 0) {
      while ((*buffer)->size() >= next_required_size_) {
        auto used_size = next_required_size_;
        switch (state_) {
          case State::INITIAL:
            RETURN_NOT_OK(ConsumeInitialBuffer(buffer));
            break;
          case State::METADATA_LENGTH:
            RETURN_NOT_OK(ConsumeMetadataLengthBuffer(buffer));
            break;
          case State::METADATA:
            if ((*buffer)->size() == next_required_size_) {
              return ConsumeMetadataBuffer(buffer);
            } else {
              auto sliced_buffer = SliceBuffer(*buffer, 0, next_required_size_);
              RETURN_NOT_OK(ConsumeMetadataBuffer(&sliced_buffer));
            }
            break;
          case State::BODY:
            if ((*buffer)->size() == next_required_size_) {
              return ConsumeBodyBuffer(buffer);
            } else {
              auto sliced_buffer = SliceBuffer(*buffer, 0, next_required_size_);
              RETURN_NOT_OK(ConsumeBodyBuffer(&sliced_buffer));
            }
            break;
          case State::EOS:
            return Status::OK();
        }
        if ((*buffer)->size() == used_size) {
          return Status::OK();
        }
        *buffer = SliceBuffer(*buffer, used_size);
      }
    }

    if ((*buffer)->size() == 0) {
      return Status::OK();
    }

    buffered_size_ += (*buffer)->size();
    chunks_.push_back(std::move(*buffer));
    return ConsumeChunks();
  }

  int64_t next_required_size() const { return next_required_size_ - buffered_size_; }

  MessageEmitter::State state() const { return state_; }

 private:
  Status ConsumeChunks() {
    while (state_ != State::EOS) {
      if (buffered_size_ < next_required_size_) {
        return Status::OK();
      }

      switch (state_) {
        case State::INITIAL:
          RETURN_NOT_OK(ConsumeInitialChunks());
          break;
        case State::METADATA_LENGTH:
          RETURN_NOT_OK(ConsumeMetadataLengthChunks());
          break;
        case State::METADATA:
          RETURN_NOT_OK(ConsumeMetadataChunks());
          break;
        case State::BODY:
          RETURN_NOT_OK(ConsumeBodyChunks());
          break;
        case State::EOS:
          return Status::OK();
      }
    }

    return Status::OK();
  }

  Status ConsumeInitialData(const uint8_t* data, int64_t size) {
    return ConsumeInitial(util::SafeLoadAs<int32_t>(data));
  }

  Status ConsumeInitialBuffer(std::shared_ptr<Buffer>* buffer) {
    ARROW_ASSIGN_OR_RAISE(auto continuation, ConsumeDataBufferInt32(buffer));
    return ConsumeInitial(continuation);
  }

  Status ConsumeInitialChunks() {
    int32_t continuation = 0;
    RETURN_NOT_OK(ConsumeDataChunks(sizeof(int32_t), &continuation));
    return ConsumeInitial(continuation);
  }

  Status ConsumeInitial(int32_t continuation) {
    if (continuation == internal::kIpcContinuationToken) {
      state_ = State::METADATA_LENGTH;
      next_required_size_ = kMessageEmitterNextRequiredSizeMetadataLength;
      // Valid IPC message, read the message length now
      return Status::OK();
    } else if (continuation == 0) {
      state_ = State::EOS;
      next_required_size_ = 0;
      return Status::OK();
    } else {
      state_ = State::METADATA;
      // ARROW-6314: Backwards compatibility for reading old IPC
      // messages produced prior to version 0.15.0
      next_required_size_ = continuation;
      return Status::OK();
    }
  }

  Status ConsumeMetadataLengthData(const uint8_t* data, int64_t size) {
    return ConsumeMetadataLength(util::SafeLoadAs<int32_t>(data));
  }

  Status ConsumeMetadataLengthBuffer(std::shared_ptr<Buffer>* buffer) {
    ARROW_ASSIGN_OR_RAISE(auto metadata_length, ConsumeDataBufferInt32(buffer));
    return ConsumeMetadataLength(metadata_length);
  }

  Status ConsumeMetadataLengthChunks() {
    int32_t metadata_length = 0;
    RETURN_NOT_OK(ConsumeDataChunks(sizeof(int32_t), &metadata_length));
    return ConsumeMetadataLength(metadata_length);
  }

  Status ConsumeMetadataLength(int32_t metadata_length) {
    if (metadata_length == 0) {
      state_ = State::EOS;
      next_required_size_ = 0;
      return Status::OK();
    } else {
      state_ = State::METADATA;
      next_required_size_ = metadata_length;
      return Status::OK();
    }
  }

  Status ConsumeMetadataBuffer(std::shared_ptr<Buffer>* buffer) {
    if ((*buffer)->is_cpu()) {
      metadata_ = std::move(*buffer);
    } else {
      ARROW_ASSIGN_OR_RAISE(
          metadata_, Buffer::ViewOrCopy(*buffer, CPUDevice::memory_manager(pool_)));
    }
    return ConsumeMetadata();
  }

  Status ConsumeMetadataChunks() {
    if (chunks_[0]->size() >= next_required_size_) {
      if (chunks_[0]->size() == next_required_size_) {
        if (chunks_[0]->is_cpu()) {
          metadata_ = std::move(chunks_[0]);
        } else {
          ARROW_ASSIGN_OR_RAISE(
              metadata_,
              Buffer::ViewOrCopy(chunks_[0], CPUDevice::memory_manager(pool_)));
        }
        chunks_.erase(chunks_.begin());
      } else {
        metadata_ = SliceBuffer(chunks_[0], 0, next_required_size_);
        if (!chunks_[0]->is_cpu()) {
          ARROW_ASSIGN_OR_RAISE(
              metadata_, Buffer::ViewOrCopy(metadata_, CPUDevice::memory_manager(pool_)));
        }
        chunks_[0] = SliceBuffer(chunks_[0], next_required_size_);
      }
      buffered_size_ -= next_required_size_;
    } else {
      ARROW_ASSIGN_OR_RAISE(auto metadata, AllocateBuffer(next_required_size_, pool_));
      metadata_ = std::shared_ptr<Buffer>(metadata.release());
      RETURN_NOT_OK(ConsumeDataChunks(next_required_size_, metadata_->mutable_data()));
    }
    return ConsumeMetadata();
  }

  Status ConsumeMetadata() {
    RETURN_NOT_OK(MaybeAlignMetadata(&metadata_));
    int64_t body_length = -1;
    RETURN_NOT_OK(CheckMetadataAndGetBodyLength(*metadata_, &body_length));

    state_ = State::BODY;
    next_required_size_ = body_length;
    if (next_required_size_ == 0) {
      ARROW_ASSIGN_OR_RAISE(auto body, AllocateBuffer(0, pool_));
      std::shared_ptr<Buffer> shared_body(body.release());
      return ConsumeBody(&shared_body);
    } else {
      return Status::OK();
    }
  }

  Status ConsumeBodyBuffer(std::shared_ptr<Buffer>* buffer) {
    return ConsumeBody(buffer);
  }

  Status ConsumeBodyChunks() {
    if (chunks_[0]->size() >= next_required_size_) {
      auto used_size = next_required_size_;
      if (chunks_[0]->size() == next_required_size_) {
        RETURN_NOT_OK(ConsumeBody(&chunks_[0]));
        chunks_.erase(chunks_.begin());
      } else {
        auto body = SliceBuffer(chunks_[0], 0, next_required_size_);
        RETURN_NOT_OK(ConsumeBody(&body));
        chunks_[0] = SliceBuffer(chunks_[0], used_size);
      }
      buffered_size_ -= used_size;
      return Status::OK();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto body, AllocateBuffer(next_required_size_, pool_));
      RETURN_NOT_OK(ConsumeDataChunks(next_required_size_, body->mutable_data()));
      std::shared_ptr<Buffer> shared_body(body.release());
      return ConsumeBody(&shared_body);
    }
  }

  Status ConsumeBody(std::shared_ptr<Buffer>* buffer) {
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Message> message,
                          Message::Open(metadata_, *buffer));

    state_ = State::INITIAL;
    next_required_size_ = kMessageEmitterNextRequiredSizeInitial;
    RETURN_NOT_OK(receiver_->Received(std::move(message)));
    return Status::OK();
  }

  Result<int32_t> ConsumeDataBufferInt32(std::shared_ptr<Buffer>* buffer) {
    if ((*buffer)->is_cpu()) {
      return util::SafeLoadAs<int32_t>((*buffer)->data());
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto cpu_buffer, Buffer::ViewOrCopy(*buffer, CPUDevice::memory_manager(pool_)));
      return util::SafeLoadAs<int32_t>(cpu_buffer->data());
    }
  }

  Status ConsumeDataChunks(int64_t nbytes, void* out) {
    size_t offset = 0;
    size_t n_used_chunks = 0;
    auto required_size = nbytes;
    std::shared_ptr<Buffer> last_chunk;
    for (auto& chunk : chunks_) {
      if (!chunk->is_cpu()) {
        ARROW_ASSIGN_OR_RAISE(
            chunk, Buffer::ViewOrCopy(chunk, CPUDevice::memory_manager(pool_)));
      }
      auto data = chunk->data();
      auto data_size = chunk->size();
      auto copy_size = std::min(required_size, data_size);
      memcpy(static_cast<uint8_t*>(out) + offset, data, copy_size);
      n_used_chunks++;
      offset += copy_size;
      required_size -= copy_size;
      if (required_size == 0) {
        if (data_size != copy_size) {
          last_chunk = SliceBuffer(chunk, copy_size);
        }
        break;
      }
    }
    chunks_.erase(chunks_.begin(), chunks_.begin() + n_used_chunks);
    if (last_chunk.get() != nullptr) {
      chunks_.insert(chunks_.begin(), std::move(last_chunk));
    }
    buffered_size_ -= offset;
    return Status::OK();
  }

  std::shared_ptr<MessageReceiver> receiver_;
  MemoryPool* pool_;
  State state_;
  int64_t next_required_size_;
  std::vector<std::shared_ptr<Buffer>> chunks_;
  int64_t buffered_size_;
  std::shared_ptr<Buffer> metadata_;  // Must be CPU buffer
};

MessageEmitter::MessageEmitter(std::shared_ptr<MessageReceiver> receiver,
                               MemoryPool* pool) {
  impl_.reset(new MessageEmitterImpl(std::move(receiver), State::INITIAL,
                                     kMessageEmitterNextRequiredSizeInitial, pool));
}

MessageEmitter::MessageEmitter(std::shared_ptr<MessageReceiver> receiver,
                               State initial_state, int64_t initial_next_required_size,
                               MemoryPool* pool) {
  impl_.reset(new MessageEmitterImpl(std::move(receiver), initial_state,
                                     initial_next_required_size, pool));
}

MessageEmitter::~MessageEmitter() {}

Status MessageEmitter::Consume(const uint8_t* data, int64_t size) {
  return impl_->ConsumeData(data, size);
}

Status MessageEmitter::Consume(std::shared_ptr<Buffer> buffer) {
  return impl_->ConsumeBuffer(&buffer);
}

int64_t MessageEmitter::next_required_size() const { return impl_->next_required_size(); }

MessageEmitter::State MessageEmitter::state() const { return impl_->state(); }

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

  Result<std::unique_ptr<Message>> ReadNextMessage() { return ReadMessage(stream_); }

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

// ----------------------------------------------------------------------
// Deprecated functions

Status ReadMessage(int64_t offset, int32_t metadata_length, io::RandomAccessFile* file,
                   std::unique_ptr<Message>* message) {
  return ReadMessage(offset, metadata_length, file).Value(message);
}

Status ReadMessage(io::InputStream* file, std::unique_ptr<Message>* out) {
  return ReadMessage(file, default_memory_pool()).Value(out);
}

}  // namespace ipc
}  // namespace arrow
