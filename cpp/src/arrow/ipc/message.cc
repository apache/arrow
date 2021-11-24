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
#include "arrow/ipc/reader.h"
#include "arrow/ipc/reader_internal.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/util/endian.h"
#include "arrow/util/future.h"
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

    if (message_->version() > flatbuf::MetadataVersion::MAX) {
      return Status::Invalid("Unsupported future MetadataVersion: ",
                             static_cast<int16_t>(message_->version()));
    }

    if (message_->custom_metadata() != nullptr) {
      // Deserialize from Flatbuffers if first time called
      std::shared_ptr<KeyValueMetadata> md;
      RETURN_NOT_OK(internal::GetKeyValueMetadata(message_->custom_metadata(), &md));
      custom_metadata_ = std::move(md);  // const-ify
    }

    return Status::OK();
  }

  MessageType type() const {
    switch (message_->header_type()) {
      case flatbuf::MessageHeader::Schema:
        return MessageType::SCHEMA;
      case flatbuf::MessageHeader::DictionaryBatch:
        return MessageType::DICTIONARY_BATCH;
      case flatbuf::MessageHeader::RecordBatch:
        return MessageType::RECORD_BATCH;
      case flatbuf::MessageHeader::Tensor:
        return MessageType::TENSOR;
      case flatbuf::MessageHeader::SparseTensor:
        return MessageType::SPARSE_TENSOR;
      default:
        return MessageType::NONE;
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

  // The reconstructed custom_metadata field from the Message Flatbuffer
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

MessageType Message::type() const { return impl_->type(); }

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
  const flatbuf::Message* fb_message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &fb_message));
  *body_length = fb_message->bodyLength();
  if (*body_length < 0) {
    return Status::IOError("Invalid IPC message: negative bodyLength");
  }
  return Status::OK();
}

Result<std::unique_ptr<Message>> Message::ReadFrom(std::shared_ptr<Buffer> metadata,
                                                   io::InputStream* stream) {
  std::unique_ptr<Message> result;
  auto listener = std::make_shared<AssignMessageDecoderListener>(&result);
  MessageDecoder decoder(listener, MessageDecoder::State::METADATA, metadata->size());
  ARROW_RETURN_NOT_OK(decoder.Consume(metadata));

  ARROW_ASSIGN_OR_RAISE(auto body, stream->Read(decoder.next_required_size()));
  if (body->size() < decoder.next_required_size()) {
    return Status::IOError("Expected to be able to read ", decoder.next_required_size(),
                           " bytes for message body, got ", body->size());
  }
  RETURN_NOT_OK(decoder.Consume(body));
  return std::move(result);
}

Result<std::unique_ptr<Message>> Message::ReadFrom(const int64_t offset,
                                                   std::shared_ptr<Buffer> metadata,
                                                   io::RandomAccessFile* file) {
  std::unique_ptr<Message> result;
  auto listener = std::make_shared<AssignMessageDecoderListener>(&result);
  MessageDecoder decoder(listener, MessageDecoder::State::METADATA, metadata->size());
  ARROW_RETURN_NOT_OK(decoder.Consume(metadata));

  ARROW_ASSIGN_OR_RAISE(auto body, file->ReadAt(offset, decoder.next_required_size()));
  if (body->size() < decoder.next_required_size()) {
    return Status::IOError("Expected to be able to read ", decoder.next_required_size(),
                           " bytes for message body, got ", body->size());
  }
  RETURN_NOT_OK(decoder.Consume(body));
  return std::move(result);
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

std::string FormatMessageType(MessageType type) {
  switch (type) {
    case MessageType::SCHEMA:
      return "schema";
    case MessageType::RECORD_BATCH:
      return "record batch";
    case MessageType::DICTIONARY_BATCH:
      return "dictionary";
    case MessageType::TENSOR:
      return "tensor";
    case MessageType::SPARSE_TENSOR:
      return "sparse tensor";
    default:
      break;
  }
  return "unknown";
}

Status ReadFieldsSubset(int64_t offset, int32_t metadata_length,
                        io::RandomAccessFile* file,
                        const FieldsLoaderFunction& fields_loader,
                        const std::shared_ptr<Buffer>& metadata, int64_t required_size,
                        std::shared_ptr<Buffer>& body) {
  const flatbuf::Message* message = nullptr;
  uint8_t continuation_metadata_size = sizeof(int32_t) + sizeof(int32_t);
  // skip 8 bytes (32-bit continuation indicator + 32-bit little-endian length prefix)
  RETURN_NOT_OK(internal::VerifyMessage(metadata->data() + continuation_metadata_size,
                                        metadata->size() - continuation_metadata_size,
                                        &message));
  auto batch = message->header_as_RecordBatch();
  if (batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not RecordBatch.");
  }
  internal::IoRecordedRandomAccessFile io_recorded_random_access_file(required_size);
  RETURN_NOT_OK(fields_loader(batch, &io_recorded_random_access_file));
  auto const& read_ranges = io_recorded_random_access_file.GetReadRanges();
  for (auto const& range : read_ranges) {
    auto read_result = file->ReadAt(offset + metadata_length + range.offset, range.length,
                                    body->mutable_data() + range.offset);
    if (!read_result.ok()) {
      return Status::IOError("Failed to read message body, error ",
                             read_result.status().ToString());
    }
  }
  return Status::OK();
}

Result<std::unique_ptr<Message>> ReadMessage(int64_t offset, int32_t metadata_length,
                                             io::RandomAccessFile* file,
                                             const FieldsLoaderFunction& fields_loader) {
  std::unique_ptr<Message> result;
  auto listener = std::make_shared<AssignMessageDecoderListener>(&result);
  MessageDecoder decoder(listener);

  if (metadata_length < decoder.next_required_size()) {
    return Status::Invalid("metadata_length should be at least ",
                           decoder.next_required_size());
  }

  ARROW_ASSIGN_OR_RAISE(auto metadata, file->ReadAt(offset, metadata_length));
  if (metadata->size() < metadata_length) {
    return Status::Invalid("Expected to read ", metadata_length,
                           " metadata bytes but got ", metadata->size());
  }
  ARROW_RETURN_NOT_OK(decoder.Consume(metadata));

  switch (decoder.state()) {
    case MessageDecoder::State::INITIAL:
      return std::move(result);
    case MessageDecoder::State::METADATA_LENGTH:
      return Status::Invalid("metadata length is missing. File offset: ", offset,
                             ", metadata length: ", metadata_length);
    case MessageDecoder::State::METADATA:
      return Status::Invalid("flatbuffer size ", decoder.next_required_size(),
                             " invalid. File offset: ", offset,
                             ", metadata length: ", metadata_length);
    case MessageDecoder::State::BODY: {
      std::shared_ptr<Buffer> body;
      if (fields_loader) {
        ARROW_ASSIGN_OR_RAISE(
            body, AllocateBuffer(decoder.next_required_size(), default_memory_pool()));
        RETURN_NOT_OK(ReadFieldsSubset(offset, metadata_length, file, fields_loader,
                                       metadata, decoder.next_required_size(), body));
      } else {
        ARROW_ASSIGN_OR_RAISE(
            body, file->ReadAt(offset + metadata_length, decoder.next_required_size()));
      }
      if (body->size() < decoder.next_required_size()) {
        return Status::IOError("Expected to be able to read ",
                               decoder.next_required_size(),
                               " bytes for message body, got ", body->size());
      }
      RETURN_NOT_OK(decoder.Consume(body));
      return std::move(result);
    }
    case MessageDecoder::State::EOS:
      return Status::Invalid("Unexpected empty message in IPC file format");
    default:
      return Status::Invalid("Unexpected state: ", decoder.state());
  }
}

Future<std::shared_ptr<Message>> ReadMessageAsync(int64_t offset, int32_t metadata_length,
                                                  int64_t body_length,
                                                  io::RandomAccessFile* file,
                                                  const io::IOContext& context) {
  struct State {
    std::unique_ptr<Message> result;
    std::shared_ptr<MessageDecoderListener> listener;
    std::shared_ptr<MessageDecoder> decoder;
  };
  auto state = std::make_shared<State>();
  state->listener = std::make_shared<AssignMessageDecoderListener>(&state->result);
  state->decoder = std::make_shared<MessageDecoder>(state->listener);

  if (metadata_length < state->decoder->next_required_size()) {
    return Status::Invalid("metadata_length should be at least ",
                           state->decoder->next_required_size());
  }
  return file->ReadAsync(context, offset, metadata_length + body_length)
      .Then([=](std::shared_ptr<Buffer> metadata) -> Result<std::shared_ptr<Message>> {
        if (metadata->size() < metadata_length) {
          return Status::Invalid("Expected to read ", metadata_length,
                                 " metadata bytes but got ", metadata->size());
        }
        ARROW_RETURN_NOT_OK(
            state->decoder->Consume(SliceBuffer(metadata, 0, metadata_length)));
        switch (state->decoder->state()) {
          case MessageDecoder::State::INITIAL:
            return std::move(state->result);
          case MessageDecoder::State::METADATA_LENGTH:
            return Status::Invalid("metadata length is missing. File offset: ", offset,
                                   ", metadata length: ", metadata_length);
          case MessageDecoder::State::METADATA:
            return Status::Invalid("flatbuffer size ",
                                   state->decoder->next_required_size(),
                                   " invalid. File offset: ", offset,
                                   ", metadata length: ", metadata_length);
          case MessageDecoder::State::BODY: {
            auto body = SliceBuffer(metadata, metadata_length, body_length);
            if (body->size() < state->decoder->next_required_size()) {
              return Status::IOError("Expected to be able to read ",
                                     state->decoder->next_required_size(),
                                     " bytes for message body, got ", body->size());
            }
            RETURN_NOT_OK(state->decoder->Consume(body));
            return std::move(state->result);
          }
          case MessageDecoder::State::EOS:
            return Status::Invalid("Unexpected empty message in IPC file format");
          default:
            return Status::Invalid("Unexpected state: ", state->decoder->state());
        }
      });
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

Status DecodeMessage(MessageDecoder* decoder, io::InputStream* file) {
  if (decoder->state() == MessageDecoder::State::INITIAL) {
    uint8_t continuation[sizeof(int32_t)];
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, file->Read(sizeof(int32_t), &continuation));
    if (bytes_read == 0) {
      // EOS without indication
      return Status::OK();
    } else if (bytes_read != decoder->next_required_size()) {
      return Status::Invalid("Corrupted message, only ", bytes_read, " bytes available");
    }
    ARROW_RETURN_NOT_OK(decoder->Consume(continuation, bytes_read));
  }

  if (decoder->state() == MessageDecoder::State::METADATA_LENGTH) {
    // Valid IPC message, read the message length now
    uint8_t metadata_length[sizeof(int32_t)];
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                          file->Read(sizeof(int32_t), &metadata_length));
    if (bytes_read != decoder->next_required_size()) {
      return Status::Invalid("Corrupted metadata length, only ", bytes_read,
                             " bytes available");
    }
    ARROW_RETURN_NOT_OK(decoder->Consume(metadata_length, bytes_read));
  }

  if (decoder->state() == MessageDecoder::State::EOS) {
    return Status::OK();
  }

  auto metadata_length = decoder->next_required_size();
  ARROW_ASSIGN_OR_RAISE(auto metadata, file->Read(metadata_length));
  if (metadata->size() != metadata_length) {
    return Status::Invalid("Expected to read ", metadata_length, " metadata bytes, but ",
                           "only read ", metadata->size());
  }
  ARROW_RETURN_NOT_OK(decoder->Consume(metadata));

  if (decoder->state() == MessageDecoder::State::BODY) {
    ARROW_ASSIGN_OR_RAISE(auto body, file->Read(decoder->next_required_size()));
    if (body->size() < decoder->next_required_size()) {
      return Status::IOError("Expected to be able to read ",
                             decoder->next_required_size(),
                             " bytes for message body, got ", body->size());
    }
    ARROW_RETURN_NOT_OK(decoder->Consume(body));
  }

  if (decoder->state() == MessageDecoder::State::INITIAL ||
      decoder->state() == MessageDecoder::State::EOS) {
    return Status::OK();
  } else {
    return Status::Invalid("Failed to decode message");
  }
}

Result<std::unique_ptr<Message>> ReadMessage(io::InputStream* file, MemoryPool* pool) {
  std::unique_ptr<Message> message;
  auto listener = std::make_shared<AssignMessageDecoderListener>(&message);
  MessageDecoder decoder(listener, pool);
  ARROW_RETURN_NOT_OK(DecodeMessage(&decoder, file));
  if (!message) {
    return nullptr;
  } else {
    return std::move(message);
  }
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

  // Write the flatbuffer size prefix including padding in little endian
  int32_t padded_flatbuffer_size =
      BitUtil::ToLittleEndian(padded_message_length - prefix_size);
  RETURN_NOT_OK(file->Write(&padded_flatbuffer_size, sizeof(int32_t)));

  // Write the flatbuffer
  RETURN_NOT_OK(file->Write(message.data(), flatbuffer_size));
  if (padding > 0) {
    RETURN_NOT_OK(file->Write(kPaddingBytes, padding));
  }

  return Status::OK();
}

// ----------------------------------------------------------------------
// Implement MessageDecoder

Status MessageDecoderListener::OnInitial() { return Status::OK(); }
Status MessageDecoderListener::OnMetadataLength() { return Status::OK(); }
Status MessageDecoderListener::OnMetadata() { return Status::OK(); }
Status MessageDecoderListener::OnBody() { return Status::OK(); }
Status MessageDecoderListener::OnEOS() { return Status::OK(); }

static constexpr auto kMessageDecoderNextRequiredSizeInitial = sizeof(int32_t);
static constexpr auto kMessageDecoderNextRequiredSizeMetadataLength = sizeof(int32_t);

class MessageDecoder::MessageDecoderImpl {
 public:
  explicit MessageDecoderImpl(std::shared_ptr<MessageDecoderListener> listener,
                              State initial_state, int64_t initial_next_required_size,
                              MemoryPool* pool)
      : listener_(std::move(listener)),
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
            RETURN_NOT_OK(ConsumeMetadataBuffer(buffer));
          } break;
          case State::BODY: {
            auto buffer = std::make_shared<Buffer>(data, next_required_size_);
            RETURN_NOT_OK(ConsumeBodyBuffer(buffer));
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

  Status ConsumeBuffer(std::shared_ptr<Buffer> buffer) {
    if (buffered_size_ == 0) {
      while (buffer->size() >= next_required_size_) {
        auto used_size = next_required_size_;
        switch (state_) {
          case State::INITIAL:
            RETURN_NOT_OK(ConsumeInitialBuffer(buffer));
            break;
          case State::METADATA_LENGTH:
            RETURN_NOT_OK(ConsumeMetadataLengthBuffer(buffer));
            break;
          case State::METADATA:
            if (buffer->size() == next_required_size_) {
              return ConsumeMetadataBuffer(buffer);
            } else {
              auto sliced_buffer = SliceBuffer(buffer, 0, next_required_size_);
              RETURN_NOT_OK(ConsumeMetadataBuffer(sliced_buffer));
            }
            break;
          case State::BODY:
            if (buffer->size() == next_required_size_) {
              return ConsumeBodyBuffer(buffer);
            } else {
              auto sliced_buffer = SliceBuffer(buffer, 0, next_required_size_);
              RETURN_NOT_OK(ConsumeBodyBuffer(sliced_buffer));
            }
            break;
          case State::EOS:
            return Status::OK();
        }
        if (buffer->size() == used_size) {
          return Status::OK();
        }
        buffer = SliceBuffer(buffer, used_size);
      }
    }

    if (buffer->size() == 0) {
      return Status::OK();
    }

    buffered_size_ += buffer->size();
    chunks_.push_back(std::move(buffer));
    return ConsumeChunks();
  }

  int64_t next_required_size() const { return next_required_size_ - buffered_size_; }

  MessageDecoder::State state() const { return state_; }

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
    return ConsumeInitial(BitUtil::FromLittleEndian(util::SafeLoadAs<int32_t>(data)));
  }

  Status ConsumeInitialBuffer(const std::shared_ptr<Buffer>& buffer) {
    ARROW_ASSIGN_OR_RAISE(auto continuation, ConsumeDataBufferInt32(buffer));
    return ConsumeInitial(BitUtil::FromLittleEndian(continuation));
  }

  Status ConsumeInitialChunks() {
    int32_t continuation = 0;
    RETURN_NOT_OK(ConsumeDataChunks(sizeof(int32_t), &continuation));
    return ConsumeInitial(BitUtil::FromLittleEndian(continuation));
  }

  Status ConsumeInitial(int32_t continuation) {
    if (continuation == internal::kIpcContinuationToken) {
      state_ = State::METADATA_LENGTH;
      next_required_size_ = kMessageDecoderNextRequiredSizeMetadataLength;
      RETURN_NOT_OK(listener_->OnMetadataLength());
      // Valid IPC message, read the message length now
      return Status::OK();
    } else if (continuation == 0) {
      state_ = State::EOS;
      next_required_size_ = 0;
      RETURN_NOT_OK(listener_->OnEOS());
      return Status::OK();
    } else if (continuation > 0) {
      state_ = State::METADATA;
      // ARROW-6314: Backwards compatibility for reading old IPC
      // messages produced prior to version 0.15.0
      next_required_size_ = continuation;
      RETURN_NOT_OK(listener_->OnMetadata());
      return Status::OK();
    } else {
      return Status::IOError("Invalid IPC stream: negative continuation token");
    }
  }

  Status ConsumeMetadataLengthData(const uint8_t* data, int64_t size) {
    return ConsumeMetadataLength(
        BitUtil::FromLittleEndian(util::SafeLoadAs<int32_t>(data)));
  }

  Status ConsumeMetadataLengthBuffer(const std::shared_ptr<Buffer>& buffer) {
    ARROW_ASSIGN_OR_RAISE(auto metadata_length, ConsumeDataBufferInt32(buffer));
    return ConsumeMetadataLength(BitUtil::FromLittleEndian(metadata_length));
  }

  Status ConsumeMetadataLengthChunks() {
    int32_t metadata_length = 0;
    RETURN_NOT_OK(ConsumeDataChunks(sizeof(int32_t), &metadata_length));
    return ConsumeMetadataLength(BitUtil::FromLittleEndian(metadata_length));
  }

  Status ConsumeMetadataLength(int32_t metadata_length) {
    if (metadata_length == 0) {
      state_ = State::EOS;
      next_required_size_ = 0;
      RETURN_NOT_OK(listener_->OnEOS());
      return Status::OK();
    } else if (metadata_length > 0) {
      state_ = State::METADATA;
      next_required_size_ = metadata_length;
      RETURN_NOT_OK(listener_->OnMetadata());
      return Status::OK();
    } else {
      return Status::IOError("Invalid IPC message: negative metadata length");
    }
  }

  Status ConsumeMetadataBuffer(const std::shared_ptr<Buffer>& buffer) {
    if (buffer->is_cpu()) {
      metadata_ = buffer;
    } else {
      ARROW_ASSIGN_OR_RAISE(metadata_,
                            Buffer::ViewOrCopy(buffer, CPUDevice::memory_manager(pool_)));
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
    RETURN_NOT_OK(listener_->OnBody());
    if (next_required_size_ == 0) {
      ARROW_ASSIGN_OR_RAISE(auto body, AllocateBuffer(0, pool_));
      std::shared_ptr<Buffer> shared_body(body.release());
      return ConsumeBody(&shared_body);
    } else {
      return Status::OK();
    }
  }

  Status ConsumeBodyBuffer(std::shared_ptr<Buffer> buffer) {
    return ConsumeBody(&buffer);
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

    RETURN_NOT_OK(listener_->OnMessageDecoded(std::move(message)));
    state_ = State::INITIAL;
    next_required_size_ = kMessageDecoderNextRequiredSizeInitial;
    RETURN_NOT_OK(listener_->OnInitial());
    return Status::OK();
  }

  Result<int32_t> ConsumeDataBufferInt32(const std::shared_ptr<Buffer>& buffer) {
    if (buffer->is_cpu()) {
      return util::SafeLoadAs<int32_t>(buffer->data());
    } else {
      ARROW_ASSIGN_OR_RAISE(auto cpu_buffer,
                            Buffer::ViewOrCopy(buffer, CPUDevice::memory_manager(pool_)));
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

  std::shared_ptr<MessageDecoderListener> listener_;
  MemoryPool* pool_;
  State state_;
  int64_t next_required_size_;
  std::vector<std::shared_ptr<Buffer>> chunks_;
  int64_t buffered_size_;
  std::shared_ptr<Buffer> metadata_;  // Must be CPU buffer
};

MessageDecoder::MessageDecoder(std::shared_ptr<MessageDecoderListener> listener,
                               MemoryPool* pool) {
  impl_.reset(new MessageDecoderImpl(std::move(listener), State::INITIAL,
                                     kMessageDecoderNextRequiredSizeInitial, pool));
}

MessageDecoder::MessageDecoder(std::shared_ptr<MessageDecoderListener> listener,
                               State initial_state, int64_t initial_next_required_size,
                               MemoryPool* pool) {
  impl_.reset(new MessageDecoderImpl(std::move(listener), initial_state,
                                     initial_next_required_size, pool));
}

MessageDecoder::~MessageDecoder() {}

Status MessageDecoder::Consume(const uint8_t* data, int64_t size) {
  return impl_->ConsumeData(data, size);
}

Status MessageDecoder::Consume(std::shared_ptr<Buffer> buffer) {
  return impl_->ConsumeBuffer(buffer);
}

int64_t MessageDecoder::next_required_size() const { return impl_->next_required_size(); }

MessageDecoder::State MessageDecoder::state() const { return impl_->state(); }

// ----------------------------------------------------------------------
// Implement InputStream message reader

/// \brief Implementation of MessageReader that reads from InputStream
class InputStreamMessageReader : public MessageReader, public MessageDecoderListener {
 public:
  explicit InputStreamMessageReader(io::InputStream* stream)
      : stream_(stream),
        owned_stream_(),
        message_(),
        decoder_(std::shared_ptr<InputStreamMessageReader>(this, [](void*) {})) {}

  explicit InputStreamMessageReader(const std::shared_ptr<io::InputStream>& owned_stream)
      : InputStreamMessageReader(owned_stream.get()) {
    owned_stream_ = owned_stream;
  }

  ~InputStreamMessageReader() {}

  Status OnMessageDecoded(std::unique_ptr<Message> message) override {
    message_ = std::move(message);
    return Status::OK();
  }

  Result<std::unique_ptr<Message>> ReadNextMessage() override {
    ARROW_RETURN_NOT_OK(DecodeMessage(&decoder_, stream_));
    return std::move(message_);
  }

 private:
  io::InputStream* stream_;
  std::shared_ptr<io::InputStream> owned_stream_;
  std::unique_ptr<Message> message_;
  MessageDecoder decoder_;
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
