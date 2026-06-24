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

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <sstream>

#include <flatbuffers/flatbuffers.h>

#include <gtest/gtest.h>

#include "Message_generated.h"

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/type_fwd.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow::ipc::internal {

using FBB = flatbuffers::FlatBufferBuilder;

// GH-40361: Test that Flatbuffer serialization matches a known output
// byte-for-byte.
//
// Our Flatbuffers code should not depend on argument evaluation order as it's
// undefined (https://en.cppreference.com/w/cpp/language/eval_order) and may
// lead to unnecessary platform- or toolchain-specific differences in
// serialization.
TEST(TestMessageInternal, TestByteIdentical) {
  DictionaryFieldMapper mapper;

  // Create a simple Schema with just two metadata KVPs
  auto f0 = field("f0", int64());
  auto f1 = field("f1", int64());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  std::shared_ptr<KeyValueMetadata> metadata =
      KeyValueMetadata::Make({"key_1", "key_2"}, {"key_1_value", "key_2_value"});
  auto schema = ::arrow::schema({f0}, Endianness::Little, metadata);

  // Serialize the Schema to a Buffer
  ASSERT_OK_AND_ASSIGN(auto out_buffer,
                       WriteSchemaMessage(*schema, mapper, IpcWriteOptions::Defaults()));

  // This is example output from macOS+ARM+LLVM
  const uint8_t expected[] = {
      0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x0E, 0x00, 0x06, 0x00, 0x05, 0x00,
      0x08, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00, 0x10, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x0A, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x04, 0x00, 0x08, 0x00, 0x0A, 0x00,
      0x00, 0x00, 0x6C, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
      0x38, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xD8, 0xFF, 0xFF, 0xFF, 0x18, 0x00,
      0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x6B, 0x65, 0x79, 0x5F,
      0x32, 0x5F, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x00, 0x05, 0x00, 0x00, 0x00, 0x6B, 0x65,
      0x79, 0x5F, 0x32, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0C, 0x00, 0x04, 0x00, 0x08, 0x00,
      0x08, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x0B, 0x00,
      0x00, 0x00, 0x6B, 0x65, 0x79, 0x5F, 0x31, 0x5F, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x00,
      0x05, 0x00, 0x00, 0x00, 0x6B, 0x65, 0x79, 0x5F, 0x31, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x10, 0x00, 0x14, 0x00, 0x08, 0x00, 0x06, 0x00,
      0x07, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x10, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x01, 0x02, 0x10, 0x00, 0x00, 0x00, 0x1C, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x66, 0x30, 0x00, 0x00, 0x08, 0x00,
      0x0C, 0x00, 0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
      0x40, 0x00, 0x00, 0x00};

  Buffer expected_buffer(expected, sizeof(expected));

  AssertBufferEqual(expected_buffer, *out_buffer);
}

TEST(TestMessageInternal, TestEndiannessRoundtrip) {
  DictionaryFieldMapper mapper;

  for (const auto endianness : {Endianness::Little, Endianness::Big}) {
    // Create a simple Schema with just two metadata KVPs
    auto f0 = field("f0", int64());
    auto f1 = field("f1", int64());
    std::vector<std::shared_ptr<Field>> fields = {f0, f1};
    std::shared_ptr<KeyValueMetadata> metadata =
        KeyValueMetadata::Make({"key_1", "key_2"}, {"key_1_value", "key_2_value"});
    auto schema = ::arrow::schema({f0}, endianness, metadata);

    // Serialize the Schema to a Buffer
    ASSERT_OK_AND_ASSIGN(
        auto out_buffer,
        WriteSchemaMessage(*schema, mapper, IpcWriteOptions::Defaults()));

    // Re-open to a new Message and parse Schema
    ASSERT_OK_AND_ASSIGN(auto message, Message::Open(out_buffer, /*body=*/nullptr));
    ASSERT_OK_AND_ASSIGN(auto parsed_schema, ReadSchema(*message, nullptr));
    AssertSchemaEqual(*schema, *parsed_schema, /*check_metadata=*/true);
  }
}

struct SampleMessageParams {
  std::shared_ptr<const KeyValueMetadata> custom_metadata = {};
  int64_t body_length = 0;
  IpcWriteOptions options = {};

  std::string ToString() const {
    std::stringstream ss;
    ss << *this;
    return std::move(ss).str();
  }

  friend std::ostream& operator<<(std::ostream& os, const SampleMessageParams& p) {
    os << "legacy IPC = " << p.options.write_legacy_ipc_format << ", "
       << "body length = " << p.body_length
       << ", metadata length = " << (p.custom_metadata ? p.custom_metadata->size() : 0);
    return os;
  }
};

struct SampleMessage {
  std::shared_ptr<Buffer> metadata_bytes;  // encapsulated with IPC framing
  std::shared_ptr<Buffer> body_bytes;
  MessageType message_type;
  int64_t num_rows = 0;
  std::shared_ptr<const KeyValueMetadata> custom_metadata;
};

class MessageDecodingTest : public ::testing::Test {
 public:
  static constexpr int64_t kNumRows = 5;

  std::vector<IpcWriteOptions> write_options() {
    return {IpcWriteOptions{}, IpcWriteOptions{.write_legacy_ipc_format = true},
            IpcWriteOptions{.alignment = 32}};
  }

  std::vector<SampleMessageParams> message_params() {
    std::vector<SampleMessageParams> params;
    for (const auto& options : write_options()) {
      for (int64_t body_length : {0, 24}) {
        params.push_back(SampleMessageParams{
            .custom_metadata = nullptr, .body_length = body_length, .options = options});
        params.push_back(SampleMessageParams{.custom_metadata = GetCustomMetadata(),
                                             .body_length = body_length,
                                             .options = options});
      }
    }
    return params;
  }

  // Return the serialized metadata encapsulated in IPC framing
  Result<std::shared_ptr<Buffer>> EncapsulateMetadata(
      const std::shared_ptr<Buffer>& metadata_bytes, const IpcWriteOptions& options) {
    ARROW_ASSIGN_OR_RAISE(auto out_stream, ::arrow::io::BufferOutputStream::Create());
    int32_t written_bytes = 0;
    RETURN_NOT_OK(
        WriteMessage(*metadata_bytes, options, out_stream.get(), &written_bytes));
    ARROW_ASSIGN_OR_RAISE(auto out, out_stream->Finish());
    return out;
  }

  Result<SampleMessage> GetSampleMessage(const SampleMessageParams& params) {
    // Create a dummy RecordBatch message
    auto field_md =
        std::vector{FieldMetadata{.length = kNumRows, .null_count = 1, .offset = 0}};
    auto buffer_md = std::vector{BufferMetadata{.offset = 64, .length = 1},
                                 BufferMetadata{.offset = 72, .length = 10}};
    std::shared_ptr<Buffer> out;
    ARROW_ASSIGN_OR_RAISE(
        out, WriteRecordBatchMessage(/*length=*/kNumRows, params.body_length,
                                     params.custom_metadata, field_md, buffer_md,
                                     /*variadic_counts=*/{}, params.options));
    ARROW_ASSIGN_OR_RAISE(out, EncapsulateMetadata(out, params.options));
    // Generate a dummy body of the advertised length
    ARROW_ASSIGN_OR_RAISE(auto body_bytes, AllocateBuffer(params.body_length));
    memset(body_bytes->mutable_data(), '!', body_bytes->size());
    return SampleMessage{.metadata_bytes = out,
                         .body_bytes = std::move(body_bytes),
                         .message_type = MessageType::RECORD_BATCH,
                         .num_rows = kNumRows,
                         .custom_metadata = params.custom_metadata};
  }

  std::shared_ptr<const KeyValueMetadata> GetCustomMetadata() {
    return KeyValueMetadata::Make(/*keys=*/{"key1", "key2"}, /*values=*/{"foo", "bar"});
  }

  void CheckSampleMessage(const Message& message, const SampleMessage& sample_message) {
    ASSERT_EQ(message.type(), sample_message.message_type);
    const auto batch = reinterpret_cast<const flatbuf::RecordBatch*>(message.header());
    ASSERT_EQ(batch->length(), sample_message.num_rows);
    ASSERT_EQ(message.body_length(), sample_message.body_bytes->size());
    if (message.body_length() > 0) {
      AssertBufferEqual(*message.body(), *sample_message.body_bytes, /*verbose=*/true);
    }
    if (sample_message.custom_metadata && sample_message.custom_metadata->size() > 0) {
      ASSERT_NE(message.custom_metadata(), nullptr);
      ASSERT_TRUE(message.custom_metadata()->Equals(*sample_message.custom_metadata));
    } else {
      ASSERT_EQ(message.custom_metadata(), nullptr);
    }
  }

  // Return concatenated metadata and body bytes
  Result<std::shared_ptr<Buffer>> ConcatenateMessage(const SampleMessage& sample_message,
                                                     int64_t padding_size = 0) {
    auto padding_buffer = Buffer::FromString(std::string(padding_size, 'x'));
    return ConcatenateBuffers({padding_buffer, sample_message.metadata_bytes,
                               sample_message.body_bytes, padding_buffer});
  }

  void CheckDecoding(const std::shared_ptr<Buffer>& buffer, int64_t chunk_size,
                     const SampleMessage& sample_message) {
    std::unique_ptr<Message> message;
    auto listener = std::make_shared<AssignMessageDecoderListener>(&message);
    MessageDecoder decoder(listener);
    int64_t offset = 0;
    ASSERT_EQ(decoder.buffered_size(), 0);
    while (offset < buffer->size()) {
      // No message was decoded yet
      ASSERT_EQ(message, nullptr);
      // The decoder is expecting more data, but not more than remaining in our buffer
      ASSERT_GT(decoder.next_required_size(), 0);
      ASSERT_LE(decoder.next_required_size(), buffer->size() - offset);
      const auto to_consume = std::min(chunk_size, buffer->size() - offset);
      ASSERT_OK(decoder.Consume(SliceBuffer(buffer, offset, to_consume)));
      offset += to_consume;
      if (offset >= 4 && offset < buffer->size()) {
        // We went past the initial 4-byte continuation
        ASSERT_NE(decoder.state(), MessageDecoder::INITIAL);
        if (offset >= buffer->size() - sample_message.body_bytes->size()) {
          // The offset points in the body
          ASSERT_EQ(decoder.state(), MessageDecoder::BODY);
        }
      }
    }
    ASSERT_EQ(decoder.buffered_size(), 0);
    ASSERT_EQ(decoder.state(), MessageDecoder::INITIAL);
    ASSERT_NE(message, nullptr);
    CheckSampleMessage(*message, sample_message);
  }

  void TestDecoding(const SampleMessage& sample_message) {
    ASSERT_OK_AND_ASSIGN(auto buffer, ConcatenateMessage(sample_message));
    for (const auto chunk_size : std::vector<int64_t>{
             1, 2, 3, buffer->size() / 3, buffer->size() - 1, buffer->size()}) {
      ARROW_SCOPED_TRACE("chunk_size = ", chunk_size);
      CheckDecoding(buffer, chunk_size, sample_message);
    }
  }

  void TestDecoding(const SampleMessageParams& params) {
    ASSERT_OK_AND_ASSIGN(auto message, GetSampleMessage(params));
    TestDecoding(message);
  }

  template <typename ReadMessageFunc>
  void CheckReadMessageOk(ReadMessageFunc read_message,
                          const SampleMessageParams& params) {
    ASSERT_OK_AND_ASSIGN(auto sample_message, GetSampleMessage(params));
    ASSERT_OK_AND_ASSIGN(auto message, read_message(sample_message));
    CheckSampleMessage(*message, sample_message);
  }

  template <typename ReadMessageFunc>
  void CheckReadMessageTruncated(ReadMessageFunc read_message,
                                 const SampleMessageParams& params,
                                 bool force_truncate_metadata = false) {
    ASSERT_OK_AND_ASSIGN(auto sample_message, GetSampleMessage(params));
    if (force_truncate_metadata || sample_message.body_bytes->size() == 0) {
      sample_message.metadata_bytes =
          SliceBuffer(sample_message.metadata_bytes, /*offset=*/0,
                      sample_message.metadata_bytes->size() - 1);
      sample_message.body_bytes = SliceBuffer(sample_message.body_bytes, /*offset=*/0,
                                              /*length=*/0);
    } else {
      sample_message.body_bytes = SliceBuffer(sample_message.body_bytes, /*offset=*/0,
                                              sample_message.body_bytes->size() - 1);
    }
    Status status = read_message(sample_message).status();
    ASSERT_TRUE(status.IsInvalid() || status.IsIOError())
        << "Unexpected status: " << status.ToString();
  }

  template <typename ReadMessageFunc>
  void CheckReadMessageOversized(ReadMessageFunc read_message,
                                 const SampleMessageParams& params) {
    ASSERT_OK_AND_ASSIGN(auto sample_message, GetSampleMessage(params));
    auto trailing_bytes = Buffer::FromString("x");
    if (sample_message.body_bytes->size() > 0) {
      ASSERT_OK_AND_ASSIGN(
          sample_message.body_bytes,
          ConcatenateBuffers({sample_message.body_bytes, trailing_bytes}));
    } else {
      ASSERT_OK_AND_ASSIGN(
          sample_message.metadata_bytes,
          ConcatenateBuffers({sample_message.metadata_bytes, trailing_bytes}));
    }
    Status status = read_message(sample_message).status();
    ASSERT_TRUE(status.IsInvalid() || status.IsIOError())
        << "Unexpected status: " << status.ToString();
  }
};

TEST_F(MessageDecodingTest, MessageDecoder) {
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    TestDecoding(params);
  }
}

TEST_F(MessageDecodingTest, ReadMessage1) {
  auto read_message = [&](const SampleMessage& sample_message) {
    std::shared_ptr<Buffer> body =
        sample_message.body_bytes->size() > 0 ? sample_message.body_bytes : nullptr;
    return ReadMessage(sample_message.metadata_bytes, body);
  };
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
    CheckReadMessageTruncated(read_message, params);
    CheckReadMessageOversized(read_message, params);
  }
}

TEST_F(MessageDecodingTest, ReadMessage2) {
  auto read_message =
      [&](const SampleMessage& sample_message) -> Result<std::unique_ptr<Message>> {
    const int kStreamOffset = 42;
    ARROW_ASSIGN_OR_RAISE(
        auto stream_buf,
        ConcatenateMessage(sample_message, /*padding_size=*/kStreamOffset));
    io::BufferReader reader(stream_buf);
    return ReadMessage(kStreamOffset,
                       static_cast<int32_t>(sample_message.metadata_bytes->size()),
                       static_cast<int64_t>(sample_message.body_bytes->size()), &reader);
  };
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
    CheckReadMessageTruncated(read_message, params);
    CheckReadMessageOversized(read_message, params);
  }
}

TEST_F(MessageDecodingTest, ReadMessageAsync) {
  auto read_message =
      [&](const SampleMessage& sample_message) -> Result<std::shared_ptr<Message>> {
    const int kStreamOffset = 42;
    ARROW_ASSIGN_OR_RAISE(
        auto stream_buf,
        ConcatenateMessage(sample_message, /*padding_size=*/kStreamOffset));
    io::BufferReader reader(stream_buf);
    return ReadMessageAsync(
               kStreamOffset, static_cast<int32_t>(sample_message.metadata_bytes->size()),
               static_cast<int64_t>(sample_message.body_bytes->size()), &reader)
        .result();
  };
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
    CheckReadMessageTruncated(read_message, params);
    CheckReadMessageOversized(read_message, params);
  }
}

TEST_F(MessageDecodingTest, ReadMessage3) {
  int padding_size;

  auto read_message =
      [&](const SampleMessage& sample_message) -> Result<std::unique_ptr<Message>> {
    // No padding, so that reading the truncated message actually fails
    ARROW_ASSIGN_OR_RAISE(auto stream_buf,
                          ConcatenateMessage(sample_message, padding_size));
    io::BufferReader reader(stream_buf);
    return ReadMessage(/*offset=*/padding_size,
                       static_cast<int32_t>(sample_message.metadata_bytes->size()),
                       &reader, /*fields_loader=*/{});
  };
  padding_size = 0;
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
    CheckReadMessageTruncated(read_message, params);
  }
  // With a non-zero padding, a truncated message wouldn't fail
  padding_size = 42;
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
  }
}

TEST_F(MessageDecodingTest, ReadMessage4) {
  FieldsLoaderFunction fields_loader = [&](const void* void_batch,
                                           io::RandomAccessFile* file) -> Status {
    auto* batch = reinterpret_cast<const flatbuf::RecordBatch*>(void_batch);
    // Check something about the message header
    EXPECT_EQ(batch->length(), kNumRows);
    // Read the entire body range from the file
    ARROW_ASSIGN_OR_RAISE(auto read_size, file->GetSize());
    return file->ReadAt(/*position=*/0, read_size, /*allow_short_read=*/false).status();
  };

  int padding_size;
  auto read_message =
      [&](const SampleMessage& sample_message) -> Result<std::unique_ptr<Message>> {
    // No padding, so that reading the truncated message actually fails
    ARROW_ASSIGN_OR_RAISE(auto stream_buf,
                          ConcatenateMessage(sample_message, padding_size));
    io::BufferReader reader(stream_buf);
    return ReadMessage(/*offset=*/padding_size,
                       static_cast<int32_t>(sample_message.metadata_bytes->size()),
                       &reader, fields_loader);
  };
  padding_size = 0;
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
    CheckReadMessageTruncated(read_message, params, /*force_truncate_metadata=*/true);
  }
  // With a non-zero padding, a truncated message wouldn't fail
  padding_size = 42;
  for (const auto& params : message_params()) {
    ARROW_SCOPED_TRACE("Params: ", params);
    CheckReadMessageOk(read_message, params);
  }
}

}  // namespace arrow::ipc::internal
