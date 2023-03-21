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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <unordered_set>

#include <flatbuffers/flatbuffers.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer_builder.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/reader_internal.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/ubsan.h"

#include "generated/Message_generated.h"  // IWYU pragma: keep

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::TemporaryDir;

namespace ipc {

using internal::FieldPosition;
using internal::IoRecordedRandomAccessFile;
using MetadataVector = std::vector<std::shared_ptr<KeyValueMetadata>>;

namespace test {

const std::vector<MetadataVersion> kMetadataVersions = {MetadataVersion::V4,
                                                        MetadataVersion::V5};

class TestMessage : public ::testing::TestWithParam<MetadataVersion> {
 public:
  void SetUp() {
    version_ = GetParam();
    fb_version_ = internal::MetadataVersionToFlatbuffer(version_);
    options_ = IpcWriteOptions::Defaults();
    options_.metadata_version = version_;
  }

 protected:
  MetadataVersion version_;
  flatbuf::MetadataVersion fb_version_;
  IpcWriteOptions options_;
};

TEST(TestMessage, Equals) {
  std::string metadata = "foo";
  std::string body = "bar";

  auto b1 = std::make_shared<Buffer>(metadata);
  auto b2 = std::make_shared<Buffer>(metadata);
  auto b3 = std::make_shared<Buffer>(body);
  auto b4 = std::make_shared<Buffer>(body);

  Message msg1(b1, b3);
  Message msg2(b2, b4);
  Message msg3(b1, nullptr);
  Message msg4(b2, nullptr);

  ASSERT_TRUE(msg1.Equals(msg2));
  ASSERT_TRUE(msg3.Equals(msg4));

  ASSERT_FALSE(msg1.Equals(msg3));
  ASSERT_FALSE(msg3.Equals(msg1));

  // same metadata as msg1, different body
  Message msg5(b2, b1);
  ASSERT_FALSE(msg1.Equals(msg5));
  ASSERT_FALSE(msg5.Equals(msg1));
}

TEST_P(TestMessage, SerializeTo) {
  const int64_t body_length = 64;

  flatbuffers::FlatBufferBuilder fbb;
  fbb.Finish(flatbuf::CreateMessage(fbb, fb_version_, flatbuf::MessageHeader::RecordBatch,
                                    0 /* header */, body_length));

  std::shared_ptr<Buffer> metadata;
  ASSERT_OK_AND_ASSIGN(metadata, internal::WriteFlatbufferBuilder(fbb));

  std::string body = "abcdef";

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Message> message,
                       Message::Open(metadata, std::make_shared<Buffer>(body)));

  auto CheckWithAlignment = [&](int32_t alignment) {
    options_.alignment = alignment;
    const int32_t prefix_size = 8;
    int64_t output_length = 0;
    ASSERT_OK_AND_ASSIGN(auto stream, io::BufferOutputStream::Create(1 << 10));
    ASSERT_OK(message->SerializeTo(stream.get(), options_, &output_length));
    ASSERT_EQ(bit_util::RoundUp(metadata->size() + prefix_size, alignment) + body_length,
              output_length);
    ASSERT_OK_AND_EQ(output_length, stream->Tell());
    ASSERT_OK_AND_ASSIGN(auto buffer, stream->Finish());
    // chech whether length is written in little endian
    auto buffer_ptr = buffer.get()->data();
    ASSERT_EQ(output_length - body_length - prefix_size,
              bit_util::FromLittleEndian(*(uint32_t*)(buffer_ptr + 4)));
  };

  CheckWithAlignment(8);
  CheckWithAlignment(64);
}

TEST_P(TestMessage, SerializeCustomMetadata) {
  std::vector<std::shared_ptr<KeyValueMetadata>> cases = {
      nullptr, key_value_metadata({}, {}),
      key_value_metadata({"foo", "bar"}, {"fizz", "buzz"})};
  for (auto metadata : cases) {
    std::shared_ptr<Buffer> serialized;
    ASSERT_OK(internal::WriteRecordBatchMessage(
        /*length=*/0, /*body_length=*/0, metadata,
        /*nodes=*/{},
        /*buffers=*/{}, options_, &serialized));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Message> message,
                         Message::Open(serialized, /*body=*/nullptr));

    if (metadata) {
      ASSERT_TRUE(message->custom_metadata()->Equals(*metadata));
    } else {
      ASSERT_EQ(nullptr, message->custom_metadata());
    }
  }
}

void BuffersOverlapEquals(const Buffer& left, const Buffer& right) {
  ASSERT_GT(left.size(), 0);
  ASSERT_GT(right.size(), 0);
  ASSERT_TRUE(left.Equals(right, std::min(left.size(), right.size())));
}

TEST_P(TestMessage, LegacyIpcBackwardsCompatibility) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntBatchSized(36, &batch));

  auto RoundtripWithOptions = [&](std::shared_ptr<Buffer>* out_serialized,
                                  std::unique_ptr<Message>* out) {
    IpcPayload payload;
    ASSERT_OK(GetRecordBatchPayload(*batch, options_, &payload));

    ASSERT_OK_AND_ASSIGN(auto stream, io::BufferOutputStream::Create(1 << 20));

    int32_t metadata_length = -1;
    ASSERT_OK(WriteIpcPayload(payload, options_, stream.get(), &metadata_length));

    ASSERT_OK_AND_ASSIGN(*out_serialized, stream->Finish());
    io::BufferReader io_reader(*out_serialized);
    ASSERT_OK(ReadMessage(&io_reader).Value(out));
  };

  std::shared_ptr<Buffer> serialized, legacy_serialized;
  std::unique_ptr<Message> message, legacy_message;

  RoundtripWithOptions(&serialized, &message);

  // First 4 bytes 0xFFFFFFFF Continuation marker
  ASSERT_EQ(-1, util::SafeLoadAs<int32_t>(serialized->data()));

  options_.write_legacy_ipc_format = true;
  RoundtripWithOptions(&legacy_serialized, &legacy_message);

  // Check that the continuation marker is not written
  ASSERT_NE(-1, util::SafeLoadAs<int32_t>(legacy_serialized->data()));

  // Have to use the smaller size to exclude padding
  BuffersOverlapEquals(*legacy_message->metadata(), *message->metadata());
  ASSERT_TRUE(legacy_message->body()->Equals(*message->body()));
}

TEST(TestMessage, Verify) {
  std::string metadata = "invalid";
  std::string body = "abcdef";

  Message message(std::make_shared<Buffer>(metadata), std::make_shared<Buffer>(body));
  ASSERT_FALSE(message.Verify());
}

INSTANTIATE_TEST_SUITE_P(TestMessage, TestMessage,
                         ::testing::ValuesIn(kMetadataVersions));

class TestSchemaMetadata : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckSchemaRoundtrip(const Schema& schema) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, SerializeSchema(schema));

    io::BufferReader reader(buffer);
    DictionaryMemo in_memo;
    ASSERT_OK_AND_ASSIGN(auto actual_schema, ReadSchema(&reader, &in_memo));
    AssertSchemaEqual(schema, *actual_schema);
  }
};

const std::shared_ptr<DataType> INT32 = std::make_shared<Int32Type>();

TEST_F(TestSchemaMetadata, PrimitiveFields) {
  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false);
  auto f2 = field("f2", std::make_shared<Int32Type>());
  auto f3 = field("f3", std::make_shared<Int64Type>());
  auto f4 = field("f4", std::make_shared<UInt8Type>());
  auto f5 = field("f5", std::make_shared<UInt16Type>());
  auto f6 = field("f6", std::make_shared<UInt32Type>());
  auto f7 = field("f7", std::make_shared<UInt64Type>());
  auto f8 = field("f8", std::make_shared<FloatType>());
  auto f9 = field("f9", std::make_shared<DoubleType>(), false);
  auto f10 = field("f10", std::make_shared<BooleanType>());

  Schema schema({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10});
  CheckSchemaRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, NestedFields) {
  auto type = list(int32());
  auto f0 = field("f0", type);

  std::shared_ptr<StructType> type2(
      new StructType({field("k1", INT32), field("k2", INT32), field("k3", INT32)}));
  auto f1 = field("f1", type2);

  Schema schema({f0, f1});
  CheckSchemaRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, DictionaryFields) {
  {
    auto dict_type = dictionary(int8(), int32(), true /* ordered */);
    auto f0 = field("f0", dict_type);
    auto f1 = field("f1", list(dict_type));

    Schema schema({f0, f1});
    CheckSchemaRoundtrip(schema);
  }
  {
    auto dict_type = dictionary(int8(), list(int32()));
    auto f0 = field("f0", dict_type);

    Schema schema({f0});
    CheckSchemaRoundtrip(schema);
  }
}

TEST_F(TestSchemaMetadata, NestedDictionaryFields) {
  {
    auto inner_dict_type = dictionary(int8(), int32(), /*ordered=*/true);
    auto dict_type = dictionary(int16(), list(inner_dict_type));

    Schema schema({field("f0", dict_type)});
    CheckSchemaRoundtrip(schema);
  }
  {
    auto dict_type1 = dictionary(int8(), utf8(), /*ordered=*/true);
    auto dict_type2 = dictionary(int32(), fixed_size_binary(24));
    auto dict_type3 = dictionary(int32(), binary());
    auto dict_type4 = dictionary(int8(), decimal(19, 7));

    auto struct_type1 = struct_({field("s1", dict_type1), field("s2", dict_type2)});
    auto struct_type2 = struct_({field("s3", dict_type3), field("s4", dict_type4)});

    Schema schema({field("f1", dictionary(int32(), struct_type1)),
                   field("f2", dictionary(int32(), struct_type2))});
    CheckSchemaRoundtrip(schema);
  }
}

TEST_F(TestSchemaMetadata, KeyValueMetadata) {
  auto field_metadata = key_value_metadata({{"key", "value"}});
  auto schema_metadata = key_value_metadata({{"foo", "bar"}, {"bizz", "buzz"}});

  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false, field_metadata);

  Schema schema({f0, f1}, schema_metadata);
  CheckSchemaRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, MetadataVersionForwardCompatibility) {
  // ARROW-9399
  std::string root;
  ASSERT_OK(GetTestResourceRoot(&root));

  // schema_v6.arrow with currently non-existent MetadataVersion::V6
  std::stringstream schema_v6_path;
  schema_v6_path << root << "/forward-compatibility/schema_v6.arrow";

  ASSERT_OK_AND_ASSIGN(auto schema_v6_file, io::ReadableFile::Open(schema_v6_path.str()));

  DictionaryMemo placeholder_memo;
  ASSERT_RAISES(Invalid, ReadSchema(schema_v6_file.get(), &placeholder_memo));
}

const std::vector<test::MakeRecordBatch*> kBatchCases = {
    &MakeIntRecordBatch,
    &MakeListRecordBatch,
    &MakeFixedSizeListRecordBatch,
    &MakeNonNullRecordBatch,
    &MakeZeroLengthRecordBatch,
    &MakeDeeplyNestedList,
    &MakeStringTypesRecordBatchWithNulls,
    &MakeStruct,
    &MakeUnion,
    &MakeDictionary,
    &MakeNestedDictionary,
    &MakeMap,
    &MakeMapOfDictionary,
    &MakeDates,
    &MakeTimestamps,
    &MakeTimes,
    &MakeFWBinary,
    &MakeNull,
    &MakeDecimal,
    &MakeBooleanBatch,
    &MakeFloatBatch,
    &MakeIntervals,
    &MakeUuid,
    &MakeComplex128,
    &MakeDictExtension};

static int g_file_number = 0;

class ExtensionTypesMixin {
 public:
  // Register the extension types required to ensure roundtripping
  ExtensionTypesMixin() : ext_guard_({uuid(), dict_extension_type(), complex128()}) {}

 protected:
  ExtensionTypeGuard ext_guard_;
};

class IpcTestFixture : public io::MemoryMapFixture, public ExtensionTypesMixin {
 public:
  void SetUp() {
    options_ = IpcWriteOptions::Defaults();
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("ipc-test-"));
  }

  std::string TempFile(std::string_view file) {
    return temp_dir_->path().Join(std::string(file)).ValueOrDie().ToString();
  }

  void DoSchemaRoundTrip(const Schema& schema, std::shared_ptr<Schema>* result) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> serialized_schema,
                         SerializeSchema(schema, options_.memory_pool));

    DictionaryMemo in_memo;
    io::BufferReader buf_reader(serialized_schema);
    ASSERT_OK_AND_ASSIGN(*result, ReadSchema(&buf_reader, &in_memo));
  }

  Result<std::shared_ptr<RecordBatch>> DoStandardRoundTrip(
      const RecordBatch& batch, const IpcWriteOptions& options,
      DictionaryMemo* dictionary_memo,
      const IpcReadOptions& read_options = IpcReadOptions::Defaults()) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> serialized_batch,
                          SerializeRecordBatch(batch, options));

    io::BufferReader buf_reader(serialized_batch);
    return ReadRecordBatch(batch.schema(), dictionary_memo, read_options, &buf_reader);
  }

  Result<std::shared_ptr<RecordBatch>> DoLargeRoundTrip(const RecordBatch& batch,
                                                        bool zero_data) {
    if (zero_data) {
      RETURN_NOT_OK(ZeroMemoryMap(mmap_.get()));
    }
    RETURN_NOT_OK(mmap_->Seek(0));

    auto options = options_;
    options.allow_64bit = true;

    ARROW_ASSIGN_OR_RAISE(auto file_writer,
                          MakeFileWriter(mmap_, batch.schema(), options));
    RETURN_NOT_OK(file_writer->WriteRecordBatch(batch));
    RETURN_NOT_OK(file_writer->Close());

    ARROW_ASSIGN_OR_RAISE(int64_t offset, mmap_->Tell());

    std::shared_ptr<RecordBatchFileReader> file_reader;
    ARROW_ASSIGN_OR_RAISE(file_reader, RecordBatchFileReader::Open(mmap_.get(), offset));

    return file_reader->ReadRecordBatch(0);
  }

  void CheckReadResult(const RecordBatch& result, const RecordBatch& expected) {
    ASSERT_OK(result.ValidateFull());
    EXPECT_EQ(expected.num_rows(), result.num_rows());

    ASSERT_TRUE(expected.schema()->Equals(*result.schema()));
    ASSERT_EQ(expected.num_columns(), result.num_columns())
        << expected.schema()->ToString() << " result: " << result.schema()->ToString();

    CompareBatchColumnsDetailed(result, expected);
  }

  void CheckRoundtrip(const RecordBatch& batch,
                      IpcWriteOptions options = IpcWriteOptions::Defaults(),
                      IpcReadOptions read_options = IpcReadOptions::Defaults(),
                      int64_t buffer_size = 1 << 20) {
    std::stringstream ss;
    ss << "test-write-row-batch-" << g_file_number++;
    ASSERT_OK_AND_ASSIGN(
        mmap_, io::MemoryMapFixture::InitMemoryMap(buffer_size, TempFile(ss.str())));

    std::shared_ptr<Schema> schema_result;
    DoSchemaRoundTrip(*batch.schema(), &schema_result);
    ASSERT_TRUE(batch.schema()->Equals(*schema_result));

    DictionaryMemo dictionary_memo;
    ASSERT_OK(::arrow::ipc::internal::CollectDictionaries(batch, &dictionary_memo));

    ASSERT_OK_AND_ASSIGN(
        auto result, DoStandardRoundTrip(batch, options, &dictionary_memo, read_options));
    CheckReadResult(*result, batch);

    ASSERT_OK_AND_ASSIGN(result, DoLargeRoundTrip(batch, /*zero_data=*/true));
    CheckReadResult(*result, batch);
  }

  void CheckRoundtrip(const std::shared_ptr<Array>& array,
                      IpcWriteOptions options = IpcWriteOptions::Defaults(),
                      int64_t buffer_size = 1 << 20) {
    auto f0 = arrow::field("f0", array->type());
    std::vector<std::shared_ptr<Field>> fields = {f0};
    auto schema = std::make_shared<Schema>(fields);

    auto batch = RecordBatch::Make(schema, array->length(), {array});
    CheckRoundtrip(*batch, options, IpcReadOptions::Defaults(), buffer_size);
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  IpcWriteOptions options_;
  std::unique_ptr<TemporaryDir> temp_dir_;
};

TEST(MetadataVersion, ForwardsCompatCheck) {
  // Verify UBSAN is ok with casting out of range metdata version.
  EXPECT_LT(flatbuf::MetadataVersion::MAX, static_cast<flatbuf::MetadataVersion>(72));
}

class TestWriteRecordBatch : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() override { IpcTestFixture::SetUp(); }
  void TearDown() override { IpcTestFixture::TearDown(); }
};

class TestIpcRoundTrip : public ::testing::TestWithParam<MakeRecordBatch*>,
                         public IpcTestFixture {
 public:
  void SetUp() override { IpcTestFixture::SetUp(); }
  void TearDown() override { IpcTestFixture::TearDown(); }

  void TestMetadataVersion(MetadataVersion expected_version) {
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(MakeIntRecordBatch(&batch));

    mmap_.reset();  // Ditch previous mmap view, to avoid errors on Windows
    ASSERT_OK_AND_ASSIGN(mmap_,
                         io::MemoryMapFixture::InitMemoryMap(1 << 16, "test-metadata"));

    int32_t metadata_length;
    int64_t body_length;
    const int64_t buffer_offset = 0;
    ASSERT_OK(WriteRecordBatch(*batch, buffer_offset, mmap_.get(), &metadata_length,
                               &body_length, options_));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Message> message,
                         ReadMessage(0, metadata_length, mmap_.get()));
    ASSERT_EQ(expected_version, message->metadata_version());
  }
};

TEST_P(TestIpcRoundTrip, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  for (const auto version : kMetadataVersions) {
    options_.metadata_version = version;
    CheckRoundtrip(*batch);
  }
}

TEST_F(TestIpcRoundTrip, DefaultMetadataVersion) {
  TestMetadataVersion(MetadataVersion::V5);
}

TEST_F(TestIpcRoundTrip, SpecificMetadataVersion) {
  options_.metadata_version = MetadataVersion::V4;
  TestMetadataVersion(MetadataVersion::V4);
  options_.metadata_version = MetadataVersion::V5;
  TestMetadataVersion(MetadataVersion::V5);
}

TEST(TestReadMessage, CorruptedSmallInput) {
  std::string data = "abc";
  io::BufferReader reader(data);
  ASSERT_RAISES(Invalid, ReadMessage(&reader));

  // But no error on unsignaled EOS
  io::BufferReader reader2("");
  ASSERT_OK_AND_ASSIGN(auto message, ReadMessage(&reader2));
  ASSERT_EQ(nullptr, message);
}

TEST(TestMetadata, GetMetadataVersion) {
  ASSERT_EQ(MetadataVersion::V1,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::V1));
  ASSERT_EQ(MetadataVersion::V2,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::V2));
  ASSERT_EQ(MetadataVersion::V3,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::V3));
  ASSERT_EQ(MetadataVersion::V4,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::V4));
  ASSERT_EQ(MetadataVersion::V5,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::V5));
  ASSERT_EQ(MetadataVersion::V1,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::MIN));
  ASSERT_EQ(MetadataVersion::V5,
            ipc::internal::GetMetadataVersion(flatbuf::MetadataVersion::MAX));
}

TEST_P(TestIpcRoundTrip, SliceRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  // Skip the zero-length case
  if (batch->num_rows() < 2) {
    return;
  }

  auto sliced_batch = batch->Slice(2, 10);
  CheckRoundtrip(*sliced_batch);
}

TEST_P(TestIpcRoundTrip, ZeroLengthArrays) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  std::shared_ptr<RecordBatch> zero_length_batch;
  if (batch->num_rows() > 2) {
    zero_length_batch = batch->Slice(2, 0);
  } else {
    zero_length_batch = batch->Slice(0, 0);
  }

  CheckRoundtrip(*zero_length_batch);

  // ARROW-544: check binary array
  ASSERT_OK_AND_ASSIGN(auto value_offsets,
                       AllocateBuffer(sizeof(int32_t), options_.memory_pool));
  *reinterpret_cast<int32_t*>(value_offsets->mutable_data()) = 0;

  std::shared_ptr<Array> bin_array = std::make_shared<BinaryArray>(
      0, std::move(value_offsets), std::make_shared<Buffer>(nullptr, 0),
      std::make_shared<Buffer>(nullptr, 0));

  // null value_offsets
  std::shared_ptr<Array> bin_array2 = std::make_shared<BinaryArray>(0, nullptr, nullptr);

  CheckRoundtrip(bin_array);
  CheckRoundtrip(bin_array2);
}

TEST_F(TestIpcRoundTrip, SparseUnionOfStructsWithReusedBuffers) {
  auto storage_type = struct_({
      field("i", int32()),
      field("f", float32()),
      field("s", utf8()),
  });
  auto storage = checked_pointer_cast<StructArray>(ArrayFromJSON(storage_type,
                                                                 R"([
    {"i": 0, "f": 0.0, "s": "a"},
    {"i": 1, "f": 0.5, "s": "b"},
    {"i": 2, "f": 1.5, "s": "c"},
    {"i": 3, "f": 3.0, "s": "d"}
  ])"));

  ASSERT_OK_AND_ASSIGN(
      auto m01, StructArray::Make({storage->field(0), storage->field(1)},
                                  {storage_type->field(0), storage_type->field(1)}));

  ASSERT_OK_AND_ASSIGN(
      auto m12, StructArray::Make({storage->field(1), storage->field(2)},
                                  {storage_type->field(1), storage_type->field(2)}));

  ASSERT_OK_AND_ASSIGN(
      auto m20, StructArray::Make({storage->field(2), storage->field(0)},
                                  {storage_type->field(2), storage_type->field(0)}));

  auto ids = ArrayFromJSON(int8(), "[1, 12, 20, 1]");

  ASSERT_OK_AND_ASSIGN(
      auto sparse,
      SparseUnionArray::Make(*ids, {m01, m12, m20}, {"m01", "m12", "m20"}, {01, 12, 20}));

  auto expected = ArrayFromJSON(sparse_union(
                                    {
                                        field("m01", m01->type()),
                                        field("m12", m12->type()),
                                        field("m20", m20->type()),
                                    },
                                    {01, 12, 20}),
                                R"([
    [1,  {"i": 0,   "f": 0.0}],
    [12, {"f": 0.5, "s": "b"}],
    [20, {"s": "c", "i": 2  }],
    [1,  {"i": 3,   "f": 3.0}]
  ])");

  AssertArraysEqual(*expected, *sparse, /*verbose=*/true);

  DictionaryMemo ignored;
  ASSERT_OK_AND_ASSIGN(
      auto roundtripped_batch,
      DoStandardRoundTrip(*RecordBatch::Make(schema({field("", sparse->type())}),
                                             sparse->length(), {sparse}),
                          IpcWriteOptions::Defaults(), &ignored));

  auto roundtripped =
      checked_pointer_cast<SparseUnionArray>(roundtripped_batch->column(0));
  AssertArraysEqual(*expected, *roundtripped, /*verbose=*/true);

  auto roundtripped_m01 = checked_pointer_cast<StructArray>(roundtripped->field(0));
  auto roundtripped_m12 = checked_pointer_cast<StructArray>(roundtripped->field(1));
  auto roundtripped_m20 = checked_pointer_cast<StructArray>(roundtripped->field(2));

  // The IPC writer does not take advantage of reusable buffers

  ASSERT_NE(roundtripped_m01->field(0)->data()->buffers,
            roundtripped_m20->field(1)->data()->buffers);

  ASSERT_NE(roundtripped_m01->field(1)->data()->buffers,
            roundtripped_m12->field(0)->data()->buffers);

  ASSERT_NE(roundtripped_m12->field(1)->data()->buffers,
            roundtripped_m20->field(0)->data()->buffers);
}

TEST_F(TestWriteRecordBatch, WriteWithCompression) {
  random::RandomArrayGenerator rg(/*seed=*/0);

  // Generate both regular and dictionary encoded because the dictionary batch
  // gets compressed also

  int64_t length = 500;

  int dict_size = 50;
  std::shared_ptr<Array> dict = rg.String(dict_size, /*min_length=*/5, /*max_length=*/5,
                                          /*null_probability=*/0);
  std::shared_ptr<Array> indices = rg.Int32(length, /*min=*/0, /*max=*/dict_size - 1,
                                            /*null_probability=*/0.1);

  auto dict_type = dictionary(int32(), utf8());
  auto dict_field = field("f1", dict_type);
  ASSERT_OK_AND_ASSIGN(auto dict_array,
                       DictionaryArray::FromArrays(dict_type, indices, dict));

  auto schema = ::arrow::schema({field("f0", utf8()), dict_field});
  auto batch =
      RecordBatch::Make(schema, length, {rg.String(500, 0, 10, 0.1), dict_array});

  std::vector<Compression::type> codecs = {Compression::LZ4_FRAME, Compression::ZSTD};
  for (auto codec : codecs) {
    if (!util::Codec::IsAvailable(codec)) {
      continue;
    }
    IpcWriteOptions write_options = IpcWriteOptions::Defaults();
    ASSERT_OK_AND_ASSIGN(write_options.codec, util::Codec::Create(codec));
    CheckRoundtrip(*batch, write_options);

    // Check non-parallel read and write
    IpcReadOptions read_options = IpcReadOptions::Defaults();
    write_options.use_threads = false;
    read_options.use_threads = false;
    CheckRoundtrip(*batch, write_options, read_options);
  }

  std::vector<Compression::type> disallowed_codecs = {
      Compression::BROTLI, Compression::BZ2, Compression::LZ4, Compression::GZIP,
      Compression::SNAPPY};
  for (auto codec : disallowed_codecs) {
    if (!util::Codec::IsAvailable(codec)) {
      continue;
    }
    IpcWriteOptions write_options = IpcWriteOptions::Defaults();
    ASSERT_OK_AND_ASSIGN(write_options.codec, util::Codec::Create(codec));
    ASSERT_RAISES(Invalid, SerializeRecordBatch(*batch, write_options));
  }
}

TEST_F(TestWriteRecordBatch, WriteWithCompressionAndMinSavings) {
  // A small batch that's known to be compressible
  auto batch = RecordBatchFromJSON(schema({field("n", int64())}), R"([
    {"n":0},{"n":1},{"n":2},{"n":3},{"n":4},
    {"n":5},{"n":6},{"n":7},{"n":8},{"n":9}])");

  auto prefixed_size = [](const Buffer& buffer) -> int64_t {
    if (buffer.size() < int64_t(sizeof(int64_t))) return 0;
    return bit_util::FromLittleEndian(util::SafeLoadAs<int64_t>(buffer.data()));
  };
  auto content_size = [](const Buffer& buffer) -> int64_t {
    return buffer.size() - sizeof(int64_t);
  };

  for (auto codec : {Compression::LZ4_FRAME, Compression::ZSTD}) {
    if (!util::Codec::IsAvailable(codec)) {
      continue;
    }

    auto write_options = IpcWriteOptions::Defaults();
    ASSERT_OK_AND_ASSIGN(write_options.codec, util::Codec::Create(codec));
    auto read_options = IpcReadOptions::Defaults();

    IpcPayload payload;
    ASSERT_OK(GetRecordBatchPayload(*batch, write_options, &payload));
    ASSERT_EQ(payload.body_buffers.size(), 2);
    // Compute the savings when body buffers are compressed unconditionally. We also
    // validate that our test batch is indeed compressible.
    const int64_t uncompressed_size = prefixed_size(*payload.body_buffers[1]);
    const int64_t compressed_size = content_size(*payload.body_buffers[1]);
    ASSERT_LT(compressed_size, uncompressed_size);
    ASSERT_GT(compressed_size, 0);
    const double expected_savings =
        1.0 - static_cast<double>(compressed_size) / uncompressed_size;

    // Using the known savings % as the minimum should yield the same outcome.
    write_options.min_space_savings = expected_savings;
    payload = IpcPayload();
    ASSERT_OK(GetRecordBatchPayload(*batch, write_options, &payload));
    ASSERT_EQ(payload.body_buffers.size(), 2);
    ASSERT_EQ(prefixed_size(*payload.body_buffers[1]), uncompressed_size);
    ASSERT_EQ(content_size(*payload.body_buffers[1]), compressed_size);
    CheckRoundtrip(*batch, write_options, read_options);

    // Slightly bump the threshold. The body buffer should now be prefixed with -1 and its
    // content left uncompressed.
    write_options.min_space_savings = std::nextafter(expected_savings, 1.0);
    payload = IpcPayload();
    ASSERT_OK(GetRecordBatchPayload(*batch, write_options, &payload));
    ASSERT_EQ(payload.body_buffers.size(), 2);
    ASSERT_EQ(prefixed_size(*payload.body_buffers[1]), -1);
    ASSERT_EQ(content_size(*payload.body_buffers[1]), uncompressed_size);
    CheckRoundtrip(*batch, write_options, read_options);

    for (double out_of_range : {std::nextafter(1.0, 2.0), std::nextafter(0.0, -1.0)}) {
      write_options.min_space_savings = out_of_range;
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          Invalid, ::testing::StartsWith("Invalid: min_space_savings not in range [0,1]"),
          SerializeRecordBatch(*batch, write_options));
    }
  }
}

TEST_F(TestWriteRecordBatch, SliceTruncatesBinaryOffsets) {
  // ARROW-6046
  std::shared_ptr<Array> array;
  ASSERT_OK(MakeRandomStringArray(500, false, default_memory_pool(), &array));

  auto f0 = field("f0", array->type());
  auto schema = ::arrow::schema({f0});
  auto batch = RecordBatch::Make(schema, array->length(), {array});
  auto sliced_batch = batch->Slice(0, 5);

  ASSERT_OK_AND_ASSIGN(
      mmap_, io::MemoryMapFixture::InitMemoryMap(/*buffer_size=*/1 << 20,
                                                 TempFile("test-truncate-offsets")));
  DictionaryMemo dictionary_memo;
  ASSERT_OK_AND_ASSIGN(
      auto result,
      DoStandardRoundTrip(*sliced_batch, IpcWriteOptions::Defaults(), &dictionary_memo));
  ASSERT_EQ(6 * sizeof(int32_t), result->column(0)->data()->buffers[1]->size());
}

TEST_F(TestWriteRecordBatch, SliceTruncatesBuffers) {
  auto CheckArray = [this](const std::shared_ptr<Array>& array) {
    auto f0 = field("f0", array->type());
    auto schema = ::arrow::schema({f0});
    auto batch = RecordBatch::Make(schema, array->length(), {array});
    auto sliced_batch = batch->Slice(0, 5);

    int64_t full_size;
    int64_t sliced_size;

    ASSERT_OK(GetRecordBatchSize(*batch, &full_size));
    ASSERT_OK(GetRecordBatchSize(*sliced_batch, &sliced_size));
    ASSERT_TRUE(sliced_size < full_size) << sliced_size << " " << full_size;

    // make sure we can write and read it
    this->CheckRoundtrip(*sliced_batch);
  };

  std::shared_ptr<Array> a0, a1;
  auto pool = default_memory_pool();

  // Integer
  ASSERT_OK(MakeRandomInt32Array(500, false, pool, &a0));
  CheckArray(a0);

  // String / Binary
  {
    auto s = MakeRandomStringArray(500, false, pool, &a0);
    ASSERT_TRUE(s.ok());
  }
  CheckArray(a0);

  // Boolean
  ASSERT_OK(MakeRandomBooleanArray(10000, false, &a0));
  CheckArray(a0);

  // List
  ASSERT_OK(MakeRandomInt32Array(500, false, pool, &a0));
  ASSERT_OK(MakeRandomListArray(a0, 200, false, pool, &a1));
  CheckArray(a1);

  // Struct
  auto struct_type = struct_({field("f0", a0->type())});
  std::vector<std::shared_ptr<Array>> struct_children = {a0};
  a1 = std::make_shared<StructArray>(struct_type, a0->length(), struct_children);
  CheckArray(a1);

  // Sparse Union
  auto union_type = sparse_union({field("f0", a0->type())}, {0});
  std::vector<int32_t> type_ids(a0->length());
  std::shared_ptr<Buffer> ids_buffer;
  ASSERT_OK(CopyBufferFromVector(type_ids, default_memory_pool(), &ids_buffer));
  a1 = std::make_shared<SparseUnionArray>(union_type, a0->length(), struct_children,
                                          ids_buffer);
  CheckArray(a1);

  // Dense union
  auto dense_union_type = dense_union({field("f0", a0->type())}, {0});
  std::vector<int32_t> type_offsets;
  for (int32_t i = 0; i < a0->length(); ++i) {
    type_offsets.push_back(i);
  }
  std::shared_ptr<Buffer> offsets_buffer;
  ASSERT_OK(CopyBufferFromVector(type_offsets, default_memory_pool(), &offsets_buffer));
  a1 = std::make_shared<DenseUnionArray>(dense_union_type, a0->length(), struct_children,
                                         ids_buffer, offsets_buffer);
  CheckArray(a1);
}

TEST_F(TestWriteRecordBatch, RoundtripPreservesBufferSizes) {
  // ARROW-7975
  random::RandomArrayGenerator rg(/*seed=*/0);

  int64_t length = 15;
  auto arr = rg.String(length, 0, 10, 0.1);
  auto batch = RecordBatch::Make(::arrow::schema({field("f0", utf8())}), length, {arr});

  ASSERT_OK_AND_ASSIGN(
      mmap_, io::MemoryMapFixture::InitMemoryMap(
                 /*buffer_size=*/1 << 20, TempFile("test-roundtrip-buffer-sizes")));
  DictionaryMemo dictionary_memo;
  ASSERT_OK_AND_ASSIGN(
      auto result,
      DoStandardRoundTrip(*batch, IpcWriteOptions::Defaults(), &dictionary_memo));

  // Make sure that the validity bitmap is size 2 as expected
  ASSERT_EQ(2, arr->data()->buffers[0]->size());

  for (size_t i = 0; i < arr->data()->buffers.size(); ++i) {
    ASSERT_EQ(arr->data()->buffers[i]->size(),
              result->column(0)->data()->buffers[i]->size());
  }
}

void TestGetRecordBatchSize(const IpcWriteOptions& options,
                            std::shared_ptr<RecordBatch> batch) {
  io::MockOutputStream mock;
  ipc::IpcPayload payload;
  int32_t mock_metadata_length = -1;
  int64_t mock_body_length = -1;
  int64_t size = -1;
  ASSERT_OK(WriteRecordBatch(*batch, 0, &mock, &mock_metadata_length, &mock_body_length,
                             options));
  ASSERT_OK(GetRecordBatchPayload(*batch, options, &payload));
  int64_t payload_size = GetPayloadSize(payload, options);
  ASSERT_OK(GetRecordBatchSize(*batch, options, &size));
  ASSERT_EQ(mock.GetExtentBytesWritten(), size);
  ASSERT_EQ(mock.GetExtentBytesWritten(), payload_size);
}

TEST_F(TestWriteRecordBatch, IntegerGetRecordBatchSize) {
  std::shared_ptr<RecordBatch> batch;

  ASSERT_OK(MakeIntRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeListRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeZeroLengthRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeNonNullRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeDeeplyNestedList(&batch));
  TestGetRecordBatchSize(options_, batch);
}

class RecursionLimits : public ::testing::Test, public io::MemoryMapFixture {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("ipc-recursion-limits-test-"));
  }

  std::string TempFile(std::string_view file) {
    return temp_dir_->path().Join(std::string(file)).ValueOrDie().ToString();
  }

  void TearDown() { io::MemoryMapFixture::TearDown(); }

  Status WriteToMmap(int recursion_level, bool override_level, int32_t* metadata_length,
                     int64_t* body_length, std::shared_ptr<RecordBatch>* batch,
                     std::shared_ptr<Schema>* schema) {
    const int batch_length = 5;
    auto type = int32();
    std::shared_ptr<Array> array;
    const bool include_nulls = true;
    RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool_, &array));
    for (int i = 0; i < recursion_level; ++i) {
      type = list(type);
      RETURN_NOT_OK(
          MakeRandomListArray(array, batch_length, include_nulls, pool_, &array));
    }

    auto f0 = field("f0", type);

    *schema = ::arrow::schema({f0});

    *batch = RecordBatch::Make(*schema, batch_length, {array});

    std::stringstream ss;
    ss << "test-write-past-max-recursion-" << g_file_number++;
    const int memory_map_size = 1 << 20;
    ARROW_ASSIGN_OR_RAISE(
        mmap_, io::MemoryMapFixture::InitMemoryMap(memory_map_size, TempFile(ss.str())));

    auto options = IpcWriteOptions::Defaults();
    if (override_level) {
      options.max_recursion_depth = recursion_level + 1;
    }
    return WriteRecordBatch(**batch, 0, mmap_.get(), metadata_length, body_length,
                            options);
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  MemoryPool* pool_;
};

TEST_F(RecursionLimits, WriteLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<RecordBatch> batch;
  ASSERT_RAISES(Invalid, WriteToMmap((1 << 8) + 1, false, &metadata_length, &body_length,
                                     &batch, &schema));
}

TEST_F(RecursionLimits, ReadLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;

  const int recursion_depth = 64;

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(WriteToMmap(recursion_depth, true, &metadata_length, &body_length, &batch,
                        &schema));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Message> message,
                       ReadMessage(0, metadata_length, mmap_.get()));

  io::BufferReader reader(message->body());

  DictionaryMemo empty_memo;
  ASSERT_RAISES(Invalid, ReadRecordBatch(*message->metadata(), schema, &empty_memo,
                                         IpcReadOptions::Defaults(), &reader));
}

// Test fails with a structured exception on Windows + Debug
#if !defined(_WIN32) || defined(NDEBUG)
TEST_F(RecursionLimits, StressLimit) {
  auto CheckDepth = [this](int recursion_depth, bool* it_works) {
    int32_t metadata_length = -1;
    int64_t body_length = -1;
    std::shared_ptr<Schema> schema;
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(WriteToMmap(recursion_depth, true, &metadata_length, &body_length, &batch,
                          &schema));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Message> message,
                         ReadMessage(0, metadata_length, mmap_.get()));

    DictionaryMemo empty_memo;

    auto options = IpcReadOptions::Defaults();
    options.max_recursion_depth = recursion_depth + 1;
    io::BufferReader reader(message->body());
    std::shared_ptr<RecordBatch> result;
    ASSERT_OK_AND_ASSIGN(result, ReadRecordBatch(*message->metadata(), schema,
                                                 &empty_memo, options, &reader));
    *it_works = result->Equals(*batch);
  };

  bool it_works = false;
  CheckDepth(100, &it_works);
  ASSERT_TRUE(it_works);

// Mitigate Valgrind's slowness
#if !defined(ARROW_VALGRIND)
  CheckDepth(500, &it_works);
  ASSERT_TRUE(it_works);
#endif
}
#endif  // !defined(_WIN32) || defined(NDEBUG)

struct FileWriterHelper {
  static constexpr bool kIsFileFormat = true;

  Status Init(const std::shared_ptr<Schema>& schema, const IpcWriteOptions& options,
              const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr) {
    num_batches_written_ = 0;

    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateResizableBuffer(0));
    sink_.reset(new io::BufferOutputStream(buffer_));
    ARROW_ASSIGN_OR_RAISE(writer_,
                          MakeFileWriter(sink_.get(), schema, options, metadata));
    return Status::OK();
  }

  Status WriteBatch(const std::shared_ptr<RecordBatch>& batch,
                    const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr) {
    RETURN_NOT_OK(writer_->WriteRecordBatch(*batch, metadata));
    num_batches_written_++;
    return Status::OK();
  }

  Status WriteTable(const RecordBatchVector& batches) {
    num_batches_written_ += static_cast<int>(batches.size());
    ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(batches));
    return writer_->WriteTable(*table);
  }

  Status Finish(WriteStats* out_stats = nullptr) {
    RETURN_NOT_OK(writer_->Close());
    if (out_stats) {
      *out_stats = writer_->stats();
    }
    RETURN_NOT_OK(sink_->Close());
    // Current offset into stream is the end of the file
    return sink_->Tell().Value(&footer_offset_);
  }

  virtual Status ReadBatches(const IpcReadOptions& options,
                             RecordBatchVector* out_batches,
                             ReadStats* out_stats = nullptr,
                             MetadataVector* out_metadata_list = nullptr) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchFileReader::Open(
                                           buf_reader.get(), footer_offset_, options));

    EXPECT_EQ(num_batches_written_, reader->num_record_batches());
    for (int i = 0; i < num_batches_written_; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto chunk_with_metadata,
                            reader->ReadRecordBatchWithCustomMetadata(i));
      auto chunk = chunk_with_metadata.batch;
      out_batches->push_back(chunk);
      if (out_metadata_list) {
        auto metadata = chunk_with_metadata.custom_metadata;
        out_metadata_list->push_back(metadata);
      }
    }
    if (out_stats) {
      *out_stats = reader->stats();
    }
    return Status::OK();
  }

  Status ReadSchema(std::shared_ptr<Schema>* out) {
    return ReadSchema(ipc::IpcReadOptions::Defaults(), out);
  }

  Status ReadSchema(const IpcReadOptions& read_options, std::shared_ptr<Schema>* out) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    ARROW_ASSIGN_OR_RAISE(
        auto reader,
        RecordBatchFileReader::Open(buf_reader.get(), footer_offset_, read_options));

    *out = reader->schema();
    return Status::OK();
  }

  Result<std::shared_ptr<const KeyValueMetadata>> ReadFooterMetadata() {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          RecordBatchFileReader::Open(buf_reader.get(), footer_offset_));
    return reader->metadata();
  }

  std::shared_ptr<ResizableBuffer> buffer_;
  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<RecordBatchWriter> writer_;
  int num_batches_written_;
  int64_t footer_offset_;
};

// Helper class since coalescing will not happen if the file is zero copy
class NoZeroCopyBufferReader : public io::BufferReader {
  using BufferReader::BufferReader;
  bool supports_zero_copy() const override { return false; }
};

template <bool kCoalesce>
struct FileGeneratorWriterHelper : public FileWriterHelper {
  Status ReadBatches(const IpcReadOptions& options, RecordBatchVector* out_batches,
                     ReadStats* out_stats = nullptr,
                     MetadataVector* out_metadata_list = nullptr) override {
    std::shared_ptr<io::RandomAccessFile> buf_reader;
    if (kCoalesce) {
      // Use a non-zero-copy enabled BufferReader so we can test paths properly
      buf_reader = std::make_shared<NoZeroCopyBufferReader>(buffer_);
    } else {
      buf_reader = std::make_shared<io::BufferReader>(buffer_);
    }
    AsyncGenerator<std::shared_ptr<RecordBatch>> generator;

    {
      auto fut = RecordBatchFileReader::OpenAsync(buf_reader, footer_offset_, options);
      // Do NOT assert OK since some tests check whether this fails properly
      EXPECT_FINISHES(fut);
      ARROW_ASSIGN_OR_RAISE(auto reader, fut.result());
      EXPECT_EQ(num_batches_written_, reader->num_record_batches());
      // Generator will keep reader alive internally
      ARROW_ASSIGN_OR_RAISE(generator, reader->GetRecordBatchGenerator(kCoalesce));
    }

    // Generator is async-reentrant
    std::vector<Future<std::shared_ptr<RecordBatch>>> futures;
    for (int i = 0; i < num_batches_written_; ++i) {
      futures.push_back(generator());
    }
    auto fut = generator();
    EXPECT_FINISHES_OK_AND_EQ(nullptr, fut);
    for (auto& future : futures) {
      EXPECT_FINISHES_OK_AND_ASSIGN(auto batch, future);
      out_batches->push_back(batch);
    }

    // The generator doesn't track stats.
    EXPECT_EQ(nullptr, out_stats);

    return Status::OK();
  }
};

struct StreamWriterHelper {
  static constexpr bool kIsFileFormat = false;

  Status Init(const std::shared_ptr<Schema>& schema, const IpcWriteOptions& options) {
    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateResizableBuffer(0));
    sink_.reset(new io::BufferOutputStream(buffer_));
    ARROW_ASSIGN_OR_RAISE(writer_, MakeStreamWriter(sink_.get(), schema, options));
    return Status::OK();
  }

  Status WriteBatch(const std::shared_ptr<RecordBatch>& batch,
                    const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr) {
    RETURN_NOT_OK(writer_->WriteRecordBatch(*batch, metadata));
    return Status::OK();
  }

  Status WriteTable(const RecordBatchVector& batches) {
    ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(batches));
    return writer_->WriteTable(*table);
  }

  Status Finish(WriteStats* out_stats = nullptr) {
    RETURN_NOT_OK(writer_->Close());
    if (out_stats) {
      *out_stats = writer_->stats();
    }
    return sink_->Close();
  }

  virtual Status ReadBatches(const IpcReadOptions& options,
                             RecordBatchVector* out_batches,
                             ReadStats* out_stats = nullptr,
                             MetadataVector* out_metadata_list = nullptr) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchStreamReader::Open(buf_reader, options))
    if (out_metadata_list) {
      while (true) {
        ARROW_ASSIGN_OR_RAISE(auto chunk_with_metadata, reader->ReadNext());
        if (chunk_with_metadata.batch == nullptr) {
          break;
        }
        out_batches->push_back(chunk_with_metadata.batch);
        out_metadata_list->push_back(chunk_with_metadata.custom_metadata);
      }
    } else {
      ARROW_ASSIGN_OR_RAISE(*out_batches, reader->ToRecordBatches());
    }

    if (out_stats) {
      *out_stats = reader->stats();
    }
    return Status::OK();
  }

  Status ReadSchema(std::shared_ptr<Schema>* out) {
    return ReadSchema(ipc::IpcReadOptions::Defaults(), out);
  }

  virtual Status ReadSchema(const IpcReadOptions& read_options,
                            std::shared_ptr<Schema>* out) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          RecordBatchStreamReader::Open(buf_reader.get(), read_options));
    *out = reader->schema();
    return Status::OK();
  }

  std::shared_ptr<ResizableBuffer> buffer_;
  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<RecordBatchWriter> writer_;
};

struct StreamDecoderWriterHelper : public StreamWriterHelper {
  Status ReadBatches(const IpcReadOptions& options, RecordBatchVector* out_batches,
                     ReadStats* out_stats = nullptr,
                     MetadataVector* out_metadata_list = nullptr) override {
    auto listener = std::make_shared<CollectListener>();
    StreamDecoder decoder(listener, options);
    RETURN_NOT_OK(DoConsume(&decoder));
    *out_batches = listener->record_batches();
    if (out_stats) {
      *out_stats = decoder.stats();
    }
    return Status::OK();
  }

  Status ReadSchema(const IpcReadOptions& read_options,
                    std::shared_ptr<Schema>* out) override {
    auto listener = std::make_shared<CollectListener>();
    StreamDecoder decoder(listener, read_options);
    RETURN_NOT_OK(DoConsume(&decoder));
    *out = listener->schema();
    return Status::OK();
  }

  virtual Status DoConsume(StreamDecoder* decoder) = 0;
};

struct StreamDecoderDataWriterHelper : public StreamDecoderWriterHelper {
  Status DoConsume(StreamDecoder* decoder) override {
    return decoder->Consume(buffer_->data(), buffer_->size());
  }
};

struct StreamDecoderBufferWriterHelper : public StreamDecoderWriterHelper {
  Status DoConsume(StreamDecoder* decoder) override { return decoder->Consume(buffer_); }
};

struct StreamDecoderSmallChunksWriterHelper : public StreamDecoderWriterHelper {
  Status DoConsume(StreamDecoder* decoder) override {
    for (int64_t offset = 0; offset < buffer_->size() - 1; ++offset) {
      RETURN_NOT_OK(decoder->Consume(buffer_->data() + offset, 1));
    }
    return Status::OK();
  }
};

struct StreamDecoderLargeChunksWriterHelper : public StreamDecoderWriterHelper {
  Status DoConsume(StreamDecoder* decoder) override {
    RETURN_NOT_OK(decoder->Consume(SliceBuffer(buffer_, 0, 1)));
    RETURN_NOT_OK(decoder->Consume(SliceBuffer(buffer_, 1)));
    return Status::OK();
  }
};

// Parameterized mixin with tests for stream / file writer

template <class WriterHelperType>
class ReaderWriterMixin : public ExtensionTypesMixin {
 public:
  using WriterHelper = WriterHelperType;

  // Check simple RecordBatch roundtripping
  void TestRoundTrip(test::MakeRecordBatch param, const IpcWriteOptions& options) {
    std::shared_ptr<RecordBatch> batch1;
    std::shared_ptr<RecordBatch> batch2;
    ASSERT_OK(param(&batch1));  // NOLINT clang-tidy gtest issue
    ASSERT_OK(param(&batch2));  // NOLINT clang-tidy gtest issue

    RecordBatchVector in_batches = {batch1, batch2};
    RecordBatchVector out_batches;

    WriterHelper writer_helper;
    ASSERT_OK(RoundTripHelper(writer_helper, in_batches, options,
                              IpcReadOptions::Defaults(), &out_batches));
    ASSERT_EQ(out_batches.size(), in_batches.size());

    // Compare batches
    for (size_t i = 0; i < in_batches.size(); ++i) {
      CompareBatch(*in_batches[i], *out_batches[i]);
    }
  }

  void TestZeroLengthRoundTrip(test::MakeRecordBatch param,
                               const IpcWriteOptions& options) {
    std::shared_ptr<RecordBatch> batch1;
    std::shared_ptr<RecordBatch> batch2;
    ASSERT_OK(param(&batch1));  // NOLINT clang-tidy gtest issue
    ASSERT_OK(param(&batch2));  // NOLINT clang-tidy gtest issue
    batch1 = batch1->Slice(0, 0);
    batch2 = batch2->Slice(0, 0);

    RecordBatchVector in_batches = {batch1, batch2};
    RecordBatchVector out_batches;

    WriterHelper writer_helper;
    ASSERT_OK(RoundTripHelper(writer_helper, in_batches, options,
                              IpcReadOptions::Defaults(), &out_batches));
    ASSERT_EQ(out_batches.size(), in_batches.size());

    // Compare batches
    for (size_t i = 0; i < in_batches.size(); ++i) {
      CompareBatch(*in_batches[i], *out_batches[i]);
    }
  }

  void TestRoundTripWithOptions(test::MakeRecordBatch make_record_batch) {
    TestRoundTrip(make_record_batch, IpcWriteOptions::Defaults());
    TestZeroLengthRoundTrip(make_record_batch, IpcWriteOptions::Defaults());

    IpcWriteOptions options;
    options.write_legacy_ipc_format = true;
    TestRoundTrip(make_record_batch, options);
    TestZeroLengthRoundTrip(make_record_batch, options);
  }

  void TestDictionaryRoundtrip() {
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(MakeDictionary(&batch));

    WriterHelper writer_helper;
    RecordBatchVector out_batches;
    ASSERT_OK(RoundTripHelper(writer_helper, {batch}, IpcWriteOptions::Defaults(),
                              IpcReadOptions::Defaults(), &out_batches));
    ASSERT_EQ(out_batches.size(), 1);

    // TODO(wesm): This was broken in ARROW-3144. I'm not sure how to
    // restore the deduplication logic yet because dictionaries are
    // corresponded to the Schema using Field pointers rather than
    // DataType as before

    // CheckDictionariesDeduplicated(*out_batches[0]);
  }

  void TestReadSubsetOfFields() {
    // Part of ARROW-7979
    auto a0 = ArrayFromJSON(utf8(), "[\"a0\", null]");
    auto a1 = ArrayFromJSON(utf8(), "[\"a1\", null]");
    auto a2 = ArrayFromJSON(utf8(), "[\"a2\", null]");
    auto a3 = ArrayFromJSON(utf8(), "[\"a3\", null]");

    auto my_schema = schema({field("a0", utf8()), field("a1", utf8()),
                             field("a2", utf8()), field("a3", utf8())},
                            key_value_metadata({"key1"}, {"value1"}));
    auto batch = RecordBatch::Make(my_schema, a0->length(), {a0, a1, a2, a3});

    IpcReadOptions options = IpcReadOptions::Defaults();

    options.included_fields = {1, 3};

    {
      WriterHelper writer_helper;
      RecordBatchVector out_batches;
      std::shared_ptr<Schema> out_schema;
      ASSERT_OK(RoundTripHelper(writer_helper, {batch}, IpcWriteOptions::Defaults(),
                                options, &out_batches, &out_schema));

      auto ex_schema = schema({field("a1", utf8()), field("a3", utf8())},
                              key_value_metadata({"key1"}, {"value1"}));
      AssertSchemaEqual(*ex_schema, *out_schema);

      auto ex_batch = RecordBatch::Make(ex_schema, a0->length(), {a1, a3});
      AssertBatchesEqual(*ex_batch, *out_batches[0], /*check_metadata=*/true);
    }

    // Duplicated or unordered indices are normalized when reading
    options.included_fields = {3, 1, 1};

    {
      WriterHelper writer_helper;
      RecordBatchVector out_batches;
      std::shared_ptr<Schema> out_schema;
      ASSERT_OK(RoundTripHelper(writer_helper, {batch}, IpcWriteOptions::Defaults(),
                                options, &out_batches, &out_schema));

      auto ex_schema = schema({field("a1", utf8()), field("a3", utf8())},
                              key_value_metadata({"key1"}, {"value1"}));
      AssertSchemaEqual(*ex_schema, *out_schema);

      auto ex_batch = RecordBatch::Make(ex_schema, a0->length(), {a1, a3});
      AssertBatchesEqual(*ex_batch, *out_batches[0], /*check_metadata=*/true);
    }

    // Out of bounds cases
    options.included_fields = {1, 3, 5};
    {
      WriterHelper writer_helper;
      RecordBatchVector out_batches;
      ASSERT_RAISES(Invalid,
                    RoundTripHelper(writer_helper, {batch}, IpcWriteOptions::Defaults(),
                                    options, &out_batches));
    }
    options.included_fields = {1, 3, -1};
    {
      WriterHelper writer_helper;
      RecordBatchVector out_batches;
      ASSERT_RAISES(Invalid,
                    RoundTripHelper(writer_helper, {batch}, IpcWriteOptions::Defaults(),
                                    options, &out_batches));
    }
  }

  void TestWriteDifferentSchema() {
    // Test writing batches with a different schema than the RecordBatchWriter
    // was initialized with.
    std::shared_ptr<RecordBatch> batch_ints, batch_bools;
    ASSERT_OK(MakeIntRecordBatch(&batch_ints));
    ASSERT_OK(MakeBooleanBatch(&batch_bools));

    std::shared_ptr<Schema> schema = batch_bools->schema();
    ASSERT_FALSE(schema->HasMetadata());
    schema = schema->WithMetadata(key_value_metadata({"some_key"}, {"some_value"}));

    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(schema, IpcWriteOptions::Defaults()));
    // Writing a record batch with a different schema
    ASSERT_RAISES(Invalid, writer_helper.WriteBatch(batch_ints));
    // Writing a record batch with the same schema (except metadata)
    ASSERT_OK(writer_helper.WriteBatch(batch_bools));
    ASSERT_OK(writer_helper.Finish());

    // The single successful batch can be read again
    RecordBatchVector out_batches;
    ASSERT_OK(writer_helper.ReadBatches(IpcReadOptions::Defaults(), &out_batches));
    ASSERT_EQ(out_batches.size(), 1);
    CompareBatch(*out_batches[0], *batch_bools, false /* compare_metadata */);
    // Metadata from the RecordBatchWriter initialization schema was kept
    ASSERT_TRUE(out_batches[0]->schema()->Equals(*schema));
  }

  void TestWriteBatchWithMetadata() {
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(MakeIntRecordBatch(&batch));

    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(batch->schema(), IpcWriteOptions::Defaults()));

    auto metadata = key_value_metadata({"some_key"}, {"some_value"});
    ASSERT_OK(writer_helper.WriteBatch(batch, metadata));
    ASSERT_OK(writer_helper.Finish());

    RecordBatchVector out_batches;
    MetadataVector out_metadata_list;
    ASSERT_OK(writer_helper.ReadBatches(IpcReadOptions::Defaults(), &out_batches, nullptr,
                                        &out_metadata_list));
    ASSERT_EQ(out_batches.size(), 1);
    ASSERT_EQ(out_metadata_list.size(), 1);
    CompareBatch(*out_batches[0], *batch, false /* compare_metadata */);
    ASSERT_TRUE(out_metadata_list[0]->Equals(*metadata));
  }

  // write multiple batches and each of them with different metadata
  void TestWriteDifferentMetadata() {
    std::shared_ptr<RecordBatch> batch_0;
    std::shared_ptr<RecordBatch> batch_1;
    auto metadata_0 = key_value_metadata({"some_key"}, {"0"});
    auto metadata_1 = key_value_metadata({"some_key"}, {"1"});
    ASSERT_OK(MakeIntRecordBatch(&batch_0));
    ASSERT_OK(MakeIntRecordBatch(&batch_1));

    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(batch_0->schema(), IpcWriteOptions::Defaults()));

    ASSERT_OK(writer_helper.WriteBatch(batch_0, metadata_0));

    // Write a batch with different metadata
    ASSERT_OK(writer_helper.WriteBatch(batch_1, metadata_1));
    ASSERT_OK(writer_helper.Finish());

    RecordBatchVector out_batches;
    MetadataVector out_metadata_list;
    ASSERT_OK(writer_helper.ReadBatches(IpcReadOptions::Defaults(), &out_batches, nullptr,
                                        &out_metadata_list));
    ASSERT_EQ(out_batches.size(), 2);
    ASSERT_EQ(out_metadata_list.size(), 2);
    CompareBatch(*out_batches[0], *batch_0, true /* compare_metadata */);
    CompareBatch(*out_batches[1], *batch_1, true /* compare_metadata */);
    ASSERT_TRUE(out_metadata_list[0]->Equals(*metadata_0));
    ASSERT_TRUE(out_metadata_list[1]->Equals(*metadata_1));
  }

  void TestWriteNoRecordBatches() {
    // Test writing no batches.
    auto schema = arrow::schema({field("a", int32())});

    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(schema, IpcWriteOptions::Defaults()));
    ASSERT_OK(writer_helper.Finish());

    RecordBatchVector out_batches;
    ASSERT_OK(writer_helper.ReadBatches(IpcReadOptions::Defaults(), &out_batches));
    ASSERT_EQ(out_batches.size(), 0);

    std::shared_ptr<Schema> actual_schema;
    ASSERT_OK(writer_helper.ReadSchema(&actual_schema));
    AssertSchemaEqual(*actual_schema, *schema);
  }

 private:
  Status RoundTripHelper(WriterHelper& writer_helper, const RecordBatchVector& in_batches,
                         const IpcWriteOptions& write_options,
                         const IpcReadOptions& read_options,
                         RecordBatchVector* out_batches,
                         std::shared_ptr<Schema>* out_schema = nullptr) {
    RETURN_NOT_OK(writer_helper.Init(in_batches[0]->schema(), write_options));
    for (const auto& batch : in_batches) {
      RETURN_NOT_OK(writer_helper.WriteBatch(batch));
    }
    RETURN_NOT_OK(writer_helper.Finish());
    RETURN_NOT_OK(writer_helper.ReadBatches(read_options, out_batches));
    if (out_schema) {
      RETURN_NOT_OK(writer_helper.ReadSchema(read_options, out_schema));
    }
    for (const auto& batch : *out_batches) {
      RETURN_NOT_OK(batch->ValidateFull());
    }
    return Status::OK();
  }

  void CheckBatchDictionaries(const RecordBatch& batch) {
    // Check that dictionaries that should be the same are the same
    auto schema = batch.schema();

    const auto& b0 = checked_cast<const DictionaryArray&>(*batch.column(0));
    const auto& b1 = checked_cast<const DictionaryArray&>(*batch.column(1));

    ASSERT_EQ(b0.dictionary().get(), b1.dictionary().get());

    // Same dictionary used for list values
    const auto& b3 = checked_cast<const ListArray&>(*batch.column(3));
    const auto& b3_value = checked_cast<const DictionaryArray&>(*b3.values());
    ASSERT_EQ(b0.dictionary().get(), b3_value.dictionary().get());
  }
};  // namespace test

class TestFileFormat : public ReaderWriterMixin<FileWriterHelper>,
                       public ::testing::TestWithParam<MakeRecordBatch*> {};

class TestFileFormatGenerator
    : public ReaderWriterMixin<FileGeneratorWriterHelper</*kCoalesce=*/false>>,
      public ::testing::TestWithParam<MakeRecordBatch*> {};

class TestFileFormatGeneratorCoalesced
    : public ReaderWriterMixin<FileGeneratorWriterHelper</*kCoalesce=*/true>>,
      public ::testing::TestWithParam<MakeRecordBatch*> {};

class TestStreamFormat : public ReaderWriterMixin<StreamWriterHelper>,
                         public ::testing::TestWithParam<MakeRecordBatch*> {};

class TestStreamDecoderData : public ReaderWriterMixin<StreamDecoderDataWriterHelper>,
                              public ::testing::TestWithParam<MakeRecordBatch*> {};
class TestStreamDecoderBuffer : public ReaderWriterMixin<StreamDecoderBufferWriterHelper>,
                                public ::testing::TestWithParam<MakeRecordBatch*> {};
class TestStreamDecoderSmallChunks
    : public ReaderWriterMixin<StreamDecoderSmallChunksWriterHelper>,
      public ::testing::TestWithParam<MakeRecordBatch*> {};
class TestStreamDecoderLargeChunks
    : public ReaderWriterMixin<StreamDecoderLargeChunksWriterHelper>,
      public ::testing::TestWithParam<MakeRecordBatch*> {};

Status MakeDictionaryBatch(std::shared_ptr<RecordBatch>* out) {
  auto f0_type = arrow::dictionary(int32(), utf8());
  auto f1_type = arrow::dictionary(int8(), utf8());

  auto dict = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");

  auto indices0 = ArrayFromJSON(int32(), "[1, 2, null, 0, 2, 0]");
  auto indices1 = ArrayFromJSON(int8(), "[0, 0, 2, 2, 1, 1]");

  auto a0 = std::make_shared<DictionaryArray>(f0_type, indices0, dict);
  auto a1 = std::make_shared<DictionaryArray>(f1_type, indices1, dict);

  // construct batch
  auto schema = ::arrow::schema({field("dict1", f0_type), field("dict2", f1_type)});

  *out = RecordBatch::Make(schema, 6, {a0, a1});
  return Status::OK();
}

// A utility that supports reading/writing record batches,
// and manually specifying dictionaries.
class DictionaryBatchHelper {
 public:
  explicit DictionaryBatchHelper(const Schema& schema) : schema_(schema) {
    buffer_ = *AllocateResizableBuffer(0);
    sink_.reset(new io::BufferOutputStream(buffer_));
    payload_writer_ = *internal::MakePayloadStreamWriter(sink_.get());
  }

  Status Start() {
    RETURN_NOT_OK(payload_writer_->Start());

    // write schema
    IpcPayload payload;
    DictionaryFieldMapper mapper(schema_);
    RETURN_NOT_OK(
        GetSchemaPayload(schema_, IpcWriteOptions::Defaults(), mapper, &payload));
    return payload_writer_->WritePayload(payload);
  }

  Status WriteDictionary(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
                         bool is_delta) {
    IpcPayload payload;
    RETURN_NOT_OK(GetDictionaryPayload(dictionary_id, is_delta, dictionary,
                                       IpcWriteOptions::Defaults(), &payload));
    RETURN_NOT_OK(payload_writer_->WritePayload(payload));
    return Status::OK();
  }

  Status WriteBatchPayload(const RecordBatch& batch) {
    // write record batch payload only
    IpcPayload payload;
    RETURN_NOT_OK(GetRecordBatchPayload(batch, IpcWriteOptions::Defaults(), &payload));
    return payload_writer_->WritePayload(payload);
  }

  Status Close() {
    RETURN_NOT_OK(payload_writer_->Close());
    return sink_->Close();
  }

  Status ReadBatch(std::shared_ptr<RecordBatch>* out_batch) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    std::shared_ptr<RecordBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(
        reader, RecordBatchStreamReader::Open(buf_reader, IpcReadOptions::Defaults()))
    return reader->ReadNext(out_batch);
  }

  std::unique_ptr<internal::IpcPayloadWriter> payload_writer_;
  const Schema& schema_;
  std::shared_ptr<ResizableBuffer> buffer_;
  std::unique_ptr<io::BufferOutputStream> sink_;
};

TEST(TestDictionaryBatch, DictionaryDelta) {
  std::shared_ptr<RecordBatch> in_batch;
  std::shared_ptr<RecordBatch> out_batch;
  ASSERT_OK(MakeDictionaryBatch(&in_batch));

  auto dict1 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\"]");
  auto dict2 = ArrayFromJSON(utf8(), "[\"baz\"]");

  DictionaryBatchHelper helper(*in_batch->schema());
  ASSERT_OK(helper.Start());

  ASSERT_OK(helper.WriteDictionary(0L, dict1, /*is_delta=*/false));
  ASSERT_OK(helper.WriteDictionary(0L, dict2, /*is_delta=*/true));

  ASSERT_OK(helper.WriteDictionary(1L, dict1, /*is_delta=*/false));
  ASSERT_OK(helper.WriteDictionary(1L, dict2, /*is_delta=*/true));

  ASSERT_OK(helper.WriteBatchPayload(*in_batch));
  ASSERT_OK(helper.Close());

  ASSERT_OK(helper.ReadBatch(&out_batch));

  ASSERT_BATCHES_EQUAL(*in_batch, *out_batch);
}

TEST(TestDictionaryBatch, DictionaryDeltaWithUnknownId) {
  std::shared_ptr<RecordBatch> in_batch;
  std::shared_ptr<RecordBatch> out_batch;
  ASSERT_OK(MakeDictionaryBatch(&in_batch));

  auto dict1 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\"]");
  auto dict2 = ArrayFromJSON(utf8(), "[\"baz\"]");

  DictionaryBatchHelper helper(*in_batch->schema());
  ASSERT_OK(helper.Start());

  ASSERT_OK(helper.WriteDictionary(0L, dict1, /*is_delta=*/false));
  ASSERT_OK(helper.WriteDictionary(0L, dict2, /*is_delta=*/true));

  /* This delta dictionary does not have a base dictionary previously in stream */
  ASSERT_OK(helper.WriteDictionary(1L, dict2, /*is_delta=*/true));

  ASSERT_OK(helper.WriteBatchPayload(*in_batch));
  ASSERT_OK(helper.Close());

  ASSERT_RAISES(KeyError, helper.ReadBatch(&out_batch));
}

TEST(TestDictionaryBatch, DictionaryReplacement) {
  std::shared_ptr<RecordBatch> in_batch;
  std::shared_ptr<RecordBatch> out_batch;
  ASSERT_OK(MakeDictionaryBatch(&in_batch));

  auto dict = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  auto dict1 = ArrayFromJSON(utf8(), "[\"foo1\", \"bar1\", \"baz1\"]");
  auto dict2 = ArrayFromJSON(utf8(), "[\"foo2\", \"bar2\", \"baz2\"]");

  DictionaryBatchHelper helper(*in_batch->schema());
  ASSERT_OK(helper.Start());

  // the old dictionaries will be overwritten by
  // the new dictionaries with the same ids.
  ASSERT_OK(helper.WriteDictionary(0L, dict1, /*is_delta=*/false));
  ASSERT_OK(helper.WriteDictionary(0L, dict, /*is_delta=*/false));

  ASSERT_OK(helper.WriteDictionary(1L, dict2, /*is_delta=*/false));
  ASSERT_OK(helper.WriteDictionary(1L, dict, /*is_delta=*/false));

  ASSERT_OK(helper.WriteBatchPayload(*in_batch));
  ASSERT_OK(helper.Close());

  ASSERT_OK(helper.ReadBatch(&out_batch));

  ASSERT_BATCHES_EQUAL(*in_batch, *out_batch);
}

TEST_P(TestFileFormat, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

TEST_P(TestFileFormatGenerator, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

TEST_P(TestFileFormatGeneratorCoalesced, RoundTrip) {
  TestRoundTripWithOptions(*GetParam());
}

TEST_P(TestStreamFormat, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

TEST_P(TestStreamDecoderData, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

TEST_P(TestStreamDecoderBuffer, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

TEST_P(TestStreamDecoderSmallChunks, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

TEST_P(TestStreamDecoderLargeChunks, RoundTrip) { TestRoundTripWithOptions(*GetParam()); }

INSTANTIATE_TEST_SUITE_P(GenericIpcRoundTripTests, TestIpcRoundTrip,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(FileRoundTripTests, TestFileFormat,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(FileRoundTripTests, TestFileFormatGenerator,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(FileRoundTripTests, TestFileFormatGeneratorCoalesced,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(StreamRoundTripTests, TestStreamFormat,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(StreamDecoderDataRoundTripTests, TestStreamDecoderData,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(StreamDecoderBufferRoundTripTests, TestStreamDecoderBuffer,
                         ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(StreamDecoderSmallChunksRoundTripTests,
                         TestStreamDecoderSmallChunks, ::testing::ValuesIn(kBatchCases));
INSTANTIATE_TEST_SUITE_P(StreamDecoderLargeChunksRoundTripTests,
                         TestStreamDecoderLargeChunks, ::testing::ValuesIn(kBatchCases));

TEST(TestIpcFileFormat, FooterMetaData) {
  // ARROW-6837
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntRecordBatch(&batch));

  auto metadata = key_value_metadata({"ARROW:example", "ARROW:example2"},
                                     {"something something", "something something2"});

  FileWriterHelper helper;
  ASSERT_OK(helper.Init(batch->schema(), IpcWriteOptions::Defaults(), metadata));
  ASSERT_OK(helper.WriteBatch(batch));
  ASSERT_OK(helper.Finish());

  ASSERT_OK_AND_ASSIGN(auto out_metadata, helper.ReadFooterMetadata());
  ASSERT_TRUE(out_metadata->Equals(*metadata));
}

TEST_F(TestWriteRecordBatch, RawAndSerializedSizes) {
  // ARROW-8823: Recording total raw and serialized record batch sizes in WriteStats
  FileWriterHelper helper;
  IpcWriteOptions options_uncompressed = IpcWriteOptions::Defaults();

  std::vector<std::shared_ptr<RecordBatch>> batches(3);
  // empty record batch
  ASSERT_OK(MakeIntBatchSized(0, &batches[0]));
  // record batch with int values
  ASSERT_OK(MakeIntBatchSized(2000, &batches[1], 100));

  // record batch with DictionaryArray
  random::RandomArrayGenerator rg(/*seed=*/0);
  int64_t length = 500;
  int dict_size = 50;
  std::shared_ptr<Array> dict =
      rg.String(dict_size, /*min_length=*/5, /*max_length=*/5, /*null_probability=*/0);
  std::shared_ptr<Array> indices =
      rg.Int32(length, /*min=*/0, /*max=*/dict_size - 1, /*null_probability=*/0.1);
  auto dict_type = dictionary(int32(), utf8());
  auto dict_field = field("f1", dict_type);
  ASSERT_OK_AND_ASSIGN(auto dict_array,
                       DictionaryArray::FromArrays(dict_type, indices, dict));

  auto schema = ::arrow::schema({field("f0", utf8()), dict_field});
  batches[2] =
      RecordBatch::Make(schema, length, {rg.String(500, 0, 10, 0.1), dict_array});

  for (size_t i = 0; i < batches.size(); ++i) {
    // without compression
    ASSERT_OK(helper.Init(batches[i]->schema(), options_uncompressed));
    ASSERT_OK(helper.WriteBatch(batches[i]));
    ASSERT_OK(helper.Finish());
    // padding can make serialized data slightly larger than the raw data size
    // when no compression is used
    ASSERT_LE(helper.writer_->stats().total_raw_body_size,
              helper.writer_->stats().total_serialized_body_size);

    if (!util::Codec::IsAvailable(Compression::LZ4_FRAME)) {
      continue;
    }

    IpcWriteOptions options_compressed = IpcWriteOptions::Defaults();
    ASSERT_OK_AND_ASSIGN(options_compressed.codec,
                         util::Codec::Create(Compression::LZ4_FRAME));

    // with compression
    ASSERT_OK(helper.Init(batches[i]->schema(), options_compressed));
    ASSERT_OK(helper.WriteBatch(batches[i]));
    ASSERT_OK(helper.Finish());
    ASSERT_GE(helper.writer_->stats().total_raw_body_size,
              helper.writer_->stats().total_serialized_body_size);
  }
}

// This test uses uninitialized memory

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER))
TEST_F(TestIpcRoundTrip, LargeRecordBatch) {
  const int64_t length = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

  TypedBufferBuilder<bool> data_builder;
  ASSERT_OK(data_builder.Reserve(length));
  ASSERT_OK(data_builder.Advance(length));
  ASSERT_EQ(data_builder.length(), length);
  ASSERT_OK_AND_ASSIGN(auto data, data_builder.Finish());

  auto array = std::make_shared<BooleanArray>(length, data, nullptr, /*null_count=*/0);

  auto f0 = arrow::field("f0", array->type());
  std::vector<std::shared_ptr<Field>> fields = {f0};
  auto schema = std::make_shared<Schema>(fields);

  auto batch = RecordBatch::Make(schema, length, {array});

  std::string path = "test-write-large-record_batch";

  // 512 MB
  constexpr int64_t kBufferSize = 1 << 29;
  ASSERT_OK_AND_ASSIGN(mmap_, io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  ASSERT_OK_AND_ASSIGN(auto result, DoLargeRoundTrip(*batch, false));
  CheckReadResult(*result, *batch);

  ASSERT_EQ(length, result->num_rows());
}
#endif

TEST_F(TestStreamFormat, DictionaryRoundTrip) { TestDictionaryRoundtrip(); }
TEST_F(TestFileFormat, DictionaryRoundTrip) { TestDictionaryRoundtrip(); }
TEST_F(TestFileFormatGenerator, DictionaryRoundTrip) { TestDictionaryRoundtrip(); }
TEST_F(TestFileFormatGeneratorCoalesced, DictionaryRoundTrip) {
  TestDictionaryRoundtrip();
}

TEST_F(TestStreamFormat, DifferentSchema) { TestWriteDifferentSchema(); }

TEST_F(TestFileFormat, BatchWithMetadata) { TestWriteBatchWithMetadata(); }

TEST_F(TestStreamFormat, BatchWithMetadata) { TestWriteBatchWithMetadata(); }

TEST_F(TestFileFormat, DifferentMetadataBatches) { TestWriteDifferentMetadata(); }

TEST_F(TestStreamFormat, DifferentMetadataBatches) { TestWriteDifferentMetadata(); }

TEST_F(TestFileFormat, DifferentSchema) { TestWriteDifferentSchema(); }
TEST_F(TestFileFormatGenerator, DifferentSchema) { TestWriteDifferentSchema(); }
TEST_F(TestFileFormatGeneratorCoalesced, DifferentSchema) { TestWriteDifferentSchema(); }

TEST_F(TestStreamFormat, NoRecordBatches) { TestWriteNoRecordBatches(); }
TEST_F(TestFileFormat, NoRecordBatches) { TestWriteNoRecordBatches(); }
TEST_F(TestFileFormatGenerator, NoRecordBatches) { TestWriteNoRecordBatches(); }
TEST_F(TestFileFormatGeneratorCoalesced, NoRecordBatches) { TestWriteNoRecordBatches(); }

TEST_F(TestStreamFormat, ReadFieldSubset) { TestReadSubsetOfFields(); }
TEST_F(TestFileFormat, ReadFieldSubset) { TestReadSubsetOfFields(); }
TEST_F(TestFileFormatGenerator, ReadFieldSubset) { TestReadSubsetOfFields(); }
TEST_F(TestFileFormatGeneratorCoalesced, ReadFieldSubset) { TestReadSubsetOfFields(); }

TEST_F(TestFileFormatGeneratorCoalesced, Errors) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntRecordBatch(&batch));

  FileWriterHelper helper;
  ASSERT_OK(helper.Init(batch->schema(), IpcWriteOptions::Defaults()));
  ASSERT_OK(helper.WriteBatch(batch));
  ASSERT_OK(helper.Finish());

  auto buf_reader = std::make_shared<NoZeroCopyBufferReader>(helper.buffer_);
  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchFileReader::Open(buf_reader.get()));

  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Cannot coalesce without an owned file",
                             reader->GetRecordBatchGenerator(/*coalesce=*/true));
}

TEST(TestRecordBatchStreamReader, EmptyStreamWithDictionaries) {
  // ARROW-6006
  auto f0 = arrow::field("f0", arrow::dictionary(arrow::int8(), arrow::utf8()));
  auto schema = arrow::schema({f0});

  ASSERT_OK_AND_ASSIGN(auto stream, io::BufferOutputStream::Create(0));

  ASSERT_OK_AND_ASSIGN(auto writer, MakeStreamWriter(stream, schema));
  ASSERT_OK(writer->Close());

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Finish());
  io::BufferReader buffer_reader(buffer);
  std::shared_ptr<RecordBatchReader> reader;
  ASSERT_OK_AND_ASSIGN(reader, RecordBatchStreamReader::Open(&buffer_reader));

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(reader->ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

// Delimit IPC stream messages and reassemble with the indicated messages
// included. This way we can remove messages from an IPC stream to test
// different failure modes or other difficult-to-test behaviors
void SpliceMessages(std::shared_ptr<Buffer> stream,
                    const std::vector<int>& included_indices,
                    std::shared_ptr<Buffer>* spliced_stream) {
  ASSERT_OK_AND_ASSIGN(auto out, io::BufferOutputStream::Create(0));

  io::BufferReader buffer_reader(stream);
  std::unique_ptr<MessageReader> message_reader = MessageReader::Open(&buffer_reader);
  std::unique_ptr<Message> msg;

  // Parse and reassemble first two messages in stream
  int message_index = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(msg, message_reader->ReadNextMessage());
    if (!msg) {
      break;
    }

    if (std::find(included_indices.begin(), included_indices.end(), message_index++) ==
        included_indices.end()) {
      // Message being dropped, continue
      continue;
    }

    IpcWriteOptions options;
    IpcPayload payload;
    payload.type = msg->type();
    payload.metadata = msg->metadata();
    payload.body_buffers.push_back(msg->body());
    payload.body_length = msg->body()->size();
    int32_t unused_metadata_length = -1;
    ASSERT_OK(ipc::WriteIpcPayload(payload, options, out.get(), &unused_metadata_length));
  }
  ASSERT_OK_AND_ASSIGN(*spliced_stream, out->Finish());
}

TEST(TestRecordBatchStreamReader, NotEnoughDictionaries) {
  // ARROW-6126
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionaryFlat(&batch));

  ASSERT_OK_AND_ASSIGN(auto out, io::BufferOutputStream::Create(0));
  ASSERT_OK_AND_ASSIGN(auto writer, MakeStreamWriter(out, batch->schema()));
  ASSERT_OK(writer->WriteRecordBatch(*batch));
  ASSERT_OK(writer->Close());

  // Now let's mangle the stream a little bit and make sure we return the right
  // error
  ASSERT_OK_AND_ASSIGN(auto buffer, out->Finish());

  auto AssertFailsWith = [](std::shared_ptr<Buffer> stream, const std::string& ex_error) {
    io::BufferReader reader(stream);
    ASSERT_OK_AND_ASSIGN(auto ipc_reader, RecordBatchStreamReader::Open(&reader));
    std::shared_ptr<RecordBatch> batch;
    Status s = ipc_reader->ReadNext(&batch);
    ASSERT_TRUE(s.IsInvalid());
    ASSERT_EQ(ex_error, s.message().substr(0, ex_error.size()));
  };

  // Stream terminates before reading all dictionaries
  std::shared_ptr<Buffer> truncated_stream;
  SpliceMessages(buffer, {0, 1}, &truncated_stream);
  std::string ex_message =
      ("IPC stream ended without reading the expected number (3)"
       " of dictionaries");
  AssertFailsWith(truncated_stream, ex_message);

  // One of the dictionaries is missing, then we see a record batch
  SpliceMessages(buffer, {0, 1, 2, 4}, &truncated_stream);
  ex_message =
      ("IPC stream did not have the expected number (3) of dictionaries "
       "at the start of the stream");
  AssertFailsWith(truncated_stream, ex_message);
}

TEST(TestRecordBatchStreamReader, MalformedInput) {
  const std::string empty_str = "";
  const std::string garbage_str = "12345678";

  auto empty = std::make_shared<Buffer>(empty_str);
  auto garbage = std::make_shared<Buffer>(garbage_str);

  io::BufferReader empty_reader(empty);
  ASSERT_RAISES(Invalid, RecordBatchStreamReader::Open(&empty_reader));

  io::BufferReader garbage_reader(garbage);
  ASSERT_RAISES(Invalid, RecordBatchStreamReader::Open(&garbage_reader));
}

TEST(TestStreamDecoder, NextRequiredSize) {
  auto listener = std::make_shared<CollectListener>();
  StreamDecoder decoder(listener);
  auto next_required_size = decoder.next_required_size();
  const uint8_t data[1] = {0};
  ASSERT_OK(decoder.Consume(data, 1));
  ASSERT_EQ(next_required_size - 1, decoder.next_required_size());
}

template <typename WriterHelperType>
class TestDictionaryReplacement : public ::testing::Test {
 public:
  using WriterHelper = WriterHelperType;

  void TestSameDictPointer() {
    auto type = dictionary(int8(), utf8());
    auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
    auto batch1 = MakeBatch(type, ArrayFromJSON(int8(), "[0, 2, null, 1]"), values);
    auto batch2 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 0, 0]"), values);
    CheckRoundtrip({batch1, batch2});

    EXPECT_EQ(read_stats_.num_messages, 4);  // including schema message
    EXPECT_EQ(read_stats_.num_record_batches, 2);
    EXPECT_EQ(read_stats_.num_dictionary_batches, 1);
    EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
    EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
  }

  void TestSameDictValues() {
    auto type = dictionary(int8(), utf8());
    // Create two separate dictionaries, but with the same contents
    auto batch1 = MakeBatch(ArrayFromJSON(type, R"(["foo", "foo", "bar", null])"));
    auto batch2 = MakeBatch(ArrayFromJSON(type, R"(["foo", "bar", "foo"])"));
    CheckRoundtrip({batch1, batch2});

    EXPECT_EQ(read_stats_.num_messages, 4);  // including schema message
    EXPECT_EQ(read_stats_.num_record_batches, 2);
    EXPECT_EQ(read_stats_.num_dictionary_batches, 1);
    EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
    EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
  }

  void TestDeltaDict() {
    auto type = dictionary(int8(), utf8());
    auto batch1 = MakeBatch(ArrayFromJSON(type, R"(["foo", "foo", "bar", null])"));
    // Potential delta
    auto batch2 = MakeBatch(ArrayFromJSON(type, R"(["foo", "bar", "quux", "foo"])"));
    // Potential delta
    auto batch3 =
        MakeBatch(ArrayFromJSON(type, R"(["foo", "bar", "quux", "zzz", "foo"])"));
    auto batch4 = MakeBatch(ArrayFromJSON(type, R"(["bar", null, "quux", "foo"])"));
    RecordBatchVector batches{batch1, batch2, batch3, batch4};
    RecordBatchVector only_deltas{batch1, batch2, batch3};

    // Emit replacements
    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(batches, 1);
    } else {
      CheckRoundtrip(batches);
      EXPECT_EQ(read_stats_.num_messages, 9);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 4);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 4);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 3);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    }

    // Emit deltas
    write_options_.emit_dictionary_deltas = true;
    if (WriterHelper::kIsFileFormat) {
      // batch4 is incompatible with the previous batches and would emit
      // a replacement
      CheckWritingFails(batches, 3);
    } else {
      CheckRoundtrip(batches);
      EXPECT_EQ(read_stats_.num_messages, 9);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 4);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 4);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 2);
    }

    CheckRoundtrip(only_deltas,
                   /*expect_expanded_dictionary=*/WriterHelper::kIsFileFormat);
    EXPECT_EQ(read_stats_.num_messages, 7);  // including schema message
    EXPECT_EQ(read_stats_.num_record_batches, 3);
    EXPECT_EQ(read_stats_.num_dictionary_batches, 3);
    EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
    EXPECT_EQ(read_stats_.num_dictionary_deltas, 2);

    // IPC file format: WriteTable should unify dicts
    RecordBatchVector actual;
    write_options_.unify_dictionaries = true;
    ASSERT_OK(RoundTripTable(batches, &actual));
    if (WriterHelper::kIsFileFormat) {
      EXPECT_EQ(read_stats_.num_messages, 6);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 4);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 1);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
      CheckBatchesLogical(batches, actual);
    } else {
      EXPECT_EQ(read_stats_.num_messages, 9);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 4);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 4);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 2);
      CheckBatches(batches, actual);
    }
  }

  void TestSameDictValuesNested() {
    auto batches = SameValuesNestedDictBatches();
    CheckRoundtrip(batches);

    EXPECT_EQ(read_stats_.num_messages, 5);  // including schema message
    EXPECT_EQ(read_stats_.num_record_batches, 2);
    EXPECT_EQ(read_stats_.num_dictionary_batches, 2);
    EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
    EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);

    write_options_.unify_dictionaries = true;
    CheckRoundtrip(batches);
    if (WriterHelper::kIsFileFormat) {
      // This fails because unification of nested dictionaries is not supported.
      // However, perhaps this should work because the dictionaries are simply equal.
      CheckWritingTableFails(batches, StatusCode::NotImplemented);
    } else {
      CheckRoundtripTable(batches);
    }
  }

  void TestDifferentDictValues() {
    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(DifferentOrderDictBatches(), 1);
      CheckWritingFails(DifferentValuesDictBatches(), 1);
    } else {
      CheckRoundtrip(DifferentOrderDictBatches());

      EXPECT_EQ(read_stats_.num_messages, 5);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 2);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);

      CheckRoundtrip(DifferentValuesDictBatches());

      EXPECT_EQ(read_stats_.num_messages, 5);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 2);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    }

    // Same, but single-shot table write
    if (WriterHelper::kIsFileFormat) {
      CheckWritingTableFails(DifferentOrderDictBatches());
      CheckWritingTableFails(DifferentValuesDictBatches());

      write_options_.unify_dictionaries = true;
      // Will unify dictionaries
      CheckRoundtripTable(DifferentOrderDictBatches());

      EXPECT_EQ(read_stats_.num_messages, 4);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 1);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);

      CheckRoundtripTable(DifferentValuesDictBatches());

      EXPECT_EQ(read_stats_.num_messages, 4);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 1);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 0);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    } else {
      CheckRoundtripTable(DifferentOrderDictBatches());

      EXPECT_EQ(read_stats_.num_messages, 5);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 2);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);

      CheckRoundtripTable(DifferentValuesDictBatches());

      EXPECT_EQ(read_stats_.num_messages, 5);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 2);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    }
  }

  void TestDifferentDictValuesNested() {
    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(DifferentValuesNestedDictBatches1(), 1);
      CheckWritingFails(DifferentValuesNestedDictBatches2(), 1);
      CheckWritingTableFails(DifferentValuesNestedDictBatches1());
      CheckWritingTableFails(DifferentValuesNestedDictBatches2());

      write_options_.unify_dictionaries = true;
      CheckWritingFails(DifferentValuesNestedDictBatches1(), 1);
      CheckWritingFails(DifferentValuesNestedDictBatches2(), 1);
      CheckWritingTableFails(DifferentValuesNestedDictBatches1(),
                             StatusCode::NotImplemented);
      CheckWritingTableFails(DifferentValuesNestedDictBatches2(),
                             StatusCode::NotImplemented);
      return;
    }
    CheckRoundtrip(DifferentValuesNestedDictBatches1());

    EXPECT_EQ(read_stats_.num_messages, 7);  // including schema message
    EXPECT_EQ(read_stats_.num_record_batches, 2);
    // Both inner and outer dict were replaced
    EXPECT_EQ(read_stats_.num_dictionary_batches, 4);
    EXPECT_EQ(read_stats_.num_replaced_dictionaries, 2);
    EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);

    CheckRoundtrip(DifferentValuesNestedDictBatches2());

    EXPECT_EQ(read_stats_.num_messages, 6);  // including schema message
    EXPECT_EQ(read_stats_.num_record_batches, 2);
    // Only inner dict was replaced
    EXPECT_EQ(read_stats_.num_dictionary_batches, 3);
    EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
    EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
  }

  void TestDeltaDictNestedOuter() {
    // Outer dict changes, inner dict remains the same
    auto value_type = list(dictionary(int8(), utf8()));
    auto type = dictionary(int8(), value_type);
    // Inner dict: ["a", "b"]
    auto batch1_values = ArrayFromJSON(value_type, R"([["a"], ["b"]])");
    // Potential delta
    auto batch2_values = ArrayFromJSON(value_type, R"([["a"], ["b"], ["a", "a"]])");
    auto batch1 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 0, 1]"), batch1_values);
    auto batch2 =
        MakeBatch(type, ArrayFromJSON(int8(), "[2, null, 0, 0]"), batch2_values);
    RecordBatchVector batches{batch1, batch2};

    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(batches, 1);
    } else {
      CheckRoundtrip(batches);
      EXPECT_EQ(read_stats_.num_messages, 6);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 3);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    }

    write_options_.emit_dictionary_deltas = true;
    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(batches, 1);
    } else {
      // Outer dict deltas are not emitted as the read path doesn't support them
      CheckRoundtrip(batches);
      EXPECT_EQ(read_stats_.num_messages, 6);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 2);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 3);
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 1);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    }
  }

  void TestDeltaDictNestedInner() {
    // Inner dict changes
    auto value_type = list(dictionary(int8(), utf8()));
    auto type = dictionary(int8(), value_type);
    // Inner dict: ["a"]
    auto batch1_values = ArrayFromJSON(value_type, R"([["a"]])");
    // Inner dict: ["a", "b"] => potential delta
    auto batch2_values = ArrayFromJSON(value_type, R"([["a"], ["b"], ["a", "a"]])");
    // Inner dict: ["a", "b", "c"] => potential delta
    auto batch3_values = ArrayFromJSON(value_type, R"([["a"], ["b"], ["c"]])");
    // Inner dict: ["a", "b", "c"]
    auto batch4_values = ArrayFromJSON(value_type, R"([["a"], ["b", "c"]])");
    // Inner dict: ["a", "c", "b"] => replacement
    auto batch5_values = ArrayFromJSON(value_type, R"([["a"], ["c"], ["b"]])");
    auto batch1 = MakeBatch(type, ArrayFromJSON(int8(), "[0, null, 0]"), batch1_values);
    auto batch2 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 0, 2]"), batch2_values);
    auto batch3 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 0, 2]"), batch3_values);
    auto batch4 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 0, null]"), batch4_values);
    auto batch5 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 0, 2]"), batch5_values);
    RecordBatchVector batches{batch1, batch2, batch3, batch4, batch5};

    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(batches, 1);
    } else {
      CheckRoundtrip(batches);
      EXPECT_EQ(read_stats_.num_messages, 15);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 5);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 9);  // 4 inner + 5 outer
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 7);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 0);
    }

    write_options_.emit_dictionary_deltas = true;
    if (WriterHelper::kIsFileFormat) {
      CheckWritingFails(batches, 1);
    } else {
      CheckRoundtrip(batches);
      EXPECT_EQ(read_stats_.num_messages, 15);  // including schema message
      EXPECT_EQ(read_stats_.num_record_batches, 5);
      EXPECT_EQ(read_stats_.num_dictionary_batches, 9);  // 4 inner + 5 outer
      EXPECT_EQ(read_stats_.num_replaced_dictionaries, 5);
      EXPECT_EQ(read_stats_.num_dictionary_deltas, 2);
    }
  }

  Status RoundTrip(const RecordBatchVector& in_batches, RecordBatchVector* out_batches) {
    WriterHelper writer_helper;
    RETURN_NOT_OK(writer_helper.Init(in_batches[0]->schema(), write_options_));
    for (const auto& batch : in_batches) {
      RETURN_NOT_OK(writer_helper.WriteBatch(batch));
    }
    RETURN_NOT_OK(writer_helper.Finish(&write_stats_));
    RETURN_NOT_OK(writer_helper.ReadBatches(read_options_, out_batches, &read_stats_));
    for (const auto& batch : *out_batches) {
      RETURN_NOT_OK(batch->ValidateFull());
    }
    return Status::OK();
  }

  Status RoundTripTable(const RecordBatchVector& in_batches,
                        RecordBatchVector* out_batches) {
    WriterHelper writer_helper;
    RETURN_NOT_OK(writer_helper.Init(in_batches[0]->schema(), write_options_));
    // WriteTable is different from a series of WriteBatch for RecordBatchFileWriter
    RETURN_NOT_OK(writer_helper.WriteTable(in_batches));
    RETURN_NOT_OK(writer_helper.Finish(&write_stats_));
    RETURN_NOT_OK(writer_helper.ReadBatches(read_options_, out_batches, &read_stats_));
    for (const auto& batch : *out_batches) {
      RETURN_NOT_OK(batch->ValidateFull());
    }
    return Status::OK();
  }

  void CheckBatches(const RecordBatchVector& expected, const RecordBatchVector& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      AssertBatchesEqual(*expected[i], *actual[i]);
    }
  }

  // Check that batches are logically equal, even if e.g. dictionaries
  // are different.
  void CheckBatchesLogical(const RecordBatchVector& expected,
                           const RecordBatchVector& actual) {
    ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches(expected));
    ASSERT_OK_AND_ASSIGN(auto actual_table, Table::FromRecordBatches(actual));
    ASSERT_OK_AND_ASSIGN(expected_table, expected_table->CombineChunks());
    ASSERT_OK_AND_ASSIGN(actual_table, actual_table->CombineChunks());
    AssertTablesEqual(*expected_table, *actual_table);
  }

  RecordBatchVector ExpandDictionaries(const RecordBatchVector& in_batches) {
    RecordBatchVector out;
    ArrayVector full_dictionaries;
    std::shared_ptr<RecordBatch> last_batch = in_batches[in_batches.size() - 1];
    for (const auto& column : last_batch->columns()) {
      std::shared_ptr<DictionaryArray> dict_array =
          checked_pointer_cast<DictionaryArray>(column);
      full_dictionaries.push_back(dict_array->dictionary());
    }

    for (const auto& batch : in_batches) {
      ArrayVector expanded_columns;
      for (int col_index = 0; col_index < batch->num_columns(); col_index++) {
        std::shared_ptr<Array> column = batch->column(col_index);
        std::shared_ptr<DictionaryArray> dict_array =
            checked_pointer_cast<DictionaryArray>(column);
        std::shared_ptr<Array> full_dict = full_dictionaries[col_index];
        std::shared_ptr<Array> expanded = std::make_shared<DictionaryArray>(
            dict_array->type(), dict_array->indices(), full_dict);
        expanded_columns.push_back(expanded);
      }
      out.push_back(
          RecordBatch::Make(batch->schema(), batch->num_rows(), expanded_columns));
    }
    return out;
  }

  void CheckRoundtrip(const RecordBatchVector& in_batches,
                      bool expect_expanded_dictionary = false) {
    RecordBatchVector out_batches;
    ASSERT_OK(RoundTrip(in_batches, &out_batches));
    CheckStatsConsistent();
    if (expect_expanded_dictionary) {
      CheckBatches(ExpandDictionaries(in_batches), out_batches);
    } else {
      CheckBatches(in_batches, out_batches);
    }
  }

  void CheckRoundtripTable(const RecordBatchVector& in_batches) {
    RecordBatchVector out_batches;
    ASSERT_OK(RoundTripTable(in_batches, &out_batches));
    CheckStatsConsistent();
    CheckBatchesLogical(in_batches, out_batches);
  }

  void CheckWritingFails(const RecordBatchVector& in_batches, size_t fails_at_batch_num) {
    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(in_batches[0]->schema(), write_options_));
    for (size_t i = 0; i < fails_at_batch_num; ++i) {
      ASSERT_OK(writer_helper.WriteBatch(in_batches[i]));
    }
    ASSERT_RAISES(Invalid, writer_helper.WriteBatch(in_batches[fails_at_batch_num]));
  }

  void CheckWritingTableFails(const RecordBatchVector& in_batches,
                              StatusCode expected_error = StatusCode::Invalid) {
    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(in_batches[0]->schema(), write_options_));
    auto st = writer_helper.WriteTable(in_batches);
    ASSERT_FALSE(st.ok());
    ASSERT_EQ(st.code(), expected_error);
  }

  void CheckStatsConsistent() {
    ASSERT_EQ(read_stats_.num_messages, write_stats_.num_messages);
    ASSERT_EQ(read_stats_.num_record_batches, write_stats_.num_record_batches);
    ASSERT_EQ(read_stats_.num_dictionary_batches, write_stats_.num_dictionary_batches);
    ASSERT_EQ(read_stats_.num_replaced_dictionaries,
              write_stats_.num_replaced_dictionaries);
    ASSERT_EQ(read_stats_.num_dictionary_deltas, write_stats_.num_dictionary_deltas);
  }

  RecordBatchVector DifferentOrderDictBatches() {
    // Create two separate dictionaries with different order
    auto type = dictionary(int8(), utf8());
    auto batch1 = MakeBatch(ArrayFromJSON(type, R"(["foo", "foo", "bar", null])"));
    auto batch2 = MakeBatch(ArrayFromJSON(type, R"(["bar", "bar", "foo"])"));
    return {batch1, batch2};
  }

  RecordBatchVector DifferentValuesDictBatches() {
    // Create two separate dictionaries with different values
    auto type = dictionary(int8(), utf8());
    auto batch1 = MakeBatch(ArrayFromJSON(type, R"(["foo", "foo", "bar", null])"));
    auto batch2 = MakeBatch(ArrayFromJSON(type, R"(["bar", "quux", "quux"])"));
    return {batch1, batch2};
  }

  RecordBatchVector SameValuesNestedDictBatches() {
    auto value_type = list(dictionary(int8(), utf8()));
    auto type = dictionary(int8(), value_type);
    auto batch1_values = ArrayFromJSON(value_type, R"([[], ["a"], ["b"], ["a", "a"]])");
    auto batch2_values = ArrayFromJSON(value_type, R"([[], ["a"], ["b"], ["a", "a"]])");
    auto batch1 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 3, 0, 3]"), batch1_values);
    auto batch2 = MakeBatch(type, ArrayFromJSON(int8(), "[2, null, 2]"), batch2_values);
    return {batch1, batch2};
  }

  RecordBatchVector DifferentValuesNestedDictBatches1() {
    // Inner dictionary values differ
    auto value_type = list(dictionary(int8(), utf8()));
    auto type = dictionary(int8(), value_type);
    auto batch1_values = ArrayFromJSON(value_type, R"([[], ["a"], ["b"], ["a", "a"]])");
    auto batch2_values = ArrayFromJSON(value_type, R"([[], ["a"], ["c"], ["a", "a"]])");
    auto batch1 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 3, 0, 3]"), batch1_values);
    auto batch2 = MakeBatch(type, ArrayFromJSON(int8(), "[2, null, 2]"), batch2_values);
    return {batch1, batch2};
  }

  RecordBatchVector DifferentValuesNestedDictBatches2() {
    // Outer dictionary values differ
    auto value_type = list(dictionary(int8(), utf8()));
    auto type = dictionary(int8(), value_type);
    auto batch1_values = ArrayFromJSON(value_type, R"([[], ["a"], ["b"], ["a", "a"]])");
    auto batch2_values = ArrayFromJSON(value_type, R"([["a"], ["b"], ["a", "a"]])");
    auto batch1 = MakeBatch(type, ArrayFromJSON(int8(), "[1, 3, 0, 3]"), batch1_values);
    auto batch2 = MakeBatch(type, ArrayFromJSON(int8(), "[2, null, 2]"), batch2_values);
    return {batch1, batch2};
  }

  // Make one-column batch
  std::shared_ptr<RecordBatch> MakeBatch(std::shared_ptr<Array> column) {
    return RecordBatch::Make(schema({field("f", column->type())}), column->length(),
                             {column});
  }

  // Make one-column batch with a dictionary array
  std::shared_ptr<RecordBatch> MakeBatch(std::shared_ptr<DataType> type,
                                         std::shared_ptr<Array> indices,
                                         std::shared_ptr<Array> dictionary) {
    auto array = *DictionaryArray::FromArrays(std::move(type), std::move(indices),
                                              std::move(dictionary));
    return MakeBatch(std::move(array));
  }

 protected:
  IpcWriteOptions write_options_ = IpcWriteOptions::Defaults();
  IpcReadOptions read_options_ = IpcReadOptions::Defaults();
  WriteStats write_stats_;
  ReadStats read_stats_;
};

using DictionaryReplacementTestTypes =
    ::testing::Types<StreamWriterHelper, StreamDecoderBufferWriterHelper,
                     FileWriterHelper>;

TYPED_TEST_SUITE(TestDictionaryReplacement, DictionaryReplacementTestTypes);

TYPED_TEST(TestDictionaryReplacement, SameDictPointer) { this->TestSameDictPointer(); }

TYPED_TEST(TestDictionaryReplacement, SameDictValues) { this->TestSameDictValues(); }

TYPED_TEST(TestDictionaryReplacement, DeltaDict) { this->TestDeltaDict(); }

TYPED_TEST(TestDictionaryReplacement, SameDictValuesNested) {
  this->TestSameDictValuesNested();
}

TYPED_TEST(TestDictionaryReplacement, DifferentDictValues) {
  this->TestDifferentDictValues();
}

TYPED_TEST(TestDictionaryReplacement, DifferentDictValuesNested) {
  this->TestDifferentDictValuesNested();
}

TYPED_TEST(TestDictionaryReplacement, DeltaDictNestedOuter) {
  this->TestDeltaDictNestedOuter();
}

TYPED_TEST(TestDictionaryReplacement, DeltaDictNestedInner) {
  this->TestDeltaDictNestedInner();
}

// ----------------------------------------------------------------------
// Miscellanea

TEST(FieldPosition, Basics) {
  FieldPosition pos;
  ASSERT_EQ(pos.path(), std::vector<int>{});
  {
    auto child = pos.child(6);
    ASSERT_EQ(child.path(), std::vector<int>{6});
    auto grand_child = child.child(42);
    ASSERT_EQ(grand_child.path(), (std::vector<int>{6, 42}));
  }
  {
    auto child = pos.child(12);
    ASSERT_EQ(child.path(), std::vector<int>{12});
  }
}

TEST(DictionaryFieldMapper, Basics) {
  DictionaryFieldMapper mapper;

  ASSERT_EQ(mapper.num_fields(), 0);

  ASSERT_OK(mapper.AddField(42, {0, 1}));
  ASSERT_OK(mapper.AddField(43, {0, 2}));
  ASSERT_OK(mapper.AddField(44, {0, 1, 3}));
  ASSERT_EQ(mapper.num_fields(), 3);

  ASSERT_OK_AND_EQ(42, mapper.GetFieldId({0, 1}));
  ASSERT_OK_AND_EQ(43, mapper.GetFieldId({0, 2}));
  ASSERT_OK_AND_EQ(44, mapper.GetFieldId({0, 1, 3}));
  ASSERT_RAISES(KeyError, mapper.GetFieldId({}));
  ASSERT_RAISES(KeyError, mapper.GetFieldId({0}));
  ASSERT_RAISES(KeyError, mapper.GetFieldId({0, 1, 2}));
  ASSERT_RAISES(KeyError, mapper.GetFieldId({1}));

  ASSERT_OK(mapper.AddField(41, {}));
  ASSERT_EQ(mapper.num_fields(), 4);
  ASSERT_OK_AND_EQ(41, mapper.GetFieldId({}));
  ASSERT_OK_AND_EQ(42, mapper.GetFieldId({0, 1}));

  // Duplicated dictionary ids are allowed
  ASSERT_OK(mapper.AddField(42, {4, 5, 6}));
  ASSERT_EQ(mapper.num_fields(), 5);
  ASSERT_OK_AND_EQ(42, mapper.GetFieldId({0, 1}));
  ASSERT_OK_AND_EQ(42, mapper.GetFieldId({4, 5, 6}));

  // Duplicated fields paths are not
  ASSERT_RAISES(KeyError, mapper.AddField(46, {0, 1}));
}

TEST(DictionaryFieldMapper, FromSchema) {
  auto f0 = field("f0", int8());
  auto f1 =
      field("f1", struct_({field("a", null()), field("b", dictionary(int8(), utf8()))}));
  auto f2 = field("f2", dictionary(int32(), list(dictionary(int8(), utf8()))));

  Schema schema({f0, f1, f2});
  DictionaryFieldMapper mapper(schema);

  ASSERT_EQ(mapper.num_fields(), 3);
  std::unordered_set<int64_t> ids;
  for (const auto& path : std::vector<std::vector<int>>{{1, 1}, {2}, {2, 0}}) {
    ASSERT_OK_AND_ASSIGN(const int64_t id, mapper.GetFieldId(path));
    ids.insert(id);
  }
  ASSERT_EQ(ids.size(), 3);  // All ids are distinct
}

static void AssertMemoDictionaryType(const DictionaryMemo& memo, int64_t id,
                                     const std::shared_ptr<DataType>& expected) {
  ASSERT_OK_AND_ASSIGN(const auto actual, memo.GetDictionaryType(id));
  AssertTypeEqual(*expected, *actual);
}

TEST(DictionaryMemo, AddDictionaryType) {
  DictionaryMemo memo;
  std::shared_ptr<DataType> type;

  ASSERT_RAISES(KeyError, memo.GetDictionaryType(42));

  ASSERT_OK(memo.AddDictionaryType(42, utf8()));
  ASSERT_OK(memo.AddDictionaryType(43, large_binary()));
  AssertMemoDictionaryType(memo, 42, utf8());
  AssertMemoDictionaryType(memo, 43, large_binary());

  // Re-adding same type with different id
  ASSERT_OK(memo.AddDictionaryType(44, utf8()));
  AssertMemoDictionaryType(memo, 42, utf8());
  AssertMemoDictionaryType(memo, 44, utf8());

  // Re-adding same type with same id
  ASSERT_OK(memo.AddDictionaryType(42, utf8()));
  AssertMemoDictionaryType(memo, 42, utf8());
  AssertMemoDictionaryType(memo, 44, utf8());

  // Trying to add different type with same id
  ASSERT_RAISES(KeyError, memo.AddDictionaryType(42, large_utf8()));
  AssertMemoDictionaryType(memo, 42, utf8());
  AssertMemoDictionaryType(memo, 43, large_binary());
  AssertMemoDictionaryType(memo, 44, utf8());
}

TEST(IoRecordedRandomAccessFile, IoRecording) {
  IoRecordedRandomAccessFile file(42);
  ASSERT_TRUE(file.GetReadRanges().empty());

  ASSERT_OK(file.ReadAt(1, 2));
  ASSERT_EQ(file.GetReadRanges().size(), 1);
  ASSERT_EQ(file.GetReadRanges()[0], (io::ReadRange{1, 2}));

  ASSERT_OK(file.ReadAt(5, 3));
  ASSERT_EQ(file.GetReadRanges().size(), 2);
  ASSERT_EQ(file.GetReadRanges()[1], (io::ReadRange{5, 3}));

  // continuous IOs will be merged
  ASSERT_OK(file.ReadAt(5 + 3, 6));
  ASSERT_EQ(file.GetReadRanges().size(), 2);
  ASSERT_EQ(file.GetReadRanges()[1], (io::ReadRange{5, 3 + 6}));

  // this should not happen but reading out of bounds will do no harm
  ASSERT_OK(file.ReadAt(43, 1));
}

TEST(IoRecordedRandomAccessFile, IoRecordingWithOutput) {
  std::shared_ptr<Buffer> out;
  IoRecordedRandomAccessFile file(42);
  ASSERT_TRUE(file.GetReadRanges().empty());
  ASSERT_EQ(file.ReadAt(1, 2, &out), 2L);
  ASSERT_EQ(file.GetReadRanges().size(), 1);
  ASSERT_EQ(file.GetReadRanges()[0], (io::ReadRange{1, 2}));

  ASSERT_EQ(file.ReadAt(5, 1, &out), 1);
  ASSERT_EQ(file.GetReadRanges().size(), 2);
  ASSERT_EQ(file.GetReadRanges()[1], (io::ReadRange{5, 1}));

  // continuous IOs will be merged
  ASSERT_EQ(file.ReadAt(5 + 1, 6, &out), 6);
  ASSERT_EQ(file.GetReadRanges().size(), 2);
  ASSERT_EQ(file.GetReadRanges()[1], (io::ReadRange{5, 1 + 6}));
}

TEST(IoRecordedRandomAccessFile, ReadWithCurrentPosition) {
  IoRecordedRandomAccessFile file(42);
  ASSERT_TRUE(file.GetReadRanges().empty());

  ASSERT_OK(file.Read(10));
  ASSERT_EQ(file.GetReadRanges().size(), 1);
  ASSERT_EQ(file.GetReadRanges()[0], (io::ReadRange{0, 10}));

  // the previous read should advance the position
  ASSERT_OK(file.Read(10));
  ASSERT_EQ(file.GetReadRanges().size(), 1);
  // the two reads are merged into single continuous IO
  ASSERT_EQ(file.GetReadRanges()[0], (io::ReadRange{0, 20}));
}

std::shared_ptr<Schema> MakeBooleanInt32Int64Schema() {
  auto f0 = field("f0", boolean());
  auto f1 = field("f1", int32());
  auto f2 = field("f2", int64());
  return ::arrow::schema({f0, f1, f2});
}

Status MakeBooleanInt32Int64Batch(const int length, std::shared_ptr<RecordBatch>* out) {
  auto schema_ = MakeBooleanInt32Int64Schema();
  std::shared_ptr<Array> a0, a1, a2;
  RETURN_NOT_OK(MakeRandomBooleanArray(length, false, &a0));
  RETURN_NOT_OK(MakeRandomInt32Array(length, false, arrow::default_memory_pool(), &a1));
  RETURN_NOT_OK(MakeRandomInt64Array(length, false, arrow::default_memory_pool(), &a2));
  *out = RecordBatch::Make(std::move(schema_), length, {a0, a1, a2});
  return Status::OK();
}

std::shared_ptr<Buffer> MakeBooleanInt32Int64File(int num_rows, int num_batches) {
  auto schema_ = MakeBooleanInt32Int64Schema();
  EXPECT_OK_AND_ASSIGN(auto sink, io::BufferOutputStream::Create(0));
  EXPECT_OK_AND_ASSIGN(auto writer, MakeFileWriter(sink.get(), schema_));

  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < num_batches; i++) {
    ARROW_EXPECT_OK(MakeBooleanInt32Int64Batch(num_rows, &batch));
    ARROW_EXPECT_OK(writer->WriteRecordBatch(*batch));
  }

  ARROW_EXPECT_OK(writer->Close());
  EXPECT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  return buffer;
}

void GetReadRecordBatchReadRanges(
    uint32_t num_rows, const std::vector<int>& included_fields,
    const std::vector<int64_t>& expected_body_read_lengths) {
  auto buffer = MakeBooleanInt32Int64File(num_rows, /*num_batches=*/1);

  io::BufferReader buffer_reader(buffer);
  std::unique_ptr<io::TrackedRandomAccessFile> tracked =
      io::TrackedRandomAccessFile::Make(&buffer_reader);

  auto read_options = IpcReadOptions::Defaults();
  // if empty, return all fields
  read_options.included_fields = included_fields;
  ASSERT_OK_AND_ASSIGN(auto reader,
                       RecordBatchFileReader::Open(tracked.get(), read_options));
  ASSERT_OK_AND_ASSIGN(auto out_batch, reader->ReadRecordBatch(0));

  ASSERT_EQ(out_batch->num_rows(), num_rows);
  ASSERT_EQ(out_batch->num_columns(),
            included_fields.empty() ? 3 : included_fields.size());

  auto read_ranges = tracked->get_read_ranges();

  // there are 3 read IOs before reading body:
  // 1) read magic and footer length IO
  // 2) read footer IO
  // 3) read record batch metadata IO
  ASSERT_EQ(read_ranges.size(), 3 + expected_body_read_lengths.size());
  const int32_t magic_size = static_cast<int>(strlen(ipc::internal::kArrowMagicBytes));
  // read magic and footer length IO
  auto file_end_size = magic_size + sizeof(int32_t);
  auto footer_length_offset = buffer->size() - file_end_size;
  auto footer_length = bit_util::FromLittleEndian(
      util::SafeLoadAs<int32_t>(buffer->data() + footer_length_offset));
  ASSERT_EQ(read_ranges[0].length, file_end_size);
  // read footer IO
  ASSERT_EQ(read_ranges[1].length, footer_length);
  // read record batch metadata.  The exact size is tricky to determine but it doesn't
  // matter for this test and it should be smaller than the footer.
  ASSERT_LT(read_ranges[2].length, footer_length);
  for (uint32_t i = 0; i < expected_body_read_lengths.size(); i++) {
    ASSERT_EQ(read_ranges[3 + i].length, expected_body_read_lengths[i]);
  }
}

void GetReadRecordBatchReadRanges(
    const std::vector<int>& included_fields,
    const std::vector<int64_t>& expected_body_read_lengths) {
  return GetReadRecordBatchReadRanges(5, included_fields, expected_body_read_lengths);
}

TEST(TestRecordBatchFileReaderIo, LoadAllFieldsShouldReadTheEntireBody) {
  // read the entire record batch body in single read
  // the batch has 5 * bool + 5 * int32 + 5 * int64
  // ==>
  // + 5 bool:  5 bits      (aligned to  8 bytes)
  // + 5 int32: 5 * 4 bytes (aligned to 24 bytes)
  // + 5 int64: 5 * 8 bytes (aligned to 40 bytes)
  GetReadRecordBatchReadRanges({}, {8 + 24 + 40});
}

TEST(TestRecordBatchFileReaderIo, ReadSingleFieldAtTheStart) {
  // read only the bool field
  // + 5 bool:  5 bits (1 byte)
  GetReadRecordBatchReadRanges({0}, {1});
}

TEST(TestRecordBatchFileReaderIo, ReadSingleFieldInTheMiddle) {
  // read only the int32 field
  // + 5 int32: 5 * 4 bytes
  GetReadRecordBatchReadRanges({1}, {20});
}

TEST(TestRecordBatchFileReaderIo, ReadSingleFieldInTheEnd) {
  // read only the int64 field
  // + 5 int64: 5 * 8 bytes
  GetReadRecordBatchReadRanges({2}, {40});
}

TEST(TestRecordBatchFileReaderIo, SkipTheFieldInTheMiddle) {
  // read the bool field and the int64 field
  // two IOs for body are expected, first for reading bool and the second for reading
  // int64
  // + 5 bool:  5 bits (1 byte)
  // + 5 int64: 5 * 8 bytes
  GetReadRecordBatchReadRanges({0, 2}, {1, 40});
}

TEST(TestRecordBatchFileReaderIo, ReadTwoContinousFields) {
  // read the int32 field and the int64 field
  // + 5 int32: 5 * 4 bytes
  // + 5 int64: 5 * 8 bytes
  GetReadRecordBatchReadRanges({1, 2}, {20, 40});
}

TEST(TestRecordBatchFileReaderIo, ReadTwoContinousFieldsWithIoMerged) {
  // change the array length to 64 so that bool field and int32 are continuous without
  // padding
  // read the bool field and the int32 field since the bool field's aligned offset
  // is continuous with next field (int32 field), two IOs are merged into one
  // + 64 bool: 64 bits (8 bytes)
  // + 64 int32: 64 * 4 bytes (256 bytes)
  GetReadRecordBatchReadRanges(64, {0, 1}, {8 + 64 * 4});
}

constexpr static int kNumBatches = 10;
// It can be difficult to know the exact size of the schema.  Instead we just make the
// row data big enough that we can easily identify if a read is for a schema or for
// row data.
//
// This needs to be large enough to space record batches kDefaultHoleSizeLimit bytes apart
// and also large enough that record batch data is more than kMaxMetadataSizeBytes bytes
constexpr static int kRowsPerBatch = 1000;
constexpr static int64_t kMaxMetadataSizeBytes = 1 << 13;
// There are always 2 reads when the file is opened
constexpr static int kNumReadsOnOpen = 2;

class PreBufferingTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    file_buffer_ = MakeBooleanInt32Int64File(kRowsPerBatch, kNumBatches);
  }

  void OpenReader() {
    buffer_reader_ = std::make_shared<io::BufferReader>(file_buffer_);
    tracked_ = io::TrackedRandomAccessFile::Make(buffer_reader_.get());
    auto read_options = IpcReadOptions::Defaults();
    if (ReadsArePlugged()) {
      // This will ensure that all reads get globbed together into one large read
      read_options.pre_buffer_cache_options.hole_size_limit =
          std::numeric_limits<int64_t>::max() - 1;
      read_options.pre_buffer_cache_options.range_size_limit =
          std::numeric_limits<int64_t>::max();
    }
    ASSERT_OK_AND_ASSIGN(reader_, RecordBatchFileReader::Open(tracked_, read_options));
  }

  bool ReadsArePlugged() { return GetParam(); }

  std::vector<int> AllBatchIndices() {
    std::vector<int> all_batch_indices(kNumBatches);
    std::iota(all_batch_indices.begin(), all_batch_indices.end(), 0);
    return all_batch_indices;
  }

  void AssertMetadataLoaded(std::vector<int> batch_indices) {
    if (batch_indices.size() == 0) {
      batch_indices = AllBatchIndices();
    }
    const auto& read_ranges = tracked_->get_read_ranges();
    if (ReadsArePlugged()) {
      // The read should have arrived as one large read
      ASSERT_EQ(kNumReadsOnOpen + 1, read_ranges.size());
      if (batch_indices.size() > 1) {
        ASSERT_GT(read_ranges[kNumReadsOnOpen].length, kMaxMetadataSizeBytes);
      }
    } else {
      // We should get many small reads of metadata only
      ASSERT_EQ(batch_indices.size() + kNumReadsOnOpen, read_ranges.size());
      for (const auto& read_range : read_ranges) {
        ASSERT_LT(read_range.length, kMaxMetadataSizeBytes);
      }
    }
  }

  std::vector<std::shared_ptr<RecordBatch>> LoadExpected() {
    auto buffer_reader = std::make_shared<io::BufferReader>(file_buffer_);
    auto read_options = IpcReadOptions::Defaults();
    EXPECT_OK_AND_ASSIGN(auto reader,
                         RecordBatchFileReader::Open(buffer_reader.get(), read_options));
    std::vector<std::shared_ptr<RecordBatch>> expected_batches;
    for (int i = 0; i < reader->num_record_batches(); i++) {
      EXPECT_OK_AND_ASSIGN(auto expected_batch, reader->ReadRecordBatch(i));
      expected_batches.push_back(expected_batch);
    }
    return expected_batches;
  }

  void CheckFileRead(int num_indices_pre_buffered) {
    auto expected_batches = LoadExpected();
    const std::vector<io::ReadRange>& read_ranges = tracked_->get_read_ranges();
    std::size_t starting_reads = read_ranges.size();
    for (int i = 0; i < reader_->num_record_batches(); i++) {
      ASSERT_OK_AND_ASSIGN(auto next_batch, reader_->ReadRecordBatch(i));
      AssertBatchesEqual(*expected_batches[i], *next_batch);
    }
    int metadata_reads = 0;
    int data_reads = 0;
    for (std::size_t i = starting_reads; i < read_ranges.size(); i++) {
      if (read_ranges[i].length > kMaxMetadataSizeBytes) {
        data_reads++;
      } else {
        metadata_reads++;
      }
    }
    ASSERT_EQ(metadata_reads, reader_->num_record_batches() - num_indices_pre_buffered);
    ASSERT_EQ(data_reads, reader_->num_record_batches());
  }

  std::vector<std::shared_ptr<RecordBatch>> batches_;
  std::shared_ptr<Buffer> file_buffer_;
  std::shared_ptr<io::BufferReader> buffer_reader_;
  std::shared_ptr<io::TrackedRandomAccessFile> tracked_;
  std::shared_ptr<RecordBatchFileReader> reader_;
};

TEST_P(PreBufferingTest, MetadataOnlyAllBatches) {
  OpenReader();
  // Should pre_buffer all metadata
  ASSERT_OK(reader_->PreBufferMetadata({}));
  AssertMetadataLoaded({});
  CheckFileRead(kNumBatches);
}

TEST_P(PreBufferingTest, MetadataOnlySomeBatches) {
  OpenReader();
  // Should pre_buffer all metadata
  ASSERT_OK(reader_->PreBufferMetadata({1, 2, 3}));
  AssertMetadataLoaded({1, 2, 3});
  CheckFileRead(3);
}

INSTANTIATE_TEST_SUITE_P(PreBufferingTests, PreBufferingTest,
                         ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           if (info.param) {
                             return "plugged";
                           } else {
                             return "not_plugged";
                           }
                         });

Result<std::shared_ptr<RecordBatch>> MakeBatchWithDictionaries(const int length) {
  auto dict_type = dictionary(int32(), int32());
  auto schema_ = ::arrow::schema(
      {::arrow::field("i32", int32()), ::arrow::field("i32d", dict_type)});
  std::shared_ptr<Array> i32, i32d_values, i32d_indices;
  RETURN_NOT_OK(MakeRandomInt32Array(length, false, arrow::default_memory_pool(), &i32));
  RETURN_NOT_OK(
      MakeRandomInt32Array(length, false, arrow::default_memory_pool(), &i32d_values));
  RETURN_NOT_OK(MakeRandomInt32Array(length, false, arrow::default_memory_pool(),
                                     &i32d_indices, 0, 0, length));
  std::shared_ptr<Array> i32d =
      std::make_shared<DictionaryArray>(dict_type, i32d_indices, i32d_values);
  return RecordBatch::Make(std::move(schema_), length, {i32, i32d});
}

Result<std::shared_ptr<io::RandomAccessFile>> MakeFileWithDictionaries(
    const std::unique_ptr<TemporaryDir>& tempdir, int rows_per_batch, int num_batches) {
  EXPECT_OK_AND_ASSIGN(auto temppath, tempdir->path().Join("testfile"));
  EXPECT_OK_AND_ASSIGN(auto batch, MakeBatchWithDictionaries(rows_per_batch));
  EXPECT_OK_AND_ASSIGN(auto sink, io::FileOutputStream::Open(temppath.ToString()));
  EXPECT_OK_AND_ASSIGN(auto writer, MakeFileWriter(sink.get(), batch->schema()));

  for (int i = 0; i < num_batches; i++) {
    ARROW_EXPECT_OK(writer->WriteRecordBatch(*batch));
  }

  ARROW_EXPECT_OK(writer->Close());
  ARROW_EXPECT_OK(sink->Close());
  return io::ReadableFile::Open(temppath.ToString());
}

TEST(PreBuffering, MixedAccess) {
  ASSERT_OK_AND_ASSIGN(auto tempdir, TemporaryDir::Make("arrow-ipc-read-write-test-"));
  ASSERT_OK_AND_ASSIGN(auto readable_file, MakeFileWithDictionaries(tempdir, 50, 2));
  auto read_options = IpcReadOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(auto reader,
                       RecordBatchFileReader::Open(readable_file, read_options));
  ASSERT_OK(reader->PreBufferMetadata({0}));
  ASSERT_OK_AND_ASSIGN(auto batch, reader->ReadRecordBatch(1));
  ASSERT_EQ(50, batch->num_rows());
  ASSERT_OK_AND_ASSIGN(batch, reader->ReadRecordBatch(0));
  ASSERT_EQ(50, batch->num_rows());
  auto stats = reader->stats();
  ASSERT_EQ(1, stats.num_dictionary_batches);
  ASSERT_EQ(2, stats.num_record_batches);
}

}  // namespace test
}  // namespace ipc
}  // namespace arrow
