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
#include <array>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer-builder.h"
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

namespace arrow {

class UUIDArray : public ExtensionArray {
 public:
  explicit UUIDArray(const std::shared_ptr<ArrayData>& data) : ExtensionArray(data) {}
};

class UUIDType : public ExtensionType {
 public:
  UUIDType() : ExtensionType(fixed_size_binary(16)) {}

  std::string extension_name() const override { return "uuid"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return true;
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    DCHECK_EQ(data->type->id(), Type::EXTENSION);
    DCHECK_EQ("uuid", static_cast<const ExtensionType&>(*data->type).extension_name());
    return std::make_shared<UUIDArray>(data);
  }

  Status Deserialize(std::shared_ptr<DataType> storage_type,
                     const std::string& serialized,
                     std::shared_ptr<DataType>* out) const override {
    if (serialized != "uuid-type-unique-code") {
      return Status::Invalid("Type identifier did not match");
    }
    DCHECK(storage_type->Equals(*fixed_size_binary(16)));
    *out = std::make_shared<UUIDType>();
    return Status::OK();
  }

  std::string Serialize() const override { return "uuid-type-unique-code"; }
};

std::shared_ptr<DataType> uuid() { return std::make_shared<UUIDType>(); }

class TestExtensionType : public ::testing::Test {
 public:
  void SetUp() { ASSERT_OK(RegisterExtensionType(std::make_shared<UUIDType>())); }

  void TearDown() {
    if (GetExtensionType("uuid")) {
      ASSERT_OK(UnregisterExtensionType("uuid"));
    }
  }
};

TEST_F(TestExtensionType, ExtensionTypeTest) {
  auto type_not_exist = GetExtensionType("uuid-unknown");
  ASSERT_EQ(type_not_exist, nullptr);

  auto registered_type = GetExtensionType("uuid");
  ASSERT_NE(registered_type, nullptr);

  auto type = uuid();
  ASSERT_EQ(type->id(), Type::EXTENSION);

  const auto& ext_type = static_cast<const ExtensionType&>(*type);
  std::string serialized = ext_type.Serialize();

  std::shared_ptr<DataType> deserialized;
  ASSERT_OK(ext_type.Deserialize(fixed_size_binary(16), serialized, &deserialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
}

auto RoundtripBatch = [](const std::shared_ptr<RecordBatch>& batch,
                         std::shared_ptr<RecordBatch>* out) {
  std::shared_ptr<io::BufferOutputStream> out_stream;
  ASSERT_OK(io::BufferOutputStream::Create(1024, default_memory_pool(), &out_stream));
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, out_stream.get()));

  std::shared_ptr<Buffer> complete_ipc_stream;
  ASSERT_OK(out_stream->Finish(&complete_ipc_stream));

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK(ipc::RecordBatchStreamReader::Open(&reader, &batch_reader));
  ASSERT_OK(batch_reader->ReadNext(out));
};

std::shared_ptr<Array> ExampleUUID() {
  auto storage_type = fixed_size_binary(16);
  auto ext_type = uuid();

  auto arr = ArrayFromJSON(
      storage_type,
      "[null, \"abcdefghijklmno0\", \"abcdefghijklmno1\", \"abcdefghijklmno2\"]");

  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

TEST_F(TestExtensionType, IpcRoundtrip) {
  auto ext_arr = ExampleUUID();
  auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);

  // Wrap type in a ListArray and ensure it also makes it
  auto offsets_arr = ArrayFromJSON(int32(), "[0, 0, 2, 4]");
  std::shared_ptr<Array> list_arr;
  ASSERT_OK(
      ListArray::FromArrays(*offsets_arr, *ext_arr, default_memory_pool(), &list_arr));
  batch = RecordBatch::Make(schema({field("f0", list(uuid()))}), 3, {list_arr});
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);
}

TEST_F(TestExtensionType, UnrecognizedExtension) {
  auto ext_arr = ExampleUUID();
  auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

  auto storage_arr = static_cast<const ExtensionArray&>(*ext_arr).storage();

  // Write full IPC stream including schema, then unregister type, then read
  // and ensure that a plain instance of the storage type is created
  std::shared_ptr<io::BufferOutputStream> out_stream;
  ASSERT_OK(io::BufferOutputStream::Create(1024, default_memory_pool(), &out_stream));
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, out_stream.get()));

  std::shared_ptr<Buffer> complete_ipc_stream;
  ASSERT_OK(out_stream->Finish(&complete_ipc_stream));

  ASSERT_OK(UnregisterExtensionType("uuid"));
  auto ext_metadata =
      key_value_metadata({{"arrow_extension_name", "uuid"},
                          {"arrow_extension_data", "uuid-type-unique-code"}});
  auto ext_field = field("f0", fixed_size_binary(16), true, ext_metadata);
  auto batch_no_ext = RecordBatch::Make(schema({ext_field}), 4, {storage_arr});

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK(ipc::RecordBatchStreamReader::Open(&reader, &batch_reader));
  std::shared_ptr<RecordBatch> read_batch;
  ASSERT_OK(batch_reader->ReadNext(&read_batch));
  CompareBatch(*batch_no_ext, *read_batch);
}

}  // namespace arrow
