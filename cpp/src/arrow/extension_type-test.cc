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

class UUIDType : public ExtensionType {
 public:
  UUIDType() : ExtensionType(::arrow::fixed_size_binary(16)) {}

  std::string extension_name() const override { return "uuid"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return true;
  }
};

class UUIDArray : public ExtensionArray {
 public:
  explicit UUIDArray(const std::shared_ptr<ArrayData>& data) : ExtensionArray(data) {}
};

class UUIDTypeAdapter : public ExtensionTypeAdapter {
 public:
  std::shared_ptr<Array> WrapArray(std::shared_ptr<ArrayData> data) override {
    DCHECK_EQ(data->type->id(), Type::EXTENSION);
    DCHECK_EQ("uuid", static_cast<const ExtensionType&>(*data->type).extension_name());
    return std::make_shared<UUIDArray>(data);
  }

  Status Deserialize(std::shared_ptr<DataType> storage_type,
                     const std::string& serialized,
                     std::shared_ptr<DataType>* out) override {
    if (serialized != "uuid-type-unique-code") {
      return Status::Invalid("Type identifier did not match");
    }
    DCHECK(storage_type->Equals(*fixed_size_binary(16)));
    *out = std::make_shared<UUIDType>();
    return Status::OK();
  }

  std::string Serialize(const ExtensionType& type) override {
    return "uuid-type-unique-code";
  }
};

class TestExtensionType : public ::testing::Test {
 public:
  void SetUp() {
    auto adapter = std::unique_ptr<ExtensionTypeAdapter>(new UUIDTypeAdapter());
    ASSERT_OK(::arrow::RegisterExtensionType("uuid", std::move(adapter)));
  }

  void TearDown() { ASSERT_OK(::arrow::UnregisterExtensionType("uuid")); }
};

TEST_F(TestExtensionType, AdapterTest) {
  auto adapter_not_exist = GetExtensionType("uuid-unknown");
  ASSERT_EQ(adapter_not_exist, nullptr);

  auto adapter = GetExtensionType("uuid");
  ASSERT_NE(adapter, nullptr);

  auto type = std::make_shared<UUIDType>();

  std::string serialized = adapter->Serialize(*type);

  std::shared_ptr<DataType> deserialized;
  ASSERT_OK(adapter->Deserialize(fixed_size_binary(16), serialized, &deserialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
}

TEST_F(TestExtensionType, IpcRoundtrip) {
  auto storage_type = fixed_size_binary(16);
  auto ext_type = std::make_shared<UUIDType>();

  auto arr = ArrayFromJSON(
      storage_type,
      "[null, \"abcdefghijklmno0\", \"abcdefghijklmno1\", \"abcdefghijklmno2\"]");

  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  auto ext_arr = MakeArray(ext_data);

  auto batch = RecordBatch::Make(schema({field("f0", ext_type)}), 4, {ext_arr});

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(ipc::SerializeRecordBatch(*batch, default_memory_pool(), &buffer));

  io::BufferReader stream(buffer);
  std::shared_ptr<RecordBatch> read_batch;
  ASSERT_OK(ipc::ReadRecordBatch(batch->schema(), &stream, &read_batch));

  CompareBatch(*batch, *read_batch);
}

}  // namespace arrow
