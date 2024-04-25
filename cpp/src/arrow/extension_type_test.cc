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
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"

namespace arrow {

class Parametric1Array : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class Parametric2Array : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

// A parametric type where the extension_name() is always the same
class Parametric1Type : public ExtensionType {
 public:
  explicit Parametric1Type(int32_t parameter)
      : ExtensionType(int32()), parameter_(parameter) {}

  int32_t parameter() const { return parameter_; }

  std::string extension_name() const override { return "parametric-type-1"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return this->parameter() == static_cast<const Parametric1Type&>(other).parameter();
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<Parametric1Array>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    DCHECK_EQ(4, serialized.size());
    const int32_t parameter = *reinterpret_cast<const int32_t*>(serialized.data());
    DCHECK(storage_type->Equals(int32()));
    return std::make_shared<Parametric1Type>(parameter);
  }

  std::string Serialize() const override {
    std::string result("    ");
    memcpy(&result[0], &parameter_, sizeof(int32_t));
    return result;
  }

 private:
  int32_t parameter_;
};

// A parametric type where the extension_name() is different for each
// parameter, and must be separately registered
class Parametric2Type : public ExtensionType {
 public:
  explicit Parametric2Type(int32_t parameter)
      : ExtensionType(int32()), parameter_(parameter) {}

  int32_t parameter() const { return parameter_; }

  std::string extension_name() const override {
    std::stringstream ss;
    ss << "parametric-type-2<param=" << parameter_ << ">";
    return ss.str();
  }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return this->parameter() == static_cast<const Parametric2Type&>(other).parameter();
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<Parametric2Array>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    DCHECK_EQ(4, serialized.size());
    const int32_t parameter = *reinterpret_cast<const int32_t*>(serialized.data());
    DCHECK(storage_type->Equals(int32()));
    return std::make_shared<Parametric2Type>(parameter);
  }

  std::string Serialize() const override {
    std::string result("    ");
    memcpy(&result[0], &parameter_, sizeof(int32_t));
    return result;
  }

 private:
  int32_t parameter_;
};

// An extension type with a non-primitive storage type
class ExtStructArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ExtStructType : public ExtensionType {
 public:
  ExtStructType()
      : ExtensionType(
            struct_({::arrow::field("a", int64()), ::arrow::field("b", float64())})) {}

  std::string extension_name() const override { return "ext-struct-type"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return true;
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<ExtStructArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    if (serialized != "ext-struct-type-unique-code") {
      return Status::Invalid("Type identifier did not match");
    }
    return std::make_shared<ExtStructType>();
  }

  std::string Serialize() const override { return "ext-struct-type-unique-code"; }
};

class TestExtensionType : public ::testing::Test {
 public:
  void SetUp() { ASSERT_OK(RegisterExtensionType(std::make_shared<UuidType>())); }

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
  ASSERT_EQ(type->bit_width(), 128);
  ASSERT_EQ(type->byte_width(), 16);

  const auto& ext_type = static_cast<const ExtensionType&>(*type);
  std::string serialized = ext_type.Serialize();

  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       ext_type.Deserialize(fixed_size_binary(16), serialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
  ASSERT_EQ(deserialized->id(), Type::EXTENSION);
  ASSERT_EQ(deserialized->bit_width(), 128);
  ASSERT_EQ(deserialized->byte_width(), 16);
}

auto RoundtripBatch = [](const std::shared_ptr<RecordBatch>& batch,
                         std::shared_ptr<RecordBatch>* out) {
  ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                        out_stream.get()));

  ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  ASSERT_OK(batch_reader->ReadNext(out));
};

TEST_F(TestExtensionType, IpcRoundtrip) {
  auto ext_arr = ExampleUuid();
  auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);

  // Wrap type in a ListArray and ensure it also makes it
  auto offsets_arr = ArrayFromJSON(int32(), "[0, 0, 2, 4]");
  ASSERT_OK_AND_ASSIGN(auto list_arr, ListArray::FromArrays(*offsets_arr, *ext_arr));
  batch = RecordBatch::Make(schema({field("f0", list(uuid()))}), 3, {list_arr});
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);
}

TEST_F(TestExtensionType, UnrecognizedExtension) {
  auto ext_arr = ExampleUuid();
  auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

  auto storage_arr = static_cast<const ExtensionArray&>(*ext_arr).storage();

  // Write full IPC stream including schema, then unregister type, then read
  // and ensure that a plain instance of the storage type is created
  ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                        out_stream.get()));

  ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

  ASSERT_OK(UnregisterExtensionType("uuid"));
  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", "uuid"},
                          {"ARROW:extension:metadata", "uuid-serialized"}});
  auto ext_field = field("f0", fixed_size_binary(16), true, ext_metadata);
  auto batch_no_ext = RecordBatch::Make(schema({ext_field}), 4, {storage_arr});

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  std::shared_ptr<RecordBatch> read_batch;
  ASSERT_OK(batch_reader->ReadNext(&read_batch));
  CompareBatch(*batch_no_ext, *read_batch);
}

std::shared_ptr<Array> ExampleParametric(std::shared_ptr<DataType> type,
                                         const std::string& json_data) {
  auto arr = ArrayFromJSON(int32(), json_data);
  auto ext_data = arr->data()->Copy();
  ext_data->type = type;
  return MakeArray(ext_data);
}

TEST_F(TestExtensionType, ParametricTypes) {
  auto p1_type = std::make_shared<Parametric1Type>(6);
  auto p1 = ExampleParametric(p1_type, "[null, 1, 2, 3]");

  auto p2_type = std::make_shared<Parametric1Type>(12);
  auto p2 = ExampleParametric(p2_type, "[2, null, 3, 4]");

  auto p3_type = std::make_shared<Parametric2Type>(2);
  auto p3 = ExampleParametric(p3_type, "[5, 6, 7, 8]");

  auto p4_type = std::make_shared<Parametric2Type>(3);
  auto p4 = ExampleParametric(p4_type, "[5, 6, 7, 9]");

  ASSERT_OK(RegisterExtensionType(std::make_shared<Parametric1Type>(-1)));
  ASSERT_OK(RegisterExtensionType(p3_type));
  ASSERT_OK(RegisterExtensionType(p4_type));

  auto batch = RecordBatch::Make(schema({field("f0", p1_type), field("f1", p2_type),
                                         field("f2", p3_type), field("f3", p4_type)}),
                                 4, {p1, p2, p3, p4});

  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);
}

TEST_F(TestExtensionType, ParametricEquals) {
  auto p1_type = std::make_shared<Parametric1Type>(6);
  auto p2_type = std::make_shared<Parametric1Type>(6);
  auto p3_type = std::make_shared<Parametric1Type>(3);

  ASSERT_TRUE(p1_type->Equals(p2_type));
  ASSERT_FALSE(p1_type->Equals(p3_type));

  ASSERT_EQ(p1_type->fingerprint(), "");
}

std::shared_ptr<Array> ExampleStruct() {
  auto ext_type = std::make_shared<ExtStructType>();
  auto storage_type = ext_type->storage_type();
  auto arr = ArrayFromJSON(storage_type, "[[1, 0.1], [2, 0.2]]");

  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

TEST_F(TestExtensionType, ValidateExtensionArray) {
  auto ext_arr1 = ExampleUuid();
  auto p1_type = std::make_shared<Parametric1Type>(6);
  auto ext_arr2 = ExampleParametric(p1_type, "[null, 1, 2, 3]");
  auto ext_arr3 = ExampleStruct();
  auto ext_arr4 = ExampleComplex128();

  ASSERT_OK(ext_arr1->ValidateFull());
  ASSERT_OK(ext_arr2->ValidateFull());
  ASSERT_OK(ext_arr3->ValidateFull());
  ASSERT_OK(ext_arr4->ValidateFull());
}

}  // namespace arrow
