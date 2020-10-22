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
#include "arrow/testing/matchers.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/extension/tensor_array.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
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

  const auto& ext_type = static_cast<const ExtensionType&>(*type);
  std::string serialized = ext_type.Serialize();

  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       ext_type.Deserialize(fixed_size_binary(16), serialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
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

TEST_F(TestExtensionType, FixedShapeTensorType) {
  using FixedShapeTensorType = extension::FixedShapeTensorType;

  std::vector<int64_t> shape = {3, 3, 4};
  std::vector<int64_t> cell_shape = {3, 4};
  auto value_type = int64();
  std::shared_ptr<DataType> cell_type = fixed_size_list(value_type, 12);

  std::vector<std::string> dim_names = {"x", "y"};
  std::vector<int64_t> strides = {96, 32, 8};
  std::vector<int64_t> column_major_strides = {8, 24, 72};
  std::vector<int64_t> neither_major_strides = {96, 8, 32};
  std::vector<int64_t> cell_strides = {32, 8};
  std::vector<int64_t> values = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
  std::vector<int64_t> values_partial = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                         12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
  std::vector<int64_t> shape_partial = {2, 3, 4};
  std::string serialized = R"({"shape":[3,4],"dim_names":["x","y"]})";

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       Tensor::Make(value_type, Buffer::Wrap(values), shape));
  ASSERT_OK_AND_ASSIGN(
      auto tensor_partial,
      Tensor::Make(value_type, Buffer::Wrap(values_partial), shape_partial));

  std::shared_ptr<ExtensionType> ext_type =
      extension::tensor_array(value_type, cell_shape, {}, dim_names);
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ext_type->Deserialize(ext_type->storage_type(), serialized));
  std::shared_ptr<ExtensionType> deserialized =
      std::reinterpret_pointer_cast<ExtensionType>(ds);

  ASSERT_TRUE(tensor->is_row_major());
  ASSERT_EQ(tensor->strides(), strides);
  ASSERT_EQ(tensor_partial->strides(), strides);

  // Test ExtensionType methods
  ASSERT_EQ(ext_type->extension_name(), "arrow.fixed_shape_tensor");
  ASSERT_TRUE(ext_type->ExtensionEquals(*exact_ext_type));
  ASSERT_TRUE(ext_type->storage_type()->Equals(*cell_type));
  ASSERT_EQ(ext_type->Serialize(), serialized);
  ASSERT_TRUE(deserialized->ExtensionEquals(*ext_type));
  ASSERT_EQ(exact_ext_type->id(), Type::EXTENSION);

  // Test FixedShapeTensorType methods
  ASSERT_EQ(exact_ext_type->ndim(), cell_shape.size());
  ASSERT_EQ(exact_ext_type->shape(), cell_shape);
  ASSERT_EQ(exact_ext_type->strides(), cell_strides);
  ASSERT_EQ(exact_ext_type->dim_names(), dim_names);

  // Test MakeArray(std::shared_ptr<ArrayData> data)
  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, Buffer::Wrap(values)};
  auto arr_data = std::make_shared<ArrayData>(value_type, values.size(), buffers, 0, 0);
  auto arr = std::make_shared<Int64Array>(arr_data);
  EXPECT_OK_AND_ASSIGN(auto fsla_arr, FixedSizeListArray::FromArrays(arr, cell_type));
  auto data = fsla_arr->data();
  data->type = ext_type;
  auto ext_arr = exact_ext_type->MakeArray(data);
  ASSERT_EQ(ext_arr->length(), shape[0]);
  ASSERT_EQ(ext_arr->null_count(), 0);

  // Test MakeArray(std::shared_ptr<Tensor> tensor)
  EXPECT_OK_AND_ASSIGN(auto ext_arr_partial, exact_ext_type->MakeArray(tensor_partial));
  ASSERT_OK(ext_arr->ValidateFull());
  ASSERT_OK(ext_arr_partial->ValidateFull());

  // Test ToTensor(std::shared_ptr<Array> array)
  EXPECT_OK_AND_ASSIGN(auto t, exact_ext_type->ToTensor(ext_arr));
  ASSERT_EQ(t->shape(), tensor->shape());
  ASSERT_EQ(t->strides(), tensor->strides());
  ASSERT_TRUE(tensor->Equals(*t));

  // Test slicing
  auto sliced = internal::checked_pointer_cast<ExtensionArray>(ext_arr->Slice(0, 2));
  auto partial = internal::checked_pointer_cast<ExtensionArray>(ext_arr_partial);
  ASSERT_OK(sliced->ValidateFull());
  ASSERT_TRUE(sliced->storage()->Equals(*partial->storage()));
  ASSERT_EQ(sliced->length(), partial->length());

  ASSERT_OK(UnregisterExtensionType(ext_type->extension_name()));
  ASSERT_OK(RegisterExtensionType(ext_type));
  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", exact_ext_type->extension_name()},
                          {"ARROW:extension:metadata", serialized}});
  auto ext_field = field("f0", exact_ext_type, true, ext_metadata);
  auto batch = RecordBatch::Make(schema({ext_field}), ext_arr->length(), {ext_arr});
  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, /*compare_metadata=*/true);
  ASSERT_OK(UnregisterExtensionType(ext_type->extension_name()));

  auto ext_type_1 = extension::tensor_array(int64(), cell_shape, {}, dim_names);
  auto ext_type_2 = extension::tensor_array(int64(), cell_shape, {}, dim_names);
  auto ext_type_3 = extension::tensor_array(int32(), cell_shape, {}, dim_names);
  ASSERT_TRUE(ext_type_1->ExtensionEquals(*ext_type_2));
  ASSERT_FALSE(ext_type_1->ExtensionEquals(*ext_type_3));

  auto ext_type_4 = extension::tensor_array(int64(), {3, 4, 7}, {}, {"x", "y", "z"});
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));
  ext_type_4 = extension::tensor_array(int64(), {3, 4, 7}, {0, 1, 2}, {"x", "y", "z"});
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));

  auto ext_type_5 = extension::tensor_array(int64(), {3, 4, 7}, {1, 0, 2});
  ASSERT_EQ(ext_type_5->strides(), (std::vector<int64_t>{56, 224, 8}));
  ASSERT_EQ(ext_type_5->Serialize(), R"({"shape":[3,4,7],"permutation":[1,0,2]})");

  auto ext_type_6 = extension::tensor_array(int64(), {3, 4, 7}, {1, 2, 0}, {});
  ASSERT_EQ(ext_type_6->strides(), (std::vector<int64_t>{56, 8, 224}));
  ASSERT_EQ(ext_type_6->Serialize(), R"({"shape":[3,4,7],"permutation":[1,2,0]})");

  auto ext_type_7 = extension::tensor_array(int64(), {3, 4, 7}, {2, 0, 1}, {});
  ASSERT_EQ(ext_type_7->strides(), (std::vector<int64_t>{8, 224, 56}));
  ASSERT_EQ(ext_type_7->Serialize(), R"({"shape":[3,4,7],"permutation":[2,0,1]})");

  auto ext_type_8 = extension::tensor_array(int64(), {3, 4}, {0, 1});
  EXPECT_OK_AND_ASSIGN(auto ext_arr_8, ext_type_8->MakeArray(tensor));

  ASSERT_OK_AND_ASSIGN(
      auto column_major_tensor,
      Tensor::Make(value_type, Buffer::Wrap(values), shape, column_major_strides));
  auto ext_type_9 = extension::tensor_array(int64(), {3, 4}, {0, 1});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr(
          "Invalid: Only first-major tensors can be zero-copy converted to arrays"),
      ext_type_9->MakeArray(column_major_tensor));
  ASSERT_THAT(ext_type_9->MakeArray(column_major_tensor), Raises(StatusCode::Invalid));

  auto neither_major_tensor = std::make_shared<Tensor>(value_type, Buffer::Wrap(values),
                                                       shape, neither_major_strides);
  auto ext_type_10 = extension::tensor_array(int64(), {3, 4}, {1, 0});
  ASSERT_OK_AND_ASSIGN(auto ext_arr_10, ext_type_10->MakeArray(neither_major_tensor));
}

}  // namespace arrow
