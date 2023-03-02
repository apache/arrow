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

#include "arrow/extension/fixed_shape_tensor.h"

#include "arrow/testing/matchers.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

class TestExtensionType : public ::testing::Test {};

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

TEST_F(TestExtensionType, FixedShapeTensorType) {
  using FixedShapeTensorType = extension::FixedShapeTensorType;
  using arrow::extension::fixed_shape_tensor;

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
      fixed_shape_tensor(value_type, cell_shape, {}, dim_names);
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ext_type->Deserialize(ext_type->storage_type(), serialized));
  std::shared_ptr<ExtensionType> deserialized =
      std::reinterpret_pointer_cast<ExtensionType>(ds);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: Expected FixedSizeList storage type"),
      ext_type->Deserialize(boolean(), serialized));
  ASSERT_TRUE(tensor->is_row_major());
  ASSERT_EQ(tensor->strides(), strides);
  ASSERT_EQ(tensor_partial->strides(), strides);

  // Test ExtensionType methods
  ASSERT_EQ(ext_type->extension_name(), "arrow.fixed_shape_tensor");
  ASSERT_TRUE(ext_type->Equals(*exact_ext_type));
  ASSERT_TRUE(ext_type->storage_type()->Equals(*cell_type));
  ASSERT_EQ(ext_type->Serialize(), serialized);
  ASSERT_TRUE(deserialized->Equals(*ext_type));
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

  auto ext_type_1 = fixed_shape_tensor(int64(), cell_shape, {}, dim_names);
  auto ext_type_2 = fixed_shape_tensor(int64(), cell_shape, {}, dim_names);
  auto ext_type_3 = fixed_shape_tensor(int32(), cell_shape, {}, dim_names);
  ASSERT_TRUE(ext_type_1->Equals(*ext_type_2));
  ASSERT_FALSE(ext_type_1->Equals(*ext_type_3));

  auto ext_type_4 = fixed_shape_tensor(int64(), {3, 4, 7}, {}, {"x", "y", "z"});
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));
  ext_type_4 = fixed_shape_tensor(int64(), {3, 4, 7}, {0, 1, 2}, {"x", "y", "z"});
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));

  auto ext_type_5 = fixed_shape_tensor(int64(), {3, 4, 7}, {1, 0, 2});
  ASSERT_EQ(ext_type_5->strides(), (std::vector<int64_t>{56, 224, 8}));
  ASSERT_EQ(ext_type_5->Serialize(), R"({"shape":[3,4,7],"permutation":[1,0,2]})");

  auto ext_type_6 = fixed_shape_tensor(int64(), {3, 4, 7}, {1, 2, 0}, {});
  ASSERT_EQ(ext_type_6->strides(), (std::vector<int64_t>{56, 8, 224}));
  ASSERT_EQ(ext_type_6->Serialize(), R"({"shape":[3,4,7],"permutation":[1,2,0]})");

  auto ext_type_7 = fixed_shape_tensor(int64(), {3, 4, 7}, {2, 0, 1}, {});
  ASSERT_EQ(ext_type_7->strides(), (std::vector<int64_t>{8, 224, 56}));
  ASSERT_EQ(ext_type_7->Serialize(), R"({"shape":[3,4,7],"permutation":[2,0,1]})");

  auto ext_type_8 = fixed_shape_tensor(int64(), {3, 4}, {0, 1});
  EXPECT_OK_AND_ASSIGN(auto ext_arr_8, ext_type_8->MakeArray(tensor));

  ASSERT_OK_AND_ASSIGN(
      auto column_major_tensor,
      Tensor::Make(value_type, Buffer::Wrap(values), shape, column_major_strides));
  auto ext_type_9 = fixed_shape_tensor(int64(), {3, 4}, {0, 1});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr(
          "Invalid: Only first-major tensors can be zero-copy converted to arrays"),
      ext_type_9->MakeArray(column_major_tensor));
  ASSERT_THAT(ext_type_9->MakeArray(column_major_tensor), Raises(StatusCode::Invalid));

  auto neither_major_tensor = std::make_shared<Tensor>(value_type, Buffer::Wrap(values),
                                                       shape, neither_major_strides);
  auto ext_type_10 = fixed_shape_tensor(int64(), {3, 4}, {1, 0});
  ASSERT_OK_AND_ASSIGN(auto ext_arr_10, ext_type_10->MakeArray(neither_major_tensor));
}

}  // namespace arrow
