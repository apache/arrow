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
#include "arrow/ipc/test_common.h"
#include "arrow/record_batch.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/sort_internal.h"

namespace arrow {

using FixedShapeTensorType = extension::FixedShapeTensorType;
using arrow::ipc::test::RoundtripBatch;
using extension::fixed_shape_tensor;
using extension::FixedShapeTensorArray;

class TestExtensionType : public ::testing::Test {
 public:
  void SetUp() override {
    shape_ = {3, 3, 4};
    element_shape_ = {3, 4};
    value_type_ = int64();
    element_type_ = fixed_size_list(value_type_, 12);
    dim_names_ = {"x", "y"};
    ext_type_ = internal::checked_pointer_cast<ExtensionType>(
        fixed_shape_tensor(value_type_, element_shape_, {}, dim_names_));
    values_ = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
               18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
    values_partial_ = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                       12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
    shape_partial_ = {2, 3, 4};
    tensor_strides_ = {96, 32, 8};
    element_strides_ = {32, 8};
    serialized_ = R"({"shape":[3,4],"dim_names":["x","y"]})";
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<int64_t> shape_partial_;
  std::vector<int64_t> element_shape_;
  std::shared_ptr<DataType> value_type_;
  std::shared_ptr<DataType> element_type_;
  std::vector<std::string> dim_names_;
  std::shared_ptr<ExtensionType> ext_type_;
  std::vector<int64_t> values_;
  std::vector<int64_t> values_partial_;
  std::vector<int64_t> tensor_strides_;
  std::vector<int64_t> element_strides_;
  std::string serialized_;
};

TEST_F(TestExtensionType, CheckDummyRegistration) {
  // We need a registered dummy type at runtime to allow for IPC deserialization
  auto registered_type = GetExtensionType("arrow.fixed_shape_tensor");
  ASSERT_TRUE(registered_type->type_id == Type::EXTENSION);
}

TEST_F(TestExtensionType, CreateExtensionType) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  // Test ExtensionType methods
  ASSERT_EQ(ext_type_->extension_name(), "arrow.fixed_shape_tensor");
  ASSERT_TRUE(ext_type_->Equals(*exact_ext_type));
  ASSERT_FALSE(ext_type_->Equals(*element_type_));
  ASSERT_TRUE(ext_type_->storage_type()->Equals(*element_type_));
  ASSERT_EQ(ext_type_->Serialize(), serialized_);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ext_type_->Deserialize(ext_type_->storage_type(), serialized_));
  auto deserialized = internal::checked_pointer_cast<ExtensionType>(ds);
  ASSERT_TRUE(deserialized->Equals(*ext_type_));

  // Test FixedShapeTensorType methods
  ASSERT_EQ(exact_ext_type->id(), Type::EXTENSION);
  ASSERT_EQ(exact_ext_type->ndim(), element_shape_.size());
  ASSERT_EQ(exact_ext_type->shape(), element_shape_);
  ASSERT_EQ(exact_ext_type->value_type(), value_type_);
  ASSERT_EQ(exact_ext_type->strides(), element_strides_);
  ASSERT_EQ(exact_ext_type->dim_names(), dim_names_);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: permutation size must match shape size."),
      FixedShapeTensorType::Make(value_type_, element_shape_, {0}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: dim_names size must match shape size."),
      FixedShapeTensorType::Make(value_type_, element_shape_, {}, {"x"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Permutation indices for 2 dimensional tensors must be "
                         "unique and within [0, 1] range. Got: [3,0]"),
      FixedShapeTensorType::Make(value_type_, {5, 6}, {3, 0}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Permutation indices for 3 dimensional tensors must be "
                         "unique and within [0, 2] range. Got: [0,1,1]"),
      FixedShapeTensorType::Make(value_type_, {1, 2, 3}, {0, 1, 1}));
}

TEST_F(TestExtensionType, EqualsCases) {
  auto ext_type_permutation_1 = fixed_shape_tensor(int64(), {3, 4}, {0, 1}, {"x", "y"});
  auto ext_type_permutation_2 = fixed_shape_tensor(int64(), {3, 4}, {1, 0}, {"x", "y"});
  auto ext_type_no_permutation = fixed_shape_tensor(int64(), {3, 4}, {}, {"x", "y"});

  ASSERT_TRUE(ext_type_permutation_1->Equals(ext_type_permutation_1));

  ASSERT_FALSE(fixed_shape_tensor(int32(), {3, 4}, {}, {"x", "y"})
                   ->Equals(ext_type_no_permutation));
  ASSERT_FALSE(fixed_shape_tensor(int64(), {2, 4}, {}, {"x", "y"})
                   ->Equals(ext_type_no_permutation));
  ASSERT_FALSE(fixed_shape_tensor(int64(), {3, 4}, {}, {"H", "W"})
                   ->Equals(ext_type_no_permutation));

  ASSERT_TRUE(ext_type_no_permutation->Equals(ext_type_permutation_1));
  ASSERT_TRUE(ext_type_permutation_1->Equals(ext_type_no_permutation));
  ASSERT_FALSE(ext_type_no_permutation->Equals(ext_type_permutation_2));
  ASSERT_FALSE(ext_type_permutation_2->Equals(ext_type_no_permutation));
  ASSERT_FALSE(ext_type_permutation_1->Equals(ext_type_permutation_2));
  ASSERT_FALSE(ext_type_permutation_2->Equals(ext_type_permutation_1));
}

TEST_F(TestExtensionType, CreateFromArray) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, Buffer::Wrap(values_)};
  auto arr_data = std::make_shared<ArrayData>(value_type_, values_.size(), buffers, 0, 0);
  auto arr = std::make_shared<Int64Array>(arr_data);
  ASSERT_OK_AND_ASSIGN(auto fsla_arr, FixedSizeListArray::FromArrays(arr, element_type_));
  auto ext_arr = ExtensionType::WrapArray(ext_type_, fsla_arr);
  ASSERT_EQ(ext_arr->length(), shape_[0]);
  ASSERT_EQ(ext_arr->null_count(), 0);
}

void CheckSerializationRoundtrip(const std::shared_ptr<DataType>& ext_type) {
  auto fst_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  auto serialized = fst_type->Serialize();
  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       fst_type->Deserialize(fst_type->storage_type(), serialized));
  ASSERT_TRUE(fst_type->Equals(*deserialized));
}

void CheckDeserializationRaises(const std::shared_ptr<DataType>& storage_type,
                                const std::string& serialized,
                                const std::string& expected_message) {
  auto fst_type = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(expected_message),
                                  fst_type->Deserialize(storage_type, serialized));
}

TEST_F(TestExtensionType, MetadataSerializationRoundtrip) {
  CheckSerializationRoundtrip(ext_type_);
  CheckSerializationRoundtrip(fixed_shape_tensor(value_type_, {}, {}, {}));
  CheckSerializationRoundtrip(fixed_shape_tensor(value_type_, {0}, {}, {}));
  CheckSerializationRoundtrip(fixed_shape_tensor(value_type_, {1}, {0}, {"x"}));
  CheckSerializationRoundtrip(
      fixed_shape_tensor(value_type_, {256, 256, 3}, {0, 1, 2}, {"H", "W", "C"}));
  CheckSerializationRoundtrip(
      fixed_shape_tensor(value_type_, {256, 256, 3}, {2, 0, 1}, {"C", "H", "W"}));

  auto storage_type = fixed_size_list(int64(), 12);
  CheckDeserializationRaises(boolean(), R"({"shape":[3,4]})",
                             "Expected FixedSizeList storage type, got bool");
  CheckDeserializationRaises(storage_type, R"({"dim_names":["x","y"]})",
                             "Invalid serialized JSON data");
  CheckDeserializationRaises(storage_type, R"({"shape":(3,4)})",
                             "Invalid serialized JSON data");
  CheckDeserializationRaises(storage_type, R"({"shape":[3,4],"permutation":[1,0,2]})",
                             "Invalid permutation");
  CheckDeserializationRaises(storage_type, R"({"shape":[3],"dim_names":["x","y"]})",
                             "Invalid dim_names");
}

TEST_F(TestExtensionType, RoundtripBatch) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, Buffer::Wrap(values_)};
  auto arr_data = std::make_shared<ArrayData>(value_type_, values_.size(), buffers, 0, 0);
  auto arr = std::make_shared<Int64Array>(arr_data);
  ASSERT_OK_AND_ASSIGN(auto fsla_arr, FixedSizeListArray::FromArrays(arr, element_type_));
  auto ext_arr = ExtensionType::WrapArray(ext_type_, fsla_arr);

  // Pass extension array, expect getting back extension array
  std::shared_ptr<RecordBatch> read_batch;
  auto ext_field = field(/*name=*/"f0", /*type=*/ext_type_);
  auto batch = RecordBatch::Make(schema({ext_field}), ext_arr->length(), {ext_arr});
  ASSERT_OK(RoundtripBatch(batch, &read_batch));
  CompareBatch(*batch, *read_batch, /*compare_metadata=*/true);

  // Pass extension metadata and storage array, expect getting back extension array
  std::shared_ptr<RecordBatch> read_batch2;
  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", exact_ext_type->extension_name()},
                          {"ARROW:extension:metadata", serialized_}});
  ext_field = field(/*name=*/"f0", /*type=*/element_type_, /*nullable=*/true,
                    /*metadata=*/ext_metadata);
  auto batch2 = RecordBatch::Make(schema({ext_field}), fsla_arr->length(), {fsla_arr});
  ASSERT_OK(RoundtripBatch(batch2, &read_batch2));
  CompareBatch(*batch, *read_batch2, /*compare_metadata=*/true);
}

TEST_F(TestExtensionType, CreateFromTensor) {
  std::vector<int64_t> column_major_strides = {8, 24, 72};
  std::vector<int64_t> neither_major_strides = {96, 8, 32};

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       Tensor::Make(value_type_, Buffer::Wrap(values_), shape_));

  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));

  ASSERT_OK(ext_arr->ValidateFull());
  ASSERT_TRUE(tensor->is_row_major());
  ASSERT_EQ(tensor->strides(), tensor_strides_);
  ASSERT_EQ(ext_arr->length(), shape_[0]);

  auto ext_type_2 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {0, 1}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_2, FixedShapeTensorArray::FromTensor(tensor));

  ASSERT_OK_AND_ASSIGN(
      auto column_major_tensor,
      Tensor::Make(value_type_, Buffer::Wrap(values_), shape_, column_major_strides));
  auto ext_type_3 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {0, 1}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr(
          "Invalid: Only first-major tensors can be zero-copy converted to arrays"),
      FixedShapeTensorArray::FromTensor(column_major_tensor));
  ASSERT_THAT(FixedShapeTensorArray::FromTensor(column_major_tensor),
              Raises(StatusCode::Invalid));

  auto neither_major_tensor = std::make_shared<Tensor>(value_type_, Buffer::Wrap(values_),
                                                       shape_, neither_major_strides);
  auto ext_type_4 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {1, 0}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_4,
                       FixedShapeTensorArray::FromTensor(neither_major_tensor));

  auto ext_type_5 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(binary(), {1, 3}));
  auto arr = ArrayFromJSON(binary(), R"(["abc", "def"])");

  ASSERT_OK_AND_ASSIGN(auto fsla_arr,
                       FixedSizeListArray::FromArrays(arr, fixed_size_list(binary(), 2)));
  auto ext_arr_5 = std::static_pointer_cast<FixedShapeTensorArray>(
      ExtensionType::WrapArray(ext_type_5, fsla_arr));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError, testing::HasSubstr("binary is not valid data type for a tensor"),
      ext_arr_5->ToTensor());

  auto ext_type_6 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {1, 2}));
  auto arr_with_null = ArrayFromJSON(int64(), "[1, 0, null, null, 1, 2]");
  ASSERT_OK_AND_ASSIGN(auto fsla_arr_6, FixedSizeListArray::FromArrays(
                                            arr_with_null, fixed_size_list(int64(), 2)));

  auto ext_type_7 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_7, FixedShapeTensorArray::FromTensor(tensor));
}

void CheckFromTensorType(const std::shared_ptr<Tensor>& tensor,
                         std::shared_ptr<DataType> expected_ext_type) {
  auto ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(expected_ext_type);
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  auto generated_ext_type =
      internal::checked_cast<const FixedShapeTensorType*>(ext_arr->extension_type());

  // Check that generated type is equal to the expected type
  ASSERT_EQ(generated_ext_type->type_name(), ext_type->type_name());
  ASSERT_EQ(generated_ext_type->shape(), ext_type->shape());
  ASSERT_EQ(generated_ext_type->dim_names(), ext_type->dim_names());
  ASSERT_EQ(generated_ext_type->permutation(), ext_type->permutation());
  ASSERT_TRUE(generated_ext_type->storage_type()->Equals(*ext_type->storage_type()));
  ASSERT_TRUE(generated_ext_type->Equals(ext_type));
}

TEST_F(TestExtensionType, TestFromTensorType) {
  auto values = Buffer::Wrap(values_);
  auto shapes =
      std::vector<std::vector<int64_t>>{{3, 3, 4}, {3, 3, 4}, {3, 4, 3}, {3, 4, 3}};
  auto strides = std::vector<std::vector<int64_t>>{
      {96, 32, 8}, {96, 8, 24}, {96, 24, 8}, {96, 8, 32}};
  auto tensor_dim_names = std::vector<std::vector<std::string>>{
      {"x", "y", "z"}, {"x", "y", "z"}, {"x", "y", "z"}, {"x", "y", "z"},
      {"x", "y", "z"}, {"x", "y", "z"}, {"x", "y", "z"}, {"x", "y", "z"}};
  auto dim_names = std::vector<std::vector<std::string>>{
      {"y", "z"}, {"z", "y"}, {"y", "z"}, {"z", "y"},
      {"y", "z"}, {"y", "z"}, {"y", "z"}, {"y", "z"}};
  auto element_shapes = std::vector<std::vector<int64_t>>{{3, 4}, {4, 3}, {4, 3}, {3, 4}};
  auto permutations = std::vector<std::vector<int64_t>>{{0, 1}, {1, 0}, {0, 1}, {1, 0}};

  for (size_t i = 0; i < shapes.size(); i++) {
    ASSERT_OK_AND_ASSIGN(auto tensor, Tensor::Make(value_type_, values, shapes[i],
                                                   strides[i], tensor_dim_names[i]));
    ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
    auto ext_type =
        fixed_shape_tensor(value_type_, element_shapes[i], permutations[i], dim_names[i]);
    CheckFromTensorType(tensor, ext_type);
  }
}

template <typename T>
void CheckToTensor(const std::vector<T>& values, const std::shared_ptr<DataType> typ,
                   const int32_t& element_size, const std::vector<int64_t>& element_shape,
                   const std::vector<int64_t>& element_permutation,
                   const std::vector<std::string>& element_dim_names,
                   const std::vector<int64_t>& tensor_shape,
                   const std::vector<std::string>& tensor_dim_names,
                   const std::vector<int64_t>& tensor_strides) {
  auto buffer = Buffer::Wrap(values);
  const std::shared_ptr<DataType> element_type = fixed_size_list(typ, element_size);
  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, buffer};
  auto arr_data = std::make_shared<ArrayData>(typ, values.size(), buffers);
  auto arr = std::make_shared<Int64Array>(arr_data);
  ASSERT_OK_AND_ASSIGN(auto fsla_arr, FixedSizeListArray::FromArrays(arr, element_type));

  ASSERT_OK_AND_ASSIGN(
      auto expected_tensor,
      Tensor::Make(typ, buffer, tensor_shape, tensor_strides, tensor_dim_names));
  const auto ext_type =
      fixed_shape_tensor(typ, element_shape, element_permutation, element_dim_names);

  auto ext_arr = ExtensionType::WrapArray(ext_type, fsla_arr);
  const auto tensor_array = std::static_pointer_cast<FixedShapeTensorArray>(ext_arr);
  ASSERT_OK_AND_ASSIGN(const auto actual_tensor, tensor_array->ToTensor());
  ASSERT_OK(actual_tensor->Validate());

  ASSERT_EQ(actual_tensor->type(), expected_tensor->type());
  ASSERT_EQ(actual_tensor->shape(), expected_tensor->shape());
  ASSERT_EQ(actual_tensor->strides(), expected_tensor->strides());
  ASSERT_EQ(actual_tensor->dim_names(), expected_tensor->dim_names());
  ASSERT_TRUE(actual_tensor->data()->Equals(*expected_tensor->data()));
  ASSERT_TRUE(actual_tensor->Equals(*expected_tensor));
}

TEST_F(TestExtensionType, ToTensor) {
  std::vector<float_t> float_values = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                       12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                       24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};

  auto element_sizes = std::vector<int32_t>{6, 6, 18, 18, 18, 18};

  auto element_shapes = std::vector<std::vector<int64_t>>{{2, 3}, {3, 2},    {3, 6},
                                                          {6, 3}, {3, 2, 3}, {3, 2, 3}};
  auto tensor_shapes = std::vector<std::vector<int64_t>>{
      {6, 2, 3}, {6, 2, 3}, {2, 3, 6}, {2, 3, 6}, {2, 3, 2, 3}, {2, 3, 2, 3}};

  auto element_permutations = std::vector<std::vector<int64_t>>{
      {0, 1}, {1, 0}, {0, 1}, {1, 0}, {0, 1, 2}, {2, 1, 0}};
  auto tensor_strides_32 =
      std::vector<std::vector<int64_t>>{{24, 12, 4}, {24, 4, 8},      {72, 24, 4},
                                        {72, 4, 12}, {72, 24, 12, 4}, {72, 4, 12, 24}};
  auto tensor_strides_64 =
      std::vector<std::vector<int64_t>>{{48, 24, 8},  {48, 8, 16},      {144, 48, 8},
                                        {144, 8, 24}, {144, 48, 24, 8}, {144, 8, 24, 48}};

  auto element_dim_names = std::vector<std::vector<std::string>>{
      {"y", "z"}, {"z", "y"}, {"y", "z"}, {"z", "y"}, {"H", "W", "C"}, {"H", "W", "C"}};
  auto tensor_dim_names = std::vector<std::vector<std::string>>{
      {"", "y", "z"}, {"", "y", "z"},      {"", "y", "z"},
      {"", "y", "z"}, {"", "H", "W", "C"}, {"", "C", "W", "H"}};

  for (size_t i = 0; i < element_shapes.size(); i++) {
    CheckToTensor(float_values, float32(), element_sizes[i], element_shapes[i],
                  element_permutations[i], element_dim_names[i], tensor_shapes[i],
                  tensor_dim_names[i], tensor_strides_32[i]);
    CheckToTensor(values_, int64(), element_sizes[i], element_shapes[i],
                  element_permutations[i], element_dim_names[i], tensor_shapes[i],
                  tensor_dim_names[i], tensor_strides_64[i]);
  }
}

void CheckTensorRoundtrip(const std::shared_ptr<Tensor>& tensor) {
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  ASSERT_OK_AND_ASSIGN(auto tensor_from_array, ext_arr->ToTensor());

  ASSERT_EQ(tensor->type(), tensor_from_array->type());
  ASSERT_EQ(tensor->shape(), tensor_from_array->shape());
  for (size_t i = 1; i < tensor->dim_names().size(); i++) {
    ASSERT_EQ(tensor->dim_names()[i], tensor_from_array->dim_names()[i]);
  }
  ASSERT_EQ(tensor->strides(), tensor_from_array->strides());
  ASSERT_TRUE(tensor->data()->Equals(*tensor_from_array->data()));
  ASSERT_TRUE(tensor->Equals(*tensor_from_array));
}

TEST_F(TestExtensionType, RoundtripTensor) {
  auto values = Buffer::Wrap(values_);

  auto shapes = std::vector<std::vector<int64_t>>{
      {3, 3, 4}, {3, 4, 3}, {3, 4, 3}, {3, 3, 4},    {6, 2, 3},
      {6, 3, 2}, {2, 3, 6}, {2, 6, 3}, {2, 3, 2, 3}, {2, 3, 2, 3}};
  auto strides = std::vector<std::vector<int64_t>>{
      {96, 32, 8}, {96, 8, 32},  {96, 24, 8},  {96, 8, 24},      {48, 24, 8},
      {48, 8, 24}, {144, 48, 8}, {144, 8, 48}, {144, 48, 24, 8}, {144, 8, 24, 48}};
  auto tensor_dim_names = std::vector<std::vector<std::string>>{
      {"x", "y", "z"},      {"x", "y", "z"},     {"x", "y", "z"}, {"x", "y", "z"},
      {"x", "y", "z"},      {"x", "y", "z"},     {"x", "y", "z"}, {"x", "y", "z"},
      {"N", "H", "W", "C"}, {"N", "H", "W", "C"}};

  for (size_t i = 0; i < shapes.size(); i++) {
    ASSERT_OK_AND_ASSIGN(auto tensor, Tensor::Make(value_type_, values, shapes[i],
                                                   strides[i], tensor_dim_names[i]));
    CheckTensorRoundtrip(tensor);
  }
}

TEST_F(TestExtensionType, SliceTensor) {
  ASSERT_OK_AND_ASSIGN(auto tensor,
                       Tensor::Make(value_type_, Buffer::Wrap(values_), shape_));
  ASSERT_OK_AND_ASSIGN(
      auto tensor_partial,
      Tensor::Make(value_type_, Buffer::Wrap(values_partial_), shape_partial_));
  ASSERT_EQ(tensor->strides(), tensor_strides_);
  ASSERT_EQ(tensor_partial->strides(), tensor_strides_);
  auto ext_type = fixed_shape_tensor(value_type_, element_shape_, {}, dim_names_);
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_partial,
                       FixedShapeTensorArray::FromTensor(tensor_partial));
  ASSERT_OK(ext_arr->ValidateFull());
  ASSERT_OK(ext_arr_partial->ValidateFull());

  auto sliced = internal::checked_pointer_cast<ExtensionArray>(ext_arr->Slice(0, 2));
  auto partial = internal::checked_pointer_cast<ExtensionArray>(ext_arr_partial);

  ASSERT_TRUE(sliced->Equals(*partial));
  ASSERT_OK(sliced->ValidateFull());
  ASSERT_OK(partial->ValidateFull());
  ASSERT_TRUE(sliced->storage()->Equals(*partial->storage()));
  ASSERT_EQ(sliced->length(), partial->length());
}

TEST_F(TestExtensionType, RoundtripBatchFromTensor) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);
  ASSERT_OK_AND_ASSIGN(auto tensor, Tensor::Make(value_type_, Buffer::Wrap(values_),
                                                 shape_, {}, {"n", "x", "y"}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  ext_arr->data()->type = exact_ext_type;

  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", ext_type_->extension_name()},
                          {"ARROW:extension:metadata", serialized_}});
  auto ext_field = field("f0", ext_type_, true, ext_metadata);
  auto batch = RecordBatch::Make(schema({ext_field}), ext_arr->length(), {ext_arr});
  std::shared_ptr<RecordBatch> read_batch;
  ASSERT_OK(RoundtripBatch(batch, &read_batch));
  CompareBatch(*batch, *read_batch, /*compare_metadata=*/true);
}

TEST_F(TestExtensionType, ComputeStrides) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  auto ext_type_1 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), element_shape_, {}, dim_names_));
  auto ext_type_2 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), element_shape_, {}, dim_names_));
  auto ext_type_3 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int32(), element_shape_, {}, dim_names_));
  ASSERT_TRUE(ext_type_1->Equals(*ext_type_2));
  ASSERT_FALSE(ext_type_1->Equals(*ext_type_3));

  auto ext_type_4 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {}, {"x", "y", "z"}));
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));
  ext_type_4 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {0, 1, 2}, {"x", "y", "z"}));
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));

  auto ext_type_5 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {1, 0, 2}));
  ASSERT_EQ(ext_type_5->strides(), (std::vector<int64_t>{56, 224, 8}));
  ASSERT_EQ(ext_type_5->Serialize(), R"({"shape":[3,4,7],"permutation":[1,0,2]})");

  auto ext_type_6 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {1, 2, 0}, {}));
  ASSERT_EQ(ext_type_6->strides(), (std::vector<int64_t>{56, 8, 224}));
  ASSERT_EQ(ext_type_6->Serialize(), R"({"shape":[3,4,7],"permutation":[1,2,0]})");
  auto ext_type_7 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int32(), {3, 4, 7}, {2, 0, 1}, {}));
  ASSERT_EQ(ext_type_7->strides(), (std::vector<int64_t>{4, 112, 16}));
  ASSERT_EQ(ext_type_7->Serialize(), R"({"shape":[3,4,7],"permutation":[2,0,1]})");
}

TEST_F(TestExtensionType, ToString) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  auto ext_type_1 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int16(), {3, 4, 7}));
  auto ext_type_2 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int32(), {3, 4, 7}, {1, 0, 2}));
  auto ext_type_3 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {}, {"C", "H", "W"}));

  std::string result_1 = ext_type_1->ToString();
  std::string expected_1 =
      "extension<arrow.fixed_shape_tensor[value_type=int16, shape=[3,4,7]]>";
  ASSERT_EQ(expected_1, result_1);

  std::string result_2 = ext_type_2->ToString();
  std::string expected_2 =
      "extension<arrow.fixed_shape_tensor[value_type=int32, shape=[3,4,7], "
      "permutation=[1,0,2]]>";
  ASSERT_EQ(expected_2, result_2);

  std::string result_3 = ext_type_3->ToString();
  std::string expected_3 =
      "extension<arrow.fixed_shape_tensor[value_type=int64, shape=[3,4,7], "
      "dim_names=[C,H,W]]>";
  ASSERT_EQ(expected_3, result_3);
}

TEST_F(TestExtensionType, GetTensor) {
  auto arr = ArrayFromJSON(element_type_,
                           "[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],"
                           "[12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]]");
  auto element_values =
      std::vector<std::vector<int64_t>>{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                                        {12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}};

  auto ext_type = fixed_shape_tensor(value_type_, element_shape_, {}, dim_names_);
  auto permuted_ext_type = fixed_shape_tensor(value_type_, {3, 4}, {1, 0}, {"x", "y"});
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  auto exact_permuted_ext_type =
      internal::checked_pointer_cast<FixedShapeTensorType>(permuted_ext_type);

  auto array = std::static_pointer_cast<FixedShapeTensorArray>(
      ExtensionType::WrapArray(ext_type, arr));
  auto permuted_array = std::static_pointer_cast<FixedShapeTensorArray>(
      ExtensionType::WrapArray(permuted_ext_type, arr));

  for (size_t i = 0; i < element_values.size(); i++) {
    // Get tensor from extension array with trivial permutation
    ASSERT_OK_AND_ASSIGN(auto scalar, array->GetScalar(i));
    auto actual_ext_scalar = internal::checked_pointer_cast<ExtensionScalar>(scalar);
    ASSERT_OK_AND_ASSIGN(auto actual_tensor,
                         exact_ext_type->MakeTensor(actual_ext_scalar));
    ASSERT_OK(actual_tensor->Validate());
    ASSERT_OK_AND_ASSIGN(auto expected_tensor,
                         Tensor::Make(value_type_, Buffer::Wrap(element_values[i]),
                                      {3, 4}, {}, {"x", "y"}));
    ASSERT_EQ(expected_tensor->shape(), actual_tensor->shape());
    ASSERT_EQ(expected_tensor->dim_names(), actual_tensor->dim_names());
    ASSERT_EQ(expected_tensor->strides(), actual_tensor->strides());
    ASSERT_EQ(actual_tensor->strides(), std::vector<int64_t>({32, 8}));
    ASSERT_EQ(expected_tensor->type(), actual_tensor->type());
    ASSERT_TRUE(expected_tensor->Equals(*actual_tensor));

    // Get tensor from extension array with non-trivial permutation
    ASSERT_OK_AND_ASSIGN(auto expected_permuted_tensor,
                         Tensor::Make(value_type_, Buffer::Wrap(element_values[i]),
                                      {4, 3}, {8, 24}, {"y", "x"}));
    ASSERT_OK_AND_ASSIGN(scalar, permuted_array->GetScalar(i));
    ASSERT_OK_AND_ASSIGN(auto actual_permuted_tensor,
                         exact_permuted_ext_type->MakeTensor(
                             internal::checked_pointer_cast<ExtensionScalar>(scalar)));
    ASSERT_OK(actual_permuted_tensor->Validate());
    ASSERT_EQ(expected_permuted_tensor->strides(), actual_permuted_tensor->strides());
    ASSERT_EQ(expected_permuted_tensor->shape(), actual_permuted_tensor->shape());
    ASSERT_EQ(expected_permuted_tensor->dim_names(), actual_permuted_tensor->dim_names());
    ASSERT_EQ(expected_permuted_tensor->type(), actual_permuted_tensor->type());
    ASSERT_EQ(expected_permuted_tensor->is_contiguous(),
              actual_permuted_tensor->is_contiguous());
    ASSERT_EQ(expected_permuted_tensor->is_column_major(),
              actual_permuted_tensor->is_column_major());
    ASSERT_TRUE(expected_permuted_tensor->Equals(*actual_permuted_tensor));
  }

  // Test null values fail
  auto element_type = fixed_size_list(int64(), 1);
  auto fsla_arr = ArrayFromJSON(element_type, "[[1], [null], null]");
  ext_type = fixed_shape_tensor(int64(), {1});
  exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  auto ext_arr = ExtensionType::WrapArray(ext_type, fsla_arr);
  auto tensor_array = internal::checked_pointer_cast<ExtensionArray>(ext_arr);

  ASSERT_OK_AND_ASSIGN(auto scalar, tensor_array->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto tensor,
                       exact_ext_type->MakeTensor(
                           internal::checked_pointer_cast<ExtensionScalar>(scalar)));

  ASSERT_OK_AND_ASSIGN(scalar, tensor_array->GetScalar(1));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: Cannot convert data with nulls to Tensor."),
      exact_ext_type->MakeTensor(
          internal::checked_pointer_cast<ExtensionScalar>(scalar)));

  ASSERT_OK_AND_ASSIGN(scalar, tensor_array->GetScalar(2));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: Cannot convert data with nulls to Tensor."),
      exact_ext_type->MakeTensor(
          internal::checked_pointer_cast<ExtensionScalar>(scalar)));

  element_type = list(utf8());
  ext_type = fixed_shape_tensor(utf8(), {1});
  exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  scalar = std::make_shared<ListScalar>(ArrayFromJSON(element_type, R"([["a", "b"]])"));
  auto ext_scalar = std::make_shared<ExtensionScalar>(scalar, ext_type);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      testing::HasSubstr("Type error: Cannot convert non-fixed-width values to Tensor."),
      exact_ext_type->MakeTensor(ext_scalar));
}

}  // namespace arrow
