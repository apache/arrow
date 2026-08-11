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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/buffer.h"
#include "arrow/c/dlpack.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/memory_pool.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow::dlpack {

struct LegacyProducer {
  using ManagedTensor = DLManagedTensor;
  static constexpr bool copy = false;  // Unsupported
  static constexpr const char* name = "Legacy";

  static Result<ManagedTensor*> Export(const std::shared_ptr<Array>& arr) {
    return ExportArray(arr);
  }
  static Result<ManagedTensor*> Export(const std::shared_ptr<Tensor>& t) {
    return ExportTensor(t);
  }
};

template <bool kCopy>
struct VersionedProducer {
  using ManagedTensor = DLManagedTensorVersioned;
  static constexpr bool copy = kCopy;
  static constexpr const char* name = copy ? "VersionedCopied" : "Versioned";

  static Result<ManagedTensor*> Export(const std::shared_ptr<Array>& arr) {
    return ExportArrayVersioned(arr, copy);
  }
  static Result<ManagedTensor*> Export(const std::shared_ptr<Tensor>& t) {
    return ExportTensorVersioned(t, copy);
  }
};

using ProducerTypes =
    ::testing::Types<LegacyProducer, VersionedProducer<false>, VersionedProducer<true>>;

struct ProducerNames {
  template <typename Producer>
  static std::string GetName(int) {
    return Producer::name;
  }
};

template <typename Producer>
class TestExportArray : public ::testing::Test {};

TYPED_TEST_SUITE(TestExportArray, ProducerTypes, ProducerNames);

/// The flat values a DLPack tensor views: the array itself, or the innermost
/// values of nested fixed size lists, windowed to what ``arr`` covers.
std::shared_ptr<Array> LeafValues(std::shared_ptr<Array> arr) {
  while (arr->type_id() == Type::FIXED_SIZE_LIST) {
    const auto& fsl = internal::checked_cast<const FixedSizeListArray&>(*arr);
    arr = fsl.values()->Slice(fsl.value_offset(0), arr->length() * fsl.value_length());
  }
  return arr;
}

template <typename Producer>
void CheckDLTensor(const std::shared_ptr<Array>& arr,
                   const std::shared_ptr<DataType>& arrow_type,
                   DLDataTypeCode dlpack_type, const std::vector<int64_t>& shape,
                   const std::vector<int64_t>& strides) {
  ASSERT_OK_AND_ASSIGN(auto* dlmtensor, Producer::Export(arr));
  auto dltensor = dlmtensor->dl_tensor;

  const auto values = LeafValues(arr);
  ASSERT_EQ(arrow_type->id(), values->type_id());
  const auto byte_width = arrow_type->byte_width();
  const auto start = values->offset() * byte_width;
  ASSERT_OK_AND_ASSIGN(auto sliced_buffer,
                       SliceBufferSafe(values->data()->buffers[1], start));
  if constexpr (Producer::copy) {
    ASSERT_NE(sliced_buffer->data(), dltensor.data);
    ASSERT_EQ(0, std::memcmp(sliced_buffer->data(), dltensor.data,
                             values->length() * byte_width));
  } else {
    ASSERT_EQ(sliced_buffer->data(), dltensor.data);
  }

  ASSERT_EQ(0, dltensor.byte_offset);
  ASSERT_EQ(shape.size(), static_cast<size_t>(dltensor.ndim));
  ASSERT_THAT(std::span(dltensor.shape, dltensor.ndim),
              ::testing::ElementsAreArray(shape));
  // Strides must be non-null with ndim>0 since 1.2
  ASSERT_THAT(std::span(dltensor.strides, dltensor.ndim),
              ::testing::ElementsAreArray(strides));

  ASSERT_EQ(dlpack_type, dltensor.dtype.code);
  ASSERT_EQ(arrow_type->bit_width(), dltensor.dtype.bits);
  ASSERT_EQ(1, dltensor.dtype.lanes);
  ASSERT_EQ(DLDeviceType::kDLCPU, dltensor.device.device_type);
  ASSERT_EQ(0, dltensor.device.device_id);

  ASSERT_OK_AND_ASSIGN(auto device, arrow::dlpack::ExportDevice(arr));
  ASSERT_EQ(DLDeviceType::kDLCPU, device.device_type);
  ASSERT_EQ(0, device.device_id);

  if constexpr (std::is_same_v<decltype(dlmtensor), DLManagedTensorVersioned*>) {
    ASSERT_EQ(DLPACK_MAJOR_VERSION, dlmtensor->version.major);
    ASSERT_EQ(DLPACK_MINOR_VERSION, dlmtensor->version.minor);
    if constexpr (Producer::copy) {
      // Arrow array data is immutable once constructed, but a copy is ours to hand out
      ASSERT_EQ(dlmtensor->flags, DLPACK_FLAG_BITMASK_IS_COPIED);
    } else {
      ASSERT_EQ(dlmtensor->flags, DLPACK_FLAG_BITMASK_READ_ONLY);
    }
  }

  dlmtensor->deleter(dlmtensor);
}

TYPED_TEST(TestExportArray, TestSupportedArray) {
  const std::vector<std::pair<std::shared_ptr<DataType>, DLDataTypeCode>> cases = {
      {int8(), DLDataTypeCode::kDLInt},
      {uint8(), DLDataTypeCode::kDLUInt},
      {
          int16(),
          DLDataTypeCode::kDLInt,
      },
      {uint16(), DLDataTypeCode::kDLUInt},
      {
          int32(),
          DLDataTypeCode::kDLInt,
      },
      {uint32(), DLDataTypeCode::kDLUInt},
      {
          int64(),
          DLDataTypeCode::kDLInt,
      },
      {uint64(), DLDataTypeCode::kDLUInt},
      {float16(), DLDataTypeCode::kDLFloat},
      {float32(), DLDataTypeCode::kDLFloat},
      {float64(), DLDataTypeCode::kDLFloat}};

  const auto allocated_bytes = arrow::default_memory_pool()->bytes_allocated();

  for (auto [arrow_type, dlpack_type] : cases) {
    const std::shared_ptr<Array> array =
        ArrayFromJSON(arrow_type, "[1, 0, 10, 0, 2, 1, 3, 5, 1, 0]");
    CheckDLTensor<TypeParam>(array, arrow_type, dlpack_type, {10}, {1});
    ASSERT_OK_AND_ASSIGN(auto sliced_1, array->SliceSafe(1, 5));
    CheckDLTensor<TypeParam>(sliced_1, arrow_type, dlpack_type, {5}, {1});
    ASSERT_OK_AND_ASSIGN(auto sliced_2, array->SliceSafe(0, 5));
    CheckDLTensor<TypeParam>(sliced_2, arrow_type, dlpack_type, {5}, {1});
    ASSERT_OK_AND_ASSIGN(auto sliced_3, array->SliceSafe(3));
    CheckDLTensor<TypeParam>(sliced_3, arrow_type, dlpack_type, {7}, {1});
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
}

TYPED_TEST(TestExportArray, TestFixedSizeList) {
  const auto type = fixed_size_list(int32(), 3);
  const std::shared_ptr<Array> array = ArrayFromJSON(
      type, "[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12], [13, 14, 15]]");
  CheckDLTensor<TypeParam>(array, int32(), DLDataTypeCode::kDLInt, {5, 3}, {3, 1});

  // Offset on the list array itself
  ASSERT_OK_AND_ASSIGN(auto sliced, array->SliceSafe(2, 3));
  CheckDLTensor<TypeParam>(sliced, int32(), DLDataTypeCode::kDLInt, {3, 3}, {3, 1});

  // Offset on the values array
  ASSERT_OK_AND_ASSIGN(
      auto values,
      ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]")->SliceSafe(3, 9));
  ASSERT_OK_AND_ASSIGN(auto from_values, FixedSizeListArray::FromArrays(values, 3));
  CheckDLTensor<TypeParam>(from_values, int32(), DLDataTypeCode::kDLInt, {3, 3}, {3, 1});

  // Offsets on both the list array and its values
  ASSERT_OK_AND_ASSIGN(auto sliced_from_values, from_values->SliceSafe(1, 2));
  CheckDLTensor<TypeParam>(sliced_from_values, int32(), DLDataTypeCode::kDLInt, {2, 3},
                           {3, 1});
}

TYPED_TEST(TestExportArray, TestNestedFixedSizeList) {
  const auto type = fixed_size_list(fixed_size_list(float32(), 2), 3);
  const std::shared_ptr<Array> array = ArrayFromJSON(type, R"([
      [[1, 2], [3, 4], [5, 6]],
      [[7, 8], [9, 10], [11, 12]],
      [[13, 14], [15, 16], [17, 18]],
      [[19, 20], [21, 22], [23, 24]]
  ])");
  CheckDLTensor<TypeParam>(array, float32(), DLDataTypeCode::kDLFloat, {4, 3, 2},
                           {6, 2, 1});

  // Offset on the outer list array only
  ASSERT_OK_AND_ASSIGN(auto sliced, array->SliceSafe(1, 2));
  CheckDLTensor<TypeParam>(sliced, float32(), DLDataTypeCode::kDLFloat, {2, 3, 2},
                           {6, 2, 1});

  // Offsets accumulated at every level: the innermost values are offset by 2, the
  // middle lists by 3 * 2, and the outer lists by 1 * 3 * 2.
  ASSERT_OK_AND_ASSIGN(auto values,
                       ArrayFromJSON(float32(),
                                     "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "
                                     "12, 13, 14, 15, 16, 17, 18, 19]")
                           ->SliceSafe(2, 18));
  ASSERT_OK_AND_ASSIGN(auto inner, FixedSizeListArray::FromArrays(values, 2));
  ASSERT_OK_AND_ASSIGN(auto outer, FixedSizeListArray::FromArrays(inner, 3));
  CheckDLTensor<TypeParam>(outer, float32(), DLDataTypeCode::kDLFloat, {3, 3, 2},
                           {6, 2, 1});
  ASSERT_OK_AND_ASSIGN(auto sliced_outer, outer->SliceSafe(1, 2));
  CheckDLTensor<TypeParam>(sliced_outer, float32(), DLDataTypeCode::kDLFloat, {2, 3, 2},
                           {6, 2, 1});
}

TYPED_TEST(TestExportArray, TestErrors) {
  const std::shared_ptr<Array> array_null = ArrayFromJSON(null(), "[]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_null->type()->ToString(),
                             TypeParam::Export(array_null));

  const std::shared_ptr<Array> array_with_null = ArrayFromJSON(int8(), "[1, 100, null]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only use DLPack on arrays with no nulls.",
                             TypeParam::Export(array_with_null));

  const std::shared_ptr<Array> array_string =
      ArrayFromJSON(utf8(), R"(["itsy", "bitsy", "spider"])");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_string->type()->ToString(),
                             TypeParam::Export(array_string));

  const std::shared_ptr<Array> list_of_string =
      ArrayFromJSON(fixed_size_list(utf8(), 1), R"([["itsy"], ["bitsy"]])");
  ASSERT_RAISES_WITH_MESSAGE(
      TypeError,
      "Type error: DataType is not compatible with DLPack spec: " + utf8()->ToString(),
      TypeParam::Export(list_of_string));

  const std::shared_ptr<Array> list_with_null =
      ArrayFromJSON(fixed_size_list(int8(), 2), "[[1, 2], null]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only use DLPack on arrays with no nulls.",
                             TypeParam::Export(list_with_null));

  const std::shared_ptr<Array> list_with_null_value =
      ArrayFromJSON(fixed_size_list(int8(), 2), "[[1, 2], [3, null]]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only use DLPack on arrays with no nulls.",
                             TypeParam::Export(list_with_null_value));

  const std::shared_ptr<Array> array_boolean = ArrayFromJSON(boolean(), "[true, false]");
  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: Bit-packed boolean data type not supported by DLPack.",
      TypeParam::Export(array_boolean));

  // ExportDevice only reports the device, it does not validate the type
  ASSERT_OK(arrow::dlpack::ExportDevice(array_boolean));
  ASSERT_OK(arrow::dlpack::ExportDevice(array_null));
}

template <typename Producer>
class TestExportTensor : public ::testing::Test {};

TYPED_TEST_SUITE(TestExportTensor, ProducerTypes, ProducerNames);

template <typename Producer>
void CheckDLTensor(const std::shared_ptr<Tensor>& t,
                   const std::shared_ptr<DataType>& tensor_type,
                   DLDataTypeCode dlpack_type, std::vector<int64_t> shape,
                   std::vector<int64_t> strides) {
  ASSERT_OK_AND_ASSIGN(auto* dlmtensor, Producer::Export(t));
  auto dltensor = dlmtensor->dl_tensor;

  if constexpr (Producer::copy) {
    ASSERT_NE(t->data()->data(), dltensor.data);
    ASSERT_EQ(0, std::memcmp(t->data()->data(), dltensor.data, t->data()->size()));
  } else {
    ASSERT_EQ(t->data()->data(), dltensor.data);
  }
  ASSERT_EQ(t->ndim(), dltensor.ndim);
  ASSERT_EQ(0, dltensor.byte_offset);
  for (int i = 0; i < t->ndim(); i++) {
    ASSERT_EQ(shape.data()[i], dltensor.shape[i]);
    ASSERT_EQ(strides.data()[i], dltensor.strides[i]);
  }

  ASSERT_EQ(dlpack_type, dltensor.dtype.code);
  ASSERT_EQ(tensor_type->bit_width(), dltensor.dtype.bits);
  ASSERT_EQ(1, dltensor.dtype.lanes);
  ASSERT_EQ(DLDeviceType::kDLCPU, dltensor.device.device_type);
  ASSERT_EQ(0, dltensor.device.device_id);

  ASSERT_OK_AND_ASSIGN(auto device, arrow::dlpack::ExportDevice(t));
  ASSERT_EQ(DLDeviceType::kDLCPU, device.device_type);
  ASSERT_EQ(0, device.device_id);

  if constexpr (std::is_same_v<decltype(dlmtensor), DLManagedTensorVersioned*>) {
    ASSERT_EQ(DLPACK_MAJOR_VERSION, dlmtensor->version.major);
    ASSERT_EQ(DLPACK_MINOR_VERSION, dlmtensor->version.minor);
    if constexpr (Producer::copy) {
      ASSERT_EQ(dlmtensor->flags, DLPACK_FLAG_BITMASK_IS_COPIED);
    } else {
      ASSERT_EQ(dlmtensor->flags, (t->is_mutable() ? 0 : DLPACK_FLAG_BITMASK_READ_ONLY));
    }
  }

  dlmtensor->deleter(dlmtensor);
}

TYPED_TEST(TestExportTensor, TestTensor) {
  const std::vector<std::pair<std::shared_ptr<DataType>, DLDataTypeCode>> cases = {
      {int8(), DLDataTypeCode::kDLInt},
      {uint8(), DLDataTypeCode::kDLUInt},
      {
          int16(),
          DLDataTypeCode::kDLInt,
      },
      {uint16(), DLDataTypeCode::kDLUInt},
      {
          int32(),
          DLDataTypeCode::kDLInt,
      },
      {uint32(), DLDataTypeCode::kDLUInt},
      {
          int64(),
          DLDataTypeCode::kDLInt,
      },
      {uint64(), DLDataTypeCode::kDLUInt},
      {float16(), DLDataTypeCode::kDLFloat},
      {float32(), DLDataTypeCode::kDLFloat},
      {float64(), DLDataTypeCode::kDLFloat}};

  const auto allocated_bytes = arrow::default_memory_pool()->bytes_allocated();

  for (auto [arrow_type, dlpack_type] : cases) {
    std::vector<int64_t> shape = {3, 6};
    std::vector<int64_t> dlpack_strides = {6, 1};
    std::shared_ptr<Tensor> tensor = TensorFromJSON(
        arrow_type, "[1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9]", shape);

    CheckDLTensor<TypeParam>(tensor, arrow_type, dlpack_type, shape, dlpack_strides);
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
}

TYPED_TEST(TestExportTensor, TestTensorReadOnly) {
  const std::vector<int64_t> shape = {2, 2};
  const std::vector<int64_t> dlpack_strides = {2, 1};
  std::shared_ptr<Tensor> tensor = TensorFromJSON(float32(), "[1, 2, 3, 4]", shape);
  ASSERT_TRUE(tensor->is_mutable());

  // Slicing yields an immutable view of the same data
  ASSERT_OK_AND_ASSIGN(auto read_only_buffer, SliceBufferSafe(tensor->data(), 0));
  ASSERT_OK_AND_ASSIGN(auto read_only_tensor,
                       Tensor::Make(float32(), read_only_buffer, shape));
  ASSERT_FALSE(read_only_tensor->is_mutable());

  if constexpr (std::is_same_v<typename TypeParam::ManagedTensor, DLManagedTensor>) {
    ASSERT_RAISES_WITH_MESSAGE(
        NotImplemented,
        "NotImplemented: Legacy DLPack support is not implemented for immutable tensors."
        " Please move to the DLPack version >=1.0",
        TypeParam::Export(read_only_tensor));
  } else {
    CheckDLTensor<TypeParam>(read_only_tensor, float32(), DLDataTypeCode::kDLFloat, shape,
                             dlpack_strides);
  }
}

TYPED_TEST(TestExportTensor, TestTensorStrided) {
  std::vector<int64_t> shape = {2, 2, 2};
  std::vector<int64_t> strides = {sizeof(float) * 4, sizeof(float) * 2,
                                  sizeof(float) * 1};
  std::vector<int64_t> dlpack_strides = {4, 2, 1};
  std::shared_ptr<Tensor> tensor =
      TensorFromJSON(float32(), "[1, 2, 3, 4, 5, 6, 1, 1]", shape, strides);

  CheckDLTensor<TypeParam>(tensor, float32(), DLDataTypeCode::kDLFloat, shape,
                           dlpack_strides);

  std::vector<int64_t> f_strides = {sizeof(float) * 1, sizeof(float) * 2,
                                    sizeof(float) * 4};
  std::vector<int64_t> f_dlpack_strides = {1, 2, 4};
  std::shared_ptr<Tensor> f_tensor =
      TensorFromJSON(float32(), "[1, 2, 3, 4, 5, 6, 1, 1]", shape, f_strides);

  CheckDLTensor<TypeParam>(f_tensor, float32(), DLDataTypeCode::kDLFloat, shape,
                           f_dlpack_strides);
}

}  // namespace arrow::dlpack
