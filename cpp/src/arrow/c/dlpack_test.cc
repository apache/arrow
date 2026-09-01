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

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <type_traits>

#include "arrow/array/array_base.h"
#include "arrow/buffer.h"
#include "arrow/c/dlpack.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/memory_pool.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"

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

template <typename Producer>
void CheckDLTensor(const std::shared_ptr<Array>& arr,
                   const std::shared_ptr<DataType>& arrow_type,
                   DLDataTypeCode dlpack_type, int64_t length) {
  ASSERT_OK_AND_ASSIGN(auto* dlmtensor, Producer::Export(arr));
  auto dltensor = dlmtensor->dl_tensor;

  const auto byte_width = arr->type()->byte_width();
  const auto start = arr->offset() * byte_width;
  ASSERT_OK_AND_ASSIGN(auto sliced_buffer,
                       SliceBufferSafe(arr->data()->buffers[1], start));
  if constexpr (Producer::copy) {
    ASSERT_NE(sliced_buffer->data(), dltensor.data);
    ASSERT_EQ(0, std::memcmp(sliced_buffer->data(), dltensor.data, length * byte_width));
  } else {
    ASSERT_EQ(sliced_buffer->data(), dltensor.data);
  }

  ASSERT_EQ(0, dltensor.byte_offset);
  ASSERT_EQ(length, dltensor.shape[0]);
  ASSERT_EQ(1, dltensor.ndim);
  ASSERT_EQ(1, *dltensor.strides);  // Must be non-null with ndim>0 since 1.2

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
    CheckDLTensor<TypeParam>(array, arrow_type, dlpack_type, 10);
    ASSERT_OK_AND_ASSIGN(auto sliced_1, array->SliceSafe(1, 5));
    CheckDLTensor<TypeParam>(sliced_1, arrow_type, dlpack_type, 5);
    ASSERT_OK_AND_ASSIGN(auto sliced_2, array->SliceSafe(0, 5));
    CheckDLTensor<TypeParam>(sliced_2, arrow_type, dlpack_type, 5);
    ASSERT_OK_AND_ASSIGN(auto sliced_3, array->SliceSafe(3));
    CheckDLTensor<TypeParam>(sliced_3, arrow_type, dlpack_type, 7);
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
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

  const std::shared_ptr<Array> array_boolean = ArrayFromJSON(boolean(), "[true, false]");
  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: Bit-packed boolean data type not supported by DLPack.",
      arrow::dlpack::ExportDevice(array_boolean));
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
