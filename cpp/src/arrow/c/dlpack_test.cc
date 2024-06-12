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

#include "arrow/array/array_base.h"
#include "arrow/c/dlpack.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/memory_pool.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::dlpack {

class TestExportArray : public ::testing::Test {
 public:
  void SetUp() {}
};

void CheckDLTensor(const std::shared_ptr<Array>& arr,
                   const std::shared_ptr<DataType>& arrow_type,
                   DLDataTypeCode dlpack_type, int64_t length) {
  ASSERT_OK_AND_ASSIGN(auto dlmtensor, arrow::dlpack::ExportArray(arr));
  auto dltensor = dlmtensor->dl_tensor;

  const auto byte_width = arr->type()->byte_width();
  const auto start = arr->offset() * byte_width;
  ASSERT_OK_AND_ASSIGN(auto sliced_buffer,
                       SliceBufferSafe(arr->data()->buffers[1], start));
  ASSERT_EQ(sliced_buffer->data(), dltensor.data);

  ASSERT_EQ(0, dltensor.byte_offset);
  ASSERT_EQ(NULL, dltensor.strides);
  ASSERT_EQ(length, dltensor.shape[0]);
  ASSERT_EQ(1, dltensor.ndim);

  ASSERT_EQ(dlpack_type, dltensor.dtype.code);
  ASSERT_EQ(arrow_type->bit_width(), dltensor.dtype.bits);
  ASSERT_EQ(1, dltensor.dtype.lanes);
  ASSERT_EQ(DLDeviceType::kDLCPU, dltensor.device.device_type);
  ASSERT_EQ(0, dltensor.device.device_id);

  ASSERT_OK_AND_ASSIGN(auto device, arrow::dlpack::ExportDevice(arr));
  ASSERT_EQ(DLDeviceType::kDLCPU, device.device_type);
  ASSERT_EQ(0, device.device_id);

  dlmtensor->deleter(dlmtensor);
}

TEST_F(TestExportArray, TestSupportedArray) {
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
    CheckDLTensor(array, arrow_type, dlpack_type, 10);
    ASSERT_OK_AND_ASSIGN(auto sliced_1, array->SliceSafe(1, 5));
    CheckDLTensor(sliced_1, arrow_type, dlpack_type, 5);
    ASSERT_OK_AND_ASSIGN(auto sliced_2, array->SliceSafe(0, 5));
    CheckDLTensor(sliced_2, arrow_type, dlpack_type, 5);
    ASSERT_OK_AND_ASSIGN(auto sliced_3, array->SliceSafe(3));
    CheckDLTensor(sliced_3, arrow_type, dlpack_type, 7);
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
}

TEST_F(TestExportArray, TestErrors) {
  const std::shared_ptr<Array> array_null = ArrayFromJSON(null(), "[]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_null->type()->ToString(),
                             arrow::dlpack::ExportArray(array_null));

  const std::shared_ptr<Array> array_with_null = ArrayFromJSON(int8(), "[1, 100, null]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only use DLPack on arrays with no nulls.",
                             arrow::dlpack::ExportArray(array_with_null));

  const std::shared_ptr<Array> array_string =
      ArrayFromJSON(utf8(), R"(["itsy", "bitsy", "spider"])");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_string->type()->ToString(),
                             arrow::dlpack::ExportArray(array_string));

  const std::shared_ptr<Array> array_boolean = ArrayFromJSON(boolean(), "[true, false]");
  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: Bit-packed boolean data type not supported by DLPack.",
      arrow::dlpack::ExportDevice(array_boolean));
}

class TestExportTensor : public ::testing::Test {
 public:
  void SetUp() {}
};

void CheckDLTensor(const std::shared_ptr<Tensor>& t,
                   const std::shared_ptr<DataType>& tensor_type,
                   DLDataTypeCode dlpack_type, std::vector<int64_t> shape,
                   std::vector<int64_t> strides) {
  ASSERT_OK_AND_ASSIGN(auto dlmtensor, arrow::dlpack::ExportTensor(t));
  auto dltensor = dlmtensor->dl_tensor;

  ASSERT_EQ(t->data()->data(), dltensor.data);
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

  dlmtensor->deleter(dlmtensor);
}

TEST_F(TestExportTensor, TestTensor) {
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

    CheckDLTensor(tensor, arrow_type, dlpack_type, shape, dlpack_strides);
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
}

TEST_F(TestExportTensor, TestTensorStrided) {
  std::vector<int64_t> shape = {2, 2, 2};
  std::vector<int64_t> strides = {sizeof(float) * 4, sizeof(float) * 2,
                                  sizeof(float) * 1};
  std::vector<int64_t> dlpack_strides = {4, 2, 1};
  std::shared_ptr<Tensor> tensor =
      TensorFromJSON(float32(), "[1, 2, 3, 4, 5, 6, 1, 1]", shape, strides);

  CheckDLTensor(tensor, float32(), DLDataTypeCode::kDLFloat, shape, dlpack_strides);

  std::vector<int64_t> f_strides = {sizeof(float) * 1, sizeof(float) * 2,
                                    sizeof(float) * 4};
  std::vector<int64_t> f_dlpack_strides = {1, 2, 4};
  std::shared_ptr<Tensor> f_tensor =
      TensorFromJSON(float32(), "[1, 2, 3, 4, 5, 6, 1, 1]", shape, f_strides);

  CheckDLTensor(f_tensor, float32(), DLDataTypeCode::kDLFloat, shape, f_dlpack_strides);
}

}  // namespace arrow::dlpack
