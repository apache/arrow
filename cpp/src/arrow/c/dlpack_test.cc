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
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow::dlpack {

// using ExportArray = arrow::dlpack::ExportArray;

class TestExportArray : public ::testing::Test {
 public:
  void SetUp() {}
};

auto check_dlptensor = [](const std::shared_ptr<Array>& arr,
                          std::shared_ptr<DataType> arrow_type,
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
};

TEST_F(TestExportArray, TestSupportedArray) {
  random::RandomArrayGenerator gen(0);

  std::vector<std::shared_ptr<DataType>> arrow_types = {
      int8(),  uint8(),  int16(),   uint16(),  int32(),   uint32(),
      int64(), uint64(), float16(), float32(), float64(),
  };

  std::vector<DLDataTypeCode> dlpack_types = {
      DLDataTypeCode::kDLInt,   DLDataTypeCode::kDLUInt,  DLDataTypeCode::kDLInt,
      DLDataTypeCode::kDLUInt,  DLDataTypeCode::kDLInt,   DLDataTypeCode::kDLUInt,
      DLDataTypeCode::kDLInt,   DLDataTypeCode::kDLUInt,  DLDataTypeCode::kDLFloat,
      DLDataTypeCode::kDLFloat, DLDataTypeCode::kDLFloat,
  };

  for (int64_t i = 0; i < 11; ++i) {
    const std::shared_ptr<Array> array = gen.ArrayOf(arrow_types[i], 10, 0);
    check_dlptensor(array, arrow_types[i], dlpack_types[i], 10);
    ASSERT_OK_AND_ASSIGN(auto sliced_1, array->SliceSafe(1, 5));
    check_dlptensor(sliced_1, arrow_types[i], dlpack_types[i], 5);
    ASSERT_OK_AND_ASSIGN(auto sliced_2, array->SliceSafe(0, 5));
    check_dlptensor(sliced_2, arrow_types[i], dlpack_types[i], 5);
    ASSERT_OK_AND_ASSIGN(auto sliced_3, array->SliceSafe(3));
    check_dlptensor(sliced_3, arrow_types[i], dlpack_types[i], 7);
  }
}

TEST_F(TestExportArray, TestUnSupportedArray) {
  random::RandomArrayGenerator gen(0);

  const std::shared_ptr<Array> array_with_null = gen.Int8(10, 1, 100, 1);
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only use DLPack on arrays with no nulls.",
                             arrow::dlpack::ExportArray(array_with_null));

  const std::shared_ptr<Array> array_string = gen.String(10, 0, 10, 0);
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_string->type()->ToString(),
                             arrow::dlpack::ExportArray(array_string));

  const std::shared_ptr<Array> array_boolean = gen.Boolean(10, 0.5, 0);
  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: Bit-packed boolean data type not supported by DLPack.",
      arrow::dlpack::ExportArray(array_boolean));

  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: Bit-packed boolean data type not supported by DLPack.",
      arrow::dlpack::ExportDevice(array_boolean));
}

}  // namespace arrow::dlpack
