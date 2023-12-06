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

#include "arrow/c/dlpack.h"

#include "arrow/array/array_base.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/device.h"
#include "arrow/type.h"

namespace arrow::dlpack {

Result<DLDataType> GetDLDataType(const DataType& type) {
  DLDataType dtype;
  dtype.lanes = 1;
  dtype.bits = type.bit_width();
  switch (type.id()) {
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
      dtype.code = DLDataTypeCode::kDLInt;
      return dtype;
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
      dtype.code = DLDataTypeCode::kDLUInt;
      return dtype;
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      dtype.code = DLDataTypeCode::kDLFloat;
      return dtype;
    case Type::BOOL:
      // DLPack supports byte-packed boolean values
      return Status::TypeError("Bit-packed boolean data type not supported by DLPack.");
    default:
      return Status::TypeError(
          "DataType is not compatible with DLPack spec: ", type.ToString());
  }
}

struct DLMTensorCtx {
  std::shared_ptr<ArrayData> ref;
  std::vector<int64_t> shape;
  DLManagedTensor tensor;
};

static void deleter(DLManagedTensor* arg) {
  delete static_cast<DLMTensorCtx*>(arg->manager_ctx);
}

Result<DLManagedTensor*> ExportArray(const std::shared_ptr<Array>& arr) {
  if (arr->null_count() > 0) {
    return Status::TypeError("Can only use DLPack on arrays with no nulls.");
  }

  // Define the DLDataType struct
  // Supported data types: int, uint, float
  const DataType* arrow_type = arr->type().get();
  ARROW_ASSIGN_OR_RAISE(auto dlpack_type, GetDLDataType(*arrow_type));

  // Create DLMTensorCtx struct with the reference to
  // the data of the array
  std::shared_ptr<ArrayData> array_ref = arr->data();
  DLMTensorCtx* DLMTensor = new DLMTensorCtx;
  DLMTensor->ref = array_ref;

  // Define DLManagedTensor struct defined by
  // DLPack (dlpack_structure.h)
  DLManagedTensor* dlm_tensor = &DLMTensor->tensor;
  dlm_tensor->manager_ctx = DLMTensor;
  dlm_tensor->deleter = &deleter;

  // Define the data pointer to the DLTensor
  // If array is of length 0, data pointer should be NULL
  if (arr->length() == 0) {
    dlm_tensor->dl_tensor.data = NULL;
  } else if (arr->offset() > 0) {
    const auto byte_width = arr->type()->byte_width();
    const auto start = arr->offset() * byte_width;
    ARROW_ASSIGN_OR_RAISE(auto sliced_buffer,
                          SliceBufferSafe(array_ref->buffers[1], start));
    dlm_tensor->dl_tensor.data =
        const_cast<void*>(reinterpret_cast<const void*>(sliced_buffer->address()));
  } else {
    dlm_tensor->dl_tensor.data = const_cast<void*>(
        reinterpret_cast<const void*>(array_ref->buffers[1]->address()));
  }

  // Define DLDevice struct
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(arr))
  dlm_tensor->dl_tensor.device = device;

  dlm_tensor->dl_tensor.ndim = 1;
  dlm_tensor->dl_tensor.dtype = dlpack_type;
  dlm_tensor->dl_tensor.shape = const_cast<int64_t*>(&array_ref->length);
  dlm_tensor->dl_tensor.strides = NULL;
  dlm_tensor->dl_tensor.byte_offset = 0;

  return dlm_tensor;
}

Result<DLDevice> ExportDevice(const std::shared_ptr<Array>& arr) {
  DLDevice device;
  if (arr->data()->buffers[1]->device_type() == DeviceAllocationType::kCPU) {
    device.device_id = 0;
    device.device_type = DLDeviceType::kDLCPU;
    return device;
  } else {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }
}

}  // namespace arrow::dlpack
