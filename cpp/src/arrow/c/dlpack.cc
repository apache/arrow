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
#include "arrow/c/dlpack_structure.h"
#include "arrow/type.h"

namespace arrow {

namespace dlpack {

Status getDLDataType(const std::shared_ptr<DataType>& type, DLDataType* out) {
  DLDataType dtype;
  dtype.lanes = 1;
  dtype.bits = type->bit_width();
  switch (type->id()) {
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
      dtype.code = DLDataTypeCode::kDLInt;
      *out = dtype;
      return Status::OK();
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
      dtype.code = DLDataTypeCode::kDLUInt;
      *out = dtype;
      return Status::OK();
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      dtype.code = DLDataTypeCode::kDLFloat;
      *out = dtype;
      return Status::OK();
    case Type::BOOL:
      // DLPack supports byte-packed boolean values
      return Status::TypeError("Bit-packed boolean data type not supported by DLPack.");
    default:
      return Status::TypeError(
          "Can only use __dlpack__ on primitive arrays without NullType and Decimal "
          "types.");
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

Status ExportArray(const std::shared_ptr<Array>& arr, DLManagedTensor** out) {
  if (arr->null_bitmap() != NULLPTR) {
    return Status::TypeError(
        "Can only use __dlpack__ on arrays with no validity buffer.");
  }

  // Define the DLDataType struct
  // Supported data types: int, uint, float
  DLDataType arr_type;
  RETURN_NOT_OK(getDLDataType(arr->type(), &arr_type));

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
  } else {
    dlm_tensor->dl_tensor.data = const_cast<void*>(
        reinterpret_cast<const void*>(array_ref->buffers[1]->address()));
  }

  // Define DLDevice struct
  DLDevice ctx;
  ctx.device_id = 0;
  ctx.device_type = DLDeviceType::kDLCPU;
  dlm_tensor->dl_tensor.device = ctx;

  dlm_tensor->dl_tensor.ndim = 1;
  dlm_tensor->dl_tensor.dtype = arr_type;
  std::vector<int64_t>* shape_arr = &DLMTensor->shape;
  shape_arr->resize(1);
  (*shape_arr)[0] = arr->length();
  dlm_tensor->dl_tensor.shape = shape_arr->data();
  dlm_tensor->dl_tensor.strides = NULL;
  dlm_tensor->dl_tensor.byte_offset = 0;

  *out = dlm_tensor;
  return Status::OK();
}

}  // namespace dlpack

}  // namespace arrow
