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

#include "arrow/dlpack.h"

#include "arrow/array/array_base.h"
#include "arrow/dlpack_structure.h"
#include "arrow/type.h"

namespace arrow {

DLDataType getDLDataType(const Array& arr) {
  DLDataType dtype;
  dtype.lanes = 1;
  dtype.bits = arr.type()->bit_width();
  switch (arr.type()->id()) {
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
      dtype.code = DLDataTypeCode::kDLInt;
      break;
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
      dtype.code = DLDataTypeCode::kDLUInt;
      break;
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      dtype.code = DLDataTypeCode::kDLFloat;
      break;
    case Type::BOOL:
      dtype.code = DLDataTypeCode::kDLBool;
      break;
    default:
      // TODO
      break;
  }
  return dtype;
}

struct DLMTensorCtx {
  std::shared_ptr<ArrayData> ref;
  std::vector<int64_t> shape;
  DLManagedTensor tensor;
};

static void deleter(DLManagedTensor* arg) {
  delete static_cast<DLMTensorCtx*>(arg->manager_ctx);
}

DLManagedTensor* toDLPack(const Array& arr) {
  std::shared_ptr<ArrayData> array_ref = arr.data();
  DLMTensorCtx* DLMTensor = new DLMTensorCtx;
  DLMTensor->ref = array_ref;

  DLManagedTensor* dlm_tensor = &DLMTensor->tensor;
  dlm_tensor->manager_ctx = DLMTensor;
  dlm_tensor->deleter = &deleter;
  dlm_tensor->dl_tensor.data =
      const_cast<void*>(reinterpret_cast<const void*>(array_ref->buffers[1]->address()));

  DLDevice ctx;
  ctx.device_id = 0;
  ctx.device_type = DLDeviceType::kDLCPU;
  dlm_tensor->dl_tensor.device = ctx;

  dlm_tensor->dl_tensor.ndim = 1;
  dlm_tensor->dl_tensor.dtype = getDLDataType(arr);

  std::vector<int64_t>* shape_arr = &DLMTensor->shape;
  shape_arr->resize(1);
  (*shape_arr)[0] = arr.length();
  dlm_tensor->dl_tensor.shape = shape_arr->data();
  dlm_tensor->dl_tensor.strides = NULL;
  dlm_tensor->dl_tensor.byte_offset = 0;

  return dlm_tensor;
}

}  // namespace arrow
