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

#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/device.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow::dlpack {

namespace {

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
      return Status::TypeError("DataType is not compatible with DLPack spec: ",
                               type.ToString());
  }
}

template <typename DT>
struct ManagerCtx {
  std::shared_ptr<ArrayData> array;
  DT tensor;
  int64_t strides = 1;
};

template <typename DT>
Result<DT*> ExportArrayImpl(const std::shared_ptr<Array>& arr) {
  // Define DLDevice struct and check if array type is supported
  // by the DLPack protocol at the same time. Raise TypeError if not.
  // Supported data types: int, uint, float with no validity buffer.
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(arr));

  // Define the DLDataType struct
  const DataType& type = *arr->type();
  ARROW_ASSIGN_OR_RAISE(auto dlpack_type, GetDLDataType(type));

  // Create ManagerCtx that will serve as the owner of the DLManagedTensor
  auto ctx = std::make_unique<ManagerCtx<DT>>();

  // Assign the Array data into the context
  ctx->array = arr->data();
  auto& data = ctx->array;

  // Define the data pointer to the DLTensor
  // If array is of length 0, data pointer should be NULL
  if (arr->length() == 0) {
    ctx->tensor.dl_tensor.data = nullptr;
  } else {
    const auto data_offset = data->offset * type.byte_width();
    ctx->tensor.dl_tensor.data =
        const_cast<uint8_t*>(data->buffers[1]->data() + data_offset);
  }

  ctx->tensor.dl_tensor.device = device;
  ctx->tensor.dl_tensor.ndim = 1;
  ctx->tensor.dl_tensor.dtype = dlpack_type;
  ctx->tensor.dl_tensor.shape = const_cast<int64_t*>(&data->length);
  ctx->tensor.dl_tensor.byte_offset = 0;
  // Strides must be non-null when ndim > 0
  ctx->tensor.dl_tensor.strides = &ctx->strides;
  if constexpr (std::is_same_v<DT, DLManagedTensorVersioned>) {
    ctx->tensor.version = {.major = DLPACK_MAJOR_VERSION, .minor = DLPACK_MINOR_VERSION};
    // Arrow contract is that array data is immutable once constructed
    ctx->tensor.flags = DLPACK_FLAG_BITMASK_READ_ONLY;
  }

  ctx->tensor.manager_ctx = ctx.get();
  ctx->tensor.deleter = [](DT* self) {
    delete reinterpret_cast<ManagerCtx<DT>*>(self->manager_ctx);
  };
  return &ctx.release()->tensor;
}

}  // namespace

Result<DLManagedTensor*> ExportArray(const std::shared_ptr<Array>& arr) {
  return ExportArrayImpl<DLManagedTensor>(arr);
}

Result<DLManagedTensorVersioned*> ExportArrayVersioned(
    const std::shared_ptr<Array>& arr) {
  return ExportArrayImpl<DLManagedTensorVersioned>(arr);
}

Result<DLDevice> ExportDevice(const std::shared_ptr<Array>& arr) {
  // Check if array is supported by the DLPack protocol.
  if (arr->null_count() > 0) {
    return Status::TypeError("Can only use DLPack on arrays with no nulls.");
  }
  const DataType& type = *arr->type();
  if (type.id() == Type::BOOL) {
    return Status::TypeError("Bit-packed boolean data type not supported by DLPack.");
  }
  if (!is_integer(type.id()) && !is_floating(type.id())) {
    return Status::TypeError("DataType is not compatible with DLPack spec: ",
                             type.ToString());
  }

  // Define DLDevice struct
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

namespace {

template <typename DT>
struct TensorManagerCtx {
  std::shared_ptr<Tensor> t;
  std::vector<int64_t> strides;
  std::vector<int64_t> shape;
  DT tensor;
};

template <typename DT>
Result<DT*> ExportTensorImpl(const std::shared_ptr<Tensor>& t) {
  // Define the DLDataType struct
  const DataType& type = *t->type();
  ARROW_ASSIGN_OR_RAISE(auto dlpack_type, GetDLDataType(type));

  // Define DLDevice struct
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(t));

  // Create TensorManagerCtx that will serve as the owner of the DLManagedTensor
  auto ctx = std::make_unique<TensorManagerCtx<DT>>();

  // Define the data pointer to the DLTensor
  // If tensor is of length 0, data pointer should be NULL
  if (t->size() == 0) {
    ctx->tensor.dl_tensor.data = nullptr;
  } else {
    ctx->tensor.dl_tensor.data = const_cast<uint8_t*>(t->raw_data());
  }

  ctx->tensor.dl_tensor.device = device;
  ctx->tensor.dl_tensor.ndim = t->ndim();
  ctx->tensor.dl_tensor.dtype = dlpack_type;
  ctx->tensor.dl_tensor.byte_offset = 0;

  std::vector<int64_t>& shape_arr = ctx->shape;
  shape_arr.reserve(t->ndim());
  for (auto i : t->shape()) {
    shape_arr.emplace_back(i);
  }
  ctx->tensor.dl_tensor.shape = shape_arr.data();

  std::vector<int64_t>& strides_arr = ctx->strides;
  strides_arr.reserve(t->ndim());
  const auto byte_width = t->type()->byte_width();
  for (auto i : t->strides()) {
    strides_arr.emplace_back(i / byte_width);
  }
  ctx->tensor.dl_tensor.strides = strides_arr.data();
  if constexpr (std::is_same_v<DT, DLManagedTensorVersioned>) {
    ctx->tensor.version = {.major = DLPACK_MAJOR_VERSION, .minor = DLPACK_MINOR_VERSION};
    ctx->tensor.flags = t->is_mutable() ? 0 : DLPACK_FLAG_BITMASK_READ_ONLY;
  }

  ctx->t = std::move(t);
  ctx->tensor.manager_ctx = ctx.get();
  ctx->tensor.deleter = [](DT* self) {
    delete reinterpret_cast<TensorManagerCtx<DT>*>(self->manager_ctx);
  };
  return &ctx.release()->tensor;
}

}  // namespace

Result<DLManagedTensor*> ExportTensor(const std::shared_ptr<Tensor>& t) {
  return ExportTensorImpl<DLManagedTensor>(t);
}

Result<DLManagedTensorVersioned*> ExportTensorVersioned(
    const std::shared_ptr<Tensor>& t) {
  return ExportTensorImpl<DLManagedTensorVersioned>(t);
}

Result<DLDevice> ExportDevice(const std::shared_ptr<Tensor>& t) {
  // Define DLDevice struct
  DLDevice device;
  if (t->data()->device_type() == DeviceAllocationType::kCPU) {
    device.device_id = 0;
    device.device_type = DLDeviceType::kDLCPU;
    return device;
  } else {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }
}

}  // namespace arrow::dlpack
