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

#include <functional>
#include <memory>
#include <numeric>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/buffer.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/device.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/small_vector.h"

namespace arrow::dlpack {

namespace {

Result<DLDataType> GetLeafDLDataType(const DataType& type) {
  auto dtype = DLDataType{};
  dtype.lanes = 1;
  dtype.bits = type.bit_width();
  if (is_signed_integer(type.id())) {
    dtype.code = DLDataTypeCode::kDLInt;
  } else if (is_unsigned_integer(type.id())) {
    dtype.code = DLDataTypeCode::kDLUInt;
  } else if (is_floating(type.id())) {
    dtype.code = DLDataTypeCode::kDLFloat;
  } else if (type.id() == Type::BOOL) {
    // DLPack supports byte-packed boolean values
    return Status::TypeError("Bit-packed boolean data type not supported by DLPack.");
  } else {
    return Status::TypeError("DataType is not compatible with DLPack spec: ",
                             type.ToString());
  }
  return {dtype};
}

template <typename DT, typename Vec>
struct ManagerCtx {
  std::shared_ptr<Buffer> buffer;
  /// DLPack managed tensor structure.
  /// Legacy `DLManagedTensor` or newer `DLManagedTensorVersioned`.
  DT tensor;
  Vec strides;
  Vec shape;
};

template <typename Vec>
struct ExportBufferParams {
  std::shared_ptr<Buffer> buffer = nullptr;
  /// Data offset in the buffer in bytes.
  int64_t buffer_offset = 0;
  /// Total number of bytes to read in the buffer.
  int64_t buffer_size = 0;
  int32_t ndim = 0;
  Vec strides = {};
  Vec shape = {};
  DLDevice device = {};
  DLDataType dtype = {};
  uint64_t flags = 0;
};

template <typename DT, typename Vec>
DT* ExportBuffer(ExportBufferParams<Vec>&& p) {
  // Create ManagerCtx that will serve as the owner of the DLManagedTensor
  using Ctx = ManagerCtx<DT, Vec>;
  auto ctx = std::make_unique<Ctx>();

  // Assign the Array data, shape, and strides into the context.
  ctx->buffer = std::move(p.buffer);
  ctx->shape = std::move(p.shape);
  ctx->strides = std::move(p.strides);

  // Define the data pointer to the DLTensor
  // If array is of length 0, data pointer should be NULL
  if (p.buffer_size == 0) {
    ctx->tensor.dl_tensor.data = nullptr;
  } else {
    ctx->tensor.dl_tensor.data =
        const_cast<uint8_t*>(ctx->buffer->data() + p.buffer_offset);
  }

  ctx->tensor.dl_tensor.device = p.device;
  ctx->tensor.dl_tensor.dtype = p.dtype;
  ctx->tensor.dl_tensor.ndim = p.ndim;
  ctx->tensor.dl_tensor.shape = ctx->shape.data();
  ctx->tensor.dl_tensor.byte_offset = 0;
  // Strides must be non-null when ndim > 0
  ctx->tensor.dl_tensor.strides = ctx->strides.data();
  if constexpr (std::is_same_v<DT, DLManagedTensorVersioned>) {
    ctx->tensor.version = {.major = DLPACK_MAJOR_VERSION, .minor = DLPACK_MINOR_VERSION};
    ctx->tensor.flags = p.flags;
  }

  ctx->tensor.manager_ctx = ctx.get();
  ctx->tensor.deleter = [](DT* self) {
    delete reinterpret_cast<Ctx*>(self->manager_ctx);
  };
  return &ctx.release()->tensor;
}

template <typename Vec>
Vec RowMajorStrides(const Vec& shape) {
  auto out = Vec(shape.size());
  std::exclusive_scan(shape.crbegin(), shape.crend(), out.rbegin(), 1,
                      std::multiplies<>{});
  return out;
}

template <typename DT>
Result<DT*> ExportArrayImpl(const std::shared_ptr<Array>& arr, bool copy) {
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(arr));

  using Vec = internal::SmallVector<int64_t, 2>;

  const auto* data = arr->data().get();  // lifetime of data is bound by arr
  const auto* type = arr->type().get();  // lifetime of data is bound by arr
  auto params = ExportBufferParams<Vec>{
      .buffer_offset = data->offset,
      .buffer_size = data->length,
      .ndim = 1,
      .shape = {data->length},
      .device = device,
  };
  if (data->GetNullCount() > 0) {
    return Status::TypeError("Can only use DLPack on arrays with no nulls.");
  }

  // Iterate over nested fixed length container types.
  // Each nested container increase the DLPack tensor dimension.
  // Nulls are not supported by DLPack, at any nesting level.
  while (type->id() == Type::FIXED_SIZE_LIST) {
    const auto* fsl = internal::checked_cast<const FixedSizeListType*>(type);
    type = fsl->value_type().get();
    data = data->child_data.front().get();
    if (data->GetNullCount() > 0) {
      return Status::TypeError("Can only use DLPack on arrays with no nulls.");
    }

    params.ndim++;
    params.buffer_offset = params.buffer_offset * fsl->list_size() + data->offset;
    params.buffer_size *= fsl->list_size();
    params.shape.push_back(fsl->list_size());
  }

  // Get the DLDataType struct of the type leaf, or fail if it is not supported.
  ARROW_ASSIGN_OR_RAISE(params.dtype, GetLeafDLDataType(*type));
  // Use byte domain indexing.
  params.buffer_offset *= type->byte_width();
  params.buffer_size *= type->byte_width();
  // Compute strides as row major.
  params.strides = RowMajorStrides(params.shape);

  if (copy) {
    // We copy the buffer slice instead of using Array copy functions to avoid copying
    // unused values outside of offset/length (e.g. with Slice).
    const auto start = std::exchange(params.buffer_offset, 0);
    const auto nbytes = params.buffer_size;
    ARROW_ASSIGN_OR_RAISE(params.buffer, data->buffers[1]->CopySlice(start, nbytes));
    // Since we make a copy only for the consumer, we do not need to mark it readonly.
    params.flags = DLPACK_FLAG_BITMASK_IS_COPIED;
  } else {
    // Shared buffer with Arrow Array. Arrays are readonly once constructed.
    params.buffer = data->buffers[1];
    params.flags = DLPACK_FLAG_BITMASK_READ_ONLY;
  }

  return ExportBuffer<DT>(std::move(params));
}

}  // namespace

Result<DLManagedTensor*> ExportArray(const std::shared_ptr<Array>& arr) {
  return ExportArrayImpl<DLManagedTensor>(arr, /* copy= */ false);
}

Result<DLManagedTensorVersioned*> ExportArrayVersioned(const std::shared_ptr<Array>& arr,
                                                       bool copy) {
  return ExportArrayImpl<DLManagedTensorVersioned>(arr, copy);
}

Result<DLDevice> ExportDevice(const std::shared_ptr<Array>& arr) {
  // ArrayData reports the device of its buffers and children
  if (arr->data()->device_type() == DeviceAllocationType::kCPU) {
    return {{.device_type = DLDeviceType::kDLCPU, .device_id = 0}};
  } else {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }
}

namespace {

template <typename DT>
Result<DT*> ExportTensorImpl(const std::shared_ptr<Tensor>& t, bool copy) {
  // Define DLDevice struct
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(t));

  // Define the DLDataType struct
  const auto& type = *t->type();
  ARROW_ASSIGN_OR_RAISE(auto dtype, GetLeafDLDataType(type));

  // Compute strides
  std::vector<int64_t> strides = {};
  strides.reserve(t->ndim());
  const auto byte_width = type.byte_width();
  for (auto i : t->strides()) {
    strides.emplace_back(i / byte_width);
  }

  auto params = ExportBufferParams<std::vector<int64_t>>{
      .buffer_size = t->size(),
      .ndim = t->ndim(),
      .strides = std::move(strides),
      .shape = t->shape(),
      .device = device,
      .dtype = dtype,
  };

  if (copy) {
    ARROW_ASSIGN_OR_RAISE(params.buffer, MemoryManager::CopyBuffer(
                                             t->data(), default_cpu_memory_manager()));
    // Since we make a copy only for the consumer, we do not need to mark it readonly.
    params.flags = DLPACK_FLAG_BITMASK_IS_COPIED;
  } else {
    // Shared buffer with Arrow Tensor.
    params.buffer = t->data();
    params.flags = t->is_mutable() ? 0 : DLPACK_FLAG_BITMASK_READ_ONLY;
  }

  return ExportBuffer<DT>(std::move(params));
}

}  // namespace

Result<DLManagedTensor*> ExportTensor(const std::shared_ptr<Tensor>& t) {
  // Legacy DLPack is not implemented initially for immutable tensor.
  // We prefer users to over to the non-legacy DLPack rather than adding new behaviour.
  if (!t->is_mutable()) {
    return Status::NotImplemented(
        "Legacy DLPack support is not implemented for immutable tensors."
        " Please move to the DLPack version >=1.0");
  }
  return ExportTensorImpl<DLManagedTensor>(t, /* copy= */ false);
}

Result<DLManagedTensorVersioned*> ExportTensorVersioned(const std::shared_ptr<Tensor>& t,
                                                        bool copy) {
  return ExportTensorImpl<DLManagedTensorVersioned>(t, copy);
}

Result<DLDevice> ExportDevice(const std::shared_ptr<Tensor>& t) {
  // Define DLDevice struct
  if (t->data()->device_type() == DeviceAllocationType::kCPU) {
    return {{.device_type = DLDeviceType::kDLCPU, .device_id = 0}};
  } else {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }
}

}  // namespace arrow::dlpack
