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

#include <array>
#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/device.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"

namespace arrow::dlpack {

extern const DLPackVersion VERSION = {
    .major = DLPACK_MAJOR_VERSION,
    .minor = DLPACK_MINOR_VERSION,
};

namespace {

/***************
 *  Producers  *
 ***************/

Result<DLDataType> GetDLDataType(const DataType& type) {
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
    return Status::TypeError(
        "DataType is not compatible with DLPack spec: ", type.ToString(),
        ", try converting to a Tensor for multi dimensional data support");
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
  /// Total number of values, i.e. the product of the shape.
  int64_t size;
  int32_t ndim;
  Vec strides;
  Vec shape;
  DLDevice device;
  DLDataType dtype;
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
  if (p.size == 0) {
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
    ctx->tensor.version = VERSION;
    ctx->tensor.flags = p.flags;
  }

  ctx->tensor.manager_ctx = ctx.get();
  ctx->tensor.deleter = [](DT* self) {
    delete reinterpret_cast<Ctx*>(self->manager_ctx);
  };
  return &ctx.release()->tensor;
}

template <typename DT>
Result<DT*> ExportArrayImpl(const std::shared_ptr<Array>& arr, bool copy) {
  if (arr->null_count() > 0) {
    return Status::TypeError("Can only use DLPack on arrays with no nulls.");
  }
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(arr));

  // Define the DLDataType struct, or fail if the type is not supported.
  const auto& type = *arr->type();
  ARROW_ASSIGN_OR_RAISE(auto dtype, GetDLDataType(type));

  auto params = ExportBufferParams<std::array<int64_t, 1>>{
      .size = arr->length(),
      .ndim = 1,
      .strides = {1},
      .shape = {arr->length()},
      .device = device,
      .dtype = dtype,
  };

  const auto& data = *arr->data();
  if (copy) {
    // We copy the buffer slice instead of using Array copy functions to avoid copying
    // unused values outside of offset/length (e.g. with Slice).
    const auto start = data.offset * type.byte_width();
    const auto nbytes = data.length * type.byte_width();
    ARROW_ASSIGN_OR_RAISE(params.buffer, data.buffers[1]->CopySlice(start, nbytes));
    // Since we make a copy only for the consumer, we do not need to mark it readonly.
    params.flags = DLPACK_FLAG_BITMASK_IS_COPIED;
  } else {
    // Shared buffer with Arrow Array. Arrays are readonly once constructed.
    params.buffer = data.buffers[1];
    params.buffer_offset = data.offset * type.byte_width();
    params.flags = DLPACK_FLAG_BITMASK_READ_ONLY;
  }

  return ExportBuffer<DT>(std::move(params));
}

template <typename T>
Result<DLDevice> ExportDeviceImpl(const std::shared_ptr<T>& a) {
  // ArrayData reports the device of its buffers and children
  if (a->data()->device_type() == DeviceAllocationType::kCPU) {
    return {{.device_type = DLDeviceType::kDLCPU, .device_id = 0}};
  } else {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }
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
  return ExportDeviceImpl(arr);
}

namespace {

template <typename DT>
Result<DT*> ExportTensorImpl(const std::shared_ptr<Tensor>& t, bool copy) {
  // Define DLDevice struct
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(t));

  // Define the DLDataType struct
  const auto& type = *t->type();
  ARROW_ASSIGN_OR_RAISE(auto dtype, GetDLDataType(type));

  // Compute strides
  std::vector<int64_t> strides = {};
  strides.reserve(t->ndim());
  const auto byte_width = type.byte_width();
  for (auto i : t->strides()) {
    strides.emplace_back(i / byte_width);
  }

  auto params = ExportBufferParams<std::vector<int64_t>>{
      .size = t->size(),
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
  return ExportDeviceImpl(t);
}

/***************
 *  Consumers  *
 ***************/

namespace {

class CppDLTensor {
 public:
  using value_type = DLManagedTensorVersioned;
  using pointer_type = value_type*;

  static Result<CppDLTensor> TakeOwnership(pointer_type ptr) {
    if (ARROW_PREDICT_FALSE(ptr == nullptr)) {
      return Status::Invalid("Received null pointer.");
    }
    // Create the wrapper before checking the version as the spec mandates that the
    // deleter MUST be called on version major mismatch.
    auto out = CppDLTensor(ptr);
    if (ARROW_PREDICT_FALSE(out.ptr_->version.major != VERSION.major)) {
      return Status::Invalid("Unsupported DLPack major version ", out.ptr_->version.major,
                             ", expected ", VERSION.major);
    }
    if (ARROW_PREDICT_FALSE(out.tensor().ndim < 0)) {
      return Status::Invalid("Invalid DLPack tensor: ndim must be >= 0");
    }
    if (ARROW_PREDICT_FALSE(out.tensor().ndim != 0 && out.tensor().shape == nullptr)) {
      return Status::Invalid(
          "Invalid DLPack tensor: shape must be non-null when ndim != 0");
    }
    if (ARROW_PREDICT_FALSE(out.tensor().ndim != 0 && out.tensor().strides == nullptr)) {
      return Status::Invalid(
          "Invalid DLPack tensor: strides must be non-null when ndim != 0");
    }
    return out;
  }

  const DLTensor& tensor() const { return ptr_->dl_tensor; }

  int64_t ndim() const {
    DCHECK_GE(tensor().ndim, 0);
    return tensor().ndim;
  }

  template <typename T>
  T* data_as() {
    return static_cast<T*>(tensor().data);
  }

  std::span<const int64_t> shape() const {
    return {tensor().shape, static_cast<std::size_t>(ndim())};
  }

  std::span<const int64_t> strides() const {
    return {tensor().strides, static_cast<std::size_t>(ndim())};
  }

  bool flag_is_set(uint8_t bits) const { return (ptr_->flags & bits) == bits; }

  bool is_readonly() const { return flag_is_set(DLPACK_FLAG_BITMASK_READ_ONLY); }

  int32_t byte_width() const { return tensor().dtype.bits / 8; }

 private:
  struct Deleter {
    void operator()(pointer_type ptr) {
      // Null is valid in DLPack spec
      if (auto del = ptr->deleter) {
        del(ptr);
      }
    }
  };

  /// Make a safe wrapper that will delete the resource in case of exception.
  std::unique_ptr<value_type, Deleter> ptr_;

  explicit CppDLTensor(pointer_type ptr) : ptr_(ptr) {}
};

Result<std::shared_ptr<FixedWidthType>> DataTypeFromDLPack(DLDataType dtype) {
  if (dtype.lanes != 1) {
    return Status::TypeError("Only type with one lane are supported.");
  }

  auto constexpr as_fw = [](auto dt) {
    return std::static_pointer_cast<FixedWidthType>(std::move(dt));
  };

  switch (dtype.code) {
    case kDLInt: {
      switch (dtype.bits) {
        case 8:
          return as_fw(int8());
        case 16:
          return as_fw(int16());
        case 32:
          return as_fw(int32());
        case 64:
          return as_fw(int64());
        default:
          return Status::Invalid("unsupported integer bit width ",
                                 static_cast<int>(dtype.bits));
      }
    }
    case kDLUInt: {
      switch (dtype.bits) {
        case 8:
          return as_fw(uint8());
        case 16:
          return as_fw(uint16());
        case 32:
          return as_fw(uint32());
        case 64:
          return as_fw(uint64());
        default:
          return Status::Invalid("unsupported unsigned integer bit width ",
                                 static_cast<int>(dtype.bits));
      }
    }
    case kDLFloat: {
      switch (dtype.bits) {
        case 16:
          return as_fw(float16());
        case 32:
          return as_fw(float32());
        case 64:
          return as_fw(float64());
        default:
          return Status::Invalid("unsupported float bit width ",
                                 static_cast<int>(dtype.bits));
      }
    }
    default: {
      return Status::Invalid("unsupported DLPack type ", static_cast<int>(dtype.code));
    }
  }
}

Result<std::vector<int64_t>> StridesInBytes(std::span<const int64_t> strides,
                                            int64_t byte_width) {
  std::vector<int64_t> out{};
  out.reserve(strides.size());
  for (const auto& s : strides) {
    int64_t stride_bytes = 0;
    if (ARROW_PREDICT_FALSE(
            internal::MultiplyWithOverflow(s, byte_width, &stride_bytes))) {
      return Status::Invalid("Overflow computing DLPack tensor stride in bytes.");
    }
    out.push_back(stride_bytes);
  }
  return out;
}

Result<std::shared_ptr<Buffer>> ImportBuffer(CppDLTensor&& dl, bool copy) {
  // DLPack strides are in number of elements, so is the size we compute from them.
  ARROW_ASSIGN_OR_RAISE(const auto nelements,
                        internal::ComputeTensorSize(dl.shape(), dl.strides(), 1));
  int64_t nbytes = 0;
  if (ARROW_PREDICT_FALSE(internal::MultiplyWithOverflow(
          nelements, static_cast<int64_t>(dl.byte_width()), &nbytes))) {
    return Status::Invalid("Overflow computing DLPack tensor size in bytes.");
  }
  // DLPack mandates a null data pointer when the tensor holds no element, so there is
  // neither anything to share nor to copy.
  uint8_t* data =
      (nbytes == 0) ? nullptr : dl.data_as<uint8_t>() + dl.tensor().byte_offset;

  std::shared_ptr<Buffer> buffer = nullptr;
  if (nbytes == 0) {
    // DLPack data pointer may be null on empty tensors
    buffer = std::make_shared<Buffer>(data, nbytes);
  } else if (copy) {
    ARROW_ASSIGN_OR_RAISE(buffer, MutableBuffer::CopyNonOwned(
                                      {data, nbytes}, default_cpu_memory_manager()));
  } else {
    const bool readonly = dl.is_readonly();
    // Trick to keep DLPack data alive taken from `Buffer::FromVector`.
    auto deleter = [dl = std::move(dl)](auto* buffer) { delete buffer; };
    if (readonly) {
      buffer = {new Buffer{data, nbytes}, std::move(deleter)};
    } else {
      buffer = std::shared_ptr<MutableBuffer>{
          new MutableBuffer{data, nbytes},
          std::move(deleter),
      };
    }
  }

  return buffer;
}

}  // namespace

Result<std::shared_ptr<Array>> ImportArrayVersioned(DLManagedTensorVersioned* unmanaged,
                                                    bool copy) {
  ARROW_ASSIGN_OR_RAISE(auto dl, CppDLTensor::TakeOwnership(unmanaged));

  if (dl.tensor().device.device_type != kDLCPU) {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }

  if (dl.ndim() != 1 || dl.strides().front() != 1) {
    return Status::Invalid(
        "Only contiguous one dimensional tensor can be imported as arrays."
        " Try importing to Tensor first.");
  }

  ARROW_ASSIGN_OR_RAISE(auto type, DataTypeFromDLPack(dl.tensor().dtype));
  const auto nelements = dl.shape().front();
  ARROW_ASSIGN_OR_RAISE(auto buffer, ImportBuffer(std::move(dl), copy));
  auto data = ArrayData::Make(type, nelements, {nullptr, std::move(buffer)});
  return MakeArray(std::move(data));
}

Result<std::shared_ptr<Tensor>> ImportTensorVersioned(DLManagedTensorVersioned* unmanaged,
                                                      bool copy) {
  ARROW_ASSIGN_OR_RAISE(auto dl, CppDLTensor::TakeOwnership(unmanaged));

  if (dl.tensor().device.device_type != kDLCPU) {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }

  ARROW_ASSIGN_OR_RAISE(auto type, DataTypeFromDLPack(dl.tensor().dtype));
  auto shape = std::vector<int64_t>(dl.shape().begin(), dl.shape().end());
  auto strides = std::vector<int64_t>(dl.strides().begin(), dl.strides().end());
  ARROW_ASSIGN_OR_RAISE(auto buffer, ImportBuffer(std::move(dl), copy));
  const auto byte_width = type->byte_width();
  ARROW_ASSIGN_OR_RAISE(auto strides_bytes, StridesInBytes(strides, byte_width));

  return Tensor::Make(std::move(type), std::move(buffer), std::move(shape),
                      std::move(strides_bytes));
}

}  // namespace arrow::dlpack
