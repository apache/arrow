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

#include "arrow/compute/kernels/add.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/generated/arithmetic-codegen-internal.h"

namespace arrow {
namespace compute {

size_t GetSize(const std::shared_ptr<DataType>& type);

template <typename TResult, typename TLhs, typename TRhs>
class AddKernelImpl : public AddKernel {
 private:
  std::shared_ptr<DataType> result_type_;
  using ArrowLhsElementType = typename CTypeTraits<TLhs>::ArrowType;
  using ArrowRhsElementType = typename CTypeTraits<TRhs>::ArrowType;
  using ArrowResultElementType = typename CTypeTraits<TResult>::ArrowType;
  using LhsArray = NumericArray<ArrowLhsElementType>;
  using RhsArray = NumericArray<ArrowRhsElementType>;
  using ResultArray = NumericArray<ArrowResultElementType>;

  Status Add(FunctionContext* ctx, const std::shared_ptr<LhsArray>& lhs,
             const std::shared_ptr<RhsArray>& rhs, std::shared_ptr<Array>* result) {
    std::shared_ptr<Buffer> buf;
    size_t size = GetSize(result_type_) / 8;
    RETURN_NOT_OK(AllocateBuffer(ctx->memory_pool(), lhs->length() * size, &buf));
    auto res_data = reinterpret_cast<TResult*>(buf->mutable_data());
    for (int i = 0; i < lhs->length(); i++) {
      res_data[i] = lhs->Value(i) + rhs->Value(i);
    }
    *result = std::make_shared<ResultArray>(lhs->length(), buf);
    return Status::OK();
  }

 public:
  AddKernelImpl(std::shared_ptr<DataType> result_type) : result_type_(result_type) {}

  Status Call(FunctionContext* ctx, const Datum& lhs, const Datum& rhs, Datum* out) {
    if (!lhs.is_array() || !rhs.is_array()) {
      return Status::Invalid("AddKernel expects array values");
    }
    if (lhs.length() != rhs.length()) {
      return Status::Invalid("AddKernel expects arrays with the same length");
    }
    auto lhs_array = lhs.make_array();
    auto rhs_array = rhs.make_array();
    if (lhs_array->null_count() || rhs_array->null_count()) {
      return Status::Invalid("AddKernel expects arrays without nulls");
    }
    std::shared_ptr<Array> result;
    RETURN_NOT_OK(this->Add(ctx, lhs_array, rhs_array, &result));
    *out = result;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return result_type_; }

  virtual Status Add(FunctionContext* ctx, const std::shared_ptr<Array>& lhs,
                     const std::shared_ptr<Array>& rhs, std::shared_ptr<Array>* result) {
    auto lhs_array = std::static_pointer_cast<LhsArray>(lhs);
    auto rhs_array = std::static_pointer_cast<RhsArray>(rhs);
    return Add(ctx, lhs_array, rhs_array, result);
  }
};

std::shared_ptr<DataType> MakeNumericType(bool sign, bool floating, size_t size) {
  if (floating) {
    switch (size) {
      case 16:
        return float16();
      case 32:
        return float32();
      case 64:
        return float64();
      default:
        return NULLPTR;
    }
  }
  if (sign) {
    switch (size) {
      case 8:
        return int8();
      case 16:
        return int16();
      case 32:
        return int32();
      case 64:
        return int64();
      default:
        return NULLPTR;
    }
  }

  switch (size) {
    case 8:
      return uint8();
    case 16:
      return uint16();
    case 32:
      return uint32();
    case 64:
      return uint64();
    default:
      return NULLPTR;
  }
}

bool IsUnsigned(const std::shared_ptr<DataType>& type) {
  return type->Equals(uint8()) || type->Equals(uint16()) || type->Equals(uint32()) ||
         type->Equals(uint64());
}

bool IsSigned(const std::shared_ptr<DataType>& type) {
  return type->Equals(int8()) || type->Equals(int16()) || type->Equals(int32()) ||
         type->Equals(int64());
}

bool IsFloating(const std::shared_ptr<DataType>& type) {
  return type->Equals(float16()) || type->Equals(float32()) || type->Equals(float64());
}

size_t GetSize(const std::shared_ptr<DataType>& type) {
  if (type->Equals(uint8()) || type->Equals(int8())) {
    return 8;
  }
  if (type->Equals(float16()) || type->Equals(int16()) || type->Equals(uint16())) {
    return 16;
  }
  if (type->Equals(float32()) || type->Equals(int32()) || type->Equals(uint32())) {
    return 32;
  }
  if (type->Equals(float64()) || type->Equals(int64()) || type->Equals(uint64())) {
    return 64;
  }
  return 1111;  // any number greater then 64 to make MakeNumericType fail
}

Status AddKernel::Make(const std::shared_ptr<DataType>& lhs_type,
                       const std::shared_ptr<DataType>& rhs_type,
                       std::unique_ptr<AddKernel>* out) {
  auto result_type = MakeNumericType(IsSigned(lhs_type) || IsSigned(rhs_type),
                                     IsFloating(lhs_type) || IsFloating(rhs_type),
                                     std::max(GetSize(lhs_type), GetSize(rhs_type)));
  if (!result_type) {
    return Status::TypeError("AddKernel can not infer appropriate type");
  }
  AddKernel* kernel;
  ARITHMETIC_TYPESWITCH(kernel, AddKernelImpl, result_type, lhs_type, rhs_type)
  if(!kernel) {
    return Status::TypeError("AddKernel can not infer appropriate type");
  }
  out->reset(kernel);
  return Status::OK();
}

Status Add(FunctionContext* ctx, const Array& lhs, const Array& rhs,
           std::shared_ptr<Array>* result) {
  Datum result_datum;
  std::unique_ptr<AddKernel> kernel;
  RETURN_NOT_OK(AddKernel::Make(lhs.type(), rhs.type(), &kernel));
  kernel->Call(ctx, Datum(lhs.data()), Datum(rhs.data()), &result_datum);
  *result = result_datum.make_array();
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow