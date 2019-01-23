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

#ifndef ARROW_COMPUTE_KERNELS_MONOID_IMPL_H
#define ARROW_COMPUTE_KERNELS_MONOID_IMPL_H

#include <algorithm>
#include <functional>
#include <mutex>

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregation.h"
#include "arrow/compute/kernels/monoid.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

template <typename MonoidType>
class IdentityVisitor {
 public:
  using ValueType = typename MonoidType::ValueType;

  Status VisitValue(const ValueType& value) {
    monoid_ += MonoidType(value);
    return Status::OK();
  }

  Status VisitNull() { return Status::OK(); }

  MonoidType Value() const { return monoid_; }

 private:
  MonoidType monoid_;
};

template <typename NumericType, typename MonoidType,
          typename MonoidVisitor = IdentityVisitor<MonoidType>>
class MonoidAggregateState : public AggregateState {
 public:
  Status Consume(FunctionContext* ctx, const Array& input) final {
    auto data = input.data();
    MonoidVisitor visitor;

    RETURN_NOT_OK(ArrayDataVisitor<NumericType>::Visit(*data, &visitor));

    {
      // Merging the state must be protected by a mutex for concurrent access.
      // The contention should be low assuming that the majority of time is
      // spent in the preceding `Visit` call.
      std::lock_guard<std::mutex> guard(monoid_mutex_);
      monoid_ += visitor.Value();
    }

    return Status::OK();
  };

  Status Finalize(FunctionContext* ctx, Datum* out) final {
    *out = Datum(Scalar(monoid_.value()));
    return Status::OK();
  }

 private:
  std::mutex monoid_mutex_;
  MonoidType monoid_;
};

#define GET_MONOID_KERNEL_CASE(T, M)                                                 \
  case T::type_id:                                                                   \
    kernel =                                                                         \
        std::unique_ptr<AggregateUnaryKernel>(new AggregateUnaryKernel(new M<T>())); \
    break

#define DEFINE_GET_MONOID_KERNEL(MonoidStateType)                     \
  static Status GetMonoidAggregateKernel(                             \
      FunctionContext* ctx, const DataType& type,                     \
      std::unique_ptr<AggregateUnaryKernel>& kernel) {                \
    switch (type.id()) {                                              \
      GET_MONOID_KERNEL_CASE(UInt8Type, MonoidStateType);             \
      GET_MONOID_KERNEL_CASE(Int8Type, MonoidStateType);              \
      GET_MONOID_KERNEL_CASE(UInt16Type, MonoidStateType);            \
      GET_MONOID_KERNEL_CASE(Int16Type, MonoidStateType);             \
      GET_MONOID_KERNEL_CASE(UInt32Type, MonoidStateType);            \
      GET_MONOID_KERNEL_CASE(Int32Type, MonoidStateType);             \
      GET_MONOID_KERNEL_CASE(UInt64Type, MonoidStateType);            \
      GET_MONOID_KERNEL_CASE(Int64Type, MonoidStateType);             \
      GET_MONOID_KERNEL_CASE(FloatType, MonoidStateType);             \
      GET_MONOID_KERNEL_CASE(DoubleType, MonoidStateType);            \
      default:                                                        \
        return Status::Invalid("Unsupported type ", type.ToString()); \
    }                                                                 \
                                                                      \
    return Status::OK();                                              \
  }

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_MONOID_IMPL_H
