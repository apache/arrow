// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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

#include <algorithm>
#include <limits>
#include <utility>

#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

struct MinMaxState : public KernelState {
  virtual void Consume(KernelContext* ctx, const ExecBatch& batch) = 0;
};

template <typename ArrowType, typename Enable = void>
struct MinMaxState {};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_integer<ArrowType>> : public KernelState {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
    this->min = std::min(this->min, rhs.min);
    this->max = std::max(this->max, rhs.max);
    return *this;
  }

  void MergeOne(T value) {
    this->min = std::min(this->min, value);
    this->max = std::max(this->max, value);
  }

  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::min();
  bool has_nulls = false;
};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_floating_point<ArrowType>> : public KernelState {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
    this->min = std::fmin(this->min, rhs.min);
    this->max = std::fmax(this->max, rhs.max);
    return *this;
  }

  void MergeOne(T value) {
    this->min = std::fmin(this->min, value);
    this->max = std::fmax(this->max, value);
  }

  T min = std::numeric_limits<T>::infinity();
  T max = -std::numeric_limits<T>::infinity();
  bool has_nulls = false;
};

struct StateVisitor {
  std::unique_ptr<KernelState> result;

  Status Visit(const DataType&) { return Status::NotImplemented("NYI"); }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    using StateType = MinMaxState<Type>;
    result.reset(new StateType());
  }
};

std::unique_ptr<KernelState> MinMaxInit(KernelContext* ctx, const Kernel& kernel,
                                        const FunctionOptions&) {
  StateVisitor state_init;
  ctx->SetStatus(VisitTypeInline(/*type*/, &state_init));
}

void MinMaxConsume(KernelContext* ctx, const ExecBatch& batch) {
  checked_cast<MinMaxState*>(ctx->state())->Consume(batch);

  Status Merge(const StateType& src, StateType* dst) const override {
    *dst += src;
    return Status::OK();
  }

  Status Finalize(const StateType& src, Datum* output) const override {
    using ScalarType = typename TypeTraits<ArrowType>::ScalarType;
    if (src.has_nulls && options_.null_handling == MinMaxOptions::OUTPUT_NULL) {
      *output = Datum(
          {Datum(std::make_shared<ScalarType>()), Datum(std::make_shared<ScalarType>())});
    } else {
      *output = Datum({Datum(src.min), Datum(src.max)});
    }

    return Status::OK();
  }

 private:
  MinMaxOptions options_;
}

// MinMax implemented for
//
// * Number types
//
// Outputs struct<min: T, max: T>

}  // namespace compute
}  // namespace arrow
