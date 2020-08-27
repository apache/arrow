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

#include <cmath>
#include <unordered_map>

#include "arrow/compute/kernels/aggregate_basic_internal.h"

namespace arrow {
namespace compute {
namespace aggregate {

namespace {

template <typename ArrowType, typename Enable = void>
struct ModeState {};

template <typename ArrowType>
struct ModeState<ArrowType, enable_if_t<(sizeof(typename ArrowType::c_type) > 1)>> {
  using ThisType = ModeState<ArrowType>;
  using T = typename ArrowType::c_type;

  void MergeFrom(const ThisType& state) {
    for (const auto& value_count : state.value_counts) {
      auto value = value_count.first;
      auto count = value_count.second;
      this->value_counts[value] += count;
    }
    if (is_floating_type<ArrowType>::value) {
      this->nan_count += state.nan_count;
    }
  }

  template <typename ArrowType_ = ArrowType>
  enable_if_t<!is_floating_type<ArrowType_>::value> MergeOne(T value) {
    ++this->value_counts[value];
  }

  template <typename ArrowType_ = ArrowType>
  enable_if_t<is_floating_type<ArrowType_>::value> MergeOne(T value) {
    if (std::isnan(value)) {
      ++this->nan_count;
    } else {
      ++this->value_counts[value];
    }
  }

  std::pair<T, int64_t> Finalize() {
    T mode = std::numeric_limits<T>::min();
    int64_t count = 0;

    for (const auto& value_count : this->value_counts) {
      auto this_value = value_count.first;
      auto this_count = value_count.second;
      if (this_count > count || (this_count == count && this_value < mode)) {
        count = this_count;
        mode = this_value;
      }
    }
    if (is_floating_type<ArrowType>::value && this->nan_count > count) {
      count = this->nan_count;
      mode = static_cast<T>(NAN);
    }
    return std::make_pair(mode, count);
  }

  int64_t nan_count = 0;  // only make sense to floating types
  std::unordered_map<T, int64_t> value_counts{};
};

// Use array to count small integers(bool, int8, uint8), improves performance 2x ~ 6x
template <typename ArrowType>
struct ModeState<ArrowType, enable_if_t<(sizeof(typename ArrowType::c_type) == 1)>> {
  using ThisType = ModeState<ArrowType>;
  using T = typename ArrowType::c_type;
  using Limits = std::numeric_limits<T>;

  ModeState() : value_counts(Limits::max() - Limits::min() + 1) {}

  void MergeFrom(const ThisType& state) {
    std::transform(this->value_counts.cbegin(), this->value_counts.cend(),
                   state.value_counts.cbegin(), this->value_counts.begin(),
                   std::plus<int64_t>{});
  }

  void MergeOne(T value) { ++this->value_counts[value - Limits::min()]; }

  std::pair<T, int64_t> Finalize() {
    T mode = Limits::min();
    int64_t count = 0;

    for (int i = 0; i <= Limits::max() - Limits::min(); ++i) {
      T this_value = static_cast<T>(i + Limits::min());
      int64_t this_count = this->value_counts[i];
      if (this_count > count || (this_count == count && this_value < mode)) {
        count = this_count;
        mode = this_value;
      }
    }
    return std::make_pair(mode, count);
  }

  std::vector<int64_t> value_counts;
};

template <typename ArrowType>
struct ModeImpl : public ScalarAggregator {
  using ThisType = ModeImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  explicit ModeImpl(const std::shared_ptr<DataType>& out_type) : out_type(out_type) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    ModeState<ArrowType> local_state;
    ArrayType arr(batch[0].array());

    if (arr.null_count() > 0) {
      BitmapReader reader(arr.null_bitmap_data(), arr.offset(), arr.length());
      for (int64_t i = 0; i < arr.length(); i++) {
        if (reader.IsSet()) {
          local_state.MergeOne(arr.Value(i));
        }
        reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length(); i++) {
        local_state.MergeOne(arr.Value(i));
      }
    }
    this->state = std::move(local_state);
  }

  void MergeFrom(KernelContext*, const KernelState& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state.MergeFrom(other.state);
  }

  void Finalize(KernelContext*, Datum* out) override {
    using ModeType = typename TypeTraits<ArrowType>::ScalarType;
    using CountType = typename TypeTraits<Int64Type>::ScalarType;

    std::vector<std::shared_ptr<Scalar>> values;
    auto mode_count = this->state.Finalize();
    auto mode = mode_count.first;
    auto count = mode_count.second;
    if (count == 0) {
      values = {std::make_shared<ModeType>(), std::make_shared<CountType>()};
    } else {
      values = {std::make_shared<ModeType>(mode), std::make_shared<CountType>(count)};
    }
    out->value = std::make_shared<StructScalar>(std::move(values), this->out_type);
  }

  std::shared_ptr<DataType> out_type;
  ModeState<ArrowType> state;
};

struct ModeInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const std::shared_ptr<DataType>& out_type;

  ModeInitState(KernelContext* ctx, const DataType& in_type,
                const std::shared_ptr<DataType>& out_type)
      : ctx(ctx), in_type(in_type), out_type(out_type) {}

  Status Visit(const DataType&) { return Status::NotImplemented("No mode implemented"); }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No mode implemented");
  }

  template <typename Type>
  enable_if_t<is_number_type<Type>::value || is_boolean_type<Type>::value, Status> Visit(
      const Type&) {
    state.reset(new ModeImpl<Type>(out_type));
    return Status::OK();
  }

  std::unique_ptr<KernelState> Create() {
    ctx->SetStatus(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

std::unique_ptr<KernelState> ModeInit(KernelContext* ctx, const KernelInitArgs& args) {
  ModeInitState visitor(ctx, *args.inputs[0].type,
                        args.kernel->signature->out_type().type());
  return visitor.Create();
}

void AddModeKernels(KernelInit init, const std::vector<std::shared_ptr<DataType>>& types,
                    ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // array[T] -> scalar[struct<mode: T, count: int64_t>]
    auto out_ty = struct_({field("mode", ty), field("count", int64())});
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func);
  }
}

}  // namespace

std::shared_ptr<ScalarAggregateFunction> AddModeAggKernels() {
  auto func = std::make_shared<ScalarAggregateFunction>("mode", Arity::Unary());
  AddModeKernels(ModeInit, {boolean()}, func.get());
  AddModeKernels(ModeInit, internal::NumericTypes(), func.get());
  return func;
}

}  // namespace aggregate
}  // namespace compute
}  // namespace arrow
