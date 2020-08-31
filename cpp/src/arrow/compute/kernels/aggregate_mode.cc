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

// Count values and return value:count map
template <typename T>
struct ValueCounter {
  virtual void CountOne(T value) = 0;
  virtual int64_t nan_count() = 0;
  virtual std::unordered_map<T, int64_t>& value_counts() = 0;
  virtual ~ValueCounter() = default;
};

// Count values by map. For floating points and general integers.
template <typename T>
class ValueCounterMap final : public ValueCounter<T> {
 public:
  void CountOne(T value) override {
    if (std::is_floating_point<T>::value && std::isnan(value)) {
      ++nan_count_;
    } else {
      ++value_counts_map_[value];
    }
  }

  int64_t nan_count() override { return nan_count_; }
  std::unordered_map<T, int64_t>& value_counts() override { return value_counts_map_; }

 private:
  int64_t nan_count_ = 0;
  std::unordered_map<T, int64_t> value_counts_map_;
};

// Count values by value indexed array. For integers in small value range.
template <typename T>
class ValueCounterVector final : public ValueCounter<T> {
 public:
  ValueCounterVector(T min, T max)
      : min_(min),
        value_counts_vector_(max - min + 1),
        value_counts_map_((max - min + 1) * 9 / 8) {}  // reserver 12.5% extra map buckets

  void CountOne(T value) override { ++value_counts_vector_[value - min_]; }

  int64_t nan_count() override { return 0; }

  // transfer to a value:count map
  std::unordered_map<T, int64_t>& value_counts() override {
    for (int i = 0; i < static_cast<int>(value_counts_vector_.size()); ++i) {
      T value = static_cast<T>(i + min_);
      int64_t count = value_counts_vector_[i];
      if (count) {
        value_counts_map_[value] = count;
      }
    }
    return value_counts_map_;
  }

 private:
  T min_;
  std::vector<int64_t> value_counts_vector_;
  std::unordered_map<T, int64_t> value_counts_map_;
};

// Calculate mode per data type and value range
template <typename ArrowType, typename Enable = void>
struct ModeConsumer {};

// Use vector based volume counter for small integer types
template <typename ArrowType>
struct ModeConsumer<ArrowType, enable_if_t<is_boolean_type<ArrowType>::value ||
                                           (is_integer_type<ArrowType>::value &&
                                            sizeof(typename ArrowType::c_type) == 1)>> {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using T = typename ArrowType::c_type;
  using Limits = std::numeric_limits<T>;

  explicit ModeConsumer(const ArrayType&) {}

  std::unique_ptr<ValueCounterVector<T>> value_counter{
      new ValueCounterVector<T>(Limits::min(), Limits::max())};
};

// Use map based volume counter for floating points
template <typename ArrowType>
struct ModeConsumer<ArrowType, enable_if_t<is_floating_type<ArrowType>::value>> {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using T = typename ArrowType::c_type;

  explicit ModeConsumer(const ArrayType&) {}

  std::unique_ptr<ValueCounterMap<T>> value_counter{new ValueCounterMap<T>()};
};

// Select map or vector based volume counter for integers per value range
template <typename ArrowType>
struct ModeConsumer<ArrowType, enable_if_t<is_integer_type<ArrowType>::value &&
                                           (sizeof(typename ArrowType::c_type) > 1)>> {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using T = typename ArrowType::c_type;

  explicit ModeConsumer(const ArrayType& array) {
    // see https://issues.apache.org/jira/browse/ARROW-9873
    static constexpr int kMinArraySize = 8192 / sizeof(T);
    static constexpr int kMaxValueRange = 16384;

    if ((array.length() - array.null_count()) >= kMinArraySize) {
      T min, max;
      std::tie(min, max) = MinMax(array);
      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <= kMaxValueRange) {
        this->value_counter.reset(new ValueCounterVector<T>(min, max));
        return;
      }
    }
    this->value_counter.reset(new ValueCounterMap<T>());
  }

  std::pair<T, T> MinMax(const ArrayType& array) {
    T min = std::numeric_limits<T>::max();
    T max = std::numeric_limits<T>::min();

    const auto values = array.raw_values();
    if (array.null_count() > 0) {
      BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
      for (int64_t i = 0; i < array.length(); ++i) {
        if (reader.IsSet()) {
          min = std::min(min, values[i]);
          max = std::max(max, values[i]);
        }
        reader.Next();
      }
    } else {
      for (int64_t i = 0; i < array.length(); ++i) {
        min = std::min(min, values[i]);
        max = std::max(max, values[i]);
      }
    }

    return std::make_pair(min, max);
  }

  std::unique_ptr<ValueCounter<T>> value_counter;
};

template <typename ArrowType>
struct ModeState {
  using ThisType = ModeState<ArrowType>;
  using T = typename ArrowType::c_type;

  void MergeFrom(const ThisType& state) {
    if (this->value_counts.empty()) {
      this->value_counts = state.value_counts;
    } else {
      for (const auto& value_count : state.value_counts) {
        auto value = value_count.first;
        auto count = value_count.second;
        this->value_counts[value] += count;
      }
    }
    if (is_floating_type<ArrowType>::value) {
      this->nan_count += state.nan_count;
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
  std::unordered_map<T, int64_t> value_counts;
};

template <typename ArrowType>
struct ModeImpl : public ScalarAggregator {
  using ThisType = ModeImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  explicit ModeImpl(const std::shared_ptr<DataType>& out_type) : out_type(out_type) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    ArrayType arr(batch[0].array());
    ModeConsumer<ArrowType> consumer(arr);

    if (arr.null_count() > 0) {
      BitmapReader reader(arr.null_bitmap_data(), arr.offset(), arr.length());
      for (int64_t i = 0; i < arr.length(); i++) {
        if (reader.IsSet()) {
          consumer.value_counter->CountOne(arr.Value(i));
        }
        reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length(); i++) {
        consumer.value_counter->CountOne(arr.Value(i));
      }
    }
    this->state.nan_count = consumer.value_counter->nan_count();
    this->state.value_counts = std::move(consumer.value_counter->value_counts());
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
