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

// {value:count} map
template <typename CType>
using CounterMap = std::unordered_map<CType, int64_t>;

// map based counter for floating points
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<std::is_floating_point<CType>::value, CounterMap<CType>> CountValuesByMap(
    const ArrayType& array, int64_t& nan_count) {
  CounterMap<CType> value_counts_map;

  nan_count = 0;
  if (array.length() > array.null_count()) {
    VisitArrayDataInline<typename ArrayType::TypeClass>(
        *array.data(),
        [&](CType value) {
          if (std::isnan(value)) {
            ++nan_count;
          } else {
            ++value_counts_map[value];
          }
        },
        []() {});
  }

  return value_counts_map;
}

// map base counter for non floating points
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<!std::is_floating_point<CType>::value, CounterMap<CType>> CountValuesByMap(
    const ArrayType& array) {
  CounterMap<CType> value_counts_map;

  if (array.length() > array.null_count()) {
    VisitArrayDataInline<typename ArrayType::TypeClass>(
        *array.data(), [&](CType value) { ++value_counts_map[value]; }, []() {});
  }

  return value_counts_map;
}

// vector based counter for bool/int8 or integers with small value range
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
CounterMap<CType> CountValuesByVector(const ArrayType& array, CType min, CType max) {
  const int range = static_cast<int>(max - min);
  DCHECK(range >= 0 && range < 64 * 1024 * 1024);

  std::vector<int64_t> value_counts_vector(range + 1);
  if (array.length() > array.null_count()) {
    VisitArrayDataInline<typename ArrayType::TypeClass>(
        *array.data(), [&](CType value) { ++value_counts_vector[value - min]; }, []() {});
  }

  // Transfer value counts to a map to be consistent with other chunks
  CounterMap<CType> value_counts_map(range + 1);
  for (int i = 0; i <= range; ++i) {
    CType value = static_cast<CType>(i + min);
    int64_t count = value_counts_vector[i];
    if (count) {
      value_counts_map[value] = count;
    }
  }

  return value_counts_map;
}

// map or vector based counter for int16/32/64 per value range
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
CounterMap<CType> CountValuesByMapOrVector(const ArrayType& array) {
  // see https://issues.apache.org/jira/browse/ARROW-9873
  static constexpr int kMinArraySize = 8192 / sizeof(CType);
  static constexpr int kMaxValueRange = 16384;

  if ((array.length() - array.null_count()) >= kMinArraySize) {
    CType min = std::numeric_limits<CType>::max();
    CType max = std::numeric_limits<CType>::min();

    VisitArrayDataInline<typename ArrayType::TypeClass>(
        *array.data(),
        [&](CType value) {
          min = std::min(min, value);
          max = std::max(max, value);
        },
        []() {});

    if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <= kMaxValueRange) {
      return CountValuesByVector(array, min, max);
    }
  }
  return CountValuesByMap(array);
}

// bool, int8
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<std::is_integral<CType>::value && sizeof(CType) == 1, CounterMap<CType>>
CountValues(const ArrayType& array, int64_t& nan_count) {
  using Limits = std::numeric_limits<CType>;
  nan_count = 0;
  return CountValuesByVector(array, Limits::min(), Limits::max());
}

// int16/32/64
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<std::is_integral<CType>::value && (sizeof(CType) > 1), CounterMap<CType>>
CountValues(const ArrayType& array, int64_t& nan_count) {
  nan_count = 0;
  return CountValuesByMapOrVector(array);
}

// float/double
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<(std::is_floating_point<CType>::value), CounterMap<CType>>  // NOLINT format
CountValues(const ArrayType& array, int64_t& nan_count) {
  nan_count = 0;
  return CountValuesByMap(array, nan_count);
}

template <typename ArrowType>
struct ModeState {
  using ThisType = ModeState<ArrowType>;
  using CType = typename ArrowType::c_type;

  void MergeFrom(ThisType&& state) {
    if (this->value_counts.empty()) {
      this->value_counts = std::move(state.value_counts);
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

  std::pair<CType, int64_t> Finalize() {
    CType mode = std::numeric_limits<CType>::min();
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
      mode = static_cast<CType>(NAN);
    }
    return std::make_pair(mode, count);
  }

  int64_t nan_count = 0;  // only make sense to floating types
  CounterMap<CType> value_counts;
};

template <typename ArrowType>
struct ModeImpl : public ScalarAggregator {
  using ThisType = ModeImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  explicit ModeImpl(const std::shared_ptr<DataType>& out_type) : out_type(out_type) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    ArrayType array(batch[0].array());
    this->state.value_counts = CountValues(array, this->state.nan_count);
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    auto& other = checked_cast<ThisType&>(src);
    this->state.MergeFrom(std::move(other.state));
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
