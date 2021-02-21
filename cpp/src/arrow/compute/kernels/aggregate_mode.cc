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
#include <queue>
#include <unordered_map>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

constexpr char kModeFieldName[] = "mode";
constexpr char kCountFieldName[] = "count";

// {value:count} map
template <typename CType>
using CounterMap = std::unordered_map<CType, int64_t>;

// map based counter for floating points
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<std::is_floating_point<CType>::value, CounterMap<CType>> CountValuesByMap(
    const ArrayType& array, int64_t& nan_count) {
  CounterMap<CType> value_counts_map;
  const ArrayData& data = *array.data();
  const CType* values = data.GetValues<CType>(1);

  nan_count = 0;
  if (array.length() > array.null_count()) {
    arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                         [&](int64_t pos, int64_t len) {
                                           for (int64_t i = 0; i < len; ++i) {
                                             const auto value = values[pos + i];
                                             if (std::isnan(value)) {
                                               ++nan_count;
                                             } else {
                                               ++value_counts_map[value];
                                             }
                                           }
                                         });
  }

  return value_counts_map;
}

// map base counter for non floating points
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<!std::is_floating_point<CType>::value, CounterMap<CType>> CountValuesByMap(
    const ArrayType& array) {
  CounterMap<CType> value_counts_map;
  const ArrayData& data = *array.data();
  const CType* values = data.GetValues<CType>(1);

  if (array.length() > array.null_count()) {
    arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                         [&](int64_t pos, int64_t len) {
                                           for (int64_t i = 0; i < len; ++i) {
                                             ++value_counts_map[values[pos + i]];
                                           }
                                         });
  }

  return value_counts_map;
}

// vector based counter for int8 or integers with small value range
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
CounterMap<CType> CountValuesByVector(const ArrayType& array, CType min, CType max) {
  const int range = static_cast<int>(max - min);
  DCHECK(range >= 0 && range < 64 * 1024 * 1024);
  const ArrayData& data = *array.data();
  const CType* values = data.GetValues<CType>(1);

  std::vector<int64_t> value_counts_vector(range + 1);
  if (array.length() > array.null_count()) {
    arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                         [&](int64_t pos, int64_t len) {
                                           for (int64_t i = 0; i < len; ++i) {
                                             ++value_counts_vector[values[pos + i] - min];
                                           }
                                         });
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
  const ArrayData& data = *array.data();
  const CType* values = data.GetValues<CType>(1);

  if ((array.length() - array.null_count()) >= kMinArraySize) {
    CType min = std::numeric_limits<CType>::max();
    CType max = std::numeric_limits<CType>::min();

    arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                         [&](int64_t pos, int64_t len) {
                                           for (int64_t i = 0; i < len; ++i) {
                                             const auto value = values[pos + i];
                                             min = std::min(min, value);
                                             max = std::max(max, value);
                                           }
                                         });

    if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <= kMaxValueRange) {
      return CountValuesByVector(array, min, max);
    }
  }
  return CountValuesByMap(array);
}

// bool
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<is_boolean_type<typename ArrayType::TypeClass>::value, CounterMap<CType>>
CountValues(const ArrayType& array, int64_t& nan_count) {
  // we need just count ones and zeros
  CounterMap<CType> map;
  if (array.length() > array.null_count()) {
    map[true] = array.true_count();
    map[false] = array.length() - array.null_count() - map[true];
  }
  nan_count = 0;
  return map;
}

// int8
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<is_integer_type<typename ArrayType::TypeClass>::value && sizeof(CType) == 1,
            CounterMap<CType>>
CountValues(const ArrayType& array, int64_t& nan_count) {
  using Limits = std::numeric_limits<CType>;
  nan_count = 0;
  return CountValuesByVector(array, Limits::min(), Limits::max());
}

// int16/32/64
template <typename ArrayType, typename CType = typename ArrayType::TypeClass::c_type>
enable_if_t<is_integer_type<typename ArrayType::TypeClass>::value && (sizeof(CType) > 1),
            CounterMap<CType>>
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

  // find top-n value/count pairs with min-heap (priority queue with '>' comparator)
  void Finalize(CType* modes, int64_t* counts, const int64_t n) {
    DCHECK(n >= 1 && n <= this->DistinctValues());

    // mode 'greater than' comparator: larger count or same count with smaller value
    using ValueCountPair = std::pair<CType, int64_t>;
    auto mode_gt = [](const ValueCountPair& lhs, const ValueCountPair& rhs) {
      const bool rhs_is_nan = rhs.first != rhs.first;  // nan as largest value
      return lhs.second > rhs.second ||
             (lhs.second == rhs.second && (lhs.first < rhs.first || rhs_is_nan));
    };

    // initialize min-heap with first n modes
    std::vector<ValueCountPair> vector(n);
    // push nan if exists
    const bool has_nan = is_floating_type<ArrowType>::value && this->nan_count > 0;
    if (has_nan) {
      vector[0] = std::make_pair(static_cast<CType>(NAN), this->nan_count);
    }
    // push n or n-1 modes
    auto it = this->value_counts.cbegin();
    for (int i = has_nan; i < n; ++i) {
      vector[i] = *it++;
    }
    // turn to min-heap
    std::priority_queue<ValueCountPair, std::vector<ValueCountPair>, decltype(mode_gt)>
        min_heap(std::move(mode_gt), std::move(vector));

    // iterate and insert modes into min-heap
    // - mode < heap top: ignore mode
    // - mode > heap top: discard heap top, insert mode
    for (; it != this->value_counts.cend(); ++it) {
      if (mode_gt(*it, min_heap.top())) {
        min_heap.pop();
        min_heap.push(*it);
      }
    }

    // pop modes from min-heap and insert into output array (in reverse order)
    DCHECK_EQ(min_heap.size(), static_cast<size_t>(n));
    for (int64_t i = n - 1; i >= 0; --i) {
      std::tie(modes[i], counts[i]) = min_heap.top();
      min_heap.pop();
    }
  }

  int64_t DistinctValues() const {
    return this->value_counts.size() +
           (is_floating_type<ArrowType>::value && this->nan_count > 0);
  }

  int64_t nan_count = 0;  // only make sense to floating types
  CounterMap<CType> value_counts;
};

template <typename ArrowType>
struct ModeImpl : public ScalarAggregator {
  using ThisType = ModeImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CType = typename ArrowType::c_type;

  ModeImpl(const std::shared_ptr<DataType>& out_type, const ModeOptions& options)
      : out_type(out_type), options(options) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    ArrayType array(batch[0].array());
    this->state.value_counts = CountValues(array, this->state.nan_count);
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    auto& other = checked_cast<ThisType&>(src);
    this->state.MergeFrom(std::move(other.state));
  }

  static std::shared_ptr<ArrayData> MakeArrayData(
      const std::shared_ptr<DataType>& data_type, int64_t n) {
    auto data = ArrayData::Make(data_type, n, 0);
    data->buffers.resize(2);
    data->buffers[0] = nullptr;
    data->buffers[1] = nullptr;
    return data;
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    const auto& mode_type = TypeTraits<ArrowType>::type_singleton();
    const auto& count_type = int64();
    const auto& out_type =
        struct_({field(kModeFieldName, mode_type), field(kCountFieldName, count_type)});

    int64_t n = this->options.n;
    if (n > state.DistinctValues()) {
      n = state.DistinctValues();
    } else if (n < 0) {
      n = 0;
    }

    auto mode_data = this->MakeArrayData(mode_type, n);
    auto count_data = this->MakeArrayData(count_type, n);
    if (n > 0) {
      KERNEL_ASSIGN_OR_RAISE(mode_data->buffers[1], ctx,
                             ctx->Allocate(n * sizeof(CType)));
      KERNEL_ASSIGN_OR_RAISE(count_data->buffers[1], ctx,
                             ctx->Allocate(n * sizeof(int64_t)));
      CType* mode_buffer = mode_data->template GetMutableValues<CType>(1);
      int64_t* count_buffer = count_data->template GetMutableValues<int64_t>(1);
      this->state.Finalize(mode_buffer, count_buffer, n);
    }

    *out = Datum(ArrayData::Make(out_type, n, {nullptr}, {mode_data, count_data}, 0));
  }

  std::shared_ptr<DataType> out_type;
  ModeState<ArrowType> state;
  ModeOptions options;
};

struct ModeInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const std::shared_ptr<DataType>& out_type;
  const ModeOptions& options;

  ModeInitState(KernelContext* ctx, const DataType& in_type,
                const std::shared_ptr<DataType>& out_type, const ModeOptions& options)
      : ctx(ctx), in_type(in_type), out_type(out_type), options(options) {}

  Status Visit(const DataType&) { return Status::NotImplemented("No mode implemented"); }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No mode implemented");
  }

  template <typename Type>
  enable_if_t<is_number_type<Type>::value || is_boolean_type<Type>::value, Status> Visit(
      const Type&) {
    state.reset(new ModeImpl<Type>(out_type, options));
    return Status::OK();
  }

  std::unique_ptr<KernelState> Create() {
    ctx->SetStatus(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

std::unique_ptr<KernelState> ModeInit(KernelContext* ctx, const KernelInitArgs& args) {
  ModeInitState visitor(ctx, *args.inputs[0].type,
                        args.kernel->signature->out_type().type(),
                        static_cast<const ModeOptions&>(*args.options));
  return visitor.Create();
}

void AddModeKernels(KernelInit init, const std::vector<std::shared_ptr<DataType>>& types,
                    ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // array[T] -> array[struct<mode: T, count: int64_t>]
    auto out_ty = struct_({field(kModeFieldName, ty), field(kCountFieldName, int64())});
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Array(out_ty));
    AddAggKernel(std::move(sig), init, func);
  }
}

const FunctionDoc mode_doc{
    "Calculate the modal (most common) values of a numeric array",
    ("Returns top-n most common values and number of times they occur in an array.\n"
     "Result is an array of `struct<mode: T, count: int64>`, where T is the input type.\n"
     "Values with larger counts are returned before smaller counts.\n"
     "If there are more than one values with same count, smaller one is returned first.\n"
     "Nulls are ignored.  If there are no non-null values in the array,\n"
     "empty array is returned."),
    {"array"},
    "ModeOptions"};

std::shared_ptr<ScalarAggregateFunction> AddModeAggKernels() {
  static auto default_mode_options = ModeOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("mode", Arity::Unary(), &mode_doc,
                                                        &default_mode_options);
  AddModeKernels(ModeInit, {boolean()}, func.get());
  AddModeKernels(ModeInit, NumericTypes(), func.get());
  return func;
}

}  // namespace

void RegisterScalarAggregateMode(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(AddModeAggKernels()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
