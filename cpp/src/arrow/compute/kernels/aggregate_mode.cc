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
#include <utility>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/result.h"
#include "arrow/stl_allocator.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using ModeState = OptionsWrapper<ModeOptions>;

constexpr char kModeFieldName[] = "mode";
constexpr char kCountFieldName[] = "count";

constexpr uint64_t kCountEOF = ~0ULL;

template <typename InType, typename CType = typename InType::c_type>
Result<std::pair<CType*, int64_t*>> PrepareOutput(int64_t n, KernelContext* ctx,
                                                  Datum* out) {
  const auto& mode_type = TypeTraits<InType>::type_singleton();
  const auto& count_type = int64();

  auto mode_data = ArrayData::Make(mode_type, /*length=*/n, /*null_count=*/0);
  mode_data->buffers.resize(2, nullptr);
  auto count_data = ArrayData::Make(count_type, n, 0);
  count_data->buffers.resize(2, nullptr);

  CType* mode_buffer = nullptr;
  int64_t* count_buffer = nullptr;

  if (n > 0) {
    ARROW_ASSIGN_OR_RAISE(mode_data->buffers[1], ctx->Allocate(n * sizeof(CType)));
    ARROW_ASSIGN_OR_RAISE(count_data->buffers[1], ctx->Allocate(n * sizeof(int64_t)));
    mode_buffer = mode_data->template GetMutableValues<CType>(1);
    count_buffer = count_data->template GetMutableValues<int64_t>(1);
  }

  const auto& out_type =
      struct_({field(kModeFieldName, mode_type), field(kCountFieldName, count_type)});
  *out = Datum(ArrayData::Make(out_type, n, {nullptr}, {mode_data, count_data}, 0));

  return std::make_pair(mode_buffer, count_buffer);
}

// find top-n value:count pairs with minimal heap
// suboptimal for tiny or large n, possibly okay as we're not in hot path
template <typename InType, typename Generator>
Status Finalize(KernelContext* ctx, Datum* out, Generator&& gen) {
  using CType = typename InType::c_type;

  using ValueCountPair = std::pair<CType, uint64_t>;
  auto gt = [](const ValueCountPair& lhs, const ValueCountPair& rhs) {
    const bool rhs_is_nan = rhs.first != rhs.first;  // nan as largest value
    return lhs.second > rhs.second ||
           (lhs.second == rhs.second && (lhs.first < rhs.first || rhs_is_nan));
  };

  std::priority_queue<ValueCountPair, std::vector<ValueCountPair>, decltype(gt)> min_heap(
      std::move(gt));

  const ModeOptions& options = ModeState::Get(ctx);
  while (true) {
    const ValueCountPair& value_count = gen();
    DCHECK_NE(value_count.second, 0);
    if (value_count.second == kCountEOF) break;
    if (static_cast<int64_t>(min_heap.size()) < options.n) {
      min_heap.push(value_count);
    } else if (gt(value_count, min_heap.top())) {
      min_heap.pop();
      min_heap.push(value_count);
    }
  }
  const int64_t n = min_heap.size();

  CType* mode_buffer;
  int64_t* count_buffer;
  ARROW_ASSIGN_OR_RAISE(std::tie(mode_buffer, count_buffer),
                        PrepareOutput<InType>(n, ctx, out));

  for (int64_t i = n - 1; i >= 0; --i) {
    std::tie(mode_buffer[i], count_buffer[i]) = min_heap.top();
    min_heap.pop();
  }

  return Status::OK();
}

// count value occurances for integers with narrow value range
// O(1) space, O(n) time
template <typename T>
struct CountModer {
  using CType = typename T::c_type;

  CType min;
  std::vector<uint64_t> counts;

  CountModer(CType min, CType max) {
    uint32_t value_range = static_cast<uint32_t>(max - min) + 1;
    DCHECK_LT(value_range, 1 << 20);
    this->min = min;
    this->counts.resize(value_range, 0);
  }

  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // count values in all chunks, ignore nulls
    const Datum& datum = batch[0];
    CountValues<CType>(this->counts.data(), datum, this->min);

    // generator to emit next value:count pair
    int index = 0;
    auto gen = [&]() {
      for (; index < static_cast<int>(counts.size()); ++index) {
        if (counts[index] != 0) {
          auto value_count =
              std::make_pair(static_cast<CType>(index + this->min), counts[index]);
          ++index;
          return value_count;
        }
      }
      return std::pair<CType, uint64_t>(0, kCountEOF);
    };

    return Finalize<T>(ctx, out, std::move(gen));
  }
};

// booleans can be handled more straightforward
template <>
struct CountModer<BooleanType> {
  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    int64_t counts[2]{};

    const Datum& datum = batch[0];
    for (const auto& array : datum.chunks()) {
      if (array->length() > array->null_count()) {
        const int64_t true_count =
            arrow::internal::checked_pointer_cast<BooleanArray>(array)->true_count();
        const int64_t false_count = array->length() - array->null_count() - true_count;
        counts[true] += true_count;
        counts[false] += false_count;
      }
    }

    const ModeOptions& options = ModeState::Get(ctx);
    const int64_t distinct_values = (counts[0] != 0) + (counts[1] != 0);
    const int64_t n = std::min(options.n, distinct_values);

    bool* mode_buffer;
    int64_t* count_buffer;
    ARROW_ASSIGN_OR_RAISE(std::tie(mode_buffer, count_buffer),
                          PrepareOutput<BooleanType>(n, ctx, out));

    if (n >= 1) {
      const bool index = counts[1] > counts[0];
      mode_buffer[0] = index;
      count_buffer[0] = counts[index];
      if (n == 2) {
        mode_buffer[1] = !index;
        count_buffer[1] = counts[!index];
      }
    }

    return Status::OK();
  }
};

// copy and sort approach for floating points or integers with wide value range
// O(n) space, O(nlogn) time
template <typename T>
struct SortModer {
  using CType = typename T::c_type;
  using Allocator = arrow::stl::allocator<CType>;

  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // copy all chunks to a buffer, ignore nulls and nans
    std::vector<CType, Allocator> in_buffer(Allocator(ctx->memory_pool()));

    uint64_t nan_count = 0;
    const Datum& datum = batch[0];
    const int64_t in_length = datum.length() - datum.null_count();
    if (in_length > 0) {
      in_buffer.resize(in_length);
      CopyNonNullValues(datum, in_buffer.data());

      // drop nan
      if (is_floating_type<T>::value) {
        const auto& it = std::remove_if(in_buffer.begin(), in_buffer.end(),
                                        [](CType v) { return v != v; });
        nan_count = in_buffer.end() - it;
        in_buffer.resize(it - in_buffer.begin());
      }
    }

    // sort the input data to count same values
    std::sort(in_buffer.begin(), in_buffer.end());

    // generator to emit next value:count pair
    auto it = in_buffer.cbegin();
    auto gen = [&]() {
      if (ARROW_PREDICT_FALSE(it == in_buffer.cend())) {
        // handle NAN at last
        if (nan_count > 0) {
          auto value_count = std::make_pair(static_cast<CType>(NAN), nan_count);
          nan_count = 0;
          return value_count;
        }
        return std::pair<CType, uint64_t>(static_cast<CType>(0), kCountEOF);
      }
      // count same values
      const CType value = *it;
      uint64_t count = 0;
      do {
        ++it;
        ++count;
      } while (it != in_buffer.cend() && *it == value);
      return std::make_pair(value, count);
    };

    return Finalize<T>(ctx, out, std::move(gen));
  }
};

// pick counting or sorting approach per integers value range
template <typename T>
struct CountOrSortModer {
  using CType = typename T::c_type;

  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // cross point to benefit from counting approach
    // about 2x improvement for int32/64 from micro-benchmarking
    static constexpr int kMinArraySize = 8192;
    static constexpr int kMaxValueRange = 32768;

    const Datum& datum = batch[0];
    if (datum.length() - datum.null_count() >= kMinArraySize) {
      CType min, max;
      std::tie(min, max) = GetMinMax<CType>(datum);

      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <= kMaxValueRange) {
        return CountModer<T>(min, max).Exec(ctx, batch, out);
      }
    }

    return SortModer<T>().Exec(ctx, batch, out);
  }
};

template <typename InType, typename Enable = void>
struct Moder;

template <>
struct Moder<Int8Type> {
  CountModer<Int8Type> impl;
  Moder() : impl(-128, 127) {}
};

template <>
struct Moder<UInt8Type> {
  CountModer<UInt8Type> impl;
  Moder() : impl(0, 255) {}
};

template <>
struct Moder<BooleanType> {
  CountModer<BooleanType> impl;
};

template <typename InType>
struct Moder<InType, enable_if_t<(is_integer_type<InType>::value &&
                                  (sizeof(typename InType::c_type) > 1))>> {
  CountOrSortModer<InType> impl;
};

template <typename InType>
struct Moder<InType, enable_if_t<is_floating_type<InType>::value>> {
  SortModer<InType> impl;
};

template <typename T>
Status ScalarMode(KernelContext* ctx, const Scalar& scalar, Datum* out) {
  using CType = typename T::c_type;
  if (scalar.is_valid) {
    bool called = false;
    return Finalize<T>(ctx, out, [&]() {
      if (!called) {
        called = true;
        return std::pair<CType, uint64_t>(UnboxScalar<T>::Unbox(scalar), 1);
      }
      return std::pair<CType, uint64_t>(static_cast<CType>(0), kCountEOF);
    });
  }
  return Finalize<T>(ctx, out, []() {
    return std::pair<CType, uint64_t>(static_cast<CType>(0), kCountEOF);
  });
}

template <typename _, typename InType>
struct ModeExecutor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (ctx->state() == nullptr) {
      return Status::Invalid("Mode requires ModeOptions");
    }
    const ModeOptions& options = ModeState::Get(ctx);
    if (options.n <= 0) {
      return Status::Invalid("ModeOption::n must be strictly positive");
    }

    if (batch[0].is_scalar()) {
      return ScalarMode<InType>(ctx, *batch[0].scalar(), out);
    }

    return Moder<InType>().impl.Exec(ctx, batch, out);
  }
};

VectorKernel NewModeKernel(const std::shared_ptr<DataType>& in_type) {
  VectorKernel kernel;
  kernel.init = ModeState::Init;
  kernel.can_execute_chunkwise = false;
  kernel.output_chunked = false;
  auto out_type =
      struct_({field(kModeFieldName, in_type), field(kCountFieldName, int64())});
  kernel.signature =
      KernelSignature::Make({InputType(in_type)}, ValueDescr::Array(out_type));
  return kernel;
}

void AddBooleanModeKernel(VectorFunction* func) {
  VectorKernel kernel = NewModeKernel(boolean());
  kernel.exec = ModeExecutor<StructType, BooleanType>::Exec;
  DCHECK_OK(func->AddKernel(kernel));
}

void AddNumericModeKernels(VectorFunction* func) {
  for (const auto& type : NumericTypes()) {
    VectorKernel kernel = NewModeKernel(type);
    kernel.exec = GenerateNumeric<ModeExecutor, StructType>(*type);
    DCHECK_OK(func->AddKernel(kernel));
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

}  // namespace

void RegisterScalarAggregateMode(FunctionRegistry* registry) {
  static auto default_options = ModeOptions::Defaults();
  auto func = std::make_shared<VectorFunction>("mode", Arity::Unary(), &mode_doc,
                                               &default_options);
  AddBooleanModeKernel(func.get());
  AddNumericModeKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
