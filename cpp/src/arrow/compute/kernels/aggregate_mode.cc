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
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/result.h"
#include "arrow/stl_allocator.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using ModeState = OptionsWrapper<ModeOptions>;

constexpr char kModeFieldName[] = "mode";
constexpr char kCountFieldName[] = "count";

constexpr uint64_t kCountEOF = ~0ULL;

template <typename InType, typename CType = typename TypeTraits<InType>::CType>
Result<std::pair<CType*, int64_t*>> PrepareOutput(int64_t n, KernelContext* ctx,
                                                  const DataType& type, ExecResult* out) {
  DCHECK_EQ(Type::STRUCT, type.id());
  const auto& out_type = checked_cast<const StructType&>(type);
  DCHECK_EQ(2, out_type.num_fields());
  const auto& mode_type = out_type.field(0)->type();
  const auto& count_type = int64();

  auto mode_data = ArrayData::Make(mode_type, /*length=*/n, /*null_count=*/0);
  mode_data->buffers.resize(2, nullptr);
  auto count_data = ArrayData::Make(count_type, n, 0);
  count_data->buffers.resize(2, nullptr);

  CType* mode_buffer = nullptr;
  int64_t* count_buffer = nullptr;

  if (n > 0) {
    const auto mode_buffer_size = bit_util::BytesForBits(n * mode_type->bit_width());
    ARROW_ASSIGN_OR_RAISE(mode_data->buffers[1], ctx->Allocate(mode_buffer_size));
    ARROW_ASSIGN_OR_RAISE(count_data->buffers[1], ctx->Allocate(n * sizeof(int64_t)));
    mode_buffer = mode_data->template GetMutableValues<CType>(1);
    count_buffer = count_data->template GetMutableValues<int64_t>(1);
  }

  out->value =
      ArrayData::Make(type.GetSharedPtr(), n, {nullptr}, {mode_data, count_data}, 0);
  return std::make_pair(mode_buffer, count_buffer);
}

// find top-n value:count pairs with minimal heap
// suboptimal for tiny or large n, possibly okay as we're not in hot path
template <typename InType, typename Generator>
Status Finalize(KernelContext* ctx, const DataType& type, ExecResult* out,
                Generator&& gen) {
  using CType = typename TypeTraits<InType>::CType;

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
                        PrepareOutput<InType>(n, ctx, type, out));

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

  Status GetResult(KernelContext* ctx, const DataType& type, ExecResult* out) {
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

    return Finalize<T>(ctx, type, out, std::move(gen));
  }

  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // count values in all chunks, ignore nulls
    const ArraySpan& values = batch[0].array;
    const ModeOptions& options = ModeState::Get(ctx);
    if ((!options.skip_nulls && values.GetNullCount() > 0) ||
        (values.length - values.GetNullCount() < options.min_count)) {
      return PrepareOutput<T>(/*n=*/0, ctx, *out->type(), out).status();
    }

    CountValues<CType>(values, this->min, this->counts.data());
    return GetResult(ctx, *out->type(), out);
  }

  Status ExecChunked(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // count values in all chunks, ignore nulls
    const ChunkedArray& values = *batch[0].chunked_array();
    const ModeOptions& options = ModeState::Get(ctx);
    ExecResult result;
    if ((!options.skip_nulls && values.null_count() > 0) ||
        (values.length() - values.null_count() < options.min_count)) {
      RETURN_NOT_OK(PrepareOutput<T>(/*n=*/0, ctx, *out->type(), &result));
    } else {
      CountValues<CType>(values, this->min, this->counts.data());
      RETURN_NOT_OK(GetResult(ctx, *out->type(), &result));
    }
    *out = result.array_data();
    return Status::OK();
  }
};

// booleans can be handled more straightforward
template <>
struct CountModer<BooleanType> {
  int64_t counts[2] = {0, 0};

  void UpdateCounts(const ArraySpan& values) {
    if (values.length > values.GetNullCount()) {
      const int64_t true_count = GetTrueCount(values);
      counts[true] += true_count;
      counts[false] += values.length - values.null_count - true_count;
    }
  }

  void UpdateCounts(const ChunkedArray& values) {
    for (const auto& chunk : values.chunks()) {
      UpdateCounts(*chunk->data());
    }
  }

  Status WrapResult(KernelContext* ctx, const ModeOptions& options, const DataType& type,
                    ExecResult* out) {
    const int64_t distinct_values = (this->counts[0] != 0) + (this->counts[1] != 0);
    const int64_t n = std::min(options.n, distinct_values);

    uint8_t* mode_buffer;
    int64_t* count_buffer;
    ARROW_ASSIGN_OR_RAISE(std::tie(mode_buffer, count_buffer),
                          (PrepareOutput<BooleanType, uint8_t>(n, ctx, type, out)));

    if (n >= 1) {
      // at most two bits are useful in mode buffer
      mode_buffer[0] = 0;
      const bool first_mode = counts[true] > counts[false];
      bit_util::SetBitTo(mode_buffer, 0, first_mode);
      count_buffer[0] = counts[first_mode];
      if (n == 2) {
        const bool second_mode = !first_mode;
        bit_util::SetBitTo(mode_buffer, 1, second_mode);
        count_buffer[1] = counts[second_mode];
      }
    }

    return Status::OK();
  }

  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& values = batch[0].array;
    const ModeOptions& options = ModeState::Get(ctx);
    if ((!options.skip_nulls && values.GetNullCount() > 0) ||
        (values.length - values.null_count < options.min_count)) {
      return PrepareOutput<BooleanType, uint8_t>(0, ctx, *out->type(), out).status();
    }
    UpdateCounts(values);
    return WrapResult(ctx, options, *out->type(), out);
  }

  Status ExecChunked(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ChunkedArray& values = *batch[0].chunked_array();
    const ModeOptions& options = ModeState::Get(ctx);
    ExecResult result;
    if ((!options.skip_nulls && values.null_count() > 0) ||
        (values.length() - values.null_count() < options.min_count)) {
      RETURN_NOT_OK((PrepareOutput<BooleanType, uint8_t>(0, ctx, *out->type(), &result)));
    } else {
      UpdateCounts(values);
      RETURN_NOT_OK(WrapResult(ctx, options, *out->type(), &result));
    }
    *out = result.array_data();
    return Status::OK();
  }
};

// copy and sort approach for floating points, decimals, or integers with wide
// value range
// O(n) space, O(nlogn) time
template <typename T>
struct SortModer {
  using CType = typename TypeTraits<T>::CType;
  using Allocator = arrow::stl::allocator<CType>;

  template <typename Type = T>
  static enable_if_floating_point<Type, CType> GetNan() {
    return static_cast<CType>(NAN);
  }

  template <typename Type = T>
  static enable_if_t<!is_floating_type<Type>::value, CType> GetNan() {
    DCHECK(false);
    return static_cast<CType>(0);
  }

  template <typename Container>
  Status ComputeMode(KernelContext* ctx, const Container& arr, int64_t length,
                     int64_t null_count, const DataType& type, ExecResult* out) {
    const ModeOptions& options = ModeState::Get(ctx);
    const int64_t in_length = length - null_count;
    if ((!options.skip_nulls && null_count > 0) || (in_length < options.min_count)) {
      return PrepareOutput<T>(/*n=*/0, ctx, type, out).status();
    }

    // copy all chunks to a buffer, ignore nulls and nans
    std::vector<CType, Allocator> values(Allocator(ctx->memory_pool()));

    uint64_t nan_count = 0;
    if (length > 0) {
      values.resize(length - null_count);
      CopyNonNullValues(arr, values.data());

      // drop nan
      if (is_floating_type<T>::value) {
        const auto& it =
            std::remove_if(values.begin(), values.end(), [](CType v) { return v != v; });
        nan_count = values.end() - it;
        values.resize(it - values.begin());
      }
    }
    // sort the input data to count same values
    std::sort(values.begin(), values.end());

    // generator to emit next value:count pair
    auto it = values.cbegin();
    auto gen = [&]() {
      if (ARROW_PREDICT_FALSE(it == values.cend())) {
        // handle NAN at last
        if (nan_count > 0) {
          auto value_count = std::make_pair(GetNan(), nan_count);
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
      } while (it != values.cend() && *it == value);
      return std::make_pair(value, count);
    };

    return Finalize<T>(ctx, type, out, std::move(gen));
  }

  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& values = batch[0].array;
    return ComputeMode(ctx, values, values.length, values.GetNullCount(), *out->type(),
                       out);
  }

  Status ExecChunked(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ChunkedArray& values = *batch[0].chunked_array();
    ExecResult result;
    RETURN_NOT_OK(ComputeMode(ctx, values, values.length(), values.null_count(),
                              *out->type(), &result));
    *out = result.array_data();
    return Status::OK();
  }
};

template <typename CType, typename Container>
bool ShouldUseCountMode(const Container& values, int64_t num_valid, CType* min,
                        CType* max) {
  // cross point to benefit from counting approach
  // about 2x improvement for int32/64 from micro-benchmarking
  static constexpr int kMinArraySize = 8192;
  static constexpr int kMaxValueRange = 32768;

  if (num_valid >= kMinArraySize) {
    std::tie(*min, *max) = GetMinMax<CType>(values);
    return static_cast<uint64_t>(*max) - static_cast<uint64_t>(*min) <= kMaxValueRange;
  }
  return false;
}

// pick counting or sorting approach per integers value range
template <typename T>
struct CountOrSortModer {
  using CType = typename T::c_type;

  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& values = batch[0].array;
    CType min, max;
    if (ShouldUseCountMode<CType>(values, values.length - values.GetNullCount(), &min,
                                  &max)) {
      return CountModer<T>(min, max).Exec(ctx, batch, out);
    }
    return SortModer<T>().Exec(ctx, batch, out);
  }

  Status ExecChunked(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ChunkedArray& values = *batch[0].chunked_array();
    CType min, max;
    if (ShouldUseCountMode<CType>(values, values.length() - values.null_count(), &min,
                                  &max)) {
      return CountModer<T>(min, max).ExecChunked(ctx, batch, out);
    }
    return SortModer<T>().ExecChunked(ctx, batch, out);
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
struct Moder<InType, enable_if_floating_point<InType>> {
  SortModer<InType> impl;
};

template <typename InType>
struct Moder<InType, enable_if_decimal<InType>> {
  SortModer<InType> impl;
};

Status CheckOptions(KernelContext* ctx) {
  if (ctx->state() == nullptr) {
    return Status::Invalid("Mode requires ModeOptions");
  }
  const ModeOptions& options = ModeState::Get(ctx);
  if (options.n <= 0) {
    return Status::Invalid("ModeOptions::n must be strictly positive");
  }
  return Status::OK();
}

template <typename OutTypeUnused, typename InType>
struct ModeExecutor {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    RETURN_NOT_OK(CheckOptions(ctx));
    return Moder<InType>().impl.Exec(ctx, batch, out);
  }
};

template <typename OutTypeUnused, typename InType>
struct ModeExecutorChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    RETURN_NOT_OK(CheckOptions(ctx));
    return Moder<InType>().impl.ExecChunked(ctx, batch, out);
  }
};

Result<TypeHolder> ModeType(KernelContext*, const std::vector<TypeHolder>& types) {
  return struct_(
      {field(kModeFieldName, types[0].GetSharedPtr()), field(kCountFieldName, int64())});
}

VectorKernel NewModeKernel(const std::shared_ptr<DataType>& in_type, ArrayKernelExec exec,
                           VectorKernel::ChunkedExec exec_chunked) {
  VectorKernel kernel;
  kernel.init = ModeState::Init;
  kernel.can_execute_chunkwise = false;
  kernel.output_chunked = false;
  switch (in_type->id()) {
    case Type::DECIMAL128:
    case Type::DECIMAL256:
      kernel.signature =
          KernelSignature::Make({InputType(in_type->id())}, OutputType(ModeType));
      break;
    default: {
      auto out_type =
          struct_({field(kModeFieldName, in_type), field(kCountFieldName, int64())});
      kernel.signature = KernelSignature::Make({in_type->id()}, std::move(out_type));
      break;
    }
  }
  kernel.exec = std::move(exec);
  kernel.exec_chunked = exec_chunked;
  return kernel;
}

const FunctionDoc mode_doc{
    "Compute the modal (most common) values of a numeric array",
    ("Compute the n most common values and their respective occurrence counts.\n"
     "The output has type `struct<mode: T, count: int64>`, where T is the\n"
     "input type.\n"
     "The results are ordered by descending `count` first, and ascending `mode`\n"
     "when breaking ties.\n"
     "Nulls are ignored.  If there are no non-null values in the array,\n"
     "an empty array is returned."),
    {"array"},
    "ModeOptions"};

}  // namespace

void RegisterScalarAggregateMode(FunctionRegistry* registry) {
  static auto default_options = ModeOptions::Defaults();
  auto func = std::make_shared<VectorFunction>("mode", Arity::Unary(), mode_doc,
                                               &default_options);
  DCHECK_OK(func->AddKernel(
      NewModeKernel(boolean(), ModeExecutor<StructType, BooleanType>::Exec,
                    ModeExecutorChunked<StructType, BooleanType>::Exec)));
  for (const auto& type : NumericTypes()) {
    // TODO(wesm):
    DCHECK_OK(func->AddKernel(NewModeKernel(
        type, GenerateNumeric<ModeExecutor, StructType>(*type),
        GenerateNumeric<ModeExecutorChunked, StructType, VectorKernel::ChunkedExec>(
            *type))));
  }
  // Type parameters are ignored
  DCHECK_OK(func->AddKernel(
      NewModeKernel(decimal128(1, 0), ModeExecutor<StructType, Decimal128Type>::Exec,
                    ModeExecutorChunked<StructType, Decimal128Type>::Exec)));
  DCHECK_OK(func->AddKernel(
      NewModeKernel(decimal256(1, 0), ModeExecutor<StructType, Decimal256Type>::Exec,
                    ModeExecutorChunked<StructType, Decimal256Type>::Exec)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
