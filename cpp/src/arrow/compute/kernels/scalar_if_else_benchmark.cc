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

#include <benchmark/benchmark.h>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/function.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

const int64_t kNumItems = 1024 * 1024;
const int64_t kFewItems = 64 * 1024;

struct GetBytesProcessedVisitor {
  const Array* arr;
  int64_t total_bytes = 0;

  explicit GetBytesProcessedVisitor(const Array* arr) : arr(arr) {}

  Status RecurseInto(const Array* child_arr) {
    GetBytesProcessedVisitor visitor(child_arr);
    RETURN_NOT_OK(VisitTypeInline(*child_arr->type(), &visitor));
    total_bytes += visitor.total_bytes;
    return Status::OK();
  }

  Status Visit(const BooleanType& type) {
    total_bytes = arr->length() / 8;
    return Status::OK();
  }

  template <typename Type>
  std::enable_if_t<is_number_type<Type>::value, Status> Visit(const Type& type) {
    using CType = typename Type::c_type;
    total_bytes += arr->length() * sizeof(CType);
    return Status::OK();
  }

  template <typename Type>
  std::enable_if_t<is_base_binary_type<Type>::value, Status> Visit(const Type& type) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    using OffsetType = typename TypeTraits<Type>::OffsetType::c_type;
    total_bytes += arr->length() * sizeof(OffsetType) +
                   internal::checked_cast<const ArrayType*>(arr)->total_values_length();
    return Status::OK();
  }

  template <typename ArrowType>
  enable_if_var_length_list_like<ArrowType, Status> Visit(const ArrowType& type) {
    using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
    using OffsetType = typename TypeTraits<ArrowType>::OffsetType::c_type;

    const auto num_offsets = is_list_view(type) ? arr->length() : arr->length() + 1;
    total_bytes += num_offsets * sizeof(OffsetType);
    // NOTE: the sizes buffer is not counted when type is a list-view as that
    // can make the throughput numbers look better just because the sizes
    // increase the number of bytes in the input.
    auto child_array = internal::checked_cast<const ArrayType*>(arr)->values();
    return RecurseInto(child_array.get());
  }

  Status Visit(const DataType& type) { return Unimplemented(type); }

  Status Unimplemented(const DataType& type) {
    return Status::NotImplemented("GetBytesProcessed not implemented for ", type);
  }
};

int64_t GetBytesProcessed(const Array& arr) {
  GetBytesProcessedVisitor visitor(&arr);
  ARROW_EXPECT_OK(VisitTypeInline(*arr.type(), &visitor));
  return visitor.total_bytes;
}

template <typename Type>
static void IfElseBench(benchmark::State& state, const std::shared_ptr<DataType>& type) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  random::RandomArrayGenerator rand(/*seed=*/0);

  auto cond = std::static_pointer_cast<BooleanArray>(
                  rand.ArrayOf(boolean(), len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto left = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto right = std::static_pointer_cast<ArrayType>(
                   rand.ArrayOf(type, len, /*null_probability=*/0.01))
                   ->Slice(offset);

  for (auto _ : state) {
    ABORT_NOT_OK(IfElse(cond, left, right));
  }

  state.SetBytesProcessed(
      state.iterations() *
      (GetBytesProcessed(*cond) + GetBytesProcessed(*left) + GetBytesProcessed(*right)));
  state.SetItemsProcessed(state.iterations() * (len - offset));
  state.counters["offset"] = static_cast<double>(offset);
}

template <typename Type>
static void IfElseBench(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  return IfElseBench<Type>(state, type);
}

template <typename ListType, typename ValueType>
static void IfElseBenchVarLengthListLike(benchmark::State& state) {
  auto value_type = TypeTraits<ValueType>::type_singleton();
  auto list_type = std::make_shared<ListType>(value_type);
  return IfElseBench<ListType>(state, list_type);
}

template <typename Type>
static void IfElseBenchContiguous(benchmark::State& state,
                                  const std::shared_ptr<DataType>& type) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  ASSERT_OK_AND_ASSIGN(auto temp1, MakeArrayFromScalar(BooleanScalar(true), len / 2));
  ASSERT_OK_AND_ASSIGN(auto temp2,
                       MakeArrayFromScalar(BooleanScalar(false), len - len / 2));
  ASSERT_OK_AND_ASSIGN(auto concat, Concatenate({temp1, temp2}));
  auto cond = std::static_pointer_cast<BooleanArray>(concat)->Slice(offset);

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto left = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto right = std::static_pointer_cast<ArrayType>(
                   rand.ArrayOf(type, len, /*null_probability=*/0.01))
                   ->Slice(offset);

  for (auto _ : state) {
    ABORT_NOT_OK(IfElse(cond, left, right));
  }

  state.SetBytesProcessed(
      state.iterations() *
      (GetBytesProcessed(*cond) + GetBytesProcessed(*left) + GetBytesProcessed(*right)));
  state.SetItemsProcessed(state.iterations() * (len - offset));
  state.counters["offset"] = static_cast<double>(offset);
}

template <typename Type>
static void IfElseBenchContiguous(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  return IfElseBenchContiguous<Type>(state, type);
}

template <typename ListType, typename ValueType>
static void IfElseBenchVarLengthListLikeContiguous(benchmark::State& state) {
  auto value_type = TypeTraits<ValueType>::type_singleton();
  auto list_type = std::make_shared<ListType>(value_type);
  return IfElseBenchContiguous<ListType>(state, list_type);
}

static void IfElseBench64(benchmark::State& state) {
  return IfElseBench<UInt64Type>(state);
}

static void IfElseBench32(benchmark::State& state) {
  return IfElseBench<UInt32Type>(state);
}

static void IfElseBenchListUInt32(benchmark::State& state) {
  return IfElseBenchVarLengthListLike<ListType, UInt32Type>(state);
}

static void IfElseBenchListString32(benchmark::State& state) {
  return IfElseBenchVarLengthListLike<ListType, StringType>(state);
}

static void IfElseBenchString32(benchmark::State& state) {
  return IfElseBench<StringType>(state);
}

static void IfElseBenchString64(benchmark::State& state) {
  return IfElseBench<LargeStringType>(state);
}

static void IfElseBench64Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt64Type>(state);
}

static void IfElseBench32Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt32Type>(state);
}

static void IfElseBenchListUInt32Contiguous(benchmark::State& state) {
  return IfElseBenchVarLengthListLikeContiguous<ListType, UInt32Type>(state);
}

static void IfElseBenchListString32Contiguous(benchmark::State& state) {
  return IfElseBenchVarLengthListLikeContiguous<ListType, StringType>(state);
}

static void IfElseBenchListViewUInt32(benchmark::State& state) {
  return IfElseBenchVarLengthListLike<ListViewType, UInt32Type>(state);
}

static void IfElseBenchListViewString32(benchmark::State& state) {
  return IfElseBenchVarLengthListLike<ListViewType, StringType>(state);
}

static void IfElseBenchListViewUInt32Contiguous(benchmark::State& state) {
  return IfElseBenchVarLengthListLikeContiguous<ListViewType, UInt32Type>(state);
}

static void IfElseBenchListViewString32Contiguous(benchmark::State& state) {
  return IfElseBenchVarLengthListLikeContiguous<ListViewType, StringType>(state);
}

static void IfElseBenchString64Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt64Type>(state);
}

static void IfElseBenchString32Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt32Type>(state);
}

template <typename Type>
static void CaseWhenBench(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  random::RandomArrayGenerator rand(/*seed=*/0);

  auto cond_field =
      field("cond", boolean(), key_value_metadata({{"null_probability", "0.01"}}));
  auto cond = rand.ArrayOf(*field("", struct_({cond_field, cond_field, cond_field}),
                                  key_value_metadata({{"null_probability", "0.0"}})),
                           len)
                  ->Slice(offset);
  auto val1 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto val2 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto val3 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto val4 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  for (auto _ : state) {
    ABORT_NOT_OK(CaseWhen(cond, {val1, val2, val3, val4}));
  }

  // Set bytes processed to ~length of output
  state.SetBytesProcessed(state.iterations() * GetBytesProcessed(*val1));
  state.SetItemsProcessed(state.iterations() * (len - offset));
}

static void CaseWhenBenchList(benchmark::State& state) {
  auto type = list(int64());
  auto fld = field("", type);

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  random::RandomArrayGenerator rand(/*seed=*/0);

  auto cond_field =
      field("cond", boolean(), key_value_metadata({{"null_probability", "0.01"}}));
  auto cond = rand.ArrayOf(*field("", struct_({cond_field, cond_field, cond_field}),
                                  key_value_metadata({{"null_probability", "0.0"}})),
                           len);
  auto val1 = rand.ArrayOf(*fld, len);
  auto val2 = rand.ArrayOf(*fld, len);
  auto val3 = rand.ArrayOf(*fld, len);
  auto val4 = rand.ArrayOf(*fld, len);
  for (auto _ : state) {
    ABORT_NOT_OK(
        CaseWhen(cond->Slice(offset), {val1->Slice(offset), val2->Slice(offset),
                                       val3->Slice(offset), val4->Slice(offset)}));
  }

  // Set bytes processed to ~length of output
  state.SetBytesProcessed(
      state.iterations() *
      GetBytesProcessed(*std::static_pointer_cast<ListArray>(val1)->values()));
  state.SetItemsProcessed(state.iterations() * (len - offset));
}

template <typename Type>
static void CaseWhenBenchContiguous(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  ASSERT_OK_AND_ASSIGN(auto trues, MakeArrayFromScalar(BooleanScalar(true), len / 3));
  ASSERT_OK_AND_ASSIGN(auto falses, MakeArrayFromScalar(BooleanScalar(false), len / 3));
  ASSERT_OK_AND_ASSIGN(auto nulls, MakeArrayOfNull(boolean(), len - 2 * (len / 3)));
  ASSERT_OK_AND_ASSIGN(auto concat, Concatenate({trues, falses, nulls}));
  auto cond1 = std::static_pointer_cast<BooleanArray>(concat);

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto cond2 = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), len, /*null_probability=*/0.01));
  auto val1 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto val2 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto val3 = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Array> cond,
      StructArray::Make({cond1, cond2}, std::vector<std::string>{"a", "b"}, nullptr,
                        /*null_count=*/0));
  cond = cond->Slice(offset);

  for (auto _ : state) {
    ABORT_NOT_OK(CaseWhen(cond, {val1, val2, val3}));
  }

  // Set bytes processed to ~length of output
  state.SetBytesProcessed(state.iterations() * GetBytesProcessed(*val1));
  state.SetItemsProcessed(state.iterations() * (len - offset));
}

static void CaseWhenBench64(benchmark::State& state) {
  return CaseWhenBench<UInt64Type>(state);
}

static void CaseWhenBench64Contiguous(benchmark::State& state) {
  return CaseWhenBenchContiguous<UInt64Type>(state);
}

static void CaseWhenBenchString(benchmark::State& state) {
  return CaseWhenBench<StringType>(state);
}

static void CaseWhenBenchStringContiguous(benchmark::State& state) {
  return CaseWhenBenchContiguous<StringType>(state);
}

struct CoalesceParams {
  int64_t length;
  int64_t num_arguments;
  double null_probability;
};

std::vector<CoalesceParams> g_coalesce_params = {
    {kNumItems, 2, 0.01}, {kNumItems, 4, 0.01}, {kNumItems, 2, 0.25},
    {kNumItems, 4, 0.25}, {kNumItems, 2, 0.50}, {kNumItems, 4, 0.50},
    {kNumItems, 2, 0.99}, {kNumItems, 4, 0.99},
};

struct CoalesceArgs : public CoalesceParams {
  explicit CoalesceArgs(benchmark::State& state) : state_(state) {
    const auto& params = g_coalesce_params[state.range(0)];
    length = params.length;
    num_arguments = params.num_arguments;
    null_probability = params.null_probability;
  }

  ~CoalesceArgs() {
    state_.counters["length"] = static_cast<double>(length);
    state_.counters["null%"] = null_probability * 100;
    state_.counters["num_args"] = static_cast<double>(num_arguments);
  }

 private:
  benchmark::State& state_;
};

template <typename Type>
static void CoalesceBench(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  CoalesceArgs params(state);
  random::RandomArrayGenerator rand(/*seed=*/0);

  std::vector<Datum> arguments;
  for (int i = 0; i < params.num_arguments; i++) {
    arguments.emplace_back(rand.ArrayOf(type, params.length, params.null_probability));
  }

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("coalesce", arguments));
  }

  state.SetBytesProcessed(state.iterations() *
                          GetBytesProcessed(*arguments.front().make_array()));
  state.SetItemsProcessed(state.iterations() * params.length);
}

template <typename Type>
static void CoalesceScalarBench(benchmark::State& state) {
  using CType = typename Type::c_type;
  auto type = TypeTraits<Type>::type_singleton();
  CoalesceArgs params(state);
  random::RandomArrayGenerator rand(/*seed=*/0);

  std::vector<Datum> arguments = {
      rand.ArrayOf(type, params.length, params.null_probability),
      Datum(CType(42)),
  };

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("coalesce", arguments));
  }

  state.SetBytesProcessed(state.iterations() *
                          GetBytesProcessed(*arguments.front().make_array()));
  state.SetItemsProcessed(state.iterations() * params.length);
}

static void CoalesceScalarStringBench(benchmark::State& state) {
  CoalesceArgs params(state);
  random::RandomArrayGenerator rand(/*seed=*/0);

  auto arr = rand.ArrayOf(utf8(), params.length, params.null_probability);
  std::vector<Datum> arguments = {arr, Datum("foobar")};

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("coalesce", arguments));
  }

  state.SetBytesProcessed(state.iterations() *
                          GetBytesProcessed(*arguments.front().make_array()));
  state.SetItemsProcessed(state.iterations() * params.length);
}

static void CoalesceBench64(benchmark::State& state) {
  return CoalesceBench<Int64Type>(state);
}

static void CoalesceScalarBench64(benchmark::State& state) {
  return CoalesceScalarBench<Int64Type>(state);
}

template <typename Type>
static void ChooseBench(benchmark::State& state) {
  constexpr int kNumChoices = 5;
  auto type = TypeTraits<Type>::type_singleton();

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  random::RandomArrayGenerator rand(/*seed=*/0);

  std::vector<Datum> arguments;
  arguments.emplace_back(
      rand.Int64(len, /*min=*/0, /*max=*/kNumChoices - 1, /*null_probability=*/0.1)
          ->Slice(offset));
  for (int i = 0; i < kNumChoices; i++) {
    arguments.emplace_back(
        rand.ArrayOf(type, len, /*null_probability=*/0.25)->Slice(offset));
  }

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("choose", arguments));
  }

  state.SetBytesProcessed(state.iterations() *
                          GetBytesProcessed(*arguments[1].make_array()));
  state.SetItemsProcessed(state.iterations() * (len - offset));
}

static void ChooseBench64(benchmark::State& state) {
  return ChooseBench<Int64Type>(state);
}

// IfElse: Numerics
BENCHMARK(IfElseBench32)->Args({kNumItems, 0});
BENCHMARK(IfElseBench64)->Args({kNumItems, 0});
BENCHMARK(IfElseBench32)->Args({kNumItems, 99});
BENCHMARK(IfElseBench64)->Args({kNumItems, 99});
BENCHMARK(IfElseBench32Contiguous)->Args({kNumItems, 0});
BENCHMARK(IfElseBench64Contiguous)->Args({kNumItems, 0});
BENCHMARK(IfElseBench32Contiguous)->Args({kNumItems, 99});
BENCHMARK(IfElseBench64Contiguous)->Args({kNumItems, 99});

// IfElse: Lists
BENCHMARK(IfElseBenchListUInt32)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchListString32)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchListUInt32Contiguous)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchListString32Contiguous)->Args({kNumItems, 0});

// IfElse: ListViews
BENCHMARK(IfElseBenchListViewUInt32)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchListViewString32)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchListViewUInt32Contiguous)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchListViewString32Contiguous)->Args({kNumItems, 0});

// IfElse: Strings
BENCHMARK(IfElseBenchString32)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchString64)->Args({kNumItems, 0});
BENCHMARK(IfElseBenchString32Contiguous)->Args({kNumItems, 99});
BENCHMARK(IfElseBenchString64Contiguous)->Args({kNumItems, 99});

// CaseWhen: Numerics
BENCHMARK(CaseWhenBench64)->Args({kNumItems, 0});
BENCHMARK(CaseWhenBench64)->Args({kNumItems, 99});
BENCHMARK(CaseWhenBench64Contiguous)->Args({kNumItems, 0});
BENCHMARK(CaseWhenBench64Contiguous)->Args({kNumItems, 99});

// CaseWhen: Lists
BENCHMARK(CaseWhenBenchList)->Args({kFewItems, 0});
BENCHMARK(CaseWhenBenchList)->Args({kFewItems, 99});

// CaseWhen: Strings
BENCHMARK(CaseWhenBenchString)->Args({kFewItems, 0});
BENCHMARK(CaseWhenBenchString)->Args({kFewItems, 99});
BENCHMARK(CaseWhenBenchStringContiguous)->Args({kFewItems, 0});
BENCHMARK(CaseWhenBenchStringContiguous)->Args({kFewItems, 99});

void CoalesceSetArgs(benchmark::internal::Benchmark* bench) {
  for (size_t i = 0; i < g_coalesce_params.size(); i++) {
    bench->Args({static_cast<int64_t>(i)});
  }
}
void CoalesceSetBinaryArgs(benchmark::internal::Benchmark* bench) {
  for (size_t i = 0; i < g_coalesce_params.size(); i++) {
    if (g_coalesce_params[i].num_arguments == 2) {
      bench->Args({static_cast<int64_t>(i)});
    }
  }
}
BENCHMARK(CoalesceBench64)->Apply(CoalesceSetArgs);
BENCHMARK(CoalesceScalarBench64)->Apply(CoalesceSetBinaryArgs);
BENCHMARK(CoalesceScalarStringBench)->Apply(CoalesceSetBinaryArgs);

BENCHMARK(ChooseBench64)->Args({kNumItems, 0});
BENCHMARK(ChooseBench64)->Args({kNumItems, 99});

}  // namespace compute
}  // namespace arrow
