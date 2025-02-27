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

#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"
#include "benchmark/benchmark.h"

namespace arrow::compute {

constexpr auto kSeed = 0x94378165;

const auto kSliceStart = 2;
const auto kSliceStop = 10;

static void BenchmarkListSlice(benchmark::State& state, const ListSliceOptions& opts,
                               std::shared_ptr<DataType> list_ty) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = rand.ArrayOf(std::move(list_ty), args.size, args.null_proportion);
  auto ctx = default_exec_context();
  std::vector<Datum> input_args = {std::move(array)};
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("list_slice", input_args, &opts, ctx).status());
  }
}

template <typename InListType = ListType>
static void ListSliceInt64List(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  BenchmarkListSlice(state, opts, std::make_shared<InListType>(int64()));
}

template <typename InListType = ListType>
static void ListSliceStringList(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  BenchmarkListSlice(state, opts, std::make_shared<InListType>(utf8()));
}

template <typename InListType = ListType>
static void ListSliceInt64ListWithStop(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  opts.stop = kSliceStop;
  BenchmarkListSlice(state, opts, std::make_shared<InListType>(int64()));
}

template <typename InListType = ListType>
static void ListSliceStringListWithStop(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  opts.stop = kSliceStop;
  BenchmarkListSlice(state, opts, std::make_shared<InListType>(utf8()));
}

template <typename InListType = ListType>
static void ListSliceInt64ListWithStepAndStop(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  opts.step = 2;
  opts.stop = kSliceStop;
  BenchmarkListSlice(state, opts, std::make_shared<InListType>(int64()));
}

template <typename InListType = ListType>
static void ListSliceStringListWithStepAndStop(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  opts.step = 2;
  opts.stop = kSliceStop;
  BenchmarkListSlice(state, opts, std::make_shared<InListType>(utf8()));
}

static void ListSliceInt64ListView(benchmark::State& state) {
  ListSliceInt64List<ListViewType>(state);
}

static void ListSliceStringListView(benchmark::State& state) {
  ListSliceStringList<ListViewType>(state);
}

static void ListSliceInt64ListViewWithStop(benchmark::State& state) {
  ListSliceInt64ListWithStop<ListViewType>(state);
}

static void ListSliceStringListViewWithStop(benchmark::State& state) {
  ListSliceStringListWithStop<ListViewType>(state);
}

static void ListSliceInt64ListViewWithStepAndStop(benchmark::State& state) {
  ListSliceInt64ListWithStepAndStop<ListViewType>(state);
}

static void ListSliceStringListViewWithStepAndStop(benchmark::State& state) {
  ListSliceStringListWithStepAndStop<ListViewType>(state);
}

static void ListSliceInt64ListToFSL(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  opts.stop = kSliceStop;
  opts.return_fixed_size_list = true;
  BenchmarkListSlice(state, opts, std::make_shared<ListType>(int64()));
}

static void ListSliceStringListToFSL(benchmark::State& state) {
  ListSliceOptions opts;
  opts.start = kSliceStart;
  opts.stop = kSliceStop;
  opts.return_fixed_size_list = true;
  BenchmarkListSlice(state, opts, std::make_shared<ListType>(utf8()));
}

BENCHMARK(ListSliceInt64List)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringList)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceInt64ListWithStop)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringListWithStop)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceInt64ListWithStepAndStop)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringListWithStepAndStop)->Apply(RegressionSetArgs);

BENCHMARK(ListSliceInt64ListView)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringListView)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceInt64ListViewWithStop)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringListViewWithStop)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceInt64ListViewWithStepAndStop)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringListViewWithStepAndStop)->Apply(RegressionSetArgs);

BENCHMARK(ListSliceInt64ListToFSL)->Apply(RegressionSetArgs);
BENCHMARK(ListSliceStringListToFSL)->Apply(RegressionSetArgs);

}  // namespace arrow::compute
