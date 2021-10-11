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

#include "benchmark/benchmark.h"

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;

void GenerateBatchesFromSchema(const std::shared_ptr<Schema>& schema, size_t num_batches,
                               BatchesWithSchema* out_batches, int multiplicity = 1,
                               int64_t batch_size = 4) {
  ::arrow::random::RandomArrayGenerator rng_(kSeed);
  if (num_batches == 0) {
    auto empty_record_batch = ExecBatch(*rng_.BatchOf(schema->fields(), 0));
    out_batches->batches.push_back(empty_record_batch);
  } else {
    for (size_t j = 0; j < num_batches; j++) {
      out_batches->batches.push_back(
          ExecBatch(*rng_.BatchOf(schema->fields(), batch_size)));
    }
  }

  size_t batch_count = out_batches->batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches->batches.push_back(out_batches->batches[i]);
    }
  }
  out_batches->schema = schema;
}

RecordBatchVector GenerateBatches(const std::shared_ptr<Schema>& schema,
                                  size_t num_batches, size_t batch_size) {
  BatchesWithSchema input_batches;

  RecordBatchVector batches;
  GenerateBatchesFromSchema(schema, num_batches, &input_batches, 1, batch_size);

  for (const auto& batch : input_batches.batches) {
    batches.push_back(batch.ToRecordBatch(schema).MoveValueUnsafe());
  }
  return batches;
}

}  // namespace compute

namespace dataset {

static std::map<std::pair<size_t, size_t>, RecordBatchVector> datasets;

void StoreBatches(size_t num_batches, size_t batch_size,
                  const RecordBatchVector& batches) {
  datasets[std::make_pair(num_batches, batch_size)] = batches;
}

RecordBatchVector GetBatches(size_t num_batches, size_t batch_size) {
  auto iter = datasets.find(std::make_pair(num_batches, batch_size));
  if (iter == datasets.end()) {
    return RecordBatchVector{};
  }
  return iter->second;
}

std::shared_ptr<Schema> GetSchema() {
  static std::shared_ptr<Schema> s = schema({field("a", int32()), field("b", boolean())});
  return s;
}

size_t GetBytesForSchema() { return sizeof(int32_t) + sizeof(bool); }

void MinimalEndToEndScan(size_t num_batches, size_t batch_size, bool async_mode) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(),
                                    ::arrow::internal::GetCpuThreadPool());

  // ensure arrow::dataset node factories are in the registry
  ::arrow::dataset::internal::Initialize();

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns,
  // ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecPlan> plan,
                       compute::ExecPlan::Make(&exec_context));

  RecordBatchVector batches = GetBatches(num_batches, batch_size);

  std::shared_ptr<Dataset> dataset =
      std::make_shared<InMemoryDataset>(GetSchema(), batches);

  auto options = std::make_shared<ScanOptions>();
  // sync scanning is not supported by ScanNode
  options->use_async = true;
  // specify the filter
  compute::Expression b_is_true = field_ref("b");
  options->filter = b_is_true;
  // for now, specify the projection as the full project expression (eventually this can
  // just be a list of materialized field names)
  compute::Expression a_times_2 = call("multiply", {field_ref("a"), literal(2)});
  options->projection =
      call("make_struct", {a_times_2}, compute::MakeStructOptions{{"a * 2"}});

  // construct the scan node
  ASSERT_OK_AND_ASSIGN(
      compute::ExecNode * scan,
      compute::MakeExecNode("scan", plan.get(), {}, ScanNodeOptions{dataset, options}));

  // pipe the scan node into a filter node
  ASSERT_OK_AND_ASSIGN(
      compute::ExecNode * filter,
      compute::MakeExecNode("filter", plan.get(), {scan},
                            compute::FilterNodeOptions{b_is_true, async_mode}));

  // pipe the filter node into a project node
  // NB: we're using the project node factory which preserves fragment/batch index
  // tagging, so we *can* reorder later if we choose. The tags will not appear in
  // our output.
  ASSERT_OK_AND_ASSIGN(
      compute::ExecNode * project,
      compute::MakeExecNode("augmented_project", plan.get(), {filter},
                            compute::ProjectNodeOptions{{a_times_2}, {}, async_mode}));

  // finally, pipe the project node into a sink node
  AsyncGenerator<util::optional<compute::ExecBatch>> sink_gen;
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * sink,
                       compute::MakeExecNode("sink", plan.get(), {project},
                                             compute::SinkNodeOptions{&sink_gen}));

  ASSERT_NE(sink, nullptr);

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      schema({field("a * 2", int32())}), std::move(sink_gen), exec_context.memory_pool());

  // start the ExecPlan
  ASSERT_OK(plan->StartProducing());

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  ASSERT_GT(collected->num_rows(), 0);

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";
}

static void MinimalEndToEndBench(benchmark::State& state) {
  size_t num_batches = state.range(0);
  size_t batch_size = state.range(1);
  bool async_mode = state.range(2);

  for (auto _ : state) {
    MinimalEndToEndScan(num_batches, batch_size, async_mode);
  }
  state.SetItemsProcessed(state.iterations() * num_batches);
  state.SetBytesProcessed(state.iterations() * num_batches * batch_size *
                          GetBytesForSchema());
}

static const std::vector<int32_t> kWorkload = {100, 1000, 10000, 100000};

static void MinimalEndToEnd_Customize(benchmark::internal::Benchmark* b) {
  for (const int32_t num_batches : kWorkload) {
    for (const int batch_size : {10, 100, 1000}) {
      for (const bool async_mode : {true, false}) {
        b->Args({num_batches, batch_size, async_mode});
        RecordBatchVector batches =
            ::arrow::compute::GenerateBatches(GetSchema(), num_batches, batch_size);
        StoreBatches(num_batches, batch_size, batches);
      }
    }
  }
  b->ArgNames({"num_batches", "batch_size", "async_mode"});
  b->UseRealTime();
}

BENCHMARK(MinimalEndToEndBench)->Apply(MinimalEndToEnd_Customize);

}  // namespace dataset
}  // namespace arrow
