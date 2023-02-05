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
#include "arrow/dataset/test_util_internal.h"
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

void MinimalEndToEndScan(
    size_t num_batches, size_t batch_size, const std::string& factory_name,
    std::function<Result<std::shared_ptr<compute::ExecNodeOptions>>(size_t, size_t)>
        options_factory) {
  // ensure arrow::dataset node factories are in the registry
  ::arrow::dataset::internal::Initialize();

  // A ScanNode is constructed from a Dataset (whose batches will be scanned), and
  // ScanOptions (to specify a filter for predicate pushdown, a projection to skip
  // materialization of unnecessary columns,
  // ...)
  RecordBatchVector batches = GetBatches(num_batches, batch_size);

  std::shared_ptr<Dataset> dataset =
      std::make_shared<InMemoryDataset>(GetSchema(), batches);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecNodeOptions> node_options,
                       options_factory(num_batches, batch_size));

  // construct the scan node
  compute::Declaration scan(factory_name, std::move(node_options));

  // pipe the scan node into a filter node
  compute::Expression b_is_true = equal(field_ref("b"), literal(true));
  compute::Declaration filter("filter", {std::move(scan)},
                              compute::FilterNodeOptions{b_is_true});

  // pipe the filter node into a project node
  // NB: we're using the project node factory which preserves fragment/batch index
  // tagging, so we *can* reorder later if we choose. The tags will not appear in
  // our output.
  compute::Expression a_times_2 = call("multiply", {field_ref("a"), literal(2)});
  compute::Declaration project("project", {std::move(filter)},
                               compute::ProjectNodeOptions{{a_times_2}, {"a*2"}});

  // Consume the plan and transform into a table
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> collected,
                       compute::DeclarationToTable(std::move(project)));

  ASSERT_GT(collected->num_rows(), 0);
}

void ScanOnly(
    size_t num_batches, size_t batch_size, const std::string& factory_name,
    std::function<Result<std::shared_ptr<compute::ExecNodeOptions>>(size_t, size_t)>
        options_factory) {
  // ensure arrow::dataset node factories are in the registry
  ::arrow::dataset::internal::Initialize();

  RecordBatchVector batches = GetBatches(num_batches, batch_size);

  std::shared_ptr<Dataset> dataset =
      std::make_shared<InMemoryDataset>(GetSchema(), batches);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecNodeOptions> node_options,
                       options_factory(num_batches, batch_size));

  // construct the plan
  compute::Declaration scan(factory_name, std::move(node_options));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> collected,
                       compute::DeclarationToTable(std::move(scan)));

  ASSERT_GT(collected->num_rows(), 0);
  ASSERT_EQ(collected->num_columns(), 2);
}

static constexpr int kScanIdx = 0;
static constexpr int kScanV2Idx = 1;

const std::function<Result<std::shared_ptr<compute::ExecNodeOptions>>(size_t, size_t)>
    kScanFactory = [](size_t num_batches, size_t batch_size) {
      RecordBatchVector batches = GetBatches(num_batches, batch_size);
      std::shared_ptr<Dataset> dataset =
          std::make_shared<InMemoryDataset>(GetSchema(), std::move(batches));

      std::shared_ptr<ScanOptions> options = std::make_shared<ScanOptions>();
      // specify the filter
      compute::Expression b_is_true = equal(field_ref("b"), literal(true));
      options->filter = b_is_true;
      options->projection = call("make_struct", {field_ref("a"), field_ref("b")},
                                 compute::MakeStructOptions{{"a", "b"}});

      return std::make_shared<ScanNodeOptions>(std::move(dataset), std::move(options));
    };

const std::function<Result<std::shared_ptr<compute::ExecNodeOptions>>(size_t, size_t)>
    kScanV2Factory =
        [](size_t num_batches,
           size_t batch_size) -> Result<std::shared_ptr<compute::ExecNodeOptions>> {
  RecordBatchVector batches = GetBatches(num_batches, batch_size);
  std::shared_ptr<Schema> sch = GetSchema();
  std::shared_ptr<Dataset> dataset =
      std::make_shared<InMemoryDataset>(sch, std::move(batches));

  std::shared_ptr<ScanV2Options> options = std::make_shared<ScanV2Options>(dataset);
  // specify the filter
  compute::Expression b_is_true = equal(field_ref("b"), literal(true));
  options->filter = b_is_true;
  options->columns = ScanV2Options::AllColumns(*dataset->schema());

  return options;
};

static void MinimalEndToEndBench(benchmark::State& state) {
  size_t num_batches = state.range(0);
  size_t batch_size = state.range(1);

  std::function<Result<std::shared_ptr<compute::ExecNodeOptions>>(size_t, size_t)>
      options_factory;
  std::string scan_factory = "scan";
  if (state.range(2) == kScanIdx) {
    options_factory = kScanFactory;
  } else if (state.range(2) == kScanV2Idx) {
    options_factory = kScanV2Factory;
    scan_factory = "scan2";
  }

  for (auto _ : state) {
    MinimalEndToEndScan(num_batches, batch_size, scan_factory, options_factory);
  }

  state.SetItemsProcessed(state.iterations() * num_batches);
  state.SetBytesProcessed(state.iterations() * num_batches * batch_size *
                          GetBytesForSchema());
}

static void ScanOnlyBench(benchmark::State& state) {
  size_t num_batches = state.range(0);
  size_t batch_size = state.range(1);

  std::function<Result<std::shared_ptr<compute::ExecNodeOptions>>(size_t, size_t)>
      options_factory;
  std::string scan_factory = "scan";
  if (state.range(2) == kScanIdx) {
    options_factory = kScanFactory;
  } else if (state.range(2) == kScanV2Idx) {
    options_factory = kScanV2Factory;
    scan_factory = "scan2";
  }

  for (auto _ : state) {
    ScanOnly(num_batches, batch_size, scan_factory, options_factory);
  }
  state.SetItemsProcessed(state.iterations() * num_batches);
  state.SetBytesProcessed(state.iterations() * num_batches * batch_size *
                          GetBytesForSchema());
}

static void ScanBenchmark_Customize(benchmark::internal::Benchmark* b) {
  for (const int32_t num_batches : {1000}) {
    for (const int batch_size : {10, 100, 1000}) {
      for (const int scan_idx : {kScanIdx, kScanV2Idx}) {
        b->Args({num_batches, batch_size, scan_idx});
        RecordBatchVector batches =
            ::arrow::compute::GenerateBatches(GetSchema(), num_batches, batch_size);
        StoreBatches(num_batches, batch_size, batches);
      }
    }
  }
  b->ArgNames({"num_batches", "batch_size", "scan_alg"});
  b->UseRealTime();
}

BENCHMARK(MinimalEndToEndBench)->Apply(ScanBenchmark_Customize);
BENCHMARK(ScanOnlyBench)->Apply(ScanBenchmark_Customize);

}  // namespace dataset
}  // namespace arrow
