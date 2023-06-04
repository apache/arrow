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

#include "arrow/dataset/scanner.h"

#include <memory>
#include <mutex>
#include <utility>

#include <gmock/gmock.h>

#include "arrow/acero/exec_plan.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/expression_internal.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/async_test_util.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/util.h"
#include "arrow/util/byte_size.h"
#include "arrow/util/range.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

#include "arrow/dataset/file_ipc.h"
#include "arrow/ipc/writer.h"

using testing::ElementsAre;
using testing::UnorderedElementsAreArray;

namespace arrow {

using internal::GetCpuThreadPool;
using internal::Iota;

namespace dataset {

// The basic evolution strategy doesn't really need any info from the dataset
// or the fragment other than the schema so we just make a dummy dataset/fragment
// here.
std::unique_ptr<Dataset> MakeDatasetFromSchema(std::shared_ptr<Schema> sch) {
  return std::make_unique<InMemoryDataset>(std::move(sch), RecordBatchVector{});
}

std::unique_ptr<Fragment> MakeSomeFragment(std::shared_ptr<Schema> sch) {
  return std::make_unique<InMemoryFragment>(std::move(sch), RecordBatchVector{});
}

TEST(BasicEvolution, MissingColumn) {
  std::unique_ptr<DatasetEvolutionStrategy> strategy =
      MakeBasicDatasetEvolutionStrategy();

  std::shared_ptr<Schema> dataset_schema =
      schema({field("A", int32()), field("B", int16()), field("C", int64())});
  std::unique_ptr<Dataset> dataset = MakeDatasetFromSchema(dataset_schema);
  std::unique_ptr<Fragment> fragment = MakeSomeFragment(std::move(dataset_schema));

  InspectedFragment inspected{{"A", "B"}};
  std::unique_ptr<FragmentEvolutionStrategy> fragment_strategy =
      strategy->GetStrategy(*dataset, *fragment, inspected);

  compute::Expression filter = equal(field_ref("C"), literal(INT64_C(7)));
  // If, after simplification, a filter somehow still references a missing field
  // then it is an error.
  ASSERT_RAISES(Invalid, fragment_strategy->DevolveFilter(filter));
  std::vector<FieldPath> selection{FieldPath({0}), FieldPath({2})};
  // Basic strategy should provide is_null guarantee for missing fields
  compute::Expression expected_guarantee = is_null(field_ref(2));
  ASSERT_OK_AND_ASSIGN(compute::Expression guarantee,
                       fragment_strategy->GetGuarantee(selection));
  ASSERT_EQ(expected_guarantee, guarantee);

  // Basic strategy should drop missing fields from selection
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FragmentSelection> devolved_selection,
                       fragment_strategy->DevolveSelection(selection));
  ASSERT_EQ(1, devolved_selection->columns().size());
  ASSERT_EQ(FieldPath({0}), devolved_selection->columns()[0].path);
  ASSERT_EQ(*int32(), *devolved_selection->columns()[0].requested_type);

  // Basic strategy should append null column to batches for missing column
  std::shared_ptr<RecordBatch> devolved_batch =
      RecordBatchFromJSON(schema({field("A", int32())}), R"([[1], [2], [3]])");
  ASSERT_OK_AND_ASSIGN(
      compute::ExecBatch evolved_batch,
      fragment_strategy->EvolveBatch(devolved_batch, selection, *devolved_selection));
  ASSERT_EQ(2, evolved_batch.values.size());
  AssertArraysEqual(*devolved_batch->column(0), *evolved_batch[0].make_array());
  ASSERT_EQ(*MakeNullScalar(int64()), *evolved_batch.values[1].scalar());
}

TEST(BasicEvolution, ReorderedColumns) {
  std::unique_ptr<DatasetEvolutionStrategy> strategy =
      MakeBasicDatasetEvolutionStrategy();

  std::shared_ptr<Schema> dataset_schema =
      schema({field("A", int32()), field("B", int16()), field("C", int64())});
  std::unique_ptr<Dataset> dataset = MakeDatasetFromSchema(dataset_schema);
  std::unique_ptr<Fragment> fragment = MakeSomeFragment(std::move(dataset_schema));

  InspectedFragment inspected{{"C", "B", "A"}};
  std::unique_ptr<FragmentEvolutionStrategy> fragment_strategy =
      strategy->GetStrategy(*dataset, *fragment, inspected);

  compute::Expression filter = equal(field_ref("C"), literal(INT64_C(7)));
  compute::Expression fragment_filter = equal(field_ref(0), literal(INT64_C(7)));
  // Devolved filter should have updated indices
  ASSERT_OK_AND_ASSIGN(compute::Expression devolved,
                       fragment_strategy->DevolveFilter(filter));
  ASSERT_EQ(fragment_filter, devolved);
  std::vector<FieldPath> selection{FieldPath({0}), FieldPath({2})};
  // No guarantees if simply reordering
  compute::Expression expected_guarantee = literal(true);
  ASSERT_OK_AND_ASSIGN(compute::Expression guarantee,
                       fragment_strategy->GetGuarantee(selection));
  ASSERT_EQ(expected_guarantee, guarantee);

  // Devolved selection should have correct indices
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FragmentSelection> devolved_selection,
                       fragment_strategy->DevolveSelection(selection));
  const std::vector<FragmentSelectionColumn>& devolved_cols =
      devolved_selection->columns();
  ASSERT_EQ(2, devolved_cols.size());
  ASSERT_EQ(FieldPath({2}), devolved_cols[0].path);
  ASSERT_EQ(FieldPath({0}), devolved_cols[1].path);
  ASSERT_EQ(*int32(), *devolved_cols[0].requested_type);
  ASSERT_EQ(*int64(), *devolved_cols[1].requested_type);

  // Basic strategy should append null column to batches for missing column
  std::shared_ptr<RecordBatch> devolved_batch = RecordBatchFromJSON(
      schema({field("C", int64()), field("A", int32())}), R"([[1,4], [2,5], [3,6]])");
  ASSERT_OK_AND_ASSIGN(
      compute::ExecBatch evolved_batch,
      fragment_strategy->EvolveBatch(devolved_batch, selection, *devolved_selection));
  ASSERT_EQ(2, evolved_batch.values.size());
  AssertArraysEqual(*devolved_batch->column(0), *evolved_batch[0].make_array());
  AssertArraysEqual(*devolved_batch->column(1), *evolved_batch[1].make_array());
}

struct MockScanTask {
  explicit MockScanTask(std::shared_ptr<RecordBatch> batch) : batch(std::move(batch)) {}

  std::shared_ptr<RecordBatch> batch;
  Future<std::shared_ptr<RecordBatch>> batch_future =
      Future<std::shared_ptr<RecordBatch>>::Make();
};

// Wraps access to std::default_random_engine to ensure only one thread
// at a time is using it
class ConcurrentGen {
 public:
  explicit ConcurrentGen(std::default_random_engine* gen) : gen_(gen) {}
  void With(std::function<void(std::default_random_engine*)> task) {
    std::lock_guard lk(mutex_);
    task(gen_);
  }

 private:
  std::default_random_engine* gen_;
  std::mutex mutex_;
};

struct MockFragmentScanner : public FragmentScanner {
  explicit MockFragmentScanner(std::vector<MockScanTask> scan_tasks)
      : scan_tasks_(std::move(scan_tasks)), has_started_(scan_tasks_.size(), false) {}

  // ### FragmentScanner API ###
  Future<std::shared_ptr<RecordBatch>> ScanBatch(int batch_number) override {
    has_started_[batch_number] = true;
    return scan_tasks_[batch_number].batch_future;
  }
  int64_t EstimatedDataBytes(int batch_number) override {
    return util::TotalBufferSize(*scan_tasks_[batch_number].batch);
  }
  int NumBatches() override { return static_cast<int>(scan_tasks_.size()); }

  // ### Unit Test API ###
  void DeliverBatches(bool slow, const std::vector<MockScanTask>& to_deliver) {
    for (MockScanTask task : to_deliver) {
      if (slow) {
        std::ignore = SleepABitAsync().Then(
            [task]() mutable { task.batch_future.MarkFinished(task.batch); });
      } else {
        task.batch_future.MarkFinished(task.batch);
      }
    }
  }

  void DeliverBatchesInOrder(bool slow) { DeliverBatches(slow, scan_tasks_); }

  void DeliverBatchesRandomly(bool slow, ConcurrentGen* gen) {
    std::vector<MockScanTask> shuffled_tasks(scan_tasks_);
    gen->With([&](std::default_random_engine* gen_instance) {
      std::shuffle(shuffled_tasks.begin(), shuffled_tasks.end(), *gen_instance);
    });
    DeliverBatches(slow, shuffled_tasks);
  }

  bool HasStarted(int batch_number) { return has_started_[batch_number]; }
  bool HasDelivered(int batch_number) {
    return scan_tasks_[batch_number].batch_future.is_finished();
  }

  std::vector<MockScanTask> scan_tasks_;
  std::vector<bool> has_started_;
};

struct MockFragment : public Fragment {
  // ### Fragment API ###

  MockFragment(std::shared_ptr<Schema> fragment_schema,
               std::vector<MockScanTask> scan_tasks,
               std::shared_ptr<InspectedFragment> inspected,
               compute::Expression partition_expression)
      : Fragment(std::move(partition_expression), std::move(fragment_schema)),
        fragment_scanner_(std::make_shared<MockFragmentScanner>(std::move(scan_tasks))),
        inspected_(std::move(inspected)) {}

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    return Status::Invalid("Not implemented because not needed by unit tests");
  };

  Future<std::shared_ptr<InspectedFragment>> InspectFragment(
      const FragmentScanOptions* format_options,
      compute::ExecContext* exec_context) override {
    has_inspected_ = true;
    return inspected_future_;
  }

  Future<std::shared_ptr<FragmentScanner>> BeginScan(
      const FragmentScanRequest& request, const InspectedFragment& inspected_fragment,
      const FragmentScanOptions* format_options,
      compute::ExecContext* exec_context) override {
    has_started_ = true;
    seen_request_ = request;
    return fragment_scanner_future_;
  }

  Future<std::optional<int64_t>> CountRows(
      compute::Expression predicate,
      const std::shared_ptr<ScanOptions>& options) override {
    return Status::Invalid("Not implemented because not needed by unit tests");
  }

  std::string type_name() const override { return "mock"; }

  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    return physical_schema_;
  };

  // ### Unit Test API ###

  void FinishInspection() { inspected_future_.MarkFinished(inspected_); }
  void FinishScanBegin() { fragment_scanner_future_.MarkFinished(fragment_scanner_); }

  Future<> DeliverInit(bool slow) {
    if (slow) {
      return SleepABitAsync().Then([this] {
        FinishInspection();
        return SleepABitAsync().Then([this] { FinishScanBegin(); });
      });
    } else {
      FinishInspection();
      FinishScanBegin();
      return Future<>::MakeFinished();
    }
  }

  void DeliverBatchesInOrder(bool slow) {
    std::ignore = DeliverInit(slow).Then(
        [this, slow] { fragment_scanner_->DeliverBatchesInOrder(slow); });
  }

  Future<> DeliverBatchesRandomly(bool slow, ConcurrentGen* gen) {
    return DeliverInit(slow).Then(
        [this, slow, gen] { fragment_scanner_->DeliverBatchesRandomly(slow, gen); });
  }

  bool has_inspected() { return has_inspected_; }
  bool has_started() { return has_started_; }
  bool HasBatchStarted(int batch_index) {
    return fragment_scanner_->HasStarted(batch_index);
  }
  bool HasBatchDelivered(int batch_index) {
    return fragment_scanner_->HasDelivered(batch_index);
  }

  std::shared_ptr<MockFragmentScanner> fragment_scanner_;
  Future<std::shared_ptr<FragmentScanner>> fragment_scanner_future_ =
      Future<std::shared_ptr<FragmentScanner>>::Make();
  std::shared_ptr<InspectedFragment> inspected_;
  Future<std::shared_ptr<InspectedFragment>> inspected_future_ =
      Future<std::shared_ptr<InspectedFragment>>::Make();
  std::atomic<bool> has_inspected_{false};
  std::atomic<bool> has_started_{false};
  FragmentScanRequest seen_request_;
};

FragmentVector AsFragmentVector(
    const std::vector<std::shared_ptr<MockFragment>>& fragments) {
  FragmentVector frag_vec;
  frag_vec.insert(frag_vec.end(), fragments.begin(), fragments.end());
  return frag_vec;
}

struct MockDataset : public FragmentDataset {
  MockDataset(std::shared_ptr<Schema> dataset_schema,
              std::vector<std::shared_ptr<MockFragment>> fragments)
      : FragmentDataset(std::move(dataset_schema), AsFragmentVector(fragments)),
        fragments_(std::move(fragments)) {}

  // ### Dataset API ###
  std::string type_name() const override { return "mock"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override {
    return Status::Invalid("Not needed for unit test");
  }

  Result<FragmentIterator> GetFragmentsImpl(compute::Expression predicate) override {
    has_started_ = true;
    return FragmentDataset::GetFragmentsImpl(std::move(predicate));
  }

  // ### Unit Test API ###
  void DeliverBatchesInOrder(bool slow) {
    for (const auto& fragment : fragments_) {
      fragment->DeliverBatchesInOrder(slow);
    }
  }

  void DeliverBatchesRandomly(bool slow) {
    const auto seed = ::arrow::internal::GetRandomSeed();
    std::default_random_engine gen(
        static_cast<std::default_random_engine::result_type>(seed));
    ConcurrentGen gen_wrapper(&gen);

    std::vector<std::shared_ptr<MockFragment>> fragments_shuffled(fragments_);
    std::shuffle(fragments_shuffled.begin(), fragments_shuffled.end(), gen);
    std::vector<Future<>> deliver_futures;
    for (const auto& fragment : fragments_shuffled) {
      deliver_futures.push_back(fragment->DeliverBatchesRandomly(slow, &gen_wrapper));
    }
    // Need to wait for fragments to finish init so gen stays valid
    AllFinished(deliver_futures).Wait();
  }

  bool has_started() { return has_started_; }
  bool HasStartedFragment(int fragment_index) {
    return fragments_[fragment_index]->has_started();
  }
  bool HasStartedBatch(int fragment_index, int batch_index) {
    return fragments_[fragment_index]->HasBatchStarted(batch_index);
  }

  bool has_started_ = false;
  std::vector<std::shared_ptr<MockFragment>> fragments_;
};

struct MockDatasetBuilder {
  explicit MockDatasetBuilder(std::shared_ptr<Schema> dataset_schema)
      : dataset_schema(std::move(dataset_schema)) {}

  void AddFragment(
      std::shared_ptr<Schema> fragment_schema,
      std::unique_ptr<InspectedFragment> inspection = nullptr,
      compute::Expression partition_expression = Fragment::kNoPartitionInformation) {
    if (!inspection) {
      inspection = std::make_unique<InspectedFragment>(fragment_schema->field_names());
    }
    fragments.push_back(std::make_shared<MockFragment>(
        std::move(fragment_schema), std::vector<MockScanTask>(), std::move(inspection),
        std::move(partition_expression)));
    active_fragment = fragments[fragments.size() - 1]->fragment_scanner_.get();
  }

  void AddBatch(std::shared_ptr<RecordBatch> batch) {
    active_fragment->scan_tasks_.emplace_back(std::move(batch));
    active_fragment->has_started_.push_back(false);
  }

  std::unique_ptr<MockDataset> Finish() {
    return std::make_unique<MockDataset>(std::move(dataset_schema), std::move(fragments));
  }

  std::shared_ptr<Schema> dataset_schema;
  std::vector<std::shared_ptr<MockFragment>> fragments;
  MockFragmentScanner* active_fragment = nullptr;
};

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_integer_type<TYPE>::value>::type>
std::shared_ptr<Array> ArrayFromRange(int start, int end, bool add_nulls) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_EXPECT_OK(builder.Reserve(end - start));
  for (int val = start; val < end; val++) {
    if (add_nulls && val % 2 == 0) {
      builder.UnsafeAppendNull();
    } else {
      builder.UnsafeAppend(val);
    }
  }
  EXPECT_OK_AND_ASSIGN(std::shared_ptr<Array> range_arr, builder.Finish());
  return range_arr;
}

struct ScannerTestParams {
  bool slow;
  int num_fragments;
  int num_batches;

  std::string ToString() const {
    std::stringstream ss;
    ss << (slow ? "slow" : "fast") << num_fragments << "f" << num_batches << "b";
    return ss.str();
  }

  static std::string ToTestNameString(
      const ::testing::TestParamInfo<ScannerTestParams>& info) {
    return info.param.ToString();
  }

  static std::vector<ScannerTestParams> Values() {
    std::vector<ScannerTestParams> values;
    for (bool slow : {false, true}) {
      values.push_back({slow, 1, 128});
      values.push_back({slow, 16, 128});
    }
    return values;
  }
};

std::ostream& operator<<(std::ostream& out, const ScannerTestParams& params) {
  out << (params.slow ? "slow-" : "fast-") << params.num_fragments << "f-"
      << params.num_batches << "b";
  return out;
}

constexpr int kRowsPerTestBatch = 16;

std::shared_ptr<Schema> ScannerTestSchema() {
  return schema({field("row_num", int32()), field("filterable", int16()),
                 field("nested", struct_({field("x", int32()), field("y", int32())}))});
}

std::shared_ptr<RecordBatch> MakeTestBatch(int idx) {
  ArrayVector arrays;
  // Row number
  arrays.push_back(ArrayFromRange<Int32Type>(idx * kRowsPerTestBatch,
                                             (idx + 1) * kRowsPerTestBatch,
                                             /*add_nulls=*/false));
  // Filterable
  arrays.push_back(ArrayFromRange<Int16Type>(0, kRowsPerTestBatch,
                                             /*add_nulls=*/true));
  // Nested
  std::shared_ptr<Array> x_vals =
      ArrayFromRange<Int32Type>(0, kRowsPerTestBatch, /*add_nulls=*/false);
  std::shared_ptr<Array> y_vals =
      ArrayFromRange<Int32Type>(0, kRowsPerTestBatch, /*add_nulls=*/true);
  EXPECT_OK_AND_ASSIGN(std::shared_ptr<Array> nested_arr,
                       StructArray::Make({std::move(x_vals), std::move(y_vals)},
                                         {field("x", int32()), field("y", int32())}));
  arrays.push_back(std::move(nested_arr));
  return RecordBatch::Make(ScannerTestSchema(), kRowsPerTestBatch, std::move(arrays));
}

std::unique_ptr<MockDataset> MakeTestDataset(int num_fragments, int batches_per_fragment,
                                             bool empty = false) {
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder dataset_builder(test_schema);
  for (int i = 0; i < num_fragments; i++) {
    dataset_builder.AddFragment(
        test_schema, std::make_unique<InspectedFragment>(test_schema->field_names()),
        Fragment::kNoPartitionInformation);
    for (int j = 0; j < batches_per_fragment; j++) {
      if (empty) {
        dataset_builder.AddBatch(
            RecordBatch::Make(schema({}), kRowsPerTestBatch, ArrayVector{}));
      } else {
        dataset_builder.AddBatch(MakeTestBatch(i * batches_per_fragment + j));
      }
    }
  }
  return dataset_builder.Finish();
}

class TestScannerBase : public ::testing::TestWithParam<ScannerTestParams> {
 protected:
  TestScannerBase() { internal::Initialize(); }

  std::shared_ptr<RecordBatch> MakeExpectedBatch() {
    RecordBatchVector batches;
    for (int frag_idx = 0; frag_idx < GetParam().num_fragments; frag_idx++) {
      for (int batch_idx = 0; batch_idx < GetParam().num_batches; batch_idx++) {
        batches.push_back(MakeTestBatch(batch_idx + (frag_idx * GetParam().num_batches)));
      }
    }
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         Table::FromRecordBatches(std::move(batches)));
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> as_one_batch,
                         table->CombineChunksToBatch());
    return as_one_batch;
  }

  acero::Declaration MakeScanNode(std::shared_ptr<Dataset> dataset) {
    ScanV2Options options(dataset);
    options.columns = ScanV2Options::AllColumns(*dataset->schema());
    return acero::Declaration("scan2", options);
  }

  RecordBatchVector RunNode(acero::Declaration scan_decl, bool ordered,
                            MockDataset* mock_dataset) {
    Future<RecordBatchVector> batches_fut =
        acero::DeclarationToBatchesAsync(std::move(scan_decl));
    if (ordered) {
      mock_dataset->DeliverBatchesInOrder(GetParam().slow);
    } else {
      mock_dataset->DeliverBatchesRandomly(GetParam().slow);
    }
    EXPECT_FINISHES_OK_AND_ASSIGN(RecordBatchVector record_batches, batches_fut);
    return record_batches;
  }

  void CheckScannedBatches(RecordBatchVector batches) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> batches_as_table,
                         Table::FromRecordBatches(std::move(batches)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> combined_data,
                         batches_as_table->CombineChunksToBatch());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Array> sort_indices,
        compute::SortIndices(combined_data->column(0), compute::SortOptions{}));
    ASSERT_OK_AND_ASSIGN(Datum sorted_data, compute::Take(combined_data, sort_indices));

    std::shared_ptr<RecordBatch> expected_data = MakeExpectedBatch();
    AssertBatchesEqual(*expected_data, *sorted_data.record_batch());
  }

  void CheckScanner(bool ordered) {
    std::shared_ptr<MockDataset> mock_dataset =
        MakeTestDataset(GetParam().num_fragments, GetParam().num_batches);
    acero::Declaration scan_decl = MakeScanNode(mock_dataset);
    RecordBatchVector scanned_batches = RunNode(scan_decl, ordered, mock_dataset.get());
    CheckScannedBatches(std::move(scanned_batches));
  }
};

TEST_P(TestScannerBase, ScanOrdered) { CheckScanner(true); }
TEST_P(TestScannerBase, ScanUnordered) { CheckScanner(false); }

// FIXME: Add test for scanning no columns

INSTANTIATE_TEST_SUITE_P(BasicNewScannerTests, TestScannerBase,
                         ::testing::ValuesIn(ScannerTestParams::Values()),
                         [](const ::testing::TestParamInfo<ScannerTestParams>& info) {
                           return std::to_string(info.index) + info.param.ToString();
                         });

void CheckScannerBackpressure(std::shared_ptr<MockDataset> dataset, ScanV2Options options,
                              int maxConcurrentFragments, int maxConcurrentBatches,
                              ::arrow::internal::ThreadPool* thread_pool) {
  // Start scanning
  acero::Declaration scan_decl = acero::Declaration("scan2", std::move(options));
  Future<RecordBatchVector> batches_fut =
      acero::DeclarationToBatchesAsync(std::move(scan_decl));

  auto get_num_inspected = [&] {
    int num_inspected = 0;
    for (const auto& frag : dataset->fragments_) {
      if (frag->has_inspected()) {
        num_inspected++;
      }
    }
    return num_inspected;
  };
  BusyWait(10, [&] {
    return get_num_inspected() == static_cast<int>(maxConcurrentFragments);
  });
  SleepABit();
  ASSERT_EQ(get_num_inspected(), static_cast<int>(maxConcurrentFragments));

  int total_batches = 0;
  for (const auto& frag : dataset->fragments_) {
    total_batches += frag->fragment_scanner_->NumBatches();
    frag->FinishInspection();
    frag->FinishScanBegin();
  }

  int batches_scanned = 0;
  while (batches_scanned < total_batches) {
    MockScanTask* next_task_to_deliver = nullptr;
    thread_pool->WaitForIdle();
    int batches_started = 0;
    for (const auto& frag : dataset->fragments_) {
      for (int i = 0; i < frag->fragment_scanner_->NumBatches(); i++) {
        if (frag->HasBatchStarted(i)) {
          batches_started++;
          if (next_task_to_deliver == nullptr && !frag->HasBatchDelivered(i)) {
            next_task_to_deliver = &frag->fragment_scanner_->scan_tasks_[i];
          }
        }
      }
    }
    ASSERT_LE(batches_started - batches_scanned, maxConcurrentBatches)
        << " too many scan tasks were allowed to run";
    ASSERT_NE(next_task_to_deliver, nullptr);
    next_task_to_deliver->batch_future.MarkFinished(next_task_to_deliver->batch);
    batches_scanned++;
  }
}

TEST(TestNewScanner, Backpressure) {
  constexpr int kNumFragments = 4;
  constexpr int kNumBatchesPerFragment = 4;
  internal::Initialize();
  std::shared_ptr<MockDataset> test_dataset =
      MakeTestDataset(kNumFragments, kNumBatchesPerFragment);

  ScanV2Options options(test_dataset);

  // No readahead
  options.dataset = test_dataset;
  options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
  options.fragment_readahead = 0;
  options.target_bytes_readahead = 0;
  CheckScannerBackpressure(test_dataset, options, 1, 1,
                           ::arrow::internal::GetCpuThreadPool());

  // Some readahead
  test_dataset = MakeTestDataset(kNumFragments, kNumBatchesPerFragment);
  options = ScanV2Options(test_dataset);
  options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
  options.fragment_readahead = 4;
  // each batch should be 14Ki so 50Ki readahead should yield 3-at-a-time
  options.target_bytes_readahead = 50 * kRowsPerTestBatch;
  CheckScannerBackpressure(test_dataset, options, 4, 3,
                           ::arrow::internal::GetCpuThreadPool());
}

TEST(TestNewScanner, NestedRead) {
  // This tests the case where the file format does not support
  // handling nested reads (e.g. JSON) and so the scanner must
  // drop the extra data
  internal::Initialize();
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder builder(test_schema);
  builder.AddFragment(test_schema);
  std::shared_ptr<RecordBatch> batch = MakeTestBatch(0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> nested_col, FieldPath({2, 0}).Get(*batch));
  std::shared_ptr<RecordBatch> one_column = RecordBatch::Make(
      schema({field("x", int32())}), batch->num_rows(), ArrayVector{nested_col});
  builder.AddBatch(std::move(one_column));
  std::shared_ptr<MockDataset> test_dataset = builder.Finish();
  test_dataset->DeliverBatchesInOrder(false);

  ScanV2Options options(test_dataset);
  // nested.x
  options.columns = {FieldPath({2, 0})};
  ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                       acero::DeclarationToBatches({"scan2", options}));
  ASSERT_EQ(1, batches.size());
  for (const auto& batch : batches) {
    ASSERT_EQ("x", batch->schema()->field(0)->name());
    ASSERT_EQ(*int32(), *batch->schema()->field(0)->type());
    ASSERT_EQ(*int32(), *batch->column(0)->type());
  }
  const FragmentScanRequest& seen_request = test_dataset->fragments_[0]->seen_request_;
  ASSERT_EQ(1, seen_request.fragment_selection->columns().size());
  ASSERT_EQ(FieldPath({2, 0}), seen_request.fragment_selection->columns()[0].path);
  ASSERT_EQ(*int32(), *seen_request.fragment_selection->columns()[0].requested_type);
}

std::shared_ptr<MockDataset> MakePartitionSkipDataset() {
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder builder(test_schema);
  builder.AddFragment(test_schema, /*inspection=*/nullptr,
                      equal(field_ref({1}), literal(100)));
  std::shared_ptr<RecordBatch> batch = MakeTestBatch(0);
  EXPECT_OK_AND_ASSIGN(batch, batch->RemoveColumn(1));
  builder.AddBatch(std::move(batch));
  builder.AddFragment(test_schema, /*inspection=*/nullptr,
                      equal(field_ref({1}), literal(50)));
  batch = MakeTestBatch(1);
  EXPECT_OK_AND_ASSIGN(batch, batch->RemoveColumn(1));
  builder.AddBatch(std::move(batch));
  return builder.Finish();
}

// Make a dataset where the dataset schema expects the "filterable" column to
// be a date but the partitioning interprets it as an integer
std::shared_ptr<MockDataset> MakeInvalidPartitionSkipDataset() {
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  EXPECT_OK_AND_ASSIGN(test_schema,
                       test_schema->SetField(1, field("filterable", date64())));
  MockDatasetBuilder builder(test_schema);
  builder.AddFragment(test_schema, /*inspection=*/nullptr,
                      equal(field_ref({1}), literal(100)));
  std::shared_ptr<RecordBatch> batch = MakeTestBatch(0);
  EXPECT_OK_AND_ASSIGN(batch, batch->RemoveColumn(1));
  builder.AddBatch(std::move(batch));
  return builder.Finish();
}

TEST(TestNewScanner, PartitionSkip) {
  internal::Initialize();
  {
    ARROW_SCOPED_TRACE("Skip second batch");
    std::shared_ptr<MockDataset> test_dataset = MakePartitionSkipDataset();
    test_dataset->DeliverBatchesInOrder(false);

    ScanV2Options options(test_dataset);
    options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
    options.filter = greater(field_ref("filterable"), literal(75));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                         acero::DeclarationToBatches({"scan2", options}));
    ASSERT_EQ(1, batches.size());
    std::shared_ptr<RecordBatch> expected = MakeTestBatch(0);
    ASSERT_OK_AND_ASSIGN(expected, expected->SetColumn(1, field("filterable", int16()),
                                                       ConstantArrayGenerator::Int16(
                                                           expected->num_rows(), 100)));
    AssertBatchesEqual(*expected, *batches[0]);
  }

  {
    ARROW_SCOPED_TRACE("Skip first batch");
    std::shared_ptr<MockDataset> test_dataset = MakePartitionSkipDataset();
    test_dataset->DeliverBatchesInOrder(false);
    ScanV2Options options = ScanV2Options(test_dataset);
    options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
    options.filter = less(field_ref("filterable"), literal(75));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                         acero::DeclarationToBatches({"scan2", options}));
    ASSERT_EQ(1, batches.size());
    std::shared_ptr<RecordBatch> expected = MakeTestBatch(1);
    ASSERT_OK_AND_ASSIGN(expected, expected->SetColumn(1, field("filterable", int16()),
                                                       ConstantArrayGenerator::Int16(
                                                           expected->num_rows(), 50)));
    AssertBatchesEqual(*expected, *batches[0]);
  }

  {
    ARROW_SCOPED_TRACE("Partitioning doesn't agree with dataset schema");
    std::shared_ptr<MockDataset> test_dataset = MakeInvalidPartitionSkipDataset();
    test_dataset->DeliverBatchesInOrder(false);
    ScanV2Options options = ScanV2Options(test_dataset);
    options.columns = ScanV2Options::AllColumns(*test_dataset->schema());

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "The dataset schema defines the field FieldRef.FieldPath(1)"),
        acero::DeclarationToBatches({"scan2", options}));
  }
}

TEST(TestNewScanner, NoFragments) {
  internal::Initialize();
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder builder(test_schema);
  std::shared_ptr<MockDataset> test_dataset = builder.Finish();

  ScanV2Options options(test_dataset);
  options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
  ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                       acero::DeclarationToBatches({"scan2", options}));
  ASSERT_EQ(0, batches.size());
}

TEST(TestNewScanner, EmptyFragment) {
  internal::Initialize();
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder builder(test_schema);
  builder.AddFragment(test_schema);
  std::shared_ptr<MockDataset> test_dataset = builder.Finish();
  test_dataset->DeliverBatchesInOrder(false);

  ScanV2Options options(test_dataset);
  options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
  ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                       acero::DeclarationToBatches({"scan2", options}));
  ASSERT_EQ(0, batches.size());
}

TEST(TestNewScanner, EmptyBatch) {
  internal::Initialize();
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder builder(test_schema);
  builder.AddFragment(test_schema);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> empty_batch,
                       RecordBatch::MakeEmpty(test_schema));
  builder.AddBatch(std::move(empty_batch));
  std::shared_ptr<MockDataset> test_dataset = builder.Finish();
  test_dataset->DeliverBatchesInOrder(false);

  ScanV2Options options(test_dataset);
  options.columns = ScanV2Options::AllColumns(*test_dataset->schema());
  ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                       acero::DeclarationToBatches({"scan2", options}));
  ASSERT_EQ(0, batches.size());
}

TEST(TestNewScanner, NoColumns) {
  constexpr int kNumFragments = 4;
  constexpr int kNumBatchesPerFragment = 4;
  internal::Initialize();
  std::shared_ptr<MockDataset> test_dataset =
      MakeTestDataset(kNumFragments, kNumBatchesPerFragment, /*empty=*/true);
  test_dataset->DeliverBatchesInOrder(false);

  ScanV2Options options(test_dataset);
  ASSERT_OK_AND_ASSIGN(acero::BatchesWithCommonSchema batches_and_schema,
                       acero::DeclarationToExecBatches({"scan2", options}));
  ASSERT_EQ(16, batches_and_schema.batches.size());
  for (const auto& batch : batches_and_schema.batches) {
    ASSERT_EQ(0, batch.values.size());
    ASSERT_EQ(kRowsPerTestBatch, batch.length);
  }
}

TEST(TestNewScanner, MissingColumn) {
  internal::Initialize();
  std::shared_ptr<Schema> test_schema = ScannerTestSchema();
  MockDatasetBuilder builder(test_schema);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> missing_schema,
                       test_schema->RemoveField(2));
  builder.AddFragment(missing_schema);
  std::shared_ptr<RecordBatch> batch = MakeTestBatch(0);
  // Remove column 2 because we are pretending it doesn't exist
  // in the fragment
  ASSERT_OK_AND_ASSIGN(batch, batch->RemoveColumn(2));
  // Remove column 1 because we aren't going to ask for it
  ASSERT_OK_AND_ASSIGN(batch, batch->RemoveColumn(1));
  builder.AddBatch(batch);

  std::shared_ptr<MockDataset> test_dataset = builder.Finish();
  test_dataset->DeliverBatchesInOrder(false);

  ScanV2Options options(test_dataset);
  options.columns = {FieldPath({0}), FieldPath({2})};

  ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                       acero::DeclarationToBatches({"scan2", options}));

  ASSERT_EQ(1, batches.size());
  AssertArraysEqual(*batch->column(0), *batches[0]->column(0));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> expected_nulls,
                       MakeArrayOfNull(test_schema->field(2)->type(), kRowsPerTestBatch));
  AssertArraysEqual(*expected_nulls, *batches[0]->column(1));
}

void WriteIpcData(const std::string& path,
                  const std::shared_ptr<fs::FileSystem> file_system,
                  const std::shared_ptr<Table> input) {
  EXPECT_OK_AND_ASSIGN(auto out_stream, file_system->OpenOutputStream(path));
  ASSERT_OK_AND_ASSIGN(
      auto file_writer,
      MakeFileWriter(out_stream, input->schema(), ipc::IpcWriteOptions::Defaults()));
  ASSERT_OK(file_writer->WriteTable(*input));
  ASSERT_OK(file_writer->Close());
}

struct TestScannerParams {
  bool use_threads;
  int num_child_datasets;
  int num_batches;
  int items_per_batch;

  std::string ToString() const {
    // GTest requires this to be alphanumeric
    std::stringstream ss;
    ss << (use_threads ? "Threaded" : "Serial") << num_child_datasets << "d"
       << num_batches << "b" << items_per_batch << "r";
    return ss.str();
  }

  static std::string ToTestNameString(
      const ::testing::TestParamInfo<TestScannerParams>& info) {
    return std::to_string(info.index) + info.param.ToString();
  }

  static std::vector<TestScannerParams> Values() {
    std::vector<TestScannerParams> values;
    for (int use_threads = 0; use_threads < 2; use_threads++) {
      values.push_back({static_cast<bool>(use_threads), 1, 1, 1024});
      values.push_back({static_cast<bool>(use_threads), 2, 16, 1024});
    }
    return values;
  }
};

std::ostream& operator<<(std::ostream& out, const TestScannerParams& params) {
  out << (params.use_threads ? "threaded-" : "serial-") << params.num_child_datasets
      << "d-" << params.num_batches << "b-" << params.items_per_batch << "i";
  return out;
}

class TestScanner : public DatasetFixtureMixinWithParam<TestScannerParams> {
 protected:
  std::shared_ptr<Scanner> MakeScanner(std::shared_ptr<Dataset> dataset) {
    ScannerBuilder builder(std::move(dataset), options_);
    ARROW_EXPECT_OK(builder.UseThreads(GetParam().use_threads));
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    return scanner;
  }

  std::shared_ptr<Scanner> MakeScanner(std::shared_ptr<RecordBatch> batch) {
    RecordBatchVector batches{static_cast<size_t>(GetParam().num_batches), batch};

    DatasetVector children{static_cast<size_t>(GetParam().num_child_datasets),
                           std::make_shared<InMemoryDataset>(batch->schema(), batches)};

    EXPECT_OK_AND_ASSIGN(auto dataset, UnionDataset::Make(batch->schema(), children));
    return MakeScanner(std::move(dataset));
  }

  void AssertScannerEqualsRepetitionsOf(
      std::shared_ptr<Scanner> scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = GetParam().num_child_datasets *
                                    GetParam().num_batches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    // Verifies that the unified BatchReader is equivalent to flattening all the
    // structures of the scanner, i.e. Scanner[Dataset[ScanTask[RecordBatch]]]
    AssertScannerEquals(expected.get(), scanner.get());
  }

  void AssertScanBatchesEqualRepetitionsOf(
      std::shared_ptr<Scanner> scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = GetParam().num_child_datasets *
                                    GetParam().num_batches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    AssertScanBatchesEquals(expected.get(), scanner.get());
  }

  void AssertNoAugmentedFields(std::shared_ptr<Scanner> scanner) {
    ASSERT_OK_AND_ASSIGN(auto table, scanner.get()->ToTable());
    auto columns = table.get()->ColumnNames();
    EXPECT_TRUE(std::none_of(columns.begin(), columns.end(), [](std::string& x) {
      return x == "__fragment_index" || x == "__batch_index" ||
             x == "__last_in_fragment" || x == "__filename";
    }));
  }

  void AssertScanBatchesUnorderedEqualRepetitionsOf(
      std::shared_ptr<Scanner> scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = GetParam().num_child_datasets *
                                    GetParam().num_batches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    AssertScanBatchesUnorderedEquals(expected.get(), scanner.get(), 1);
  }
};

TEST_P(TestScanner, Scan) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch), batch);
}

TEST_P(TestScanner, ScanBatches) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch), batch);
}

TEST_P(TestScanner, ScanBatchesUnordered) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch), batch);
}

TEST_P(TestScanner, ScanWithCappedBatchSize) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  options_->batch_size = GetParam().items_per_batch / 2;
  auto expected = batch->Slice(GetParam().items_per_batch / 2);
  AssertScanBatchesEqualRepetitionsOf(
      MakeScanner(batch), expected,
      GetParam().num_child_datasets * GetParam().num_batches * 2);
}

TEST_P(TestScanner, FilteredScan) {
  SetSchema({field("f64", float64())});

  double value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(float64(), GetParam().items_per_batch,
                                               GetParam().items_per_batch / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 builder->UnsafeAppend(-value);
                                                 value += 1.0;
                                               }));

  SetFilter(greater(field_ref("f64"), literal(0.0)));

  auto batch = RecordBatch::Make(schema_, f64->length(), {f64});

  value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64_filtered,
                       ArrayFromBuilderVisitor(float64(), GetParam().items_per_batch / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 value += 1.0;
                                               }));

  auto filtered_batch =
      RecordBatch::Make(schema_, f64_filtered->length(), {f64_filtered});

  AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch), filtered_batch);
}

TEST_P(TestScanner, FilteredScanNested) {
  auto struct_ty = struct_({field("f64", float64())});
  SetSchema({field("struct", struct_ty)});

  double value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(float64(), GetParam().items_per_batch,
                                               GetParam().items_per_batch / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 builder->UnsafeAppend(-value);
                                                 value += 1.0;
                                               }));

  SetFilter(greater(field_ref(FieldRef("struct", "f64")), literal(0.0)));

  auto batch = RecordBatch::Make(
      schema_, f64->length(),
      {
          std::make_shared<StructArray>(struct_ty, f64->length(), ArrayVector{f64}),
      });

  value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64_filtered,
                       ArrayFromBuilderVisitor(float64(), GetParam().items_per_batch / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 value += 1.0;
                                               }));

  auto filtered_batch = RecordBatch::Make(
      schema_, f64_filtered->length(),
      {
          std::make_shared<StructArray>(struct_ty, f64_filtered->length(),
                                        ArrayVector{f64_filtered}),
      });

  AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch), filtered_batch);
}

TEST_P(TestScanner, ProjectedScan) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  SetProjectedColumns({"i32"});
  auto batch_in = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto batch_out = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch,
                                                  schema({field("i32", int32())}));
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch_in), batch_out);
}

TEST_P(TestScanner, ProjectionDefaults) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch_in = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto just_i32 = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch,
                                                 schema({field("i32", int32())}));
  // If we don't specify anything we should scan everything
  {
    ARROW_SCOPED_TRACE("User does not specify projection or projected_schema");
    options_->projection = literal(true);
    options_->projected_schema = nullptr;
    AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch_in), batch_in);
    AssertNoAugmentedFields(MakeScanner(batch_in));
  }
  // If we only specify a projection expression then infer the projected schema
  // from the projection expression
  auto projection_desc = ProjectionDescr::FromNames({"i32"}, *schema_);
  {
    ARROW_SCOPED_TRACE("User only specifies projection");
    options_->projection = projection_desc->expression;
    options_->projected_schema = nullptr;
    AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch_in), just_i32);
  }
  // If we only specify a projected schema then infer the projection expression
  // from the schema
  {
    ARROW_SCOPED_TRACE("User only specifies projected_schema");
    options_->projection = literal(true);
    options_->projected_schema = projection_desc->schema;
    AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch_in), just_i32);
  }
}

TEST_P(TestScanner, ProjectedScanNested) {
  SetSchema({
      field("struct", struct_({field("i32", int32()), field("f64", float64())})),
      field("nested", struct_({field("left", int32()),
                               field("right", struct_({field("i32", int32()),
                                                       field("f64", float64())}))})),
  });
  ASSERT_OK_AND_ASSIGN(auto descr, ProjectionDescr::FromExpressions(
                                       {field_ref(FieldRef("struct", "i32")),
                                        field_ref(FieldRef("nested", "right", "f64"))},
                                       {"i32", "f64"}, *options_->dataset_schema))
  SetProjection(options_.get(), std::move(descr));
  auto batch_in = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto batch_out = ConstantArrayGenerator::Zeroes(
      GetParam().items_per_batch,
      schema({field("i32", int32()), field("f64", float64())}));
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch_in), batch_out);
}

TEST_P(TestScanner, ProjectedScanNestedFromNames) {
  SetSchema({
      field("struct", struct_({field("i32", int32()), field("f64", float64())})),
      field("nested", struct_({field("left", int32()),
                               field("right", struct_({field("i32", int32()),
                                                       field("f64", float64())}))})),
  });
  ASSERT_OK_AND_ASSIGN(auto descr,
                       ProjectionDescr::FromNames({".struct.i32", "nested.right.f64"},
                                                  *options_->dataset_schema))
  SetProjection(options_.get(), std::move(descr));
  auto batch_in = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto batch_out = ConstantArrayGenerator::Zeroes(
      GetParam().items_per_batch,
      schema({field("i32", int32()), field("f64", float64())}));
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch_in), batch_out);
}

TEST_P(TestScanner, MaterializeMissingColumn) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch_missing_f64 = ConstantArrayGenerator::Zeroes(
      GetParam().items_per_batch, schema({field("i32", int32())}));

  auto fragment_missing_f64 = std::make_shared<InMemoryFragment>(
      RecordBatchVector{
          static_cast<size_t>(GetParam().num_child_datasets * GetParam().num_batches),
          batch_missing_f64},
      equal(field_ref("f64"), literal(2.5)));

  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(
                           float64(), GetParam().items_per_batch,
                           [&](DoubleBuilder* builder) { builder->UnsafeAppend(2.5); }));
  auto batch_with_f64 =
      RecordBatch::Make(schema_, f64->length(), {batch_missing_f64->column(0), f64});

  FragmentVector fragments{fragment_missing_f64};
  auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
  auto scanner = MakeScanner(std::move(dataset));
  AssertScanBatchesEqualRepetitionsOf(scanner, batch_with_f64);
}

TEST_P(TestScanner, ToTable) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches{
      static_cast<std::size_t>(GetParam().num_batches * GetParam().num_child_datasets),
      batch};

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));

  auto scanner = MakeScanner(batch);
  std::shared_ptr<Table> actual;

  // There is no guarantee on the ordering when using multiple threads, but
  // since the RecordBatch is always the same it will pass.
  ASSERT_OK_AND_ASSIGN(actual, scanner->ToTable());
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
}

TEST_P(TestScanner, ScanWithVisitor) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto scanner = MakeScanner(batch);
  ASSERT_OK(scanner->Scan([batch](TaggedRecordBatch scanned_batch) {
    AssertBatchesEqual(*batch, *scanned_batch.record_batch, /*same_chunk_layout=*/false);
    return Status::OK();
  }));
}

TEST_P(TestScanner, TakeIndices) {
  auto batch_size = GetParam().items_per_batch;
  auto num_batches = GetParam().num_batches;
  auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  ArrayVector arrays(2);
  ArrayFromVector<Int32Type>(Iota<int32_t>(batch_size), &arrays[0]);
  ArrayFromVector<DoubleType>(Iota<double>(static_cast<double>(batch_size)), &arrays[1]);
  auto batch = RecordBatch::Make(schema_, batch_size, arrays);

  auto scanner = MakeScanner(batch);

  std::shared_ptr<Array> indices;
  {
    ArrayFromVector<Int64Type>(Iota(batch_size), &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches({batch}));
    ASSERT_EQ(expected->num_rows(), batch_size);
    AssertTablesEqual(*expected, *taken, /*same_chunk_layout=*/false);
  }
  {
    ArrayFromVector<Int64Type>({7, 5, 3, 1}, &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 4);
    AssertTablesEqual(*expected.table(), *taken, /*same_chunk_layout=*/false);
  }
  if (num_batches > 1) {
    ArrayFromVector<Int64Type>({batch_size + 2, batch_size + 1}, &indices);
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 2);
    AssertTablesEqual(*expected.table(), *taken, /*same_chunk_layout=*/false);
  }
  if (num_batches > 1) {
    ArrayFromVector<Int64Type>({1, 3, 5, 7, batch_size + 1, 2 * batch_size + 2},
                               &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 6);
    AssertTablesEqual(*expected.table(), *taken, /*same_chunk_layout=*/false);
  }
  {
    auto base = num_datasets * num_batches * batch_size;
    ArrayFromVector<Int64Type>({base + 1}, &indices);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IndexError,
        ::testing::HasSubstr("Some indices were out of bounds: " +
                             std::to_string(base + 1)),
        scanner->TakeRows(*indices));
  }
  {
    auto base = num_datasets * num_batches * batch_size;
    ArrayFromVector<Int64Type>(
        {1, 2, base + 1, base + 2, base + 3, base + 4, base + 5, base + 6}, &indices);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IndexError,
        ::testing::HasSubstr(
            "Some indices were out of bounds: " + std::to_string(base + 1) + ", " +
            std::to_string(base + 2) + ", " + std::to_string(base + 3) + ", ..."),
        scanner->TakeRows(*indices));
  }
}

TEST_P(TestScanner, CountRows) {
  const auto items_per_batch = GetParam().items_per_batch;
  const auto num_batches = GetParam().num_batches;
  const auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  ArrayVector arrays(2);
  ArrayFromVector<Int32Type>(Iota<int32_t>(static_cast<int32_t>(items_per_batch)),
                             &arrays[0]);
  ArrayFromVector<DoubleType>(Iota<double>(static_cast<double>(items_per_batch)),
                              &arrays[1]);
  auto batch = RecordBatch::Make(schema_, items_per_batch, arrays);
  auto scanner = MakeScanner(batch);

  ASSERT_OK_AND_ASSIGN(auto rows, scanner->CountRows());
  ASSERT_EQ(rows, num_datasets * num_batches * items_per_batch);

  ASSERT_OK_AND_ASSIGN(options_->filter,
                       greater_equal(field_ref("i32"), literal(64)).Bind(*schema_));
  ASSERT_OK_AND_ASSIGN(rows, scanner->CountRows());
  ASSERT_EQ(rows, num_datasets * num_batches * (items_per_batch - 64));
}

TEST_P(TestScanner, EmptyFragment) {
  // Regression test for ARROW-13982
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto empty_batch = ConstantArrayGenerator::Zeroes(0, schema_);
  RecordBatchVector batches{
      static_cast<std::size_t>(GetParam().num_batches * GetParam().num_child_datasets),
      batch};

  FragmentVector fragments{
      std::make_shared<InMemoryFragment>(RecordBatchVector{empty_batch}),
      std::make_shared<InMemoryFragment>(batches)};
  auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
  auto scanner = MakeScanner(dataset);

  // There is no guarantee on the ordering when using multiple threads, but
  // since the RecordBatch is always the same (or empty) it will pass.
  ASSERT_OK_AND_ASSIGN(auto gen, scanner->ScanBatchesAsync());
  ASSERT_FINISHES_OK_AND_ASSIGN(auto tagged, CollectAsyncGenerator(gen));
  RecordBatchVector actual_batches;
  for (const auto& batch : tagged) {
    actual_batches.push_back(batch.record_batch);
  }

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));
  ASSERT_OK_AND_ASSIGN(auto actual, Table::FromRecordBatches(std::move(actual_batches)));
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
}

class CountRowsOnlyFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  Future<std::optional<int64_t>> CountRows(compute::Expression predicate,
                                           const std::shared_ptr<ScanOptions>&) override {
    if (compute::FieldsInExpression(predicate).size() > 0) {
      return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
    }
    int64_t sum = 0;
    for (const auto& batch : record_batches_) {
      sum += batch->num_rows();
    }
    return Future<std::optional<int64_t>>::MakeFinished(sum);
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>&) override {
    return Status::Invalid("Don't scan me!");
  }
};

class ScanOnlyFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  Future<std::optional<int64_t>> CountRows(compute::Expression predicate,
                                           const std::shared_ptr<ScanOptions>&) override {
    return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>&) override {
    return MakeVectorGenerator(record_batches_);
  }
};

// Ensure the pipeline does not break on an empty batch
TEST_P(TestScanner, CountRowsEmpty) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto empty_batch = ConstantArrayGenerator::Zeroes(0, schema_);
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {empty_batch, batch};
  ScannerBuilder builder(
      std::make_shared<FragmentDataset>(
          schema_, FragmentVector{std::make_shared<ScanOnlyFragment>(batches)}),
      options_);
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  ASSERT_OK_AND_EQ(batch->num_rows(), scanner->CountRows());
}

// Regression test for ARROW-12668: ensure failures are properly handled
class CountFailFragment : public InMemoryFragment {
 public:
  explicit CountFailFragment(RecordBatchVector record_batches)
      : InMemoryFragment(std::move(record_batches)),
        count(Future<std::optional<int64_t>>::Make()) {}

  Future<std::optional<int64_t>> CountRows(compute::Expression,
                                           const std::shared_ptr<ScanOptions>&) override {
    return count;
  }

  Future<std::optional<int64_t>> count;
};
TEST_P(TestScanner, CountRowsFailure) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {batch};
  auto fragment1 = std::make_shared<CountFailFragment>(batches);
  auto fragment2 = std::make_shared<CountFailFragment>(batches);
  ScannerBuilder builder(
      std::make_shared<FragmentDataset>(schema_, FragmentVector{fragment1, fragment2}),
      options_);
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  fragment1->count.MarkFinished(Status::Invalid(""));
  // Should immediately stop the count
  ASSERT_RAISES(Invalid, scanner->CountRows());
  // Fragment 2 doesn't complete until after the count stops - should not break anything
  // under ASan, etc.
  fragment2->count.MarkFinished(std::nullopt);
}

TEST_P(TestScanner, CountRowsWithMetadata) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {batch, batch, batch, batch};
  ScannerBuilder builder(
      std::make_shared<FragmentDataset>(
          schema_, FragmentVector{std::make_shared<CountRowsOnlyFragment>(batches)}),
      options_);
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  ASSERT_OK_AND_EQ(4 * batch->num_rows(), scanner->CountRows());

  ASSERT_OK(builder.Filter(equal(field_ref("i32"), literal(5))));
  ASSERT_OK_AND_ASSIGN(scanner, builder.Finish());
  // Scanner should fall back on reading data and hit the error
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Don't scan me!"),
                                  scanner->CountRows());
}

TEST_P(TestScanner, ToRecordBatchReader) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches{
      static_cast<std::size_t>(GetParam().num_batches * GetParam().num_child_datasets),
      batch};

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));

  auto scanner = MakeScanner(batch);
  ASSERT_OK_AND_ASSIGN(auto reader, scanner->ToRecordBatchReader());
  scanner.reset();
  ASSERT_OK_AND_ASSIGN(auto actual, reader->ToTable());
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
}

class FailingFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    struct {
      Future<std::shared_ptr<RecordBatch>> operator()() {
        if (index > 16) {
          return Status::Invalid("Oh no, we failed!");
        }
        auto batch = batches[index++ % batches.size()];
        return Future<std::shared_ptr<RecordBatch>>::MakeFinished(batch);
      }
      RecordBatchVector batches;
      int index = 0;
    } Generator;
    Generator.batches = record_batches_;
    return Generator;
  }
};

class FailingScanFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  // There are two places to fail - during iteration (covered by FailingFragment) or at
  // the initial scan (covered here)
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    return Status::Invalid("Oh no, we failed!");
  }
};

template <typename It, typename GetBatch>
bool CheckIteratorRaises(const RecordBatch& batch, It batch_it, GetBatch get_batch) {
  while (true) {
    auto maybe_batch = batch_it.Next();
    if (maybe_batch.ok()) {
      EXPECT_OK_AND_ASSIGN(auto scanned_batch, maybe_batch);
      if (IsIterationEnd(scanned_batch)) break;
      AssertBatchesEqual(batch, *get_batch(scanned_batch));
    } else {
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Oh no, we failed!"),
                                      maybe_batch);
      return true;
    }
  }
  return false;
}

TEST_P(TestScanner, ScanBatchesFailure) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {batch, batch, batch, batch};

  auto check_scanner = [](const RecordBatch& batch, Scanner* scanner) {
    auto maybe_batch_it = scanner->ScanBatchesUnordered();
    if (!maybe_batch_it.ok()) {
      // SyncScanner can fail here as it eagerly consumes the first value
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Oh no, we failed!"),
                                      std::move(maybe_batch_it));
    } else {
      ASSERT_OK_AND_ASSIGN(auto batch_it, std::move(maybe_batch_it));
      EXPECT_TRUE(CheckIteratorRaises(
          batch, std::move(batch_it),
          [](const EnumeratedRecordBatch& batch) { return batch.record_batch.value; }))
          << "ScanBatchesUnordered() did not raise an error";
    }

    auto maybe_tagged_batch_it = scanner->ScanBatches();
    if (!maybe_tagged_batch_it.ok()) {
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Oh no, we failed!"),
                                      std::move(maybe_tagged_batch_it));
    } else {
      ASSERT_OK_AND_ASSIGN(auto tagged_batch_it, std::move(maybe_tagged_batch_it));
      EXPECT_TRUE(CheckIteratorRaises(
          batch, std::move(tagged_batch_it),
          [](const TaggedRecordBatch& batch) { return batch.record_batch; }))
          << "ScanBatches() did not raise an error";
    }
  };

  // Case 1: failure when getting next scan task
  {
    FragmentVector fragments{std::make_shared<FailingFragment>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }

  // Case 2: failure when calling ScanTask::Execute
  {
    FragmentVector fragments{std::make_shared<FailingScanFragment>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }

  // Case 3: failure when calling RecordBatchIterator::Next
  {
    FragmentVector fragments{std::make_shared<FailingScanFragment>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }
}

TEST_P(TestScanner, Head) {
  auto batch_size = GetParam().items_per_batch;
  auto num_batches = GetParam().num_batches;
  auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(batch_size, schema_);

  auto scanner = MakeScanner(batch);
  std::shared_ptr<Table> expected, actual;

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(0));
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {batch}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size));
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {batch->Slice(0, 1)}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(1));
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);

  if (num_batches > 1) {
    ASSERT_OK_AND_ASSIGN(expected,
                         Table::FromRecordBatches(schema_, {batch, batch->Slice(0, 1)}));
    ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size + 1));
    AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
  }

  ASSERT_OK_AND_ASSIGN(expected, scanner->ToTable());
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size * num_batches * num_datasets));
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);

  ASSERT_OK_AND_ASSIGN(expected, scanner->ToTable());
  ASSERT_OK_AND_ASSIGN(actual,
                       scanner->Head(batch_size * num_batches * num_datasets + 100));
  AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
}

TEST_P(TestScanner, FromReader) {
  auto batch_size = GetParam().items_per_batch;
  auto num_batches = GetParam().num_batches;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(batch_size, schema_);
  auto source_reader = ConstantArrayGenerator::Repeat(num_batches, batch);
  auto target_reader = ConstantArrayGenerator::Repeat(num_batches, batch);

  auto builder = ScannerBuilder::FromRecordBatchReader(source_reader);
  ARROW_EXPECT_OK(builder->UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder->Finish());
  AssertScannerEquals(target_reader.get(), scanner.get());

  // Such datasets can only be scanned once (but you can get fragments multiple times)
  auto maybe_batch_it = scanner->ScanBatches();
  if (maybe_batch_it.ok()) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("OneShotFragment was already scanned"),
        (*maybe_batch_it).Next());
  } else {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("OneShotFragment was already scanned"),
        std::move(maybe_batch_it));
  }

  // TODO(ARROW-16072) At the moment, we can't be sure that the scanner has completely
  // shutdown, even though the plan has finished, because errors are not handled cleanly
  // in the scanner/execplan relationship.  Once ARROW-16072 is fixed this should be
  // reliable and we can get rid of this.  See also ARROW-17198
  ::arrow::internal::GetCpuThreadPool()->WaitForIdle();
}

INSTANTIATE_TEST_SUITE_P(TestScannerThreading, TestScanner,
                         ::testing::ValuesIn(TestScannerParams::Values()),
                         [](const ::testing::TestParamInfo<TestScannerParams>& info) {
                           return std::to_string(info.index) + info.param.ToString();
                         });

/// These ControlledXyz classes allow for controlling the order in which things are
/// delivered so that we can test out of order resequencing.  The dataset allows
/// batches to be delivered on any fragment.  When delivering batches a num_rows
/// parameter is taken which can be used to differentiate batches.
class ControlledFragment : public Fragment {
 public:
  explicit ControlledFragment(std::shared_ptr<Schema> schema)
      : Fragment(literal(true), std::move(schema)),
        record_batch_generator_(),
        tracking_generator_(record_batch_generator_) {}

  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    return physical_schema_;
  }
  std::string type_name() const override { return "scanner_test.cc::ControlledFragment"; }

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    return tracking_generator_;
  };

  int NumBatchesRead() { return tracking_generator_.num_read(); }

  void Finish() { ARROW_UNUSED(record_batch_generator_.producer().Close()); }
  void DeliverBatch(uint32_t num_rows) {
    auto batch = ConstantArrayGenerator::Zeroes(num_rows, physical_schema_);
    record_batch_generator_.producer().Push(std::move(batch));
  }

 private:
  PushGenerator<std::shared_ptr<RecordBatch>> record_batch_generator_;
  util::TrackingGenerator<std::shared_ptr<RecordBatch>> tracking_generator_;
};

// TODO(ARROW-8163) Add testing for fragments arriving out of order
class ControlledDataset : public Dataset {
 public:
  explicit ControlledDataset(int num_fragments)
      : Dataset(arrow::schema({field("i32", int32())})), fragments_() {
    for (int i = 0; i < num_fragments; i++) {
      fragments_.push_back(std::make_shared<ControlledFragment>(schema_));
    }
  }

  std::string type_name() const override { return "scanner_test.cc::ControlledDataset"; }
  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override {
    return Status::NotImplemented("Should not be called by unit test");
  }

  void DeliverBatch(int fragment_index, int num_rows) {
    fragments_[fragment_index]->DeliverBatch(num_rows);
  }

  void FinishFragment(int fragment_index) { fragments_[fragment_index]->Finish(); }

 protected:
  Result<FragmentIterator> GetFragmentsImpl(compute::Expression predicate) override {
    std::vector<std::shared_ptr<Fragment>> casted_fragments(fragments_.begin(),
                                                            fragments_.end());
    return MakeVectorIterator(std::move(casted_fragments));
  }

 private:
  std::vector<std::shared_ptr<ControlledFragment>> fragments_;
};

constexpr int kNumFragments = 2;

class TestReordering : public ::testing::Test {
 public:
  void SetUp() override { dataset_ = std::make_shared<ControlledDataset>(kNumFragments); }

  // Given a vector of fragment indices (one per batch) return a vector
  // (one per fragment) mapping fragment index to the last occurrence of that
  // index in order
  //
  // This allows us to know when to mark a fragment as finished
  std::vector<int> GetLastIndices(const std::vector<int>& order) {
    std::vector<int> last_indices(kNumFragments);
    for (std::size_t i = 0; i < kNumFragments; i++) {
      auto last_p = std::find(order.rbegin(), order.rend(), static_cast<int>(i));
      EXPECT_NE(last_p, order.rend());
      last_indices[i] = static_cast<int>(std::distance(last_p, order.rend())) - 1;
    }
    return last_indices;
  }

  /// We buffer one item in order to enumerate it (technically this could be avoided if
  /// delivering in order but easier to have a single code path).  We also can't deliver
  /// items that don't come next.  These two facts make for some pretty complex logic
  /// to determine when items are ready to be collected.
  std::vector<TaggedRecordBatch> DeliverAndCollect(std::vector<int> order,
                                                   TaggedRecordBatchGenerator gen) {
    std::vector<TaggedRecordBatch> collected;
    auto last_indices = GetLastIndices(order);
    int num_fragments = static_cast<int>(last_indices.size());
    std::vector<int> batches_seen_for_fragment(num_fragments);
    auto current_fragment_index = 0;
    auto seen_fragment = false;
    for (std::size_t i = 0; i < order.size(); i++) {
      auto fragment_index = order[i];
      dataset_->DeliverBatch(fragment_index, static_cast<int>(i));
      batches_seen_for_fragment[fragment_index]++;
      if (static_cast<int>(i) == last_indices[fragment_index]) {
        dataset_->FinishFragment(fragment_index);
      }
      if (current_fragment_index == fragment_index) {
        if (seen_fragment) {
          EXPECT_FINISHES_OK_AND_ASSIGN(auto next, gen());
          collected.push_back(std::move(next));
        } else {
          seen_fragment = true;
        }
        if (static_cast<int>(i) == last_indices[fragment_index]) {
          // Immediately collect your bonus fragment
          EXPECT_FINISHES_OK_AND_ASSIGN(auto next, gen());
          collected.push_back(std::move(next));
          // Now collect any batches freed up that couldn't be delivered because they came
          // from the wrong fragment
          auto last_fragment_index = fragment_index;
          fragment_index++;
          seen_fragment = batches_seen_for_fragment[fragment_index] > 0;
          while (fragment_index < num_fragments &&
                 fragment_index != last_fragment_index) {
            last_fragment_index = fragment_index;
            for (int j = 0; j < batches_seen_for_fragment[fragment_index] - 1; j++) {
              EXPECT_FINISHES_OK_AND_ASSIGN(auto next, gen());
              collected.push_back(std::move(next));
            }
            if (static_cast<int>(i) >= last_indices[fragment_index]) {
              EXPECT_FINISHES_OK_AND_ASSIGN(auto next, gen());
              collected.push_back(std::move(next));
              fragment_index++;
              if (fragment_index < num_fragments) {
                seen_fragment = batches_seen_for_fragment[fragment_index] > 0;
              }
            }
          }
        }
      }
    }
    return collected;
  }

  struct FragmentStats {
    int last_index;
    bool seen;
  };

  std::vector<FragmentStats> GetFragmentStats(const std::vector<int>& order) {
    auto last_indices = GetLastIndices(order);
    std::vector<FragmentStats> fragment_stats;
    for (std::size_t i = 0; i < last_indices.size(); i++) {
      fragment_stats.push_back({last_indices[i], false});
    }
    return fragment_stats;
  }

  /// When data arrives out of order then we first have to buffer up 1 item in order to
  /// know when the last item has arrived (so we can mark it as the last).  This means
  /// sometimes we deliver an item and don't get one (first in a fragment) and sometimes
  /// we deliver an item and we end up getting two (last in a fragment)
  std::vector<EnumeratedRecordBatch> DeliverAndCollect(
      std::vector<int> order, EnumeratedRecordBatchGenerator gen) {
    std::vector<EnumeratedRecordBatch> collected;
    auto fragment_stats = GetFragmentStats(order);
    for (std::size_t i = 0; i < order.size(); i++) {
      auto fragment_index = order[i];
      dataset_->DeliverBatch(fragment_index, static_cast<int>(i));
      if (static_cast<int>(i) == fragment_stats[fragment_index].last_index) {
        dataset_->FinishFragment(fragment_index);
        EXPECT_FINISHES_OK_AND_ASSIGN(auto next, gen());
        collected.push_back(std::move(next));
      }
      if (!fragment_stats[fragment_index].seen) {
        fragment_stats[fragment_index].seen = true;
      } else {
        EXPECT_FINISHES_OK_AND_ASSIGN(auto next, gen());
        collected.push_back(std::move(next));
      }
    }
    return collected;
  }

  std::shared_ptr<Scanner> MakeScanner(int fragment_readahead = 0) {
    ScannerBuilder builder(dataset_);
    if (fragment_readahead != 0) {
      ARROW_EXPECT_OK(builder.FragmentReadahead(fragment_readahead));
    }
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    return scanner;
  }

  void AssertBatchesInOrder(const std::vector<TaggedRecordBatch>& batches,
                            std::vector<int> expected_order) {
    ASSERT_EQ(expected_order.size(), batches.size());
    for (std::size_t i = 0; i < batches.size(); i++) {
      ASSERT_EQ(expected_order[i], batches[i].record_batch->num_rows());
    }
  }

  void AssertBatchesInOrder(const std::vector<EnumeratedRecordBatch>& batches,
                            std::vector<int> expected_batch_indices,
                            std::vector<int> expected_row_sizes) {
    ASSERT_EQ(expected_batch_indices.size(), batches.size());
    for (std::size_t i = 0; i < batches.size(); i++) {
      ASSERT_EQ(expected_row_sizes[i], batches[i].record_batch.value->num_rows());
      ASSERT_EQ(expected_batch_indices[i], batches[i].record_batch.index);
    }
  }

  std::shared_ptr<ControlledDataset> dataset_;
};

TEST_F(TestReordering, ScanBatches) {
  auto scanner = MakeScanner();
  ASSERT_OK_AND_ASSIGN(auto batch_gen, scanner->ScanBatchesAsync());
  auto collected = DeliverAndCollect({0, 0, 1, 1, 0}, std::move(batch_gen));
  AssertBatchesInOrder(collected, {0, 1, 4, 2, 3});
}

TEST_F(TestReordering, ScanBatchesUnordered) {
  auto scanner = MakeScanner();
  ASSERT_OK_AND_ASSIGN(auto batch_gen, scanner->ScanBatchesUnorderedAsync());
  auto collected = DeliverAndCollect({0, 0, 1, 1, 0}, std::move(batch_gen));
}

static constexpr uint64_t kBatchSizeBytes = 40;
static constexpr uint64_t kMaxBatchesInSink = 8;
static constexpr uint64_t kResumeIfBelowBytes = kBatchSizeBytes * kMaxBatchesInSink / 2;
static constexpr uint64_t kPauseIfAboveBytes = kBatchSizeBytes * kMaxBatchesInSink;
// This is deterministic but rather odd to figure out.  Because of sequencing we have to
// read in a few extra batches for each fragment before we hit the backpressure limit
static constexpr int32_t kMaxBatchesRead =
    kDefaultFragmentReadahead * 3 + kMaxBatchesInSink + 1;

class TestBackpressure : public ::testing::Test {
 protected:
  static constexpr int NFRAGMENTS = 10;
  static constexpr int NBATCHES = 50;
  static constexpr int NROWS = 10;

  FragmentVector MakeFragmentsAndDeliverInitialBatches() {
    FragmentVector fragments;
    for (int i = 0; i < NFRAGMENTS; i++) {
      controlled_fragments_.emplace_back(std::make_shared<ControlledFragment>(schema_));
      fragments.push_back(controlled_fragments_[i]);
      // We only emit one batch on the first fragment.  This triggers the sequencing
      // generator to dig really deep to try and find the second batch
      int num_to_emit = NBATCHES;
      if (i == 0) {
        num_to_emit = 1;
      }
      for (int j = 0; j < num_to_emit; j++) {
        controlled_fragments_[i]->DeliverBatch(NROWS);
      }
    }
    return fragments;
  }

  void DeliverAdditionalBatches() {
    // Deliver a bunch of batches that should not be read in
    for (int i = 1; i < NFRAGMENTS; i++) {
      for (int j = 0; j < NBATCHES; j++) {
        controlled_fragments_[i]->DeliverBatch(NROWS);
      }
    }
  }

  std::shared_ptr<Dataset> MakeDataset() {
    FragmentVector fragments = MakeFragmentsAndDeliverInitialBatches();
    return std::make_shared<FragmentDataset>(schema_, std::move(fragments));
  }

  std::shared_ptr<Scanner> MakeScanner(::arrow::internal::Executor* io_executor) {
    acero::BackpressureOptions low_backpressure(kResumeIfBelowBytes, kPauseIfAboveBytes);
    io::IOContext io_context(default_memory_pool(), io_executor);
    std::shared_ptr<Dataset> dataset = MakeDataset();
    std::shared_ptr<ScanOptions> options = std::make_shared<ScanOptions>();
    options->io_context = io_context;
    ScannerBuilder builder(std::move(dataset), options);
    ARROW_EXPECT_OK(builder.UseThreads(false));
    ARROW_EXPECT_OK(builder.Backpressure(low_backpressure));
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    return scanner;
  }

  int TotalBatchesRead() {
    int sum = 0;
    for (const auto& controlled_fragment : controlled_fragments_) {
      sum += controlled_fragment->NumBatchesRead();
    }
    return sum;
  }

  template <typename T>
  void Finish(AsyncGenerator<T> gen) {
    for (const auto& controlled_fragment : controlled_fragments_) {
      controlled_fragment->Finish();
    }
    ASSERT_FINISHES_OK(VisitAsyncGenerator(gen, [](T batch) { return Status::OK(); }));
  }

  std::shared_ptr<Schema> schema_ = schema({field("values", int32())});
  std::vector<std::shared_ptr<ControlledFragment>> controlled_fragments_;
};

TEST_F(TestBackpressure, ScanBatchesUnordered) {
  // By forcing the plan to run on a single thread we know that the backpressure signal
  // will make it down before we try and read the next item which gives us much more exact
  // backpressure numbers
  ASSERT_OK_AND_ASSIGN(auto thread_pool, ::arrow::internal::ThreadPool::Make(1));
  std::shared_ptr<Scanner> scanner = MakeScanner(nullptr);
  auto initial_scan_fut = DeferNotOk(thread_pool->Submit(
      [&] { return scanner->ScanBatchesUnorderedAsync(thread_pool.get()); }));
  ASSERT_FINISHES_OK_AND_ASSIGN(AsyncGenerator<EnumeratedRecordBatch> gen,
                                initial_scan_fut);
  GetCpuThreadPool()->WaitForIdle();
  // By this point the plan will have been created and started and filled up to max
  // backpressure.  The exact measurement of "max backpressure" is a little hard to pin
  // down but it is deterministic since we're only using one thread.
  ASSERT_LE(TotalBatchesRead(), 155);
  DeliverAdditionalBatches();
  SleepABit();

  ASSERT_LE(TotalBatchesRead(), 160);
  Finish(std::move(gen));
}

TEST_F(TestBackpressure, ScanBatchesOrdered) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, ::arrow::internal::ThreadPool::Make(1));
  std::shared_ptr<Scanner> scanner = MakeScanner(nullptr);
  auto initial_scan_fut = DeferNotOk(
      thread_pool->Submit([&] { return scanner->ScanBatchesAsync(thread_pool.get()); }));
  ASSERT_FINISHES_OK_AND_ASSIGN(AsyncGenerator<TaggedRecordBatch> gen, initial_scan_fut);
  GetCpuThreadPool()->WaitForIdle();
  ASSERT_LE(TotalBatchesRead(), kMaxBatchesRead);
  DeliverAdditionalBatches();
  SleepABit();

  ASSERT_LE(TotalBatchesRead(), kMaxBatchesRead);
  Finish(std::move(gen));
}

struct BatchConsumer {
  explicit BatchConsumer(EnumeratedRecordBatchGenerator generator)
      : generator(std::move(generator)), next() {}

  void AssertCanConsume() {
    if (!next.is_valid()) {
      next = generator();
    }
    ASSERT_FINISHES_OK(next);
    next = Future<EnumeratedRecordBatch>();
  }

  void AssertCannotConsume() {
    if (!next.is_valid()) {
      next = generator();
    }
    SleepABit();
    ASSERT_FALSE(next.is_finished());
  }

  void AssertFinished() {
    if (!next.is_valid()) {
      next = generator();
    }
    ASSERT_FINISHES_OK_AND_ASSIGN(auto last, next);
    ASSERT_TRUE(IsIterationEnd(last));
  }

  EnumeratedRecordBatchGenerator generator;
  Future<EnumeratedRecordBatch> next;
};

TEST_F(TestReordering, FileReadahead) {
  auto scanner = MakeScanner(/*fragment_readahead=*/1);
  ASSERT_OK_AND_ASSIGN(auto batch_gen, scanner->ScanBatchesUnorderedAsync());
  BatchConsumer consumer(std::move(batch_gen));
  dataset_->DeliverBatch(0, 0);
  dataset_->DeliverBatch(0, 1);
  consumer.AssertCanConsume();
  consumer.AssertCannotConsume();
  dataset_->DeliverBatch(1, 0);
  consumer.AssertCannotConsume();
  dataset_->FinishFragment(1);
  // Even though fragment 1 is finished we cannot read it because fragment_readahead
  // is 1 so we should only be reading fragment 0
  consumer.AssertCannotConsume();
  dataset_->FinishFragment(0);
  consumer.AssertCanConsume();
  consumer.AssertCanConsume();
  consumer.AssertFinished();
}

class TestScannerBuilder : public ::testing::Test {
  void SetUp() override {
    DatasetVector sources;

    schema_ = schema({
        field("b", boolean()),
        field("i8", int8()),
        field("i16", int16()),
        field("i32", int32()),
        field("i64", int64()),
        field("nested", struct_({field("str", utf8())})),
    });

    ASSERT_OK_AND_ASSIGN(dataset_, UnionDataset::Make(schema_, sources));
  }

 protected:
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>();
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Dataset> dataset_;
};

TEST_F(TestScannerBuilder, TestProject) {
  ScannerBuilder builder(dataset_, options_);

  // It is valid to request no columns, e.g. `SELECT 1 FROM t WHERE t.a > 0`.
  // still needs to touch the `a` column.
  ASSERT_OK(builder.Project({}));
  ASSERT_OK(builder.Project({"i64", "b", "i8"}));
  ASSERT_OK(builder.Project({"i16", "i16"}));
  ASSERT_OK(builder.Project(
      {field_ref("i16"), call("multiply", {field_ref("i16"), literal(2)})},
      {"i16 renamed", "i16 * 2"}));
  ASSERT_OK(builder.Project({field_ref(FieldRef("nested", "str"))}, {".nested.str"}));

  ASSERT_RAISES(Invalid, builder.Project({"not_found_column"}));
  ASSERT_RAISES(Invalid, builder.Project({"i8", "not_found_column"}));
  ASSERT_RAISES(Invalid,
                builder.Project({field_ref("not_found_column"),
                                 call("multiply", {field_ref("i16"), literal(2)})},
                                {"i16 renamed", "i16 * 2"}));

  ASSERT_RAISES(Invalid, builder.Project({field_ref(FieldRef("nested", "not_a_column"))},
                                         {"nested column"}));

  // provided more field names than column exprs or vice versa
  ASSERT_RAISES(Invalid, builder.Project({}, {"i16 renamed", "i16 * 2"}));
  ASSERT_RAISES(Invalid, builder.Project({literal(2), field_ref("a")}, {"a"}));
}

TEST_F(TestScannerBuilder, TestFilter) {
  ScannerBuilder builder(dataset_, options_);

  ASSERT_OK(builder.Filter(literal(true)));
  ASSERT_OK(builder.Filter(equal(field_ref("i64"), literal<int64_t>(10))));
  ASSERT_OK(builder.Filter(or_(equal(field_ref("i64"), literal<int64_t>(10)),
                               equal(field_ref("b"), literal(true)))));
  ASSERT_OK(builder.Filter(equal(field_ref(FieldRef("nested", "str")), literal(""))));

  ASSERT_OK(builder.Filter(equal(field_ref("i64"), literal<double>(10))));

  ASSERT_RAISES(Invalid, builder.Filter(equal(field_ref("not_a_column"), literal(true))));

  ASSERT_RAISES(Invalid,
                builder.Filter(
                    equal(field_ref(FieldRef("nested", "not_a_column")), literal(true))));

  ASSERT_RAISES(Invalid,
                builder.Filter(or_(equal(field_ref("i64"), literal<int64_t>(10)),
                                   equal(field_ref("not_a_column"), literal(true)))));
}

TEST(ScanOptions, TestMaterializedFields) {
  auto i32 = field("i32", int32());
  auto i64 = field("i64", int64());
  auto opts = std::make_shared<ScanOptions>();

  auto set_projection_from_names = [&opts](std::vector<std::string> names) {
    ASSERT_OK_AND_ASSIGN(auto projection, ProjectionDescr::FromNames(
                                              std::move(names), *opts->dataset_schema));
    SetProjection(opts.get(), std::move(projection));
  };

  // empty dataset, project nothing = nothing materialized
  opts->dataset_schema = schema({});
  set_projection_from_names({});
  ASSERT_EQ(opts->MaterializedFields().size(), 0);

  // non-empty dataset, project nothing = nothing materialized
  opts->dataset_schema = schema({i32, i64});
  ASSERT_EQ(opts->MaterializedFields().size(), 0);

  // project nothing, filter on i32 = materialize i32
  opts->filter = equal(field_ref("i32"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre(FieldRef("i32")));

  // project i32 & i64, filter nothing = materialize i32 & i64
  opts->filter = literal(true);
  set_projection_from_names({"i32", "i64"});
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64"));

  // project i32 + i64, filter nothing = materialize i32 & i64
  opts->filter = literal(true);
  ASSERT_OK_AND_ASSIGN(auto projection,
                       ProjectionDescr::FromExpressions(
                           {call("add", {field_ref("i32"), field_ref("i64")})},
                           {"i32 + i64"}, *opts->dataset_schema));
  SetProjection(opts.get(), std::move(projection));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64"));

  // project i32, filter nothing = materialize i32
  set_projection_from_names({"i32"});
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32"));

  // project i32, filter on i32 = materialize i32 (reported twice)
  opts->filter = equal(field_ref("i32"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre(FieldRef("i32"), FieldRef("i32")));

  // project i32, filter on i32 & i64 = materialize i64, i32 (reported twice)
  opts->filter = less(field_ref("i32"), field_ref("i64"));
  EXPECT_THAT(opts->MaterializedFields(),
              ElementsAre(FieldRef("i32"), FieldRef("i64"), FieldRef("i32")));

  // project i32, filter on i64 = materialize i32 & i64
  opts->filter = equal(field_ref("i64"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre(FieldRef("i64"), FieldRef("i32")));

  auto nested = field("nested", struct_({i32, i64}));
  opts->dataset_schema = schema({nested});

  // project top-level field, filter nothing
  opts->filter = literal(true);
  ASSERT_OK_AND_ASSIGN(projection,
                       ProjectionDescr::FromNames({"nested"}, *opts->dataset_schema));
  SetProjection(opts.get(), std::move(projection));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre(FieldRef("nested")));

  // project child field, filter nothing
  opts->filter = literal(true);
  ASSERT_OK_AND_ASSIGN(projection, ProjectionDescr::FromExpressions(
                                       {field_ref(FieldRef("nested", "i64"))},
                                       {"nested.i64"}, *opts->dataset_schema));
  SetProjection(opts.get(), std::move(projection));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre(FieldRef("nested", "i64")));

  // project nothing, filter child field
  opts->filter = equal(field_ref(FieldRef("nested", "i64")), literal(10));
  ASSERT_OK_AND_ASSIGN(projection,
                       ProjectionDescr::FromExpressions({}, {}, *opts->dataset_schema));
  SetProjection(opts.get(), std::move(projection));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre(FieldRef("nested", "i64")));

  // project child field, filter child field
  opts->filter = equal(field_ref(FieldRef("nested", "i64")), literal(10));
  ASSERT_OK_AND_ASSIGN(projection, ProjectionDescr::FromExpressions(
                                       {field_ref(FieldRef("nested", "i32"))},
                                       {"nested.i32"}, *opts->dataset_schema));
  SetProjection(opts.get(), std::move(projection));
  EXPECT_THAT(opts->MaterializedFields(),
              ElementsAre(FieldRef("nested", "i64"), FieldRef("nested", "i32")));
}

namespace {
struct TestPlan {
  explicit TestPlan(compute::ExecContext* ctx = compute::threaded_exec_context())
      : plan(acero::ExecPlan::Make(*ctx).ValueOrDie()) {
    internal::Initialize();
  }

  Future<std::vector<compute::ExecBatch>> Run() {
    RETURN_NOT_OK(plan->Validate());
    plan->StartProducing();

    auto collected_fut = CollectAsyncGenerator(sink_gen);

    return AllFinished({plan->finished(), Future<>(collected_fut)})
        .Then([collected_fut]() -> Result<std::vector<compute::ExecBatch>> {
          ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
          return ::arrow::internal::MapVector(
              [](std::optional<compute::ExecBatch> batch) {
                return batch.value_or(compute::ExecBatch());
              },
              std::move(collected));
        });
  }

  acero::ExecPlan* get() { return plan.get(); }

  std::shared_ptr<acero::ExecPlan> plan;
  AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;
};

struct DatasetAndBatches {
  std::shared_ptr<Dataset> dataset;
  std::vector<compute::ExecBatch> batches;
};

DatasetAndBatches DatasetAndBatchesFromJSON(
    const std::shared_ptr<Schema>& dataset_schema,
    const std::shared_ptr<Schema>& physical_schema,
    const std::vector<std::vector<std::string>>& fragment_batch_strs,
    const std::vector<compute::Expression>& guarantees) {
  // If guarantees are provided we must have one for each batch
  if (!guarantees.empty()) {
    EXPECT_EQ(fragment_batch_strs.size(), guarantees.size());
  }

  RecordBatchVector record_batches;
  FragmentVector fragments;
  fragments.reserve(fragment_batch_strs.size());

  // construct fragments first
  for (size_t frag_ndx = 0; frag_ndx < fragment_batch_strs.size(); frag_ndx++) {
    const auto& batch_strs = fragment_batch_strs[frag_ndx];
    RecordBatchVector fragment_batches;
    fragment_batches.reserve(batch_strs.size());
    for (const auto& batch_str : batch_strs) {
      fragment_batches.push_back(RecordBatchFromJSON(physical_schema, batch_str));
    }
    record_batches.insert(record_batches.end(), fragment_batches.begin(),
                          fragment_batches.end());
    fragments.push_back(std::make_shared<InMemoryFragment>(
        physical_schema, std::move(fragment_batches),
        guarantees.empty() ? literal(true) : guarantees[frag_ndx]));
  }

  // then construct ExecBatches that can reference fields from constructed Fragments
  std::vector<compute::ExecBatch> batches;
  auto batch_it = record_batches.begin();
  for (size_t frag_ndx = 0; frag_ndx < fragment_batch_strs.size(); ++frag_ndx) {
    size_t frag_batch_count = fragment_batch_strs[frag_ndx].size();

    for (size_t batch_index = 0; batch_index < frag_batch_count; ++batch_index) {
      const auto& batch = *batch_it++;

      // the scanned ExecBatches will begin with physical columns
      batches.emplace_back(*batch);

      // augment scanned ExecBatch with columns for this fragment's guarantee
      if (!guarantees.empty()) {
        EXPECT_OK_AND_ASSIGN(auto known_fields,
                             ExtractKnownFieldValues(guarantees[frag_ndx]));
        for (const auto& known_field : known_fields.map) {
          batches.back().values.emplace_back(known_field.second);
        }
      }

      // scanned batches will be augmented with fragment and batch indices
      batches.back().values.emplace_back(static_cast<int>(frag_ndx));
      batches.back().values.emplace_back(static_cast<int>(batch_index));

      // ... and with the last-in-fragment flag
      batches.back().values.emplace_back(batch_index == frag_batch_count - 1);
      batches.back().values.emplace_back(fragments[frag_ndx]->ToString());

      // each batch carries a guarantee inherited from its Fragment's partition expression
      batches.back().guarantee = fragments[frag_ndx]->partition_expression();
    }
  }

  auto dataset = std::make_shared<FragmentDataset>(dataset_schema, std::move(fragments));
  return {std::move(dataset), std::move(batches)};
}

DatasetAndBatches MakeBasicDataset() {
  const auto dataset_schema = ::arrow::schema({
      field("a", int32()),
      field("b", boolean()),
      field("c", int32()),
  });

  const auto physical_schema = SchemaFromColumnNames(dataset_schema, {"a", "b"});

  return DatasetAndBatchesFromJSON(dataset_schema, physical_schema,
                                   {
                                       {
                                           R"([{"a": 1,    "b": null},
                                               {"a": 2,    "b": true}])",
                                           R"([{"a": null, "b": true},
                                               {"a": 3,    "b": false}])",
                                       },
                                       {
                                           R"([{"a": null, "b": true},
                                               {"a": 4,    "b": false}])",
                                           R"([{"a": 5,    "b": null},
                                               {"a": 6,    "b": false},
                                               {"a": 7,    "b": false}])",
                                       },
                                   },
                                   {
                                       equal(field_ref("c"), literal(23)),
                                       equal(field_ref("c"), literal(47)),
                                   });
}

DatasetAndBatches MakeNestedDataset() {
  const auto dataset_schema = ::arrow::schema({
      field("a", int32()),
      field("b", boolean()),
      field("c", struct_({
                     field("d", int64()),
                     field("e", float64()),
                 })),
  });
  const auto physical_schema = ::arrow::schema({
      field("a", int32()),
      field("b", boolean()),
      field("c", struct_({
                     field("e", int64()),
                 })),
  });

  return DatasetAndBatchesFromJSON(dataset_schema, physical_schema,
                                   {
                                       {
                                           R"([{"a": 1,    "b": null,  "c": {"e": 0}},
                                               {"a": 2,    "b": true,  "c": {"e": 1}}])",
                                           R"([{"a": null, "b": true,  "c": {"e": 2}},
                                               {"a": 3,    "b": false, "c": {"e": null}}])",
                                           R"([{"a": null, "b": null,  "c": null}])",
                                       },
                                       {
                                           R"([{"a": null, "b": true,  "c": {"e": 4}},
                                               {"a": 4,    "b": false, "c": null}])",
                                           R"([{"a": 5,    "b": null,  "c": {"e": 6}},
                                               {"a": 6,    "b": false, "c": {"e": 7}},
                                               {"a": 7,    "b": false, "c": {"e": null}}])",
                                       },
                                   },
                                   /*guarantees=*/{});
}

compute::Expression Materialize(std::vector<std::string> names,
                                bool include_aug_fields = false) {
  if (include_aug_fields) {
    for (auto aug_name :
         {"__fragment_index", "__batch_index", "__last_in_fragment", "__filename"}) {
      names.emplace_back(aug_name);
    }
  }

  std::vector<compute::Expression> exprs;
  for (const auto& name : names) {
    exprs.push_back(field_ref(name));
  }

  return project(exprs, names);
}
}  // namespace

TEST(ScanNode, Schema) {
  TestPlan plan;

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->projection = Materialize({});  // set an empty projection

  ASSERT_OK_AND_ASSIGN(auto scan,
                       acero::MakeExecNode("scan", plan.get(), {},
                                           ScanNodeOptions{basic.dataset, options}));

  auto fields = basic.dataset->schema()->fields();
  fields.push_back(field("__fragment_index", int32()));
  fields.push_back(field("__batch_index", int32()));
  fields.push_back(field("__last_in_fragment", boolean()));
  fields.push_back(field("__filename", utf8()));
  // output_schema is *always* the full augmented dataset schema, regardless of
  // projection (but some columns *may* be placeholder null Scalars if not projected)
  AssertSchemaEqual(Schema(fields), *scan->output_schema());
}

TEST(ScanNode, Trivial) {
  TestPlan plan;

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  // ensure all fields are materialized
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK(
      acero::Declaration::Sequence({
                                       {"scan", ScanNodeOptions{basic.dataset, options}},
                                       {"sink", acero::SinkNodeOptions{&plan.sink_gen}},
                                   })
          .AddToPlan(plan.get()));

  // trivial scan: the batches are returned unmodified
  auto expected = basic.batches;
  ASSERT_THAT(plan.Run(), Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, FilteredOnVirtualColumn) {
  TestPlan plan;

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->filter = less(field_ref("c"), literal(30));
  // ensure all fields are materialized
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK(
      acero::Declaration::Sequence({
                                       {"scan", ScanNodeOptions{basic.dataset, options}},
                                       {"sink", acero::SinkNodeOptions{&plan.sink_gen}},
                                   })
          .AddToPlan(plan.get()));

  auto expected = basic.batches;

  // only the first fragment will make it past the filter
  expected.pop_back();
  expected.pop_back();

  ASSERT_THAT(plan.Run(), Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, DeferredFilterOnPhysicalColumn) {
  TestPlan plan;

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->filter = greater(field_ref("a"), literal(4));
  // ensure all fields are materialized
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK(
      acero::Declaration::Sequence({
                                       {"scan", ScanNodeOptions{basic.dataset, options}},
                                       {"sink", acero::SinkNodeOptions{&plan.sink_gen}},
                                   })
          .AddToPlan(plan.get()));

  // No post filtering is performed by ScanNode: all batches will be yielded whole.
  // To filter out rows from individual batches, construct a FilterNode.
  auto expected = basic.batches;

  ASSERT_THAT(plan.Run(), Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, DISABLED_ProjectionPushdown) {
  // ARROW-13263
  TestPlan plan;

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->projection = Materialize({"b"}, /*include_aug_fields=*/true);

  ASSERT_OK(
      acero::Declaration::Sequence({
                                       {"scan", ScanNodeOptions{basic.dataset, options}},
                                       {"sink", acero::SinkNodeOptions{&plan.sink_gen}},
                                   })
          .AddToPlan(plan.get()));

  auto expected = basic.batches;

  int a_index = basic.dataset->schema()->GetFieldIndex("a");
  int c_index = basic.dataset->schema()->GetFieldIndex("c");
  for (auto& batch : expected) {
    // "a", "c" were not projected or filtered so they are dropped eagerly
    batch.values[a_index] = MakeNullScalar(batch.values[a_index].type());
    batch.values[c_index] = MakeNullScalar(batch.values[c_index].type());
  }

  ASSERT_THAT(plan.Run(), Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, MaterializationOfVirtualColumn) {
  TestPlan plan;

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK(acero::Declaration::Sequence(
                {
                    {"scan", ScanNodeOptions{basic.dataset, options}},
                    {"augmented_project",
                     acero::ProjectNodeOptions{
                         {field_ref("a"), field_ref("b"), field_ref("c")}}},
                    {"sink", acero::SinkNodeOptions{&plan.sink_gen}},
                })
                .AddToPlan(plan.get()));

  auto expected = basic.batches;

  for (auto& batch : expected) {
    // ProjectNode overwrites "c" placeholder with non-null drawn from guarantee
    const auto& value = *batch.guarantee.call()->arguments[1].literal();
    batch.values[2] = value;
  }

  ASSERT_THAT(plan.Run(), Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, MaterializationOfNestedVirtualColumn) {
  TestPlan plan;

  auto basic = MakeNestedDataset();

  auto options = std::make_shared<ScanOptions>();
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK(acero::Declaration::Sequence(
                {
                    {"scan", ScanNodeOptions{basic.dataset, options}},
                    {"augmented_project",
                     acero::ProjectNodeOptions{
                         {field_ref("a"), field_ref("b"), field_ref("c")}}},
                    {"sink", acero::SinkNodeOptions{&plan.sink_gen}},
                })
                .AddToPlan(plan.get()));

  // TODO(ARROW-1888): allow scanner to "patch up" structs with casts
  EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
      plan.Run());
}

TEST(ScanNode, MinimalEndToEnd) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(),
                                    ::arrow::internal::GetCpuThreadPool());

  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns,
  // ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<acero::ExecPlan> plan,
                       acero::ExecPlan::Make(exec_context));

  std::shared_ptr<Dataset> dataset = std::make_shared<InMemoryDataset>(
      TableFromJSON(schema({field("a", int32()), field("b", boolean())}),
                    {
                        R"([{"a": 1,    "b": null},
                            {"a": 2,    "b": true}])",
                        R"([{"a": null, "b": true},
                            {"a": 3,    "b": false}])",
                        R"([{"a": null, "b": true},
                            {"a": 4,    "b": false}])",
                        R"([{"a": 5,    "b": null},
                            {"a": 6,    "b": false},
                            {"a": 7,    "b": false}])",
                    }));

  auto options = std::make_shared<ScanOptions>();
  // specify the filter
  compute::Expression b_is_true = field_ref("b");
  options->filter = b_is_true;
  // for now, specify the projection as the full project expression (eventually this can
  // just be a list of materialized field names)
  compute::Expression a_times_2 = call("multiply", {field_ref("a"), literal(2)});
  // set the projection such that required project experssion field is included as a
  // field_ref
  compute::Expression project_expr = field_ref("a");
  options->projection =
      call("make_struct", {project_expr}, compute::MakeStructOptions{{"a * 2"}});

  // construct the scan node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * scan,
      acero::MakeExecNode("scan", plan.get(), {}, ScanNodeOptions{dataset, options}));

  // pipe the scan node into a filter node
  ASSERT_OK_AND_ASSIGN(acero::ExecNode * filter,
                       acero::MakeExecNode("filter", plan.get(), {scan},
                                           acero::FilterNodeOptions{b_is_true}));

  // pipe the filter node into a project node
  // NB: we're using the project node factory which preserves fragment/batch index
  // tagging, so we *can* reorder later if we choose. The tags will not appear in
  // our output.
  ASSERT_OK_AND_ASSIGN(acero::ExecNode * project,
                       acero::MakeExecNode("augmented_project", plan.get(), {filter},
                                           acero::ProjectNodeOptions{{a_times_2}}));

  // finally, pipe the project node into a sink node
  AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;
  ASSERT_OK(acero::MakeExecNode("ordered_sink", plan.get(), {project},
                                acero::SinkNodeOptions{&sink_gen}));

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = acero::MakeGeneratorReader(
      schema({field("a * 2", int32())}), std::move(sink_gen), exec_context.memory_pool());

  // start the ExecPlan
  plan->StartProducing();

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  // Sort table
  ASSERT_OK_AND_ASSIGN(
      auto indices,
      compute::SortIndices(collected, compute::SortOptions({compute::SortKey(
                                          "a * 2", compute::SortOrder::Ascending)})));
  ASSERT_OK_AND_ASSIGN(auto sorted, compute::Take(collected, indices));

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";

  auto expected = TableFromJSON(schema({field("a * 2", int32())}), {
                                                                       R"([
                                               {"a * 2": 4},
                                               {"a * 2": null},
                                               {"a * 2": null}
                                          ])"});
  AssertTablesEqual(*expected, *sorted.table(), /*same_chunk_layout=*/false);
}

TEST(ScanNode, MinimalScalarAggEndToEnd) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(), GetCpuThreadPool());

  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns,
  // ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<acero::ExecPlan> plan,
                       acero::ExecPlan::Make(exec_context));

  std::shared_ptr<Dataset> dataset = std::make_shared<InMemoryDataset>(
      TableFromJSON(schema({field("a", int32()), field("b", boolean())}),
                    {
                        R"([{"a": 1,    "b": null},
                            {"a": 2,    "b": true}])",
                        R"([{"a": null, "b": true},
                            {"a": 3,    "b": false}])",
                        R"([{"a": null, "b": true},
                            {"a": 4,    "b": false}])",
                        R"([{"a": 5,    "b": null},
                            {"a": 6,    "b": false},
                            {"a": 7,    "b": false}])",
                    }));

  auto options = std::make_shared<ScanOptions>();
  // specify the filter
  compute::Expression b_is_true = field_ref("b");
  options->filter = b_is_true;
  // for now, specify the projection as the full project expression (eventually this can
  // just be a list of materialized field names)
  compute::Expression a_times_2 = call("multiply", {field_ref("a"), literal(2)});
  // set the projection such that required project experssion field is included as a
  // field_ref
  compute::Expression project_expr = field_ref("a");
  options->projection =
      call("make_struct", {project_expr}, compute::MakeStructOptions{{"a * 2"}});

  // construct the scan node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * scan,
      acero::MakeExecNode("scan", plan.get(), {}, ScanNodeOptions{dataset, options}));

  // pipe the scan node into a filter node
  ASSERT_OK_AND_ASSIGN(acero::ExecNode * filter,
                       acero::MakeExecNode("filter", plan.get(), {scan},
                                           acero::FilterNodeOptions{b_is_true}));

  // pipe the filter node into a project node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * project,
      acero::MakeExecNode("project", plan.get(), {filter},
                          acero::ProjectNodeOptions{{a_times_2}, {"a * 2"}}));

  // pipe the projection into a scalar aggregate node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * aggregate,
      acero::MakeExecNode("aggregate", plan.get(), {project},
                          acero::AggregateNodeOptions{{compute::Aggregate{
                              "sum", nullptr, "a * 2", "sum(a * 2)"}}}));

  // finally, pipe the aggregate node into a sink node
  AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;
  ASSERT_OK(acero::MakeExecNode("sink", plan.get(), {aggregate},
                                acero::SinkNodeOptions{&sink_gen}));

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader =
      acero::MakeGeneratorReader(schema({field("a*2 sum", int64())}), std::move(sink_gen),
                                 exec_context.memory_pool());

  // start the ExecPlan
  plan->StartProducing();

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";

  auto expected = TableFromJSON(schema({field("a*2 sum", int64())}), {
                                                                         R"([
                                               {"a*2 sum": 4}
                                          ])"});
  AssertTablesEqual(*expected, *collected, /*same_chunk_layout=*/false);
}

TEST(ScanNode, MinimalGroupedAggEndToEnd) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(), GetCpuThreadPool());

  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns,
  // ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<acero::ExecPlan> plan,
                       acero::ExecPlan::Make(exec_context));

  std::shared_ptr<Dataset> dataset = std::make_shared<InMemoryDataset>(
      TableFromJSON(schema({field("a", int32()), field("b", boolean())}),
                    {
                        R"([{"a": 1,    "b": null},
                            {"a": 2,    "b": true}])",
                        R"([{"a": null, "b": true},
                            {"a": 3,    "b": false}])",
                        R"([{"a": null, "b": true},
                            {"a": 4,    "b": false}])",
                        R"([{"a": 5,    "b": null},
                            {"a": 6,    "b": false},
                            {"a": 7,    "b": false}])",
                    }));

  auto options = std::make_shared<ScanOptions>();
  // specify the filter
  compute::Expression b_is_true = field_ref("b");
  options->filter = b_is_true;
  // for now, specify the projection as the full project expression (eventually this can
  // just be a list of materialized field names)
  compute::Expression a_times_2 = call("multiply", {field_ref("a"), literal(2)});
  // set the projection such that required project experssion field is included as a
  // field_ref
  compute::Expression a = field_ref("a");
  compute::Expression b = field_ref("b");
  options->projection =
      call("make_struct", {a, b}, compute::MakeStructOptions{{"a * 2", "b"}});

  // construct the scan node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * scan,
      acero::MakeExecNode("scan", plan.get(), {}, ScanNodeOptions{dataset, options}));

  // pipe the scan node into a project node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * project,
      acero::MakeExecNode("project", plan.get(), {scan},
                          acero::ProjectNodeOptions{{a_times_2, b}, {"a * 2", "b"}}));

  // pipe the projection into a grouped aggregate node
  ASSERT_OK_AND_ASSIGN(
      acero::ExecNode * aggregate,
      acero::MakeExecNode(
          "aggregate", plan.get(), {project},
          acero::AggregateNodeOptions{
              {compute::Aggregate{"hash_sum", nullptr, "a * 2", "sum(a * 2)"}},
              /*keys=*/{"b"}}));

  // finally, pipe the aggregate node into a sink node
  AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;
  ASSERT_OK(acero::MakeExecNode("sink", plan.get(), {aggregate},
                                acero::SinkNodeOptions{&sink_gen}));

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = acero::MakeGeneratorReader(
      schema({field("b", boolean()), field("sum(a * 2)", int64())}), std::move(sink_gen),
      exec_context.memory_pool());

  // start the ExecPlan
  plan->StartProducing();

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  // Sort table
  ASSERT_OK_AND_ASSIGN(
      auto indices, compute::SortIndices(
                        collected, compute::SortOptions({compute::SortKey(
                                       "sum(a * 2)", compute::SortOrder::Ascending)})));
  ASSERT_OK_AND_ASSIGN(auto sorted, compute::Take(collected, indices));

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";

  auto expected = TableFromJSON(
      schema({field("b", boolean()), field("sum(a * 2)", int64())}), {
                                                                         R"JSON([
                                               {"b": true, "sum(a * 2)": 4},
                                               {"b": null, "sum(a * 2)": 12},
                                               {"b": false, "sum(a * 2)": 40}
                                          ])JSON"});
  AssertTablesEqual(*expected, *sorted.table(), /*same_chunk_layout=*/false);
}

TEST(ScanNode, OnlyLoadProjectedFields) {
  compute::ExecContext exec_context;
  arrow::dataset::internal::Initialize();
  ASSERT_OK_AND_ASSIGN(auto plan, acero::ExecPlan::Make());

  auto dummy_schema = schema(
      {field("key", int64()), field("shared", int64()), field("distinct", int64())});

  // creating a dummy dataset using a dummy table
  auto table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 20]
    ])"});

  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  const std::string file_name = "plan_scan_disk_test.arrow";

  ASSERT_OK_AND_ASSIGN(auto tempdir,
                       arrow::internal::TemporaryDir::Make("plan-test-tempdir-"));
  ASSERT_OK_AND_ASSIGN(auto file_path, tempdir->path().Join(file_name));
  std::string file_path_str = file_path.ToString();

  WriteIpcData(file_path_str, filesystem, table);

  std::vector<fs::FileInfo> files;
  const std::vector<std::string> f_paths = {file_path_str};

  for (const auto& f_path : f_paths) {
    ASSERT_OK_AND_ASSIGN(auto f_file, filesystem->GetFileInfo(f_path));
    files.push_back(std::move(f_file));
  }

  ASSERT_OK_AND_ASSIGN(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                            filesystem, std::move(files), format, {}));
  ASSERT_OK_AND_ASSIGN(auto dataset, ds_factory->Finish(dummy_schema));

  auto scan_options = std::make_shared<dataset::ScanOptions>();
  compute::Expression extract_expr = compute::field_ref("shared");
  // don't use a function.
  scan_options->projection =
      call("make_struct", {extract_expr}, compute::MakeStructOptions{{"shared"}});

  auto declarations = acero::Declaration::Sequence(
      {acero::Declaration({"scan", dataset::ScanNodeOptions{dataset, scan_options}})});
  ASSERT_OK_AND_ASSIGN(auto actual, acero::DeclarationToTable(declarations));
  // Scan node always emits augmented fields so we drop those
  ASSERT_OK_AND_ASSIGN(auto actualMinusAgumented, actual->SelectColumns({0, 1, 2}));
  auto expected = TableFromJSON(dummy_schema, {R"([
      [null, 1, null],
      [null, 4, null]
  ])"});
  AssertTablesEqual(*expected, *actualMinusAgumented, /*same_chunk_layout=*/false);
}

}  // namespace dataset
}  // namespace arrow
