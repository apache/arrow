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

#include <condition_variable>
#include <memory>
#include <mutex>

#include "arrow/dataset/scanner_internal.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/util.h"

using testing::ElementsAre;
using testing::IsEmpty;

namespace arrow {
namespace dataset {

constexpr int64_t kNumberChildDatasets = 2;
constexpr int64_t kNumberBatches = 16;
constexpr int64_t kBatchSize = 1024;

struct GatedDatasetState {
  GatedDatasetState() : unlocked(false), delivered(0) {}

  std::mutex mx;
  std::atomic<bool> unlocked;
  std::atomic<int> delivered;
  std::condition_variable unlock_cv;
  std::condition_variable delivered_cv;
};

class GatedScanTask : public ScanTask,
                      public std::enable_shared_from_this<GatedScanTask> {
 public:
  explicit GatedScanTask(std::shared_ptr<ScanOptions> options,
                         std::shared_ptr<Fragment> fragment, RecordBatchVector rbs,
                         bool gated, std::shared_ptr<GatedDatasetState> state)
      : ScanTask(std::move(options), std::move(fragment)),
        rbs_(std::move(rbs)),
        gated_(gated),
        state_(std::move(state)) {}

  Result<RecordBatchGenerator> ExecuteAsync() override {
    if (gated_) {
      return FutureFirstGenerator<std::shared_ptr<RecordBatch>>(DelayedExecute());
    } else {
      return GetGenerator();
    }
  };

 private:
  Future<RecordBatchGenerator> DelayedExecute() {
    auto thread_pool = internal::GetCpuThreadPool();
    auto self = shared_from_this();
    return DeferNotOk(
        thread_pool->Submit([self] { return self->WaitAndGetGenerator(); }));
  }

  Result<RecordBatchGenerator> WaitAndGetGenerator() {
    std::unique_lock<std::mutex> lk(state_->mx);
    auto state = state_;
    if (state_->unlock_cv.wait_for(lk, std::chrono::seconds(10),
                                   [state] { return state->unlocked.load(); })) {
      return GetGenerator();
    } else {
      ADD_FAILURE() << "After 10 seconds the gating scan task was not executed";
      return Status::Invalid("Expired gating task");
    }
  }

  RecordBatchGenerator GetGenerator() {
    auto source = MakeVectorGenerator(rbs_);
    auto self = shared_from_this();
    return [self, source]() {
      self->state_->delivered++;
      self->state_->delivered_cv.notify_one();
      return source();
    };
  }

  RecordBatchVector rbs_;
  bool gated_;
  std::shared_ptr<GatedDatasetState> state_;
};

class GatedFragment : public Fragment {
 public:
  explicit GatedFragment(std::shared_ptr<Schema> physical_schema,
                         std::shared_ptr<ScanOptions> scan_options,
                         std::shared_ptr<GatedDatasetState> state, bool gated)
      : physical_schema_(std::move(physical_schema)),
        scan_options_(std::move(scan_options)),
        state_(std::move(state)),
        gated_(gated) {}
  virtual ~GatedFragment() {}

  RecordBatchVector MakeRecordBatches(bool second_scan_task) {
    int offset = 0;
    if (!gated_) {
      offset += 2;
    }
    if (second_scan_task) {
      offset += 1;
    }
    auto arr = ConstantArrayGenerator::Int32(1, offset);
    auto batch = RecordBatch::Make(physical_schema_, 1, {arr});
    return {batch};
  }

  ScanTaskVector MakeScanTasks() {
    ScanTaskVector scan_tasks;
    auto fragment = shared_from_this();
    scan_tasks.push_back(std::make_shared<GatedScanTask>(
        scan_options_, fragment, MakeRecordBatches(false), true, state_));
    scan_tasks.push_back(std::make_shared<GatedScanTask>(
        scan_options_, fragment, MakeRecordBatches(true), false, state_));
    return scan_tasks;
  }

  Future<ScanTaskVector> Scan(std::shared_ptr<ScanOptions> options) override {
    ScanTaskVector scan_tasks = MakeScanTasks();
    auto state = state_;
    state_->delivered++;
    state_->delivered_cv.notify_one();
    if (gated_) {
      return DeferNotOk(
                 internal::GetCpuThreadPool()->Submit([scan_tasks, state]() -> Status {
                   std::unique_lock<std::mutex> lk(state->mx);
                   if (!state->unlock_cv.wait_for(lk, std::chrono::seconds(10), [state] {
                         return state->unlocked.load();
                       })) {
                     ADD_FAILURE() << "Timed out waiting for fragment to unlock";
                   }
                   return Status::OK();
                 }))
          .Then([scan_tasks](const detail::Empty& emp) { return scan_tasks; });
    }
    return Future<ScanTaskVector>::MakeFinished(scan_tasks);
  }

  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    return physical_schema_;
  }

  std::string type_name() const override { return "simple"; };

 private:
  std::shared_ptr<Schema> physical_schema_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<GatedDatasetState> state_;
  bool gated_;
};

class GatedDataset : public Dataset {
 public:
  // Dataset API
  explicit GatedDataset(std::shared_ptr<Schema> schema,
                        std::shared_ptr<ScanOptions> scan_options)
      : Dataset(schema),
        scan_options_(scan_options),
        state_(std::make_shared<GatedDatasetState>()) {}

  virtual ~GatedDataset() {}
  virtual std::string type_name() const { return "unit-test-gated-in-memory"; }
  virtual Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const {
    return Status::NotImplemented("Should not be called by unit test");
  }

  void WaitForDelivered(int num_delivered) {
    std::unique_lock<std::mutex> lk(state_->mx);
    auto state = state_;
    if (!state->delivered_cv.wait_for(lk, std::chrono::seconds(10),
                                      [state, num_delivered] {
                                        return state->delivered.load() >= num_delivered;
                                      })) {
      ADD_FAILURE() << "After 10 seconds there were not " << num_delivered
                    << " tasks delivered";
    }
  }

  void Unlock() {
    state_->unlocked.store(true);
    state_->unlock_cv.notify_all();
  }

 protected:
  virtual Future<FragmentVector> GetFragmentsImpl(Expression predicate) {
    auto gated_fragment =
        std::make_shared<GatedFragment>(schema_, scan_options_, state_, true);
    auto ungated_fragment =
        std::make_shared<GatedFragment>(schema_, scan_options_, state_, false);
    FragmentVector fragments = {gated_fragment, ungated_fragment};
    return Future<FragmentVector>::MakeFinished(fragments);
  }

 private:
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<GatedDatasetState> state_;
};

class TestScanner : public DatasetFixtureMixin {
 protected:
  Scanner MakeScanner(std::shared_ptr<RecordBatch> batch) {
    std::vector<std::shared_ptr<RecordBatch>> batches{static_cast<size_t>(kNumberBatches),
                                                      batch};

    DatasetVector children{static_cast<size_t>(kNumberChildDatasets),
                           std::make_shared<InMemoryDataset>(batch->schema(), batches)};

    EXPECT_OK_AND_ASSIGN(auto dataset, UnionDataset::Make(batch->schema(), children));

    return Scanner{dataset, options_};
  }

  void AssertScannerEqualsRepetitionsOf(
      Scanner scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = kNumberChildDatasets * kNumberBatches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    // Verifies that the unified BatchReader is equivalent to flattening all the
    // structures of the scanner, i.e. Scanner[Dataset[ScanTask[RecordBatch]]]
    AssertScannerEquals(expected.get(), &scanner);
  }
};

TEST_F(TestScanner, Scan) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  AssertScannerEqualsRepetitionsOf(MakeScanner(batch), batch);
}

TEST_F(TestScanner, ScanWithCappedBatchSize) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  options_->batch_size = kBatchSize / 2;
  auto expected = batch->Slice(kBatchSize / 2);
  AssertScannerEqualsRepetitionsOf(MakeScanner(batch), expected,
                                   kNumberChildDatasets * kNumberBatches * 2);
}

TEST_F(TestScanner, FilteredScan) {
  SetSchema({field("f64", float64())});

  double value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(float64(), kBatchSize, kBatchSize / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 builder->UnsafeAppend(-value);
                                                 value += 1.0;
                                               }));

  SetFilter(greater(field_ref("f64"), literal(0.0)));

  auto batch = RecordBatch::Make(schema_, f64->length(), {f64});

  value = 0.5;
  ASSERT_OK_AND_ASSIGN(
      auto f64_filtered,
      ArrayFromBuilderVisitor(float64(), kBatchSize / 2, [&](DoubleBuilder* builder) {
        builder->UnsafeAppend(value);
        value += 1.0;
      }));

  auto filtered_batch =
      RecordBatch::Make(schema_, f64_filtered->length(), {f64_filtered});

  AssertScannerEqualsRepetitionsOf(MakeScanner(batch), filtered_batch);
}

TEST_F(TestScanner, MaterializeMissingColumn) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch_missing_f64 =
      ConstantArrayGenerator::Zeroes(kBatchSize, schema({field("i32", int32())}));

  auto fragment_missing_f64 = std::make_shared<InMemoryFragment>(
      RecordBatchVector{static_cast<size_t>(kNumberChildDatasets * kNumberBatches),
                        batch_missing_f64},
      equal(field_ref("f64"), literal(2.5)));

  ASSERT_OK_AND_ASSIGN(auto f64, ArrayFromBuilderVisitor(float64(), kBatchSize,
                                                         [&](DoubleBuilder* builder) {
                                                           builder->UnsafeAppend(2.5);
                                                         }));
  auto batch_with_f64 =
      RecordBatch::Make(schema_, f64->length(), {batch_missing_f64->column(0), f64});

  ScannerBuilder builder{schema_, fragment_missing_f64, options_};
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());

  AssertScannerEqualsRepetitionsOf(*scanner, batch_with_f64);
}

TEST_F(TestScanner, PreservesOrder) {
  auto sch = schema({field("i32", int32())});
  auto scan_options = std::make_shared<ScanOptions>();
  scan_options->use_threads = true;
  auto dataset = std::make_shared<GatedDataset>(sch, scan_options);

  ScannerBuilder builder{dataset, scan_options};
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());

  auto table_fut = scanner->ToTableAsync();
  dataset->WaitForDelivered(1);
  SleepABit();
  dataset->Unlock();
  ASSERT_FINISHES_OK_AND_ASSIGN(auto table, table_fut);
  auto chunks = table->column(0)->chunks();
  ASSERT_EQ(4, chunks.size());
  ASSERT_EQ(4, table->num_rows());
  EXPECT_EQ("0", chunks[0]->GetScalar(0).ValueOrDie()->ToString());
  EXPECT_EQ("1", chunks[1]->GetScalar(0).ValueOrDie()->ToString());
  EXPECT_EQ("2", chunks[2]->GetScalar(0).ValueOrDie()->ToString());
  EXPECT_EQ("3", chunks[3]->GetScalar(0).ValueOrDie()->ToString());
}

TEST_F(TestScanner, ToTable) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches * kNumberChildDatasets,
                                                    batch};

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));

  auto scanner = MakeScanner(batch);
  std::shared_ptr<Table> actual;

  options_->use_threads = false;
  ASSERT_OK_AND_ASSIGN(actual, scanner.ToTable());
  AssertTablesEqual(*expected, *actual);

  // There is no guarantee on the ordering when using multiple threads, but
  // since the RecordBatch is always the same it will pass.
  options_->use_threads = true;
  ASSERT_OK_AND_ASSIGN(actual, scanner.ToTable());
  AssertTablesEqual(*expected, *actual);
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
    });

    ASSERT_OK_AND_ASSIGN(dataset_, UnionDataset::Make(schema_, sources));
  }

 protected:
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>();
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Dataset> dataset_;
};

TEST_F(TestScannerBuilder, DefaultOptions) {
  ScannerBuilder builder(dataset_);
  ASSERT_OK(builder.Finish());
}

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

  ASSERT_RAISES(Invalid, builder.Project({"not_found_column"}));
  ASSERT_RAISES(Invalid, builder.Project({"i8", "not_found_column"}));
  ASSERT_RAISES(Invalid,
                builder.Project({field_ref("not_found_column"),
                                 call("multiply", {field_ref("i16"), literal(2)})},
                                {"i16 renamed", "i16 * 2"}));

  ASSERT_RAISES(NotImplemented, builder.Project({field_ref(FieldRef("nested", "column"))},
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

  ASSERT_OK(builder.Filter(equal(field_ref("i64"), literal<double>(10))));

  ASSERT_RAISES(Invalid, builder.Filter(equal(field_ref("not_a_column"), literal(true))));

  ASSERT_RAISES(
      NotImplemented,
      builder.Filter(equal(field_ref(FieldRef("nested", "column")), literal(true))));

  ASSERT_RAISES(Invalid,
                builder.Filter(or_(equal(field_ref("i64"), literal<int64_t>(10)),
                                   equal(field_ref("not_a_column"), literal(true)))));
}

TEST(ScanOptions, TestMaterializedFields) {
  auto i32 = field("i32", int32());
  auto i64 = field("i64", int64());
  auto opts = std::make_shared<ScanOptions>();

  // empty dataset, project nothing = nothing materialized
  opts->dataset_schema = schema({});
  ASSERT_OK(SetProjection(opts.get(), {}, {}));
  EXPECT_THAT(opts->MaterializedFields(), IsEmpty());

  // non-empty dataset, project nothing = nothing materialized
  opts->dataset_schema = schema({i32, i64});
  EXPECT_THAT(opts->MaterializedFields(), IsEmpty());

  // project nothing, filter on i32 = materialize i32
  opts->filter = equal(field_ref("i32"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32"));

  // project i32 & i64, filter nothing = materialize i32 & i64
  opts->filter = literal(true);
  ASSERT_OK(SetProjection(opts.get(), {"i32", "i64"}));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64"));

  // project i32 + i64, filter nothing = materialize i32 & i64
  opts->filter = literal(true);
  ASSERT_OK(SetProjection(opts.get(), {call("add", {field_ref("i32"), field_ref("i64")})},
                          {"i32 + i64"}));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64"));

  // project i32, filter nothing = materialize i32
  ASSERT_OK(SetProjection(opts.get(), {"i32"}));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32"));

  // project i32, filter on i32 = materialize i32 (reported twice)
  opts->filter = equal(field_ref("i32"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i32"));

  // project i32, filter on i32 & i64 = materialize i64, i32 (reported twice)
  opts->filter = less(field_ref("i32"), field_ref("i64"));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64", "i32"));

  // project i32, filter on i64 = materialize i32 & i64
  opts->filter = equal(field_ref("i64"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i64", "i32"));
}

}  // namespace dataset
}  // namespace arrow
