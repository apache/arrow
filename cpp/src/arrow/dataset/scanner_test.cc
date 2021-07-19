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
#include <utility>

#include <gmock/gmock.h>

#include "arrow/compute/api.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/util.h"
#include "arrow/util/range.h"
#include "arrow/util/vector.h"

using testing::ElementsAre;
using testing::IsEmpty;
using testing::UnorderedElementsAreArray;

namespace arrow {
namespace dataset {

struct TestScannerParams {
  bool use_async;
  bool use_threads;
  int num_child_datasets;
  int num_batches;
  int items_per_batch;

  std::string ToString() const {
    // GTest requires this to be alphanumeric
    std::stringstream ss;
    ss << (use_async ? "Async" : "Sync") << (use_threads ? "Threaded" : "Serial")
       << num_child_datasets << "d" << num_batches << "b" << items_per_batch << "r";
    return ss.str();
  }

  static std::string ToTestNameString(
      const ::testing::TestParamInfo<TestScannerParams>& info) {
    return std::to_string(info.index) + info.param.ToString();
  }

  static std::vector<TestScannerParams> Values() {
    std::vector<TestScannerParams> values;
    for (int sync = 0; sync < 2; sync++) {
      for (int use_threads = 0; use_threads < 2; use_threads++) {
        values.push_back(
            {static_cast<bool>(sync), static_cast<bool>(use_threads), 1, 1, 1024});
        values.push_back(
            {static_cast<bool>(sync), static_cast<bool>(use_threads), 2, 16, 1024});
      }
    }
    return values;
  }
};

std::ostream& operator<<(std::ostream& out, const TestScannerParams& params) {
  out << (params.use_async ? "async-" : "sync-")
      << (params.use_threads ? "threaded-" : "serial-") << params.num_child_datasets
      << "d-" << params.num_batches << "b-" << params.items_per_batch << "i";
  return out;
}

class TestScanner : public DatasetFixtureMixinWithParam<TestScannerParams> {
 protected:
  std::shared_ptr<Scanner> MakeScanner(std::shared_ptr<Dataset> dataset) {
    ScannerBuilder builder(std::move(dataset), options_);
    ARROW_EXPECT_OK(builder.UseThreads(GetParam().use_threads));
    ARROW_EXPECT_OK(builder.UseAsync(GetParam().use_async));
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    return scanner;
  }

  std::shared_ptr<Scanner> MakeScanner(std::shared_ptr<RecordBatch> batch) {
    std::vector<std::shared_ptr<RecordBatch>> batches{
        static_cast<size_t>(GetParam().num_batches), batch};

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

TEST_P(TestScanner, ProjectedScan) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  SetProjectedColumns({"i32"});
  auto batch_in = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto batch_out = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch,
                                                  schema({field("i32", int32())}));
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
  std::vector<std::shared_ptr<RecordBatch>> batches{
      static_cast<std::size_t>(GetParam().num_batches * GetParam().num_child_datasets),
      batch};

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));

  auto scanner = MakeScanner(batch);
  std::shared_ptr<Table> actual;

  // There is no guarantee on the ordering when using multiple threads, but
  // since the RecordBatch is always the same it will pass.
  ASSERT_OK_AND_ASSIGN(actual, scanner->ToTable());
  AssertTablesEqual(*expected, *actual);
}

TEST_P(TestScanner, ScanWithVisitor) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto scanner = MakeScanner(batch);
  ASSERT_OK(scanner->Scan([batch](TaggedRecordBatch scanned_batch) {
    AssertBatchesEqual(*batch, *scanned_batch.record_batch);
    return Status::OK();
  }));
}

TEST_P(TestScanner, TakeIndices) {
  auto batch_size = GetParam().items_per_batch;
  auto num_batches = GetParam().num_batches;
  auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  ArrayVector arrays(2);
  ArrayFromVector<Int32Type>(internal::Iota<int32_t>(batch_size), &arrays[0]);
  ArrayFromVector<DoubleType>(internal::Iota<double>(static_cast<double>(batch_size)),
                              &arrays[1]);
  auto batch = RecordBatch::Make(schema_, batch_size, arrays);

  auto scanner = MakeScanner(batch);

  std::shared_ptr<Array> indices;
  {
    ArrayFromVector<Int64Type>(internal::Iota(batch_size), &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches({batch}));
    ASSERT_EQ(expected->num_rows(), batch_size);
    AssertTablesEqual(*expected, *taken);
  }
  {
    ArrayFromVector<Int64Type>({7, 5, 3, 1}, &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 4);
    AssertTablesEqual(*expected.table(), *taken);
  }
  if (num_batches > 1) {
    ArrayFromVector<Int64Type>({batch_size + 2, batch_size + 1}, &indices);
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 2);
    AssertTablesEqual(*expected.table(), *taken);
  }
  if (num_batches > 1) {
    ArrayFromVector<Int64Type>({1, 3, 5, 7, batch_size + 1, 2 * batch_size + 2},
                               &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 6);
    AssertTablesEqual(*expected.table(), *taken);
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
  ArrayFromVector<Int32Type>(
      internal::Iota<int32_t>(static_cast<int32_t>(items_per_batch)), &arrays[0]);
  ArrayFromVector<DoubleType>(
      internal::Iota<double>(static_cast<double>(items_per_batch)), &arrays[1]);
  auto batch = RecordBatch::Make(schema_, items_per_batch, arrays);
  auto scanner = MakeScanner(batch);

  ASSERT_OK_AND_ASSIGN(auto rows, scanner->CountRows());
  ASSERT_EQ(rows, num_datasets * num_batches * items_per_batch);

  ASSERT_OK_AND_ASSIGN(options_->filter,
                       greater_equal(field_ref("i32"), literal(64)).Bind(*schema_));
  ASSERT_OK_AND_ASSIGN(rows, scanner->CountRows());
  ASSERT_EQ(rows, num_datasets * num_batches * (items_per_batch - 64));
}

class CountRowsOnlyFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  Future<util::optional<int64_t>> CountRows(
      compute::Expression predicate, const std::shared_ptr<ScanOptions>&) override {
    if (compute::FieldsInExpression(predicate).size() > 0) {
      return Future<util::optional<int64_t>>::MakeFinished(util::nullopt);
    }
    int64_t sum = 0;
    for (const auto& batch : record_batches_) {
      sum += batch->num_rows();
    }
    return Future<util::optional<int64_t>>::MakeFinished(sum);
  }
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions>) override {
    return Status::Invalid("Don't scan me!");
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>&) override {
    return Status::Invalid("Don't scan me!");
  }
};

class ScanOnlyFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  Future<util::optional<int64_t>> CountRows(
      compute::Expression predicate, const std::shared_ptr<ScanOptions>&) override {
    return Future<util::optional<int64_t>>::MakeFinished(util::nullopt);
  }
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    auto self = shared_from_this();
    ScanTaskVector tasks{
        std::make_shared<InMemoryScanTask>(record_batches_, options, self)};
    return MakeVectorIterator(std::move(tasks));
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
  ASSERT_OK(builder.UseAsync(GetParam().use_async));
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  ASSERT_OK_AND_EQ(batch->num_rows(), scanner->CountRows());
}

// Regression test for ARROW-12668: ensure failures are properly handled
class CountFailFragment : public InMemoryFragment {
 public:
  explicit CountFailFragment(RecordBatchVector record_batches)
      : InMemoryFragment(std::move(record_batches)),
        count(Future<util::optional<int64_t>>::Make()) {}

  Future<util::optional<int64_t>> CountRows(
      compute::Expression, const std::shared_ptr<ScanOptions>&) override {
    return count;
  }

  Future<util::optional<int64_t>> count;
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
  ASSERT_OK(builder.UseAsync(GetParam().use_async));
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  fragment1->count.MarkFinished(Status::Invalid(""));
  // Should immediately stop the count
  ASSERT_RAISES(Invalid, scanner->CountRows());
  // Fragment 2 doesn't complete until after the count stops - should not break anything
  // under ASan, etc.
  fragment2->count.MarkFinished(util::nullopt);
}

TEST_P(TestScanner, CountRowsWithMetadata) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {batch, batch, batch, batch};
  ScannerBuilder builder(
      std::make_shared<FragmentDataset>(
          schema_, FragmentVector{std::make_shared<CountRowsOnlyFragment>(batches)}),
      options_);
  ASSERT_OK(builder.UseAsync(GetParam().use_async));
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
  std::vector<std::shared_ptr<RecordBatch>> batches{
      static_cast<std::size_t>(GetParam().num_batches * GetParam().num_child_datasets),
      batch};

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));

  std::shared_ptr<Table> actual;
  auto scanner = MakeScanner(batch);
  ASSERT_OK_AND_ASSIGN(auto reader, scanner->ToRecordBatchReader());
  scanner.reset();
  ASSERT_OK(reader->ReadAll(&actual));
  AssertTablesEqual(*expected, *actual);
}

class FailingFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    int index = 0;
    auto self = shared_from_this();
    return MakeFunctionIterator([=]() mutable -> Result<std::shared_ptr<ScanTask>> {
      if (index > 16) {
        return Status::Invalid("Oh no, we failed!");
      }
      RecordBatchVector batches = {record_batches_[index++ % record_batches_.size()]};
      return std::make_shared<InMemoryScanTask>(batches, options, self);
    });
  }

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

class FailingExecuteScanTask : public InMemoryScanTask {
 public:
  using InMemoryScanTask::InMemoryScanTask;

  Result<RecordBatchIterator> Execute() override {
    return Status::Invalid("Oh no, we failed!");
  }
};

class FailingIterationScanTask : public InMemoryScanTask {
 public:
  using InMemoryScanTask::InMemoryScanTask;

  Result<RecordBatchIterator> Execute() override {
    int index = 0;
    auto batches = record_batches_;
    return MakeFunctionIterator(
        [index, batches]() mutable -> Result<std::shared_ptr<RecordBatch>> {
          if (index < 1) {
            return batches[index++];
          }
          return Status::Invalid("Oh no, we failed!");
        });
  }
};

template <typename T>
class FailingScanTaskFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    auto self = shared_from_this();
    ScanTaskVector scan_tasks{std::make_shared<T>(record_batches_, options, self)};
    return MakeVectorIterator(std::move(scan_tasks));
  }

  // Unlike the sync case, there's only two places to fail - during
  // iteration (covered by FailingFragment) or at the initial scan
  // (covered here)
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
    ASSERT_OK_AND_ASSIGN(auto tagged_batch_it, scanner->ScanBatches());
    EXPECT_TRUE(CheckIteratorRaises(
        batch, std::move(tagged_batch_it),
        [](const TaggedRecordBatch& batch) { return batch.record_batch; }))
        << "ScanBatches() did not raise an error";
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
    FragmentVector fragments{
        std::make_shared<FailingScanTaskFragment<FailingExecuteScanTask>>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }

  // Case 3: failure when calling RecordBatchIterator::Next
  {
    FragmentVector fragments{
        std::make_shared<FailingScanTaskFragment<FailingIterationScanTask>>(batches)};
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
  AssertTablesEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {batch}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size));
  AssertTablesEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {batch->Slice(0, 1)}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(1));
  AssertTablesEqual(*expected, *actual);

  if (num_batches > 1) {
    ASSERT_OK_AND_ASSIGN(expected,
                         Table::FromRecordBatches(schema_, {batch, batch->Slice(0, 1)}));
    ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size + 1));
    AssertTablesEqual(*expected, *actual);
  }

  ASSERT_OK_AND_ASSIGN(expected, scanner->ToTable());
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size * num_batches * num_datasets));
  AssertTablesEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(expected, scanner->ToTable());
  ASSERT_OK_AND_ASSIGN(actual,
                       scanner->Head(batch_size * num_batches * num_datasets + 100));
  AssertTablesEqual(*expected, *actual);
}

TEST_P(TestScanner, FromReader) {
  if (GetParam().use_async) {
    GTEST_SKIP() << "Async scanner does not support construction from reader";
  }
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
  ASSERT_OK_AND_ASSIGN(auto batch_it, scanner->ScanBatches());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("OneShotFragment was already scanned"),
      batch_it.Next());
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
      : Fragment(literal(true), std::move(schema)) {}

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    return Status::NotImplemented(
        "Not needed for testing.  Sync can only return things in-order.");
  }
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    return physical_schema_;
  }
  std::string type_name() const override { return "scanner_test.cc::ControlledFragment"; }

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    return record_batch_generator_;
  };

  void Finish() { ARROW_UNUSED(record_batch_generator_.producer().Close()); }
  void DeliverBatch(uint32_t num_rows) {
    auto batch = ConstantArrayGenerator::Zeroes(num_rows, physical_schema_);
    record_batch_generator_.producer().Push(std::move(batch));
  }

 private:
  PushGenerator<std::shared_ptr<RecordBatch>> record_batch_generator_;
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
    // Reordering tests only make sense for async
    ARROW_EXPECT_OK(builder.UseAsync(true));
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
  AssertBatchesInOrder(collected, {0, 0, 1, 1, 2}, {0, 2, 3, 1, 4});
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

namespace {

Future<std::vector<compute::ExecBatch>> StartAndCollect(
    compute::ExecPlan* plan, AsyncGenerator<util::optional<compute::ExecBatch>> gen) {
  RETURN_NOT_OK(plan->Validate());
  RETURN_NOT_OK(plan->StartProducing());

  auto collected_fut = CollectAsyncGenerator(gen);

  return AllComplete({plan->finished(), Future<>(collected_fut)})
      .Then([collected_fut]() -> Result<std::vector<compute::ExecBatch>> {
        ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
        return internal::MapVector(
            [](util::optional<compute::ExecBatch> batch) { return std::move(*batch); },
            std::move(collected));
      });
}

struct DatasetAndBatches {
  std::shared_ptr<Dataset> dataset;
  std::vector<compute::ExecBatch> batches;
};

DatasetAndBatches MakeBasicDataset() {
  const auto dataset_schema = ::arrow::schema({
      field("a", int32()),
      field("b", boolean()),
      field("c", int32()),
  });

  const auto physical_schema = SchemaFromColumnNames(dataset_schema, {"a", "b"});

  RecordBatchVector record_batches{
      RecordBatchFromJSON(physical_schema, R"([{"a": 1,    "b": null},
                                               {"a": 2,    "b": true}])"),
      RecordBatchFromJSON(physical_schema, R"([{"a": null, "b": true},
                                               {"a": 3,    "b": false}])"),
      RecordBatchFromJSON(physical_schema, R"([{"a": null, "b": true},
                                               {"a": 4,    "b": false}])"),
      RecordBatchFromJSON(physical_schema, R"([{"a": 5,    "b": null},
                                               {"a": 6,    "b": false},
                                               {"a": 7,    "b": false}])"),
  };

  auto dataset = std::make_shared<FragmentDataset>(
      dataset_schema,
      FragmentVector{
          std::make_shared<InMemoryFragment>(
              physical_schema, RecordBatchVector{record_batches[0], record_batches[1]},
              equal(field_ref("c"), literal(23))),
          std::make_shared<InMemoryFragment>(
              physical_schema, RecordBatchVector{record_batches[2], record_batches[3]},
              equal(field_ref("c"), literal(47))),
      });

  std::vector<compute::ExecBatch> batches;

  auto batch_it = record_batches.begin();
  for (int fragment_index = 0; fragment_index < 2; ++fragment_index) {
    for (int batch_index = 0; batch_index < 2; ++batch_index) {
      const auto& batch = *batch_it++;

      // the scanned ExecBatches will begin with physical columns
      batches.emplace_back(*batch);

      // a placeholder will be inserted for partition field "c"
      batches.back().values.emplace_back(std::make_shared<Int32Scalar>());

      // scanned batches will be augmented with fragment and batch indices
      batches.back().values.emplace_back(fragment_index);
      batches.back().values.emplace_back(batch_index);

      // ... and with the last-in-fragment flag
      batches.back().values.emplace_back(batch_index == 1);

      // each batch carries a guarantee inherited from its Fragment's partition expression
      batches.back().guarantee =
          equal(field_ref("c"), literal(fragment_index == 0 ? 23 : 47));
    }
  }

  return {dataset, batches};
}

compute::Expression Materialize(std::vector<std::string> names,
                                bool include_aug_fields = false) {
  if (include_aug_fields) {
    for (auto aug_name : {"__fragment_index", "__batch_index", "__last_in_fragment"}) {
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
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->use_async = true;
  options->projection = Materialize({});  // set an empty projection

  ASSERT_OK_AND_ASSIGN(auto scan, MakeScanNode(plan.get(), basic.dataset, options));

  auto fields = basic.dataset->schema()->fields();
  fields.push_back(field("__fragment_index", int32()));
  fields.push_back(field("__batch_index", int32()));
  fields.push_back(field("__last_in_fragment", boolean()));
  // output_schema is *always* the full augmented dataset schema, regardless of projection
  // (but some columns *may* be placeholder null Scalars if not projected)
  AssertSchemaEqual(Schema(fields), *scan->output_schema());
}

TEST(ScanNode, Trivial) {
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->use_async = true;
  // ensure all fields are materialized
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK_AND_ASSIGN(auto scan, MakeScanNode(plan.get(), basic.dataset, options));
  auto sink_gen = MakeSinkNode(scan, "sink");

  // trivial scan: the batches are returned unmodified
  auto expected = basic.batches;
  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, FilteredOnVirtualColumn) {
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->use_async = true;
  options->filter = less(field_ref("c"), literal(30));
  // ensure all fields are materialized
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK_AND_ASSIGN(auto scan, MakeScanNode(plan.get(), basic.dataset, options));

  auto sink_gen = MakeSinkNode(scan, "sink");

  auto expected = basic.batches;

  // only the first fragment will make it past the filter
  expected.pop_back();
  expected.pop_back();

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, DeferredFilterOnPhysicalColumn) {
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->use_async = true;
  options->filter = greater(field_ref("a"), literal(4));
  // ensure all fields are materialized
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK_AND_ASSIGN(auto scan, MakeScanNode(plan.get(), basic.dataset, options));

  auto sink_gen = MakeSinkNode(scan, "sink");

  // No post filtering is performed by ScanNode: all batches will be yielded whole.
  // To filter out rows from individual batches, construct a FilterNode.
  auto expected = basic.batches;

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, DISABLED_ProjectionPushdown) {
  // ARROW-13263
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->use_async = true;
  options->projection = Materialize({"b"}, /*include_aug_fields=*/true);

  ASSERT_OK_AND_ASSIGN(auto scan, MakeScanNode(plan.get(), basic.dataset, options));

  auto sink_gen = MakeSinkNode(scan, "sink");

  auto expected = basic.batches;

  int a_index = basic.dataset->schema()->GetFieldIndex("a");
  int c_index = basic.dataset->schema()->GetFieldIndex("c");
  for (auto& batch : expected) {
    // "a", "c" were not projected or filtered so they are dropped eagerly
    batch.values[a_index] = MakeNullScalar(batch.values[a_index].type());
    batch.values[c_index] = MakeNullScalar(batch.values[c_index].type());
  }

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, MaterializationOfVirtualColumn) {
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());

  auto basic = MakeBasicDataset();

  auto options = std::make_shared<ScanOptions>();
  options->use_async = true;
  options->projection = Materialize({"a", "b", "c"}, /*include_aug_fields=*/true);

  ASSERT_OK_AND_ASSIGN(auto scan, MakeScanNode(plan.get(), basic.dataset, options));

  ASSERT_OK_AND_ASSIGN(
      auto project,
      dataset::MakeAugmentedProjectNode(
          scan, "project", {field_ref("a"), field_ref("b"), field_ref("c")}));

  auto sink_gen = MakeSinkNode(project, "sink");

  auto expected = basic.batches;

  for (auto& batch : expected) {
    // ProjectNode overwrites "c" placeholder with non-null drawn from guarantee
    const auto& value = *batch.guarantee.call()->arguments[1].literal();
    batch.values[project->output_schema()->GetFieldIndex("c")] = value;
  }

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(expected))));
}

TEST(ScanNode, MinimalEndToEnd) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(), internal::GetCpuThreadPool());

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns, ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecPlan> plan,
                       compute::ExecPlan::Make(&exec_context));

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
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * scan,
                       dataset::MakeScanNode(plan.get(), dataset, options));

  // pipe the scan node into a filter node
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * filter,
                       compute::MakeFilterNode(scan, "filter", b_is_true));

  // pipe the filter node into a project node
  // NB: we're using the project node factory which preserves fragment/batch index
  // tagging, so we *can* reorder later if we choose. The tags will not appear in
  // our output.
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * project,
                       dataset::MakeAugmentedProjectNode(filter, "project", {a_times_2}));

  // finally, pipe the project node into a sink node
  // NB: if we don't need ordering, we could use compute::MakeSinkNode instead
  ASSERT_OK_AND_ASSIGN(auto sink_gen, dataset::MakeOrderedSinkNode(project, "sink"));

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      schema({field("a * 2", int32())}), std::move(sink_gen), exec_context.memory_pool());

  // start the ExecPlan
  ASSERT_OK(plan->StartProducing());

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";

  auto expected = TableFromJSON(schema({field("a * 2", int32())}), {
                                                                       R"([
                                               {"a * 2": 4},
                                               {"a * 2": null},
                                               {"a * 2": null}
                                          ])"});
  AssertTablesEqual(*expected, *collected, /*same_chunk_layout=*/false);
}

TEST(ScanNode, MinimalScalarAggEndToEnd) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(), internal::GetCpuThreadPool());

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns, ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecPlan> plan,
                       compute::ExecPlan::Make(&exec_context));

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
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * scan,
                       dataset::MakeScanNode(plan.get(), dataset, options));

  // pipe the scan node into a filter node
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * filter,
                       compute::MakeFilterNode(scan, "filter", b_is_true));

  // pipe the filter node into a project node
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * project,
                       compute::MakeProjectNode(filter, "project", {a_times_2}));

  // pipe the projection into a scalar aggregate node
  ASSERT_OK_AND_ASSIGN(
      compute::ExecNode * sum,
      compute::MakeScalarAggregateNode(project, "scalar_agg",
                                       {compute::internal::Aggregate{"sum", nullptr}}));

  // finally, pipe the project node into a sink node
  auto sink_gen = compute::MakeSinkNode(sum, "sink");

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      schema({field("sum", int64())}), std::move(sink_gen), exec_context.memory_pool());

  // start the ExecPlan
  ASSERT_OK(plan->StartProducing());

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";

  auto expected = TableFromJSON(schema({field("sum", int64())}), {
                                                                     R"([
                                               {"sum": 4}
                                          ])"});
  AssertTablesEqual(*expected, *collected, /*same_chunk_layout=*/false);
}

TEST(ScanNode, MinimalGroupedAggEndToEnd) {
  // NB: This test is here for didactic purposes

  // Specify a MemoryPool and ThreadPool for the ExecPlan
  compute::ExecContext exec_context(default_memory_pool(), internal::GetCpuThreadPool());

  // A ScanNode is constructed from an ExecPlan (into which it is inserted),
  // a Dataset (whose batches will be scanned), and ScanOptions (to specify a filter for
  // predicate pushdown, a projection to skip materialization of unnecessary columns, ...)
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecPlan> plan,
                       compute::ExecPlan::Make(&exec_context));

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
  // sync scanning is not supported by ScanNode
  options->use_async = true;
  // specify the filter
  compute::Expression b_is_true = field_ref("b");
  options->filter = b_is_true;
  // for now, specify the projection as the full project expression (eventually this can
  // just be a list of materialized field names)
  compute::Expression a_times_2 = call("multiply", {field_ref("a"), literal(2)});
  compute::Expression b = field_ref("b");
  options->projection =
      call("make_struct", {a_times_2, b}, compute::MakeStructOptions{{"a * 2", "b"}});

  // construct the scan node
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * scan,
                       dataset::MakeScanNode(plan.get(), dataset, options));

  // pipe the scan node into a project node
  ASSERT_OK_AND_ASSIGN(
      compute::ExecNode * project,
      compute::MakeProjectNode(scan, "project", {a_times_2, b}, {"a * 2", "b"}));

  // pipe the projection into a grouped aggregate node
  ASSERT_OK_AND_ASSIGN(compute::ExecNode * sum,
                       compute::MakeGroupByNode(
                           project, "grouped_agg", /*keys=*/{"b"}, /*targets=*/{"a * 2"},
                           {compute::internal::Aggregate{"hash_sum", nullptr}}));

  // finally, pipe the project node into a sink node
  auto sink_gen = compute::MakeSinkNode(sum, "sink");

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      schema({field("hash_sum", int64()), field("b", boolean())}), std::move(sink_gen),
      exec_context.memory_pool());

  // start the ExecPlan
  ASSERT_OK(plan->StartProducing());

  // collect sink_reader into a Table
  ASSERT_OK_AND_ASSIGN(auto collected, Table::FromRecordBatchReader(sink_reader.get()));

  // wait 1s for completion
  ASSERT_TRUE(plan->finished().Wait(/*seconds=*/1)) << "ExecPlan didn't finish within 1s";

  auto expected =
      TableFromJSON(schema({field("hash_sum", int64()), field("b", boolean())}), {
                                                                                     R"([
                                               {"hash_sum": 12, "b": null},
                                               {"hash_sum": 4,  "b": true},
                                               {"hash_sum": 40, "b": false}
                                          ])"});
  AssertTablesEqual(*expected, *collected, /*same_chunk_layout=*/false);
}

}  // namespace dataset
}  // namespace arrow
