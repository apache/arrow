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

#include "arrow/compute/context.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace dataset {

class TestSimpleScanner : public DatasetFixtureMixin {};

TEST_F(TestSimpleScanner, Scan) {
  constexpr int64_t kNumberFragments = 4;
  constexpr int64_t kNumberBatches = 16;
  constexpr int64_t kBatchSize = 1024;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);

  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};
  auto fragment = std::make_shared<SimpleDataFragment>(batches);
  DataFragmentVector fragments{kNumberFragments, fragment};

  std::vector<std::shared_ptr<DataSource>> sources = {
      std::make_shared<SimpleDataSource>(fragments),
      std::make_shared<SimpleDataSource>(fragments),
  };

  const int64_t total_batches = sources.size() * kNumberBatches * kNumberFragments;
  auto reader = ConstantArrayGenerator::Repeat(total_batches, batch);

  SimpleScanner scanner{sources, options_, ctx_};

  // Verifies that the unified BatchReader is equivalent to flattening all the
  // structures of the scanner, i.e. Scanner[DataSource[ScanTask[RecordBatch]]]
  AssertScannerEquals(reader.get(), &scanner);
}

TEST_F(TestSimpleScanner, FilteredScan) {
  constexpr int64_t kNumberFragments = 4;
  constexpr int64_t kNumberBatches = 16;
  constexpr int64_t kBatchSize = 1024;

  double value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(float64(), kBatchSize, kBatchSize / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 builder->UnsafeAppend(-value);
                                                 value += 1.0;
                                               }));
  value = 0.5;
  ASSERT_OK_AND_ASSIGN(
      auto f64_filtered,
      ArrayFromBuilderVisitor(float64(), kBatchSize / 2, [&](DoubleBuilder* builder) {
        builder->UnsafeAppend(value);
        value += 1.0;
      }));

  auto s = schema({field("f64", float64())});
  auto batch = RecordBatch::Make(s, f64->length(), {f64});
  auto batch_filtered = RecordBatch::Make(s, f64_filtered->length(), {f64_filtered});

  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};

  options_ = ScanOptions::Defaults();
  options_->filter = ("f64"_ > 0.0).Copy();

  auto fragment = std::make_shared<SimpleDataFragment>(batches, options_);
  DataFragmentVector fragments{kNumberFragments, fragment};

  std::vector<std::shared_ptr<DataSource>> sources = {
      std::make_shared<SimpleDataSource>(fragments),
      std::make_shared<SimpleDataSource>(fragments),
  };

  const int64_t total_batches = sources.size() * kNumberBatches * kNumberFragments;
  auto reader = ConstantArrayGenerator::Repeat(total_batches, batch_filtered);

  compute::FunctionContext ctx;
  options_->evaluator = std::make_shared<TreeEvaluator>(&ctx);
  SimpleScanner scanner{sources, options_, ctx_};

  // Verifies that the unified BatchReader is equivalent to flattening all the
  // structures of the scanner, i.e. Scanner[DataSource[ScanTask[RecordBatch]]]
  AssertScannerEquals(reader.get(), &scanner);
}

TEST_F(TestSimpleScanner, ToTable) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);
  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};

  std::shared_ptr<Table> expected;
  ASSERT_OK(Table::FromRecordBatches(batches, &expected));

  auto fragment = std::make_shared<SimpleDataFragment>(batches);
  DataFragmentVector fragments{1, fragment};

  std::vector<std::shared_ptr<DataSource>> sources = {
      std::make_shared<SimpleDataSource>(fragments),
  };

  auto scanner = std::make_shared<SimpleScanner>(sources, options_, ctx_);
  std::shared_ptr<Table> actual;
  ASSERT_OK(Scanner::ToTable(scanner, &actual));

  AssertTablesEqual(*expected, *actual);
}

}  // namespace dataset
}  // namespace arrow
