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

#include "arrow/dataset/dataset.h"

#include "arrow/dataset/test_util.h"
#include "arrow/testing/generator.h"

namespace arrow {
namespace dataset {

class TestSimpleDataFragment : public DatasetFixtureMixin {};

TEST_F(TestSimpleDataFragment, Scan) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  // Creates a SimpleDataFragment of the same repeated batch.
  auto fragment = SimpleDataFragment({kNumberBatches, batch});

  AssertFragmentEquals(reader.get(), &fragment);
}

class TestSimpleDataSource : public DatasetFixtureMixin {};

TEST_F(TestSimpleDataSource, GetFragments) {
  constexpr int64_t kNumberFragments = 4;
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches * kNumberFragments, batch);

  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};
  auto fragment = std::make_shared<SimpleDataFragment>(batches);
  // It is safe to copy fragment multiple time since Scan() does not consume
  // the internal array.
  auto source = SimpleDataSource({kNumberFragments, fragment});

  AssertDataSourceEquals(reader.get(), &source);
}

class TestDataset : public DatasetFixtureMixin {};

TEST_F(TestDataset, TrivialScan) {
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

  const int64_t total_batches = sources.size() * kNumberFragments * kNumberBatches;
  auto reader = ConstantArrayGenerator::Repeat(total_batches, batch);

  std::shared_ptr<Dataset> dataset;
  ASSERT_OK(Dataset::Make(sources, s, &dataset));

  AssertDatasetEquals(reader.get(), dataset.get());
}

}  // namespace dataset
}  // namespace arrow
