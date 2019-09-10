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

#include "arrow/dataset/dataset_internal.h"
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

TEST(TestProjector, MismatchedType) {
  constexpr int64_t kBatchSize = 1024;

  auto from_schema = schema({field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);
  auto to_schema = schema({field("f64", int32())});

  RecordBatchProjector projector(default_memory_pool(), to_schema);

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_RAISES(TypeError, projector.Project(*batch, &reconciled_batch));
}

TEST(TestProjector, AugmentWithNull) {
  constexpr int64_t kBatchSize = 1024;

  auto from_schema = schema({field("f64", float64()), field("b", boolean())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);
  auto to_schema = schema({field("i32", int32()), field("f64", float64())});

  RecordBatchProjector projector(default_memory_pool(), to_schema);

  std::shared_ptr<Array> null_i32;
  ASSERT_OK(MakeArrayOfNull(int32(), batch->num_rows(), &null_i32));
  auto expected_batch =
      RecordBatch::Make(to_schema, batch->num_rows(), {null_i32, batch->column(0)});

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_OK(projector.Project(*batch, &reconciled_batch));

  AssertBatchesEqual(*expected_batch, *reconciled_batch);
}

TEST(TestProjector, AugmentWithScalar) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int32_t kScalarValue = 3;

  auto from_schema = schema({field("f64", float64()), field("b", boolean())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);
  auto to_schema = schema({field("i32", int32()), field("f64", float64())});

  auto scalar_i32 = std::make_shared<Int32Scalar>(kScalarValue);

  RecordBatchProjector projector(default_memory_pool(), to_schema, {scalar_i32, nullptr});

  ASSERT_OK_AND_ASSIGN(auto array_i32,
                       ArrayFromBuilderVisitor(int32(), kBatchSize, [](Int32Builder* b) {
                         b->UnsafeAppend(kScalarValue);
                       }));

  auto expected_batch =
      RecordBatch::Make(to_schema, batch->num_rows(), {array_i32, batch->column(0)});

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_OK(projector.Project(*batch, &reconciled_batch));

  AssertBatchesEqual(*expected_batch, *reconciled_batch);
}

TEST(TestProjector, NonTrivial) {
  constexpr int64_t kBatchSize = 1024;

  constexpr float kScalarValue = 3.14f;

  auto from_schema =
      schema({field("i8", int8()), field("u8", uint8()), field("i16", int16()),
              field("u16", uint16()), field("i32", int32()), field("u32", uint32())});

  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);

  auto to_schema =
      schema({field("i32", int32()), field("f64", float64()), field("u16", uint16()),
              field("u8", uint8()), field("b", boolean()), field("u32", uint32()),
              field("f32", float32())});

  auto scalar_f32 = std::make_shared<FloatScalar>(kScalarValue);
  auto scalar_f64 = std::make_shared<DoubleScalar>(kScalarValue);

  RecordBatchProjector projector(
      default_memory_pool(), to_schema,
      {nullptr /* i32 */, scalar_f64, nullptr /* u16 */, nullptr /* u8 */,
       nullptr /* b */, nullptr /* u32 */, scalar_f32});

  ASSERT_OK_AND_ASSIGN(
      auto array_f32, ArrayFromBuilderVisitor(float32(), kBatchSize, [](FloatBuilder* b) {
        b->UnsafeAppend(kScalarValue);
      }));
  ASSERT_OK_AND_ASSIGN(auto array_f64, ArrayFromBuilderVisitor(
                                           float64(), kBatchSize, [](DoubleBuilder* b) {
                                             b->UnsafeAppend(kScalarValue);
                                           }));
  ASSERT_OK_AND_ASSIGN(
      auto null_b, ArrayFromBuilderVisitor(boolean(), kBatchSize, [](BooleanBuilder* b) {
        b->UnsafeAppendNull();
      }));

  auto expected_batch = RecordBatch::Make(
      to_schema, batch->num_rows(),
      {batch->GetColumnByName("i32"), array_f64, batch->GetColumnByName("u16"),
       batch->GetColumnByName("u8"), null_b, batch->GetColumnByName("u32"), array_f32});

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_OK(projector.Project(*batch, &reconciled_batch));

  AssertBatchesEqual(*expected_batch, *reconciled_batch);
}

}  // namespace dataset
}  // namespace arrow
