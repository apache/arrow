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

#include <gmock/gmock-matchers.h>

#include <numeric>
#include <random>
#include <unordered_set>

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

using testing::UnorderedElementsAreArray;

namespace arrow {
namespace compute {

void CheckRunOutput(const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r0_batches,
                    const BatchesWithSchema& r1_batches,
                    const BatchesWithSchema& exp_batches, const FieldRef time,
                    const FieldRef keys, const int64_t tolerance) {
  auto exec_ctx =
      arrow::internal::make_unique<ExecContext>(default_memory_pool(), nullptr);
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  AsofJoinNodeOptions join_options(time, keys, tolerance);
  Declaration join{"asofjoin", join_options};

  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r0_batches.schema, r0_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r1_batches.schema, r1_batches.gen(false, false)}});

  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_FINISHES_OK_AND_ASSIGN(auto res, StartAndCollect(plan.get(), sink_gen));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  ASSERT_OK_AND_ASSIGN(auto res_table, TableFromExecBatches(exp_batches.schema, res));

  AssertTablesEqual(*exp_table, *res_table,
                    /*same_chunk_layout=*/true, /*flatten=*/true);
}

void DoRunBasicTest(const std::vector<util::string_view>& l_data,
                    const std::vector<util::string_view>& r0_data,
                    const std::vector<util::string_view>& r1_data,
                    const std::vector<util::string_view>& exp_data, int64_t tolerance) {
  auto l_schema =
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())});
  auto r0_schema =
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())});
  auto r1_schema =
      schema({field("time", int64()), field("key", int32()), field("r1_v0", float32())});

  auto exp_schema = schema({
      field("time", int64()),
      field("key", int32()),
      field("l_v0", float64()),
      field("r0_v0", float64()),
      field("r1_v0", float32()),
  });

  // Test three table join
  BatchesWithSchema l_batches, r0_batches, r1_batches, exp_batches;
  l_batches = MakeBatchesFromString(l_schema, l_data);
  r0_batches = MakeBatchesFromString(r0_schema, r0_data);
  r1_batches = MakeBatchesFromString(r1_schema, r1_data);
  exp_batches = MakeBatchesFromString(exp_schema, exp_data);
  CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time", "key",
                 tolerance);
}

void DoRunInvalidTypeTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  BatchesWithSchema l_batches = MakeBatchesFromString(l_schema, {R"([])"});
  BatchesWithSchema r_batches = MakeBatchesFromString(r_schema, {R"([])"});

  ExecContext exec_ctx;
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(&exec_ctx));

  AsofJoinNodeOptions join_options("time", "key", 0);
  Declaration join{"asofjoin", join_options};
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(false, false)}});

  ASSERT_RAISES(Invalid, join.AddToPlan(plan.get()));
}

class AsofJoinTest : public testing::Test {};

TEST(AsofJoinTest, TestBasic1) {
  // Single key, single batch
  DoRunBasicTest(
      /*l*/ {R"([[0, 1, 1.0], [1000, 1, 2.0]])"},
      /*r0*/ {R"([[0, 1, 11.0]])"},
      /*r1*/ {R"([[1000, 1, 101.0]])"},
      /*exp*/ {R"([[0, 1, 1.0, 11.0, null], [1000, 1, 2.0, 11.0, 101.0]])"}, 1000);
}

TEST(AsofJoinTest, TestBasic2) {
  // Single key, multiple batches
  DoRunBasicTest(
      /*l*/ {R"([[0, 1, 1.0]])", R"([[1000, 1, 2.0]])"},
      /*r0*/ {R"([[0, 1, 11.0]])", R"([[1000, 1, 12.0]])"},
      /*r1*/ {R"([[0, 1, 101.0]])", R"([[1000, 1, 102.0]])"},
      /*exp*/ {R"([[0, 1, 1.0, 11.0, 101.0], [1000, 1, 2.0, 12.0, 102.0]])"}, 1000);
}

TEST(AsofJoinTest, TestBasic3) {
  // Single key, multiple left batches, single right batches
  DoRunBasicTest(
      /*l*/ {R"([[0, 1, 1.0]])", R"([[1000, 1, 2.0]])"},
      /*r0*/ {R"([[0, 1, 11.0], [1000, 1, 12.0]])"},
      /*r1*/ {R"([[0, 1, 101.0], [1000, 1, 102.0]])"},
      /*exp*/ {R"([[0, 1, 1.0, 11.0, 101.0], [1000, 1, 2.0, 12.0, 102.0]])"}, 1000);
}

TEST(AsofJoinTest, TestBasic4) {
  // Multi key, multiple batches, misaligned batches
  DoRunBasicTest(
      /*l*/
      {R"([[0, 1, 1.0], [0, 2, 21.0], [500, 1, 2.0], [1000, 2, 22.0], [1500, 1, 3.0], [1500, 2, 23.0]])",
       R"([[2000, 1, 4.0], [2000, 2, 24.0]])"},
      /*r0*/
      {R"([[0, 1, 11.0], [500, 2, 31.0], [1000, 1, 12.0]])",
       R"([[1500, 2, 32.0], [2000, 1, 13.0], [2500, 2, 33.0]])"},
      /*r1*/
      {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
       R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
      /*exp*/
      {R"([[0, 1, 1.0, 11.0, null], [0, 2, 21.0, null, 1001.0], [500, 1, 2.0, 11.0, 101.0], [1000, 2, 22.0, 31.0, 1001.0], [1500, 1, 3.0, 12.0, 102.0], [1500, 2, 23.0, 32.0, 1002.0]])",
       R"([[2000, 1, 4.0, 13.0, 103.0], [2000, 2, 24.0, 32.0, 1002.0]])"},
      1000);
}

TEST(AsofJoinTest, TestBasic5) {
  // Multi key, multiple batches, misaligned batches, smaller tolerance
  DoRunBasicTest(/*l*/
                 {R"([[0, 1, 1.0], [0, 2, 21.0], [500, 1, 2.0], [1000, 2, 22.0], [1500, 1, 3.0], [1500, 2, 23.0]])",
                  R"([[2000, 1, 4.0], [2000, 2, 24.0]])"},
                 /*r0*/
                 {R"([[0, 1, 11.0], [500, 2, 31.0], [1000, 1, 12.0]])",
                  R"([[1500, 2, 32.0], [2000, 1, 13.0], [2500, 2, 33.0]])"},
                 /*r1*/
                 {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
                  R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
                 /*exp*/
                 {R"([[0, 1, 1.0, 11.0, null], [0, 2, 21.0, null, 1001.0], [500, 1, 2.0, 11.0, 101.0], [1000, 2, 22.0, 31.0, null], [1500, 1, 3.0, 12.0, 102.0], [1500, 2, 23.0, 32.0, 1002.0]])",
                  R"([[2000, 1, 4.0, 13.0, 103.0], [2000, 2, 24.0, 32.0, 1002.0]])"},
                 500);
}

TEST(AsofJoinTest, TestBasic6) {
  // Multi key, multiple batches, misaligned batches, zero tolerance
  DoRunBasicTest(/*l*/
                 {R"([[0, 1, 1.0], [0, 2, 21.0], [500, 1, 2.0], [1000, 2, 22.0], [1500, 1, 3.0], [1500, 2, 23.0]])",
                  R"([[2000, 1, 4.0], [2000, 2, 24.0]])"},
                 /*r0*/
                 {R"([[0, 1, 11.0], [500, 2, 31.0], [1000, 1, 12.0]])",
                  R"([[1500, 2, 32.0], [2000, 1, 13.0], [2500, 2, 33.0]])"},
                 /*r1*/
                 {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
                  R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
                 /*exp*/
                 {R"([[0, 1, 1.0, 11.0, null], [0, 2, 21.0, null, 1001.0], [500, 1, 2.0, null, 101.0], [1000, 2, 22.0, null, null], [1500, 1, 3.0, null, null], [1500, 2, 23.0, 32.0, 1002.0]])",
                  R"([[2000, 1, 4.0, 13.0, 103.0], [2000, 2, 24.0, null, null]])"},
                 0);
}

TEST(AsofJoinTest, TestEmpty1) {
  // Empty left batch
  DoRunBasicTest(/*l*/
                 {R"([])", R"([[2000, 1, 4.0], [2000, 2, 24.0]])"},
                 /*r0*/
                 {R"([[0, 1, 11.0], [500, 2, 31.0], [1000, 1, 12.0]])",
                  R"([[1500, 2, 32.0], [2000, 1, 13.0], [2500, 2, 33.0]])"},
                 /*r1*/
                 {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
                  R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
                 /*exp*/
                 {R"([[2000, 1, 4.0, 13.0, 103.0], [2000, 2, 24.0, 32.0, 1002.0]])"},
                 1000);
}

TEST(AsofJoinTest, TestEmpty2) {
  // Empty left input
  DoRunBasicTest(/*l*/
                 {R"([])"},
                 /*r0*/
                 {R"([[0, 1, 11.0], [500, 2, 31.0], [1000, 1, 12.0]])",
                  R"([[1500, 2, 32.0], [2000, 1, 13.0], [2500, 2, 33.0]])"},
                 /*r1*/
                 {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
                  R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
                 /*exp*/
                 {R"([])"}, 1000);
}

TEST(AsofJoinTest, TestEmpty3) {
  // Empty right batch
  DoRunBasicTest(/*l*/
                 {R"([[0, 1, 1.0], [0, 2, 21.0], [500, 1, 2.0], [1000, 2, 22.0], [1500, 1, 3.0], [1500, 2, 23.0]])",
                  R"([[2000, 1, 4.0], [2000, 2, 24.0]])"},
                 /*r0*/
                 {R"([])", R"([[1500, 2, 32.0], [2000, 1, 13.0], [2500, 2, 33.0]])"},
                 /*r1*/
                 {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
                  R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
                 /*exp*/
                 {R"([[0, 1, 1.0, null, null], [0, 2, 21.0, null, 1001.0], [500, 1, 2.0, null, 101.0], [1000, 2, 22.0, null, 1001.0], [1500, 1, 3.0, null, 102.0], [1500, 2, 23.0, 32.0, 1002.0]])",
                  R"([[2000, 1, 4.0, 13.0, 103.0], [2000, 2, 24.0, 32.0, 1002.0]])"},
                 1000);
}

TEST(AsofJoinTest, TestEmpty4) {
  // Empty right input
  DoRunBasicTest(/*l*/
                 {R"([[0, 1, 1.0], [0, 2, 21.0], [500, 1, 2.0], [1000, 2, 22.0], [1500, 1, 3.0], [1500, 2, 23.0]])",
                  R"([[2000, 1, 4.0], [2000, 2, 24.0]])"},
                 /*r0*/
                 {R"([])"},
                 /*r1*/
                 {R"([[0, 2, 1001.0], [500, 1, 101.0]])",
                  R"([[1000, 1, 102.0], [1500, 2, 1002.0], [2000, 1, 103.0]])"},
                 /*exp*/
                 {R"([[0, 1, 1.0, null, null], [0, 2, 21.0, null, 1001.0], [500, 1, 2.0, null, 101.0], [1000, 2, 22.0, null, 1001.0], [1500, 1, 3.0, null, 102.0], [1500, 2, 23.0, null, 1002.0]])",
                  R"([[2000, 1, 4.0, null, 103.0], [2000, 2, 24.0, null, 1002.0]])"},
                 1000);
}

TEST(AsofJoinTest, TestEmpty5) {
  // All empty
  DoRunBasicTest(/*l*/
                 {R"([])"},
                 /*r0*/
                 {R"([])"},
                 /*r1*/
                 {R"([])"},
                 /*exp*/
                 {R"([])"}, 1000);
}

TEST(AsofJoinTest, TestUnsupportedOntype) {
  DoRunInvalidTypeTest(
      schema({field("time", utf8()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", utf8()), field("key", int32()), field("r0_v0", float32())}));
}

TEST(AsofJoinTest, TestUnsupportedBytype) {
  DoRunInvalidTypeTest(
      schema({field("time", int64()), field("key", utf8()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", utf8()), field("r0_v0", float32())}));
}

TEST(AsofJoinTest, TestUnsupportedDatatype) {
  // Utf8 is unsupported
  DoRunInvalidTypeTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", utf8())}));
}

TEST(AsofJoinTest, TestMissingKeys) {
  DoRunInvalidTypeTest(
      schema({field("time1", int64()), field("key", int32()), field("l_v0", float64())}),
      schema(
          {field("time1", int64()), field("key", int32()), field("r0_v0", float64())}));

  DoRunInvalidTypeTest(
      schema({field("time", int64()), field("key1", int32()), field("l_v0", float64())}),
      schema(
          {field("time", int64()), field("key1", int32()), field("r0_v0", float64())}));
}

}  // namespace compute
}  // namespace arrow
