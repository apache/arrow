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

// Regression tests for GH-601: concurrent Projector::Make()/Filter::Make() calls for the
// *same* expression cache key.
//
// Before the fix, Make() read the shared object cache once to decide `is_cached`, then
// SetLLVMObjectCache() performed a second, independent read of the same key. A thread that
// saw a miss on the first read and a hit on the second one would both pre-load the cached
// object (defining e.g. "expr_0_0" in its JITDylib) *and* add its own freshly compiled IR
// module defining the same symbol, so LLVM ORC's duplicate-symbol detection fired and
// Make() returned "Failed to add IR module to LLJIT: Duplicate definition of symbol".
//
// Note that this surfaces as a returned Status rather than a crash, so every Make() below
// must be asserted on -- that is precisely what the pre-existing Java-side repro
// (ProjectorTest#testMakeProjectorParallel) failed to do.

#include <gtest/gtest.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "arrow/memory_pool.h"
#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;

namespace {

// Releases every waiting thread at once, so the racing Make() calls actually overlap
// instead of trickling in as each thread is spawned. Hand-rolled rather than std::latch
// to avoid depending on the standard library's C++20 <latch> support.
class StartGate {
 public:
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return open_; });
  }

  void Open() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      open_ = true;
    }
    cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool open_ = false;
};

int NumThreads() {
  auto hw = static_cast<int>(std::thread::hardware_concurrency());
  return std::max(8, 2 * std::max(1, hw));
}

// Each iteration needs a cache key that has never been built before, so that every thread
// in that iteration genuinely starts from a miss. Note that TestConfiguration() returns
// the *same* shared Configuration instance every call, and ExpressionCacheKey compares
// configurations by pointer identity, so varying the configuration would not produce a
// miss -- vary the schema instead.
constexpr int kIterations = 25;

}  // namespace

class TestConcurrentMake : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestConcurrentMake, TestProjectorMakeSameCacheKey) {
  const int num_threads = NumThreads();

  for (int iter = 0; iter < kIterations; ++iter) {
    // Unique per iteration => guaranteed cache miss for the first thread to get there.
    auto field0 = field("f0_" + std::to_string(iter), int32());
    auto field1 = field("f1_" + std::to_string(iter), int32());
    auto schema = arrow::schema({field0, field1});
    auto field_sum = field("add_" + std::to_string(iter), int32());
    auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
    auto configuration = TestConfiguration();

    StartGate gate;
    std::vector<std::thread> threads;
    std::vector<Status> statuses(num_threads);
    std::vector<std::shared_ptr<Projector>> projectors(num_threads);

    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([&, i] {
        gate.Wait();
        statuses[i] = Projector::Make(schema, {sum_expr}, configuration, &projectors[i]);
      });
    }
    gate.Open();
    for (auto& thread : threads) {
      thread.join();
    }

    // Create a row-batch with some sample data.
    int num_records = 4;
    auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, true});
    auto array1 = MakeArrowArrayInt32({11, 13, 15, 17}, {true, true, true, true});
    auto exp_sum = MakeArrowArrayInt32({12, 15, 18, 21}, {true, true, true, true});
    auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

    for (int i = 0; i < num_threads; ++i) {
      ASSERT_OK(statuses[i]) << "iteration " << iter << ", thread " << i;
      ASSERT_NE(projectors[i], nullptr) << "iteration " << iter << ", thread " << i;

      // Every projector must evaluate correctly, including the ones built from the cache
      // hit path. No pre-existing test evaluates a cache-hit projector, so a
      // corrupt-but-non-null cached module would otherwise go unnoticed.
      arrow::ArrayVector outputs;
      ASSERT_OK(projectors[i]->Evaluate(*in_batch, pool_, &outputs))
          << "iteration " << iter << ", thread " << i;
      EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
    }
  }
}

TEST_F(TestConcurrentMake, TestFilterMakeSameCacheKey) {
  const int num_threads = NumThreads();

  for (int iter = 0; iter < kIterations; ++iter) {
    auto field0 = field("g0_" + std::to_string(iter), int32());
    auto field1 = field("g1_" + std::to_string(iter), int32());
    auto schema = arrow::schema({field0, field1});

    // Condition: f0 + f1 < 10
    auto node_f0 = TreeExprBuilder::MakeField(field0);
    auto node_f1 = TreeExprBuilder::MakeField(field1);
    auto sum_func =
        TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
    auto literal_10 = TreeExprBuilder::MakeLiteral(static_cast<int32_t>(10));
    auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                      arrow::boolean());
    auto condition = TreeExprBuilder::MakeCondition(less_than_10);
    auto configuration = TestConfiguration();

    StartGate gate;
    std::vector<std::thread> threads;
    std::vector<Status> statuses(num_threads);
    std::vector<std::shared_ptr<Filter>> filters(num_threads);

    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([&, i] {
        gate.Wait();
        statuses[i] = Filter::Make(schema, condition, configuration, &filters[i]);
      });
    }
    gate.Open();
    for (auto& thread : threads) {
      thread.join();
    }

    int num_records = 5;
    auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
    auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 3}, {true, true, false, true, true});
    auto exp = MakeArrowArrayUint16({0, 4});
    auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

    for (int i = 0; i < num_threads; ++i) {
      ASSERT_OK(statuses[i]) << "iteration " << iter << ", thread " << i;
      ASSERT_NE(filters[i], nullptr) << "iteration " << iter << ", thread " << i;

      std::shared_ptr<SelectionVector> selection_vector;
      ASSERT_OK(SelectionVector::MakeInt16(num_records, pool_, &selection_vector));
      ASSERT_OK(filters[i]->Evaluate(*in_batch, selection_vector))
          << "iteration " << iter << ", thread " << i;
      EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
    }
  }
}

}  // namespace gandiva
