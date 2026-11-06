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

// Deterministic regression test for GH-51663.
//
// Projector::Make() and Filter::Make() read the shared expression cache once to decide
// `is_cached`. Before the fix, SetLLVMObjectCache() performed a *second*, independent
// read of the same key. A thread that saw a miss on the first read and a hit on the
// second one would both pre-load the cached object (defining e.g. "expr_0_0" in its
// JITDylib) and add its own compiled IR module defining the same symbol, so LLVM ORC's
// duplicate-symbol detection fired. (Originally reported against the Java bindings as
// apache/arrow-java#601.)

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/memory_pool.h"
#include "gandiva/cache.h"
#include "gandiva/configuration.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/exported_funcs_registry.h"
#include "gandiva/expression_cache_key.h"
#include "gandiva/filter.h"
#include "gandiva/llvm_generator.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;

namespace {

using ObjectCacheType = Cache<ExpressionCacheKey, std::shared_ptr<llvm::MemoryBuffer>>;

struct PendingInjection {
  std::shared_ptr<ObjectCacheType> cache;
  std::shared_ptr<ExpressionCacheKey> key;
  std::shared_ptr<llvm::MemoryBuffer> object;
  std::atomic<bool> armed{false};
};

PendingInjection* GetPendingInjection() {
  static PendingInjection instance;
  return &instance;
}

// Registered once; fires only while armed.
class CacheRaceInjector : public ExportedFuncsBase {
 public:
  arrow::Status AddMappings(Engine* engine) const override {
    auto* pending = GetPendingInjection();
    // exchange() disarms, so this injects exactly once per arming even though the
    // registry keeps calling us on every subsequent Engine::Init().
    if (pending->armed.exchange(false)) {
      pending->cache->PutObjectCode(*pending->key, pending->object);
    }
    return arrow::Status::OK();
  }
};

REGISTER_EXPORTED_FUNCS(CacheRaceInjector);

// Arms the injection that will run inside the next Make()'s engine construction.
void ArmInjection(std::shared_ptr<ObjectCacheType> cache,
                  std::shared_ptr<ExpressionCacheKey> key,
                  std::shared_ptr<llvm::MemoryBuffer> object) {
  auto* pending = GetPendingInjection();
  pending->cache = std::move(cache);
  pending->key = std::move(key);
  pending->object = std::move(object);
  pending->armed.store(true);
}

}  // namespace

class TestCacheRace : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

  void TearDown() { GetPendingInjection()->armed.store(false); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestCacheRace, TestProjectorCacheInsertedDuringMake) {
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});
  auto field_sum = field("add", int32());
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  ExpressionVector exprs = {sum_expr};

  // Two separately built configurations with identical content: same hash, different
  // keys.
  auto config_a = ConfigurationBuilder().build();
  auto config_b = ConfigurationBuilder().build();

  // Build once under config_a so the expression is cached.
  std::shared_ptr<Projector> warm_projector;
  ASSERT_OK(Projector::Make(schema, exprs, config_a, &warm_projector));

  auto cache = LLVMGenerator::GetCache();
  ExpressionCacheKey key_a(schema, config_a, exprs, SelectionVector::Mode::MODE_NONE);
  auto cached_object = cache->GetObjectCode(key_a);
  ASSERT_NE(cached_object, nullptr) << "expected the first Make() to populate the cache";

  // config_b has never been built, so Make()'s first read is a guaranteed miss.
  auto key_b = std::make_shared<ExpressionCacheKey>(schema, config_b, exprs,
                                                    SelectionVector::Mode::MODE_NONE);
  ASSERT_EQ(cache->GetObjectCode(*key_b), nullptr) << "key_b should start uncached";

  // Fires inside Engine::Init(), i.e. after the first read and before the (pre-fix)
  // second.
  ArmInjection(cache, key_b, cached_object);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, config_b, &projector);

  ASSERT_OK(status) << "a cache entry appearing mid-Make() must not produce a duplicate "
                       "symbol -- see GH-51663";
  ASSERT_NE(projector, nullptr);
  EXPECT_FALSE(GetPendingInjection()->armed.load())
      << "injection never fired; the window this test targets was not reached";

  // The projector must also be usable: a silently wrong module is as bad as an error.
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, true});
  auto array1 = MakeArrowArrayInt32({11, 13, 15, 17}, {true, true, true, true});
  auto exp_sum = MakeArrowArrayInt32({12, 15, 18, 21}, {true, true, true, true});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  arrow::ArrayVector outputs;
  ASSERT_OK(projector->Evaluate(*in_batch, pool_, &outputs));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
}

TEST_F(TestCacheRace, TestFilterCacheInsertedDuringMake) {
  auto field0 = field("g0", int32());
  auto field1 = field("g1", int32());
  auto schema = arrow::schema({field0, field1});

  // g0 + g1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func = TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral(static_cast<int32_t>(10));
  auto less_than_10 =
      TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10}, boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_10);

  auto config_a = ConfigurationBuilder().build();
  auto config_b = ConfigurationBuilder().build();

  std::shared_ptr<Filter> warm_filter;
  ASSERT_OK(Filter::Make(schema, condition, config_a, &warm_filter));

  auto cache = LLVMGenerator::GetCache();
  // Filter::Make uses the Condition overload of the key.
  Condition condition_for_key = *(condition.get());
  ExpressionCacheKey key_a(schema, config_a, condition_for_key);
  auto cached_object = cache->GetObjectCode(key_a);
  ASSERT_NE(cached_object, nullptr) << "expected the first Make() to populate the cache";

  auto key_b = std::make_shared<ExpressionCacheKey>(schema, config_b, condition_for_key);
  ASSERT_EQ(cache->GetObjectCode(*key_b), nullptr) << "key_b should start uncached";

  ArmInjection(cache, key_b, cached_object);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, config_b, &filter);

  ASSERT_OK(status) << "a cache entry appearing mid-Make() must not produce a duplicate "
                       "symbol -- see GH-51663";
  ASSERT_NE(filter, nullptr);
  EXPECT_FALSE(GetPendingInjection()->armed.load())
      << "injection never fired; the window this test targets was not reached";

  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 3}, {true, true, false, true, true});
  auto exp = MakeArrowArrayUint16({0, 4});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  std::shared_ptr<SelectionVector> selection_vector;
  ASSERT_OK(SelectionVector::MakeInt16(num_records, pool_, &selection_vector));
  ASSERT_OK(filter->Evaluate(*in_batch, selection_vector));
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

}  // namespace gandiva
