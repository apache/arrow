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

#include <future>
#include "arrow/acero/concurrent_queue_internal.h"
#include "arrow/acero/hash_join_node.h"
#include "arrow/acero/schema_util.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
using testing::Eq;

namespace arrow {
namespace acero {

const char* kLeftSuffix = ".left";
const char* kRightSuffix = ".right";

TEST(FieldMap, Trivial) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("i32.right", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, TrivialDuplicates) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), "", ""));

  auto output = schema_mgr.MakeOutputSchema("", "");
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32", int32()),
                           field("i32", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, SingleKeyField) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32()), field("str", utf8())});
  auto right = schema({field("f32", float32()), field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("str", utf8()),
                           field("f32", float32()),
                           field("i32.right", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, TwoKeyFields) {
  HashJoinSchema schema_mgr;

  auto left = schema({
      field("i32", int32()),
      field("str", utf8()),
      field("bool", boolean()),
  });
  auto right = schema({
      field("i32", int32()),
      field("str", utf8()),
      field("f32", float32()),
      field("f64", float64()),
  });

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32", "str"}, *right,
                            {"i32", "str"}, literal(true), kLeftSuffix, kRightSuffix));

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("str.left", utf8()),
                           field("bool", boolean()),

                           field("i32.right", int32()),
                           field("str.right", utf8()),
                           field("f32", float32()),
                           field("f64", float64()),
                       })));
}

TEST(FieldMap, ExtensionTypeSwissJoin) {
  // For simpler types swiss join will be used.
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32()), field("ext", uuid())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({field("i32.left", int32()), field("ext", uuid()),
                                  field("i32.right", int32())})));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, ExtensionTypeHashJoin) {
  // Swiss join doesn't support dictionaries so HashJoin will be used.
  HashJoinSchema schema_mgr;

  auto dict_type = dictionary(int64(), int8());
  auto left = schema({field("i32", int32()), field("ext", uuid())});
  auto right = schema({field("i32", int32()), field("dict_type", dict_type)});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("ext", uuid()),
                           field("i32.right", int32()),
                           field("dict_type", dict_type),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

template <typename Queue>
void ConcurrentQueueBasicTest(Queue& queue) {
#ifndef ARROW_ENABLE_THREADING
  GTEST_SKIP() << "Test requires threading enabled";
#endif
  ASSERT_TRUE(queue.Empty());
  queue.Push(1);
  ASSERT_FALSE(queue.Empty());
  ASSERT_EQ(queue.TryPop(), std::make_optional(1));
  ASSERT_TRUE(queue.Empty());

  auto fut_pop = std::async(std::launch::async, [&]() { return queue.WaitAndPop(); });
  ASSERT_EQ(fut_pop.wait_for(std::chrono::milliseconds(10)), std::future_status::timeout);
  queue.Push(2);
  queue.Push(3);
  queue.Push(4);
  // Note we should use wait() which guarantees the future is ready, but this will make
  // the test hang forever if the code is broken. Thus we in turn use wait_for() with a
  // large enough timeout which should be enough in practice.
  ASSERT_EQ(fut_pop.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(fut_pop.get(), 2);
  fut_pop = std::async(std::launch::async, [&]() { return queue.WaitAndPop(); });
  // Ditto.
  ASSERT_EQ(fut_pop.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(fut_pop.get(), 3);
  ASSERT_FALSE(queue.Empty());
  ASSERT_EQ(queue.TryPop(), std::make_optional(4));
  ASSERT_EQ(queue.TryPop(), std::nullopt);
  queue.Push(5);
  ASSERT_FALSE(queue.Empty());
  ASSERT_EQ(queue.Front(), 5);
  ASSERT_FALSE(queue.Empty());
  queue.Clear();
  ASSERT_TRUE(queue.Empty());
}

TEST(ConcurrentQueue, BasicTest) {
  ConcurrentQueue<int> queue;
  ConcurrentQueueBasicTest(queue);
}

class BackpressureTestExecNode : public ExecNode {
 public:
  BackpressureTestExecNode() : ExecNode(nullptr, {}, {}, nullptr) {}
  const char* kind_name() const override { return "BackpressureTestNode"; }
  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    return Status::NotImplemented("Test only node");
  }
  Status InputFinished(ExecNode* input, int total_batches) override {
    return Status::NotImplemented("Test only node");
  }
  Status StartProducing() override { return Status::NotImplemented("Test only node"); }

 protected:
  Status StopProducingImpl() override {
    stopped = true;
    return Status::OK();
  }

 public:
  void PauseProducing(ExecNode* output, int32_t counter) override { paused = true; }
  void ResumeProducing(ExecNode* output, int32_t counter) override { paused = false; }
  bool paused{false};
  bool stopped{false};
};

class TestBackpressureControl : public BackpressureControl {
 public:
  explicit TestBackpressureControl(BackpressureTestExecNode* test_node)
      : test_node(test_node) {}
  virtual void Pause() { test_node->PauseProducing(nullptr, 0); }
  virtual void Resume() { test_node->ResumeProducing(nullptr, 0); }
  BackpressureTestExecNode* test_node;
};

TEST(BackpressureConcurrentQueue, BasicTest) {
  BackpressureTestExecNode dummy_node;
  auto ctrl = std::make_unique<TestBackpressureControl>(&dummy_node);
  ASSERT_OK_AND_ASSIGN(auto handler, BackpressureHandler::Make(2, 4, std::move(ctrl)));
  BackpressureConcurrentQueue<int> queue(std::move(handler));

  ConcurrentQueueBasicTest(queue);
  ASSERT_FALSE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
}

TEST(BackpressureConcurrentQueue, BackpressureTest) {
  BackpressureTestExecNode dummy_node;
  auto ctrl = std::make_unique<TestBackpressureControl>(&dummy_node);
  ASSERT_OK_AND_ASSIGN(auto handler, BackpressureHandler::Make(2, 4, std::move(ctrl)));
  BackpressureConcurrentQueue<int> queue(std::move(handler));

  queue.Push(6);
  queue.Push(7);
  queue.Push(8);
  ASSERT_FALSE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  queue.Push(9);
  ASSERT_TRUE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  ASSERT_EQ(queue.TryPop(), std::make_optional(6));
  ASSERT_TRUE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  ASSERT_EQ(queue.TryPop(), std::make_optional(7));
  ASSERT_FALSE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  queue.Push(10);
  ASSERT_FALSE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  queue.Push(11);
  ASSERT_TRUE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  queue.ForceShutdown();
  ASSERT_FALSE(dummy_node.paused);
}

TEST(BackpressureConcurrentQueue, BackpressureTestStayUnpaused) {
  BackpressureTestExecNode dummy_node;
  auto ctrl = std::make_unique<TestBackpressureControl>(&dummy_node);
  ASSERT_OK_AND_ASSIGN(
      auto handler, BackpressureHandler::Make(/*low_threshold=*/2, /*high_threshold=*/4,
                                              std::move(ctrl)));
  BackpressureConcurrentQueue<int> queue(std::move(handler));

  queue.Push(6);
  queue.Push(7);
  queue.Push(8);
  ASSERT_FALSE(dummy_node.paused);
  ASSERT_FALSE(dummy_node.stopped);
  queue.ForceShutdown();
  for (int i = 0; i < 10; ++i) {
    queue.Push(i);
  }
  ASSERT_FALSE(dummy_node.paused);
}

}  // namespace acero
}  // namespace arrow
