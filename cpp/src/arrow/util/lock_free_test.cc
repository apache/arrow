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

#include <algorithm>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/lock_free.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::TaskGroup;

namespace util {

static constexpr int kLots = 512;
// static constexpr int kLots = 16 * 1024;

class TestLockFreeStack : public ::testing::Test {
 public:
  void TearDown() override {
    fflush(stdout);
    fflush(stderr);
  }

  void AppendLots(LockFreeStack<int>* ints) {
    for (int j = 0; j < kLots; ++j) {
      ints->Push(j);
    }
  }

  void AppendLots(std::vector<int>* ints) {
    for (int j = 0; j < kLots; ++j) {
      ints->push_back(j);
    }
  }

  template <typename T>
  std::vector<T> ToVector(LockFreeStack<T> stack) {
    std::vector<T> out;
    for (auto&& element : stack) {
      out.push_back(std::move(element));
    }
    std::reverse(out.begin(), out.end());
    return out;
  }

  template <typename T>
  std::vector<T> Flatten(std::vector<std::vector<T>> nested) {
    size_t flat_size = 0;
    for (auto&& v : nested) {
      flat_size += v.size();
    }

    std::vector<int> flat(flat_size);
    auto it = flat.begin();
    for (auto&& v : nested) {
      it = std::move(v.begin(), v.end(), it);
    }

    EXPECT_EQ(it, flat.end());
    return flat;
  }

  std::shared_ptr<TaskGroup> task_group =
      TaskGroup::MakeThreaded(::arrow::internal::GetCpuThreadPool());
};

TEST_F(TestLockFreeStack, AppendOnly) {
  LockFreeStack<int> ints;

  for (int i = 0; i < task_group->parallelism(); ++i) {
    task_group->Append([&] {
      AppendLots(&ints);
      return Status::OK();
    });
  }

  ASSERT_OK(task_group->Finish());

  std::vector<int> actual = ToVector(std::move(ints)), expected;

  for (int i = 0; i < task_group->parallelism(); ++i) {
    AppendLots(&expected);
  }

  for (auto v : {&actual, &expected}) {
    std::sort(v->begin(), v->end());
  }

  EXPECT_THAT(actual, testing::ElementsAreArray(expected));
}

TEST_F(TestLockFreeStack, ProduceThenConsumeMoved) {
  LockFreeStack<int> ints;

  std::vector<std::vector<int>> per_thread_actual(task_group->parallelism());

  for (int i = 0; i < task_group->parallelism(); ++i) {
    task_group->Append([&, i] {
      AppendLots(&ints);
      per_thread_actual[i] = ToVector(std::move(ints));
      return Status::OK();
    });
  }

  ASSERT_OK(task_group->Finish());

  std::vector<int> actual = Flatten(std::move(per_thread_actual)), expected;

  for (int i = 0; i < task_group->parallelism(); ++i) {
    AppendLots(&expected);
  }

  for (auto v : {&actual, &expected}) {
    std::sort(v->begin(), v->end());
  }

  EXPECT_THAT(actual, testing::ElementsAreArray(expected));
}

TEST_F(TestLockFreeStack, AlternatingProduceAndConsumeMoved) {
  LockFreeStack<int> ints;

  std::vector<std::vector<int>> per_thread_actual(task_group->parallelism());

  for (int i = 0; i < task_group->parallelism(); ++i) {
    task_group->Append([&, i] {
      for (int j = 0; j < kLots; ++j) {
        ints.Push(j);
        auto local = std::move(ints);
        for (int got : local) {
          per_thread_actual[i].push_back(got);
        }
      }
      return Status::OK();
    });
  }

  ASSERT_OK(task_group->Finish());

  std::vector<int> actual = Flatten(std::move(per_thread_actual)), expected;

  for (int i = 0; i < task_group->parallelism(); ++i) {
    AppendLots(&expected);
  }

  for (auto v : {&actual, &expected}) {
    std::sort(v->begin(), v->end());
  }

  EXPECT_THAT(actual, testing::ElementsAreArray(expected));
}

}  // namespace util
}  // namespace arrow
