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

#pragma once

#include <chrono>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/future.h"

// This macro should be called by futures that are expected to
// complete pretty quickly.  2 seconds is the default max wait
// here.  Anything longer than that and it's a questionable
// unit test anyways.
#define ASSERT_FINISHES_IMPL(fut)                            \
  do {                                                       \
    ASSERT_TRUE(fut.Wait(10));                               \
    if (!fut.is_finished()) {                                \
      FAIL() << "Future did not finish in a timely fashion"; \
    }                                                        \
  } while (false)

#define ASSERT_FINISHES_OK(expr)                                              \
  do {                                                                        \
    auto&& _fut = (expr);                                                     \
    ASSERT_TRUE(_fut.Wait(300));                                              \
    if (!_fut.is_finished()) {                                                \
      FAIL() << "Future did not finish in a timely fashion";                  \
    }                                                                         \
    auto& _st = _fut.status();                                                \
    if (!_st.ok()) {                                                          \
      FAIL() << "'" ARROW_STRINGIFY(expr) "' failed with " << _st.ToString(); \
    }                                                                         \
  } while (false)

#define ASSERT_FINISHES_AND_RAISES(ENUM, expr) \
  do {                                         \
    auto&& fut = (expr);                       \
    ASSERT_FINISHES_IMPL(fut);                 \
    ASSERT_RAISES(ENUM, fut.status());         \
  } while (false)

#define ASSERT_FINISHES_OK_AND_ASSIGN_IMPL(lhs, rexpr, future_name) \
  auto future_name = (rexpr);                                       \
  ASSERT_FINISHES_IMPL(future_name);                                \
  ASSERT_OK_AND_ASSIGN(lhs, future_name.result());

#define ASSERT_FINISHES_OK_AND_ASSIGN(lhs, rexpr) \
  ASSERT_FINISHES_OK_AND_ASSIGN_IMPL(lhs, rexpr,  \
                                     ARROW_ASSIGN_OR_RAISE_NAME(_fut, __COUNTER__))

#define ASSERT_FINISHES_OK_AND_EQ(expected, expr)        \
  do {                                                   \
    ASSERT_FINISHES_OK_AND_ASSIGN(auto _actual, (expr)); \
    ASSERT_EQ(expected, _actual);                        \
  } while (0)

namespace arrow {

template <typename T>
void AssertNotFinished(const Future<T>& fut) {
  ASSERT_FALSE(IsFutureFinished(fut.state()));
}

template <typename T>
void AssertFinishesInReasonableTime(const Future<T>& fut) {
  auto start = std::chrono::system_clock::now();
  bool finished_quick = fut.Wait(10);
  if (!finished_quick) {
    bool finished_slow = fut.Wait(290);
    if (finished_slow) {
      auto end = std::chrono::system_clock::now();
      auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      FAIL() << "The future took more than 10 seconds to finish (but it did finish in "
             << elapsed << "ms)";
    } else {
      FAIL() << "Even after 300 seconds the future was still not finished";
    }
  }
}

template <typename T>
void AssertFinished(const Future<T>& fut) {
  ASSERT_TRUE(IsFutureFinished(fut.state()));
}

// Assert the future is successful *now*
template <typename T>
void AssertSuccessful(const Future<T>& fut) {
  if (IsFutureFinished(fut.state())) {
    ASSERT_EQ(fut.state(), FutureState::SUCCESS);
    ASSERT_OK(fut.status());
  } else {
    FAIL() << "Expected future to be completed successfully but it was still pending";
  }
}

// Assert the future is failed *now*
template <typename T>
void AssertFailed(const Future<T>& fut) {
  if (IsFutureFinished(fut.state())) {
    ASSERT_EQ(fut.state(), FutureState::FAILURE);
    ASSERT_FALSE(fut.status().ok());
  } else {
    FAIL() << "Expected future to have failed but it was still pending";
  }
}

}  // namespace arrow
