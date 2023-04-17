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

#include <atomic>
#include <cmath>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <signal.h>
#ifndef _WIN32
#include <sys/time.h>  // for setitimer()
#include <sys/types.h>
#include <unistd.h>
#endif

#include "arrow/testing/gtest_util.h"
#include "arrow/util/cancel.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {

class CancelTest : public ::testing::Test {};

TEST_F(CancelTest, StopBasics) {
  {
    StopSource source;
    StopToken token = source.token();
    ASSERT_FALSE(token.IsStopRequested());
    ASSERT_OK(token.Poll());

    source.RequestStop();
    ASSERT_TRUE(token.IsStopRequested());
    ASSERT_RAISES(Cancelled, token.Poll());
  }
  {
    StopSource source;
    StopToken token = source.token();
    source.RequestStop(Status::IOError("Operation cancelled"));
    ASSERT_TRUE(token.IsStopRequested());
    ASSERT_RAISES(IOError, token.Poll());
  }
}

TEST_F(CancelTest, StopTokenCopy) {
  StopSource source;
  StopToken token = source.token();
  ASSERT_FALSE(token.IsStopRequested());
  ASSERT_OK(token.Poll());

  StopToken token2 = token;
  ASSERT_FALSE(token2.IsStopRequested());
  ASSERT_OK(token2.Poll());

  source.RequestStop();
  StopToken token3 = token;

  ASSERT_TRUE(token.IsStopRequested());
  ASSERT_TRUE(token2.IsStopRequested());
  ASSERT_TRUE(token3.IsStopRequested());
  ASSERT_RAISES(Cancelled, token.Poll());
  ASSERT_EQ(token2.Poll(), token.Poll());
  ASSERT_EQ(token3.Poll(), token.Poll());
}

TEST_F(CancelTest, RequestStopTwice) {
  StopSource source;
  StopToken token = source.token();
  source.RequestStop();
  // Second RequestStop() call is ignored
  source.RequestStop(Status::IOError("Operation cancelled"));
  ASSERT_TRUE(token.IsStopRequested());
  ASSERT_RAISES(Cancelled, token.Poll());
}

TEST_F(CancelTest, Unstoppable) {
  StopToken token = StopToken::Unstoppable();
  ASSERT_FALSE(token.IsStopRequested());
  ASSERT_OK(token.Poll());
}

TEST_F(CancelTest, SourceVanishes) {
  {
    std::optional<StopSource> source{StopSource()};
    StopToken token = source->token();
    ASSERT_FALSE(token.IsStopRequested());
    ASSERT_OK(token.Poll());

    source.reset();
    ASSERT_FALSE(token.IsStopRequested());
    ASSERT_OK(token.Poll());
  }
  {
    std::optional<StopSource> source{StopSource()};
    StopToken token = source->token();
    source->RequestStop();

    source.reset();
    ASSERT_TRUE(token.IsStopRequested());
    ASSERT_RAISES(Cancelled, token.Poll());
  }
}

static void noop_signal_handler(int signum) {
  internal::ReinstateSignalHandler(signum, &noop_signal_handler);
}

#ifndef _WIN32
static std::optional<StopSource> signal_stop_source;

static void signal_handler(int signum) {
  signal_stop_source->RequestStopFromSignal(signum);
}

// SIGALRM will be received once after the specified wait
static void SetITimer(double seconds) {
  const double fractional = std::modf(seconds, &seconds);
  struct itimerval it;
  it.it_value.tv_sec = seconds;
  it.it_value.tv_usec = 1e6 * fractional;
  it.it_interval.tv_sec = 0;
  it.it_interval.tv_usec = 0;
  ASSERT_EQ(0, setitimer(ITIMER_REAL, &it, nullptr)) << "setitimer failed";
}

TEST_F(CancelTest, RequestStopFromSignal) {
  signal_stop_source = StopSource();  // Start with a fresh StopSource
  StopToken signal_token = signal_stop_source->token();
  SignalHandlerGuard guard(SIGALRM, &signal_handler);

  // Timer will be triggered once in 100 usecs
  SetITimer(0.0001);

  BusyWait(1.0, [&]() { return signal_token.IsStopRequested(); });
  ASSERT_TRUE(signal_token.IsStopRequested());
  auto st = signal_token.Poll();
  ASSERT_RAISES(Cancelled, st);
  ASSERT_EQ(st.message(), "Operation cancelled");
  ASSERT_EQ(internal::SignalFromStatus(st), SIGALRM);
}
#endif

class SignalCancelTest : public CancelTest {
 public:
  void SetUp() override {
    // Setup a dummy signal handler to avoid crashing when receiving signal
    guard_.emplace(expected_signal_, &noop_signal_handler);
    ASSERT_OK_AND_ASSIGN(auto stop_source, SetSignalStopSource());
    stop_token_ = stop_source->token();
  }

  void TearDown() override {
    UnregisterCancellingSignalHandler();
    ResetSignalStopSource();
  }

  void RegisterHandler() {
    ASSERT_OK(RegisterCancellingSignalHandler({expected_signal_}));
  }

#ifdef _WIN32
  void TriggerSignal() {
    std::thread([]() { ASSERT_OK(internal::SendSignal(SIGINT)); }).detach();
  }
#else
  // On Unix, use setitimer() to exercise signal-async-safety
  void TriggerSignal() { SetITimer(0.0001); }
#endif

  void AssertStopNotRequested() {
    SleepFor(0.01);
    ASSERT_FALSE(stop_token_->IsStopRequested());
    ASSERT_OK(stop_token_->Poll());
  }

  void AssertStopRequested() {
    BusyWait(1.0, [&]() { return stop_token_->IsStopRequested(); });
    ASSERT_TRUE(stop_token_->IsStopRequested());
    auto st = stop_token_->Poll();
    ASSERT_RAISES(Cancelled, st);
    ASSERT_EQ(st.message(), "Operation cancelled");
    ASSERT_EQ(internal::SignalFromStatus(st), expected_signal_);
  }

#ifndef _WIN32
  void RunInChild(std::function<void()> func) {
    auto child_pid = fork();
    if (child_pid == -1) {
      ASSERT_OK(internal::IOErrorFromErrno(errno, "Error calling fork(): "));
    }
    if (child_pid == 0) {
      // Child
      ASSERT_NO_FATAL_FAILURE(func()) << "Failure in child process";
      std::exit(0);
    } else {
      // Parent
      AssertChildExit(child_pid);
    }
  }
#endif

 protected:
#ifdef _WIN32
  const int expected_signal_ = SIGINT;
#else
  const int expected_signal_ = SIGALRM;
#endif
  std::optional<SignalHandlerGuard> guard_;
  std::optional<StopToken> stop_token_;
};

TEST_F(SignalCancelTest, Register) {
  RegisterHandler();

  TriggerSignal();
  AssertStopRequested();
}

TEST_F(SignalCancelTest, RegisterUnregister) {
  // The signal stop source was set up but no handler was registered,
  // so the token shouldn't be signalled.
  TriggerSignal();
  AssertStopNotRequested();

  // Register and then unregister: same
  RegisterHandler();
  UnregisterCancellingSignalHandler();

  TriggerSignal();
  AssertStopNotRequested();

  // Register again and raise the signal: token will be signalled.
  RegisterHandler();

  TriggerSignal();
  AssertStopRequested();
}

#if !(defined(_WIN32) || defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || \
      defined(THREAD_SANITIZER))
TEST_F(SignalCancelTest, ForkSafetyUnregisteredHandlers) {
  RunInChild([&]() {
    // Child
    TriggerSignal();
    AssertStopNotRequested();

    RegisterHandler();
    TriggerSignal();
    AssertStopRequested();
  });

  // Parent: shouldn't notice signals raised in child
  AssertStopNotRequested();

  // Stop source still usable in parent
  TriggerSignal();
  AssertStopNotRequested();

  RegisterHandler();
  TriggerSignal();
  AssertStopRequested();
}

TEST_F(SignalCancelTest, ForkSafetyRegisteredHandlers) {
  RegisterHandler();

  RunInChild([&]() {
    // Child: signal handlers are unregistered and need to be re-registered
    TriggerSignal();
    AssertStopNotRequested();

    // Can re-register and receive signals
    RegisterHandler();
    TriggerSignal();
    AssertStopRequested();
  });

  // Parent: shouldn't notice signals raised in child
  AssertStopNotRequested();

  // Stop source still usable in parent
  TriggerSignal();
  AssertStopRequested();
}
#endif

TEST_F(CancelTest, ThreadedPollSuccess) {
  constexpr int kNumThreads = 10;

  std::vector<Status> results(kNumThreads);
  std::vector<std::thread> threads;

  StopSource source;
  StopToken token = source.token();
  std::atomic<bool> terminate_flag{false};

  const auto worker_func = [&](int thread_num) {
    while (token.Poll().ok() && !terminate_flag) {
    }
    results[thread_num] = token.Poll();
  };
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(std::bind(worker_func, i));
  }

  // Let the threads start and hammer on Poll() for a while
  SleepFor(1e-2);
  // Tell threads to stop
  terminate_flag = true;
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& st : results) {
    ASSERT_OK(st);
  }
}

TEST_F(CancelTest, ThreadedPollCancel) {
  constexpr int kNumThreads = 10;

  std::vector<Status> results(kNumThreads);
  std::vector<std::thread> threads;

  StopSource source;
  StopToken token = source.token();
  std::atomic<bool> terminate_flag{false};
  const auto stop_error = Status::IOError("Operation cancelled");

  const auto worker_func = [&](int thread_num) {
    while (token.Poll().ok() && !terminate_flag) {
    }
    results[thread_num] = token.Poll();
  };

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(std::bind(worker_func, i));
  }
  // Let the threads start
  SleepFor(1e-2);
  // Cancel token while threads are hammering on Poll()
  source.RequestStop(stop_error);
  // Tell threads to stop
  terminate_flag = true;
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& st : results) {
    ASSERT_EQ(st, stop_error);
  }
}

}  // namespace arrow
