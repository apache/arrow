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

#include "./arrow_types.h"
#include "./safe-call-into-r.h"

#include <functional>
#include <thread>

MainRThread& MainRThread::GetInstance() {
  static MainRThread main_r_thread;
  return main_r_thread;
}

// [[arrow::export]]
void InitializeMainRThread() { MainRThread::GetInstance().Initialize(); }

// [[arrow::export]]
void DeinitializeMainRThread() { MainRThread::GetInstance().Deinitialize(); }

// [[arrow::export]]
bool SetEnableSignalStopSource(bool enabled) {
  bool was_enabled = MainRThread::GetInstance().SignalStopSourceEnabled();
  if (was_enabled && !enabled) {
    MainRThread::GetInstance().DisableSignalStopSource();
  } else if (!was_enabled && enabled) {
    MainRThread::GetInstance().EnableSignalStopSource();
  }

  return was_enabled;
}

// [[arrow::export]]
bool CanRunWithCapturedR() {
#if defined(HAS_UNWIND_PROTECT)
  return MainRThread::GetInstance().Executor() == nullptr;
#else
  return false;
#endif
}

// [[arrow::export]]
std::string TestSafeCallIntoR(cpp11::function r_fun_that_returns_a_string,
                              std::string opt) {
  if (opt == "async_with_executor") {
    std::thread thread;

    auto result = RunWithCapturedR<std::string>([&thread, r_fun_that_returns_a_string]() {
      auto fut = arrow::Future<std::string>::Make();
      thread = std::thread([&fut, r_fun_that_returns_a_string]() {
        auto result = SafeCallIntoR<std::string>(
            [&] { return cpp11::as_cpp<std::string>(r_fun_that_returns_a_string()); });

        fut.MarkFinished(result);
      });

      return fut;
    });

    if (thread.joinable()) {
      thread.join();
    }

    return arrow::ValueOrStop(result);
  } else if (opt == "async_without_executor") {
    auto fut = arrow::Future<std::string>::Make();
    std::thread thread([&fut, r_fun_that_returns_a_string]() {
      auto result = SafeCallIntoR<std::string>(
          [&] { return cpp11::as_cpp<std::string>(r_fun_that_returns_a_string()); });

      if (result.ok()) {
        fut.MarkFinished(result.ValueUnsafe());
      } else {
        fut.MarkFinished(result.status());
      }
    });

    thread.join();

    // We should be able to get this far, but fut will contain an error
    // because it tried to evaluate R code from another thread
    return arrow::ValueOrStop(fut.result());

  } else if (opt == "on_main_thread") {
    auto result = SafeCallIntoR<std::string>(
        [&]() { return cpp11::as_cpp<std::string>(r_fun_that_returns_a_string()); });
    arrow::StopIfNotOk(result.status());
    return result.ValueUnsafe();
  } else {
    cpp11::stop("Unknown `opt`");
  }
}
