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

#ifndef SAFE_CALL_INTO_R_INCLUDED
#define SAFE_CALL_INTO_R_INCLUDED

#include "./arrow_types.h"

#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>

#include <functional>
#include <thread>

// The MainRThread class keeps track of the thread on which it is safe
// to call the R API to facilitate its safe use (or erroring
// if it is not safe). The MainRThread singleton can be accessed from
// any thread using GetMainRThread(); the preferred way to call
// the R API where it may not be safe to do so is to use
// SafeCallIntoR<cpp_type>([&]() { ... }).
class MainRThread {
 public:
  MainRThread() : initialized_(false), executor_(nullptr) {}

  // Call this method from the R thread (e.g., on package load)
  // to save an internal copy of the thread id.
  void Initialize() {
    thread_id_ = std::this_thread::get_id();
    initialized_ = true;
    SetError(R_NilValue);
  }

  bool IsInitialized() { return initialized_; }

  // Check if the current thread is the main R thread
  bool IsMainThread() { return initialized_ && std::this_thread::get_id() == thread_id_; }

  // The Executor that is running on the main R thread, if it exists
  arrow::internal::Executor*& Executor() { return executor_; }

  // Save an error token generated from a cpp11::unwind_exception
  // so that it can be properly handled after some cleanup code
  // has run (e.g., cancelling some futures or waiting for them
  // to finish).
  void SetError(cpp11::sexp token) { error_token_ = token; }

  void ResetError() { error_token_ = R_NilValue; }

  // Check if there is a saved error
  bool HasError() { return error_token_ != R_NilValue; }

  // Throw a cpp11::unwind_exception() with the saved token if it exists
  void ClearError() {
    if (HasError()) {
      cpp11::unwind_exception e(error_token_);
      ResetError();
      throw e;
    }
  }

 private:
  bool initialized_;
  std::thread::id thread_id_;
  cpp11::sexp error_token_;
  arrow::internal::Executor* executor_;
};

// Retrieve the MainRThread singleton
MainRThread& GetMainRThread();

// Call into R and return a C++ object. Note that you can't return
// a SEXP (use cpp11::as_cpp<T> to convert it to a C++ type inside
// `fun`).
template <typename T>
arrow::Future<T> SafeCallIntoRAsync(std::function<T(void)> fun) {
  MainRThread& main_r_thread = GetMainRThread();
  if (main_r_thread.IsMainThread()) {
    // If we're on the main thread, run the task immediately and let
    // the cpp11::unwind_exception be thrown since it will be caught
    // at the top level.
    return fun();
  } else if (main_r_thread.Executor() != nullptr) {
    // If we are not on the main thread and have an Executor,
    // use it to run the task on the main R thread. We can't throw
    // a cpp11::unwind_exception here, so we need to propagate it back
    // to RunWithCapturedR through the MainRThread singleton.
    return DeferNotOk(main_r_thread.Executor()->Submit([fun]() {
      if (GetMainRThread().HasError()) {
        return arrow::Result<T>(arrow::Status::UnknownError("R code execution error"));
      }

      try {
        return arrow::Result<T>(fun());
      } catch (cpp11::unwind_exception& e) {
        GetMainRThread().SetError(e.token);
        return arrow::Result<T>(arrow::Status::UnknownError("R code execution error"));
      }
    }));
  } else {
    return arrow::Status::NotImplemented(
        "Call to R from a non-R thread without calling RunWithCapturedR");
  }
}

template <typename T>
arrow::Result<T> SafeCallIntoR(std::function<T(void)> fun) {
  arrow::Future<T> future = SafeCallIntoRAsync<T>(std::move(fun));
  return future.result();
}

template <typename T>
arrow::Result<T> RunWithCapturedR(std::function<arrow::Future<T>()> make_arrow_call) {
  if (GetMainRThread().Executor() != nullptr) {
    return arrow::Status::AlreadyExists("Attempt to use more than one R Executor()");
  }

  GetMainRThread().ResetError();

  arrow::Result<T> result = arrow::internal::SerialExecutor::RunInSerialExecutor<T>(
      [make_arrow_call](arrow::internal::Executor* executor) {
        GetMainRThread().Executor() = executor;
        return make_arrow_call();
      });

  GetMainRThread().Executor() = nullptr;
  GetMainRThread().ClearError();

  return result;
}

#endif
