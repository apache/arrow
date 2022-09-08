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

#include <arrow/io/interfaces.h>
#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>

#include <functional>
#include <thread>

// Unwind protection was added in R 3.5 and some calls here use it
// and crash R in older versions (ARROW-16201). Implementation provided
// in safe-call-into-r-impl.cpp so that we can skip some tests
// when this feature is not provided. This also checks that there
// is not already an event loop registered (via MainRThread::Executor()),
// because only one of these can exist at any given time.
bool CanRunWithCapturedR();

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
    ResetError();
  }

  bool IsInitialized() { return initialized_; }

  // Check if the current thread is the main R thread
  bool IsMainThread() { return initialized_ && std::this_thread::get_id() == thread_id_; }

  // Check if a SafeCallIntoR call is able to execute
  bool CanExecuteSafeCallIntoR() { return IsMainThread() || executor_ != nullptr; }

  // The Executor that is running on the main R thread, if it exists
  arrow::internal::Executor*& Executor() { return executor_; }

  // Save an error (possibly with an error token generated from
  // a cpp11::unwind_exception) so that it can be properly handled
  // after some cleanup code  has run (e.g., cancelling some futures
  // or waiting for them to finish).
  void SetError(arrow::Status status) { status_ = status; }

  void ResetError() { status_ = arrow::Status::OK(); }

  // Check if there is a saved error
  bool HasError() { return !status_.ok(); }

  // Throw a cpp11::unwind_exception() if
  void ClearError() {
    arrow::Status maybe_error_status = status_;
    ResetError();
    arrow::StopIfNotOk(maybe_error_status);
  }

 private:
  bool initialized_;
  std::thread::id thread_id_;
  arrow::Status status_;
  arrow::internal::Executor* executor_;
};

// Retrieve the MainRThread singleton
MainRThread& GetMainRThread();

// Call into R and return a C++ object. Note that you can't return
// a SEXP (use cpp11::as_cpp<T> to convert it to a C++ type inside
// `fun`).
template <typename T>
arrow::Future<T> SafeCallIntoRAsync(std::function<arrow::Result<T>(void)> fun,
                                    std::string reason = "unspecified") {
  MainRThread& main_r_thread = GetMainRThread();
  if (main_r_thread.IsMainThread()) {
    // If we're on the main thread, run the task immediately and let
    // the cpp11::unwind_exception be thrown since it will be caught
    // at the top level.
    return fun();
  } else if (main_r_thread.CanExecuteSafeCallIntoR()) {
    // If we are not on the main thread and have an Executor,
    // use it to run the task on the main R thread. We can't throw
    // a cpp11::unwind_exception here, so we need to propagate it back
    // to RunWithCapturedR through the MainRThread singleton.
    return DeferNotOk(main_r_thread.Executor()->Submit([fun, reason]() {
      // This occurs when some other R code that was previously scheduled to run
      // has errored, in which case we skip execution and let the original
      // error surface.
      if (GetMainRThread().HasError()) {
        return arrow::Result<T>(
            arrow::Status::Cancelled("Previous R code execution error (", reason, ")"));
      }

      try {
        return fun();
      } catch (cpp11::unwind_exception& e) {
        // Here we save the token and set the main R thread to an error state
        GetMainRThread().SetError(arrow::StatusUnwindProtect(e.token));

        // We also return an error although this should not surface because
        // main_r_thread.ClearError() will get called before this value can be
        // returned and will StopIfNotOk(). We don't save the error token here
        // to ensure that it will only get thrown once.
        return arrow::Result<T>(
            arrow::Status::UnknownError("R code execution error (", reason, ")"));
      }
    }));
  } else {
    return arrow::Status::NotImplemented(
        "Call to R (", reason, ") from a non-R thread from an unsupported context");
  }
}

template <typename T>
arrow::Result<T> SafeCallIntoR(std::function<T(void)> fun,
                               std::string reason = "unspecified") {
  arrow::Future<T> future = SafeCallIntoRAsync<T>(std::move(fun), reason);
  return future.result();
}

static inline arrow::Status SafeCallIntoRVoid(std::function<void(void)> fun,
                                              std::string reason = "unspecified") {
  arrow::Future<bool> future = SafeCallIntoRAsync<bool>(
      [&fun]() {
        fun();
        return true;
      },
      reason);
  return future.status();
}

// Performs an Arrow call (e.g., run an exec plan) in such a way that background threads
// can use SafeCallIntoR(). This version is useful for Arrow calls that already
// return a Future<>.
template <typename T>
arrow::Result<T> RunWithCapturedR(std::function<arrow::Future<T>()> make_arrow_call) {
  if (!CanRunWithCapturedR()) {
    return arrow::Status::NotImplemented("RunWithCapturedR() without UnwindProtect");
  }

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

// Performs an Arrow call (e.g., run an exec plan) in such a way that background threads
// can use SafeCallIntoR(). This version is useful for Arrow calls that do not already
// return a Future<>(). If it is not possible to use RunWithCapturedR() (i.e.,
// CanRunWithCapturedR() returns false), this will run make_arrow_call on the main
// R thread (which will cause background threads that try to SafeCallIntoR() to
// error).
template <typename T>
arrow::Result<T> RunWithCapturedRIfPossible(
    std::function<arrow::Result<T>()> make_arrow_call) {
  if (CanRunWithCapturedR()) {
    // Note that the use of the io_context here is arbitrary (i.e. we could use
    // any construct that launches a background thread).
    const auto& io_context = arrow::io::default_io_context();
    return RunWithCapturedR<T>([&]() {
      return DeferNotOk(io_context.executor()->Submit(std::move(make_arrow_call)));
    });
  } else {
    return make_arrow_call();
  }
}

// Like RunWithCapturedRIfPossible<>() but for arrow calls that don't return
// a Result.
static inline arrow::Status RunWithCapturedRIfPossibleVoid(
    std::function<arrow::Status()> make_arrow_call) {
  auto result = RunWithCapturedRIfPossible<bool>([&]() -> arrow::Result<bool> {
    ARROW_RETURN_NOT_OK(make_arrow_call());
    return true;
  });
  ARROW_RETURN_NOT_OK(result);
  return arrow::Status::OK();
}

#endif
