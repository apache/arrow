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

  // Class whose run() method will be called from the main R thread
  // but whose results may be accessed (as class fields) from
  // potentially another thread.
  class Task {
   public:
    virtual ~Task() {}
    virtual arrow::Status run() = 0;
  };

  // Run `task` if it is safe to do so or return an error otherwise.
  arrow::Status RunTask(Task* task);

  // The Executor that is running on the main R thread, if it exists
  arrow::internal::Executor*& Executor() { return executor_; }

  // Save an error token generated from a cpp11::unwind_exception
  // so that it can be properly handled after some cleanup code
  // has run (e.g., cancelling some futures or waiting for them
  // to finish).
  void SetError(cpp11::sexp token) { error_token_ = token; }

  // Check if there is a saved error
  bool HasError() { return error_token_ != R_NilValue; }

  // Throw a cpp11::unwind_exception() with the saved token if it exists
  void ClearError() {
    if (HasError()) {
      cpp11::unwind_exception e(error_token_);
      SetError(R_NilValue);
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
// a SEXP (use cpp11::as_sexp<T> to convert it to a C++ type inside
// `fun`).
template <typename T>
arrow::Result<T> SafeCallIntoR(std::function<T(void)> fun) {
  class TypedTask : public MainRThread::Task {
   public:
    explicit TypedTask(std::function<T(void)> fun) : fun_(fun) {}

    arrow::Status run() {
      result = fun_();
      return arrow::Status::OK();
    }

    T result;

   private:
    std::function<T(void)> fun_;
  };

  TypedTask task(fun);
  ARROW_RETURN_NOT_OK(GetMainRThread().RunTask(&task));
  return task.result;
}

template <typename T>
arrow::Result<T> RunWithCapturedR(std::function<arrow::Future<T>()> task) {
  if (GetMainRThread().Executor() != nullptr) {
    return arrow::Status::AlreadyExists("Attempt to use more than one R Executor()");
  }

  arrow::Result<T> result = arrow::internal::SerialExecutor::RunInSerialExecutor<T>(
      [task](arrow::internal::Executor* executor) {
        GetMainRThread().Executor() = executor;
        arrow::Future<T> result = task();
        return result;
      });

  GetMainRThread().Executor() = nullptr;

  return result;
}

#endif
