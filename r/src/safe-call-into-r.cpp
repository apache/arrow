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

#include "safe-call-into-r.h"
#include <functional>
#include <thread>

static MainRThread main_r_thread;

MainRThread* GetMainRThread() { return &main_r_thread; }

arrow::Status MainRThread::RunTask(Task* task) {
  if (IsMainThread()) {
    // If we're on the main thread, run the task immediately
    try {
      ARROW_RETURN_NOT_OK(task->run());
      return arrow::Status::OK();
    } catch (cpp11::unwind_exception& e) {
      SetError(e.token);
      return arrow::Status::UnknownError("R code execution error");
    }
  } else if (executor_ != nullptr) {
    // If we are not on the main thread and have an Executor
    // use it to run the task on the main R thread.
    auto fut = executor_->Submit([task]() { return task->run(); });
    ARROW_RETURN_NOT_OK(fut);
    ARROW_RETURN_NOT_OK(fut.ValueUnsafe().result());
    return arrow::Status::OK();
  } else {
    return arrow::Status::NotImplemented("Call to R from a non-R thread without an event loop");
  }
}

// [[arrow::export]]
void InitializeMainRThread() { main_r_thread.Initialize(); }

// [[arrow::export]]
std::string TestSafeCallIntoR(cpp11::sexp fun_that_returns_a_string, std::string opt) {
  if (opt == "async_with_executor") {
    // std::thread* thread_ptr;


    // thread_ptr->join();
    // delete thread_ptr;
    return "";
  } else if (opt == "async_without_executor") {
    std::thread* thread_ptr;

    SEXP fun_sexp = fun_that_returns_a_string;
    auto fut = arrow::Future<std::string>::Make();

    thread_ptr = new std::thread([fut, fun_sexp]() mutable {
        auto result = SafeCallIntoR<std::string>([&] {
          cpp11::function fun(fun_sexp);
          return cpp11::as_cpp<std::string>(fun());
        });

        if (result.ok()) {
          fut.MarkFinished(result.ValueUnsafe());
        } else {
          fut.MarkFinished(result.status());
        }
    });

    thread_ptr->join();
    delete thread_ptr;

    // We didn't evaluate any R code because it wasn't safe, but
    // if we did and there was an error, we need to stop() here
    GetMainRThread()->ClearError();

    // We should be able to get this far, but fut will contain an error
    // because it tried to evaluate R code from another thread
    return arrow::ValueOrStop(fut.result());

  } else if (opt == "on_main_thread") {
    auto result = SafeCallIntoR<std::string>([&]() {
        cpp11::function fun(fun_that_returns_a_string);
        return cpp11::as_cpp<std::string>(fun());
      });
    GetMainRThread()->ClearError();
    arrow::StopIfNotOk(result.status());
    return result.ValueUnsafe();
  } else {
    return "";
  }
}
