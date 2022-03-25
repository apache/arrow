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

#include "./safe-call-into-r.h"
#include <functional>
#include <thread>

static MainRThread main_r_thread;

MainRThread* GetMainRThread() { return &main_r_thread; }

arrow::Status MainRThread::RunTask(Task* task) {
  if (IsMainThread()) {
    try {
      ARROW_RETURN_NOT_OK(task->run());
      return arrow::Status::OK();
    } catch (cpp11::unwind_exception& e) {
      SetError(e.token);
      return arrow::Status::UnknownError("R code execution error");
    }
  } else {
    return arrow::Status::NotImplemented("Call to R from a non-R thread");
  }
}

// [[arrow::export]]
void InitializeMainRThread() { main_r_thread.Initialize(); }

// [[arrow::export]]
cpp11::strings TestSafeCallIntoR(cpp11::list funs_that_return_a_string, bool async) {
  std::vector<std::string> results;

  if (async) {
    // Probably a better test would be to submit all the jobs at once and
    // wait for them all to finish, but I'm not sure how to do that yet!

    // This simulates the Arrow thread pool. Just imagine it is static and lives forever.
    std::thread* thread_ptr;

    SEXP funs_sexp = funs_that_return_a_string;
    R_xlen_t n_funs = funs_that_return_a_string.size();

    auto fut = arrow::Future<std::vector<std::string>>::Make();

    thread_ptr = new std::thread([fut, funs_sexp, n_funs]() mutable {
      std::vector<std::string> results_local;

      for (R_xlen_t i = 0; i < n_funs; i++) {
        auto result = SafeCallIntoR<std::string>([&] {
          cpp11::function fun(VECTOR_ELT(funs_sexp, i));
          return cpp11::as_cpp<std::string>(fun());
        });

        if (result.ok()) {
          results_local.push_back(result.ValueUnsafe());
        } else {
          fut.MarkFinished(result.status());
          return;
        }
      }

      fut.MarkFinished(results_local);
    });

    // Simulated thread pool
    thread_ptr->join();
    delete thread_ptr;

    // We didn't evaluate any R code because it wasn't safe, but
    // if we did and there was an error, we need to stop() here
    GetMainRThread()->ClearError();

    // We should be able to get this far, but fut will contain an error
    // because it tried to evaluate R code from another thread
    results = arrow::ValueOrStop(fut.result());

  } else {
    for (R_xlen_t i = 0; i < funs_that_return_a_string.size(); i++) {
      auto result = SafeCallIntoR<std::string>([&]() {
        cpp11::function fun(funs_that_return_a_string[i]);
        return cpp11::as_cpp<std::string>(fun());
      });
      GetMainRThread()->ClearError();
      arrow::StopIfNotOk(result.status());
      results.push_back(result.ValueUnsafe());
    }
  }

  // To make sure the results are correct
  cpp11::writable::strings results_sexp;

  for (std::string& result : results) {
    results_sexp.push_back(result);
  }

  return results_sexp;
}
