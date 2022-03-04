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

#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_set>

class MainRThreadTasks {
 public:
  class Task {
   public:
    virtual ~Task() {}
    virtual void run() {}
  };

  class EventLoop {
   public:
    EventLoop();
    ~EventLoop();
    std::thread::id thread() { return thread_; }

   private:
    std::thread::id thread_;
  };

  void Register(EventLoop* loop) { loop_ = loop; }

  void Unregister() { loop_ = nullptr; }

  EventLoop* Loop() { return loop_; }

 private:
  EventLoop* loop_;
};

void SafeCallIntoRBase(MainRThreadTasks::Task* task);

template <typename T>
T SafeCallIntoR(std::function<cpp11::sexp(void)> fun) {
  class TypedTask : public MainRThreadTasks::Task {
   public:
    TypedTask(std::function<cpp11::sexp(void)> fun) : fun_(fun){};

    void run() { result = cpp11::as_cpp<T>(fun_()); }

    T result;

   private:
    std::function<cpp11::sexp(void)> fun_;
  };

  TypedTask task(fun);
  SafeCallIntoRBase(&task);
  return task.result;
}

#endif
