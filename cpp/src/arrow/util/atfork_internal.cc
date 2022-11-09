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

#include "arrow/util/atfork_internal.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <vector>

#ifndef _WIN32
#include <pthread.h>
#endif

#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

namespace {

struct RunningHandler {
  // A temporary owning copy of a handler, to make sure that a handler
  // that runs before fork can still run after fork.
  std::shared_ptr<AtForkHandler> handler;
  // The token returned by the before-fork handler, to pass to after-fork handlers.
  std::any token;

  explicit RunningHandler(std::shared_ptr<AtForkHandler> handler)
      : handler(std::move(handler)) {}
};

std::mutex g_mutex;
std::vector<std::weak_ptr<AtForkHandler>> g_handlers;
std::vector<RunningHandler> g_handlers_while_forking;

void MaintainHandlersUnlocked() {
  auto it = std::remove_if(
      g_handlers.begin(), g_handlers.end(),
      [](const std::weak_ptr<AtForkHandler>& ptr) { return ptr.expired(); });
  g_handlers.erase(it, g_handlers.end());
}

void BeforeFork() {
  // Lock the mutex and keep it locked until the end of AfterForkParent(),
  // to avoid multiple concurrent forks and atforks.
  g_mutex.lock();

  DCHECK(g_handlers_while_forking.empty());  // AfterForkParent clears it

  for (const auto& weak_handler : g_handlers) {
    if (auto handler = weak_handler.lock()) {
      g_handlers_while_forking.emplace_back(std::move(handler));
    }
  }

  // XXX can the handler call RegisterAtFork()?
  for (auto&& handler : g_handlers_while_forking) {
    if (handler.handler->before) {
      handler.token = handler.handler->before();
    }
  }
}

void AfterForkParent() {
  // The mutex was locked by BeforeFork()

  auto handlers = std::move(g_handlers_while_forking);
  g_handlers_while_forking.clear();
  // Execute handlers in reverse order
  for (auto it = handlers.rbegin(); it != handlers.rend(); ++it) {
    auto&& handler = *it;
    if (handler.handler->parent_after) {
      handler.handler->parent_after(std::move(handler.token));
    }
  }

  g_mutex.unlock();
  // handlers will be destroyed here without the mutex locked, so that
  // any action taken by destructors might call RegisterAtFork
}

void AfterForkChild() {
  // Need to reinitialize the mutex as it is probably invalid.  Also, the
  // old mutex destructor may fail.
  // Fortunately, we are a single thread in the child process by now, so no
  // additional synchronization is needed.
  new (&g_mutex) std::mutex;

  auto handlers = std::move(g_handlers_while_forking);
  g_handlers_while_forking.clear();
  // Execute handlers in reverse order
  for (auto it = handlers.rbegin(); it != handlers.rend(); ++it) {
    auto&& handler = *it;
    if (handler.handler->child_after) {
      handler.handler->child_after(std::move(handler.token));
    }
  }
}

struct AtForkInitializer {
  AtForkInitializer() {
#ifndef _WIN32
    int r = pthread_atfork(&BeforeFork, &AfterForkParent, &AfterForkChild);
    if (r != 0) {
      IOErrorFromErrno(r, "Error when calling pthread_atfork: ").Abort();
    }
#endif
  }
};

};  // namespace

void RegisterAtFork(std::weak_ptr<AtForkHandler> weak_handler) {
  static AtForkInitializer initializer;

  std::lock_guard<std::mutex> lock(g_mutex);
  MaintainHandlersUnlocked();
  g_handlers.push_back(std::move(weak_handler));
}

}  // namespace internal
}  // namespace arrow
