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

#include <memory>
#include <string>
#include "gandiva/simple_arena.h"

namespace gandiva {

/// Execution context during llvm evaluation
class ExecutionContext {
 public:
  explicit ExecutionContext(arrow::MemoryPool* pool = arrow::default_memory_pool())
      : arena_(pool) {}
  std::string get_error() const { return error_msg_; }

  void set_error_msg(const char* error_msg) {
    // Remember the first error only.
    if (error_msg_.empty()) {
      error_msg_ = std::string(error_msg);
    }
  }

  bool has_error() const { return !error_msg_.empty(); }

  SimpleArena* arena() { return &arena_; }

  // Byte offset of the arena's "used" flag from the start of a context. Generated code
  // reads the flag through the context pointer it already has.
  static int64_t arena_used_offset() {
    static const int64_t offset = [] {
      ExecutionContext context;
      return reinterpret_cast<char*>(context.arena()->used_ptr()) -
             reinterpret_cast<char*>(&context);
    }();
    return offset;
  }

  void Reset() {
    error_msg_.clear();
    arena_.Reset();
  }

 private:
  std::string error_msg_;
  SimpleArena arena_;
};

}  // namespace gandiva
