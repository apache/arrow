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
#if defined(ARROW_R_WITH_ARROW)
#include <arrow/config.h>
#include <arrow/util/mutex.h>
#include <arrow/util/visibility.h>

class GcRuntimeInfo : public arrow::RuntimeInfo {
 public:
   GcRuntimeInfo() : runtimeinfo_(arrow::default_runtime_info()) {}

  std::string simd_level() const { return runtimeinfo_->simd_level(); }

  std::string detected_simd_level() const { return runtimeinfo_->detected_simd_level(); }

 private:
  template <typename Call>
  arrow::Status GcAndTryAgain(const Call& call) {
    if (call().ok()) {
      return arrow::Status::OK();
    } else {
      auto lock = mutex_.Lock();

      // ARROW-10080: Allocation may fail spuriously since the garbage collector is lazy.
      // Force it to run then try again in case any reusable allocations have been freed.
      static cpp11::function gc = cpp11::package("base")["gc"];
      gc();
    }
    return call();
  }

  arrow::RuntimeInfo* runtimeinfo_;
};

static GcRuntimeInfo g_runtimeinfo;

arrow::RuntimeInfo* gc_runtime_info() { return &g_runtimeinfo; }

// [[arrow::export]]
std::shared_ptr<arrow::RuntimeInfo> RuntimeInfo__default() {
  return std::shared_ptr<arrow::RuntimeInfo>(&g_runtimeinfo, [](...) {});
}

// [[arrow::export]]
double RuntimeInfo__simd_level(const std::shared_ptr<arrow::RuntimeInfo>& runtimeinfo) {
  return runtimeinfo->simd_level();
}

// [[arrow::export]]
double RuntimeInfo__detected_simd_level(const std::shared_ptr<arrow::RuntimeInfo>& runtimeinfo) {
  return runtimeinfo->detected_simd_level();
}

#endif
