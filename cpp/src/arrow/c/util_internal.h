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

#include "arrow/c/helpers.h"

namespace arrow {
namespace internal {

struct SchemaExportTraits {
  typedef struct ArrowSchema CType;
  static constexpr auto IsReleasedFunc = &ArrowSchemaIsReleased;
  static constexpr auto ReleaseFunc = &ArrowSchemaRelease;
};

struct ArrayExportTraits {
  typedef struct ArrowArray CType;
  static constexpr auto IsReleasedFunc = &ArrowArrayIsReleased;
  static constexpr auto ReleaseFunc = &ArrowArrayRelease;
};

struct ArrayStreamExportTraits {
  typedef struct ArrowArrayStream CType;
  static constexpr auto IsReleasedFunc = &ArrowArrayStreamIsReleased;
  static constexpr auto ReleaseFunc = &ArrowArrayStreamRelease;
};

// A RAII-style object to release a C Array / Schema struct at block scope exit.
template <typename Traits>
class ExportGuard {
 public:
  using CType = typename Traits::CType;

  explicit ExportGuard(CType* c_export) : c_export_(c_export) {}

  ExportGuard(ExportGuard&& other) : c_export_(other.c_export_) {
    other.c_export_ = nullptr;
  }

  ExportGuard& operator=(ExportGuard&& other) {
    Release();
    c_export_ = other.c_export_;
    other.c_export_ = nullptr;
  }

  ~ExportGuard() { Release(); }

  void Detach() { c_export_ = nullptr; }

  void Reset(CType* c_export) { c_export_ = c_export; }

  void Release() {
    if (c_export_) {
      Traits::ReleaseFunc(c_export_);
      c_export_ = nullptr;
    }
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExportGuard);

  CType* c_export_;
};

using SchemaExportGuard = ExportGuard<SchemaExportTraits>;
using ArrayExportGuard = ExportGuard<ArrayExportTraits>;
using ArrayStreamExportGuard = ExportGuard<ArrayStreamExportTraits>;

}  // namespace internal
}  // namespace arrow
