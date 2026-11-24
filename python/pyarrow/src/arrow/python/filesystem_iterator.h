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
#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"

namespace arrow::py::fs {

class FileInfoVectorIterator {
 public:
  FileInfoVectorIterator() = default;
  explicit FileInfoVectorIterator(arrow::fs::FileInfoGenerator generator)
      : generator_(std::move(generator)) {}

  Result<arrow::fs::FileInfoVector> Next() {
    if (!generator_) {
      return IterationEnd<arrow::fs::FileInfoVector>();
    }
    auto next = generator_().result();
    if (!next.ok() || IsIterationEnd(*next)) {
      generator_ = nullptr;
    }
    return next;
  }

 private:
  arrow::fs::FileInfoGenerator generator_;
};

inline FileInfoVectorIterator MakeFileInfoVectorIterator(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
    const arrow::fs::FileSelector& selector) {
  return FileInfoVectorIterator(filesystem->GetFileInfoGenerator(selector));
}

}  // namespace arrow::py::fs
