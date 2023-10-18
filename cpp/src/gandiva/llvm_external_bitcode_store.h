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
#include <vector>

#include <llvm/Support/MemoryBuffer.h>

#include <arrow/status.h>
#include <gandiva/visibility.h>

namespace gandiva {
using arrow::Status;

class GANDIVA_EXPORT LLVMExternalBitcodeStore {
 public:
  /// \brief add an LLVM IR to the store from a given bitcode file path
  static Status Add(const std::string& bitcode_file_path);

  /// \brief add an LLVM memory buffer containing bitcode to the store
  static Status Add(std::unique_ptr<llvm::MemoryBuffer> buffer);

  /// \brief get a list of LLVM memory buffers saved in the store
  static const std::vector<std::unique_ptr<llvm::MemoryBuffer>>& GetBitcodeBuffers();
};
}  // namespace gandiva
