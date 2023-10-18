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

#include "gandiva/llvm_external_bitcode_store.h"

#include <llvm/Bitcode/BitcodeReader.h>

namespace gandiva {

static std::vector<std::unique_ptr<llvm::MemoryBuffer>>* get_stored_memory_buffers() {
  static std::vector<std::unique_ptr<llvm::MemoryBuffer>> memory_buffers;
  return &memory_buffers;
}

Status LLVMExternalBitcodeStore::Add(const std::string& bitcode_file_path) {
  auto buffer_or_error = llvm::MemoryBuffer::getFile(bitcode_file_path);

  ARROW_RETURN_IF(!buffer_or_error,
                  Status::IOError("Could not load module from bitcode file: ",
                                  bitcode_file_path +
                                      " Error: " + buffer_or_error.getError().message()));

  auto buffer = std::move(buffer_or_error.get());
  get_stored_memory_buffers()->emplace_back(std::move(buffer));
  return Status::OK();
}

Status LLVMExternalBitcodeStore::Add(std::unique_ptr<llvm::MemoryBuffer> buffer) {
  get_stored_memory_buffers()->emplace_back(std::move(buffer));
  return Status::OK();
}

const std::vector<std::unique_ptr<llvm::MemoryBuffer>>&
LLVMExternalBitcodeStore::GetBitcodeBuffers() {
  return *get_stored_memory_buffers();
}

}  // namespace gandiva
