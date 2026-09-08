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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"

#include "parquet/types.h"

namespace parquet::internal {

/// Parquet adapter around the vendored CWI FSST codec.
///
/// The CWI library owns FSST8 training and compression. This class remaps its
/// native code order to the portable, length-ordered Parquet representation.
class PARQUET_EXPORT FsstSymbolTable {
 public:
  ~FsstSymbolTable();

  static std::shared_ptr<FsstSymbolTable> Train(const std::vector<std::string>& values);

  static std::shared_ptr<FsstSymbolTable> TrainBatches(
      const std::vector<std::vector<std::string>>& value_batches);

  static std::shared_ptr<FsstSymbolTable> Deserialize(
      const std::shared_ptr<::arrow::Buffer>& body);

  std::shared_ptr<::arrow::Buffer> Serialize(::arrow::MemoryPool* pool) const;

  bool CompressBatch(const std::vector<std::string>& input, size_t max_output_size,
                     std::vector<int32_t>* end_offsets,
                     std::vector<uint8_t>* output) const;
  bool Compress(std::string_view input, size_t max_output_size,
                std::vector<uint8_t>* output) const;
  void Decompress(const uint8_t* input, int64_t input_size,
                  std::vector<uint8_t>* output) const;

  uint32_t symbol_count() const { return static_cast<uint32_t>(symbols_.size()); }
  const std::string& symbol(uint32_t code) const { return symbols_[code]; }

 private:
  struct CwiState;

  explicit FsstSymbolTable(std::vector<std::string> symbols);
  static std::shared_ptr<FsstSymbolTable> TrainFromInputs(
      std::vector<size_t> lengths, std::vector<const unsigned char*> inputs);
  void InitializeCwiDecoder();

  std::vector<std::string> symbols_;
  std::unique_ptr<CwiState> cwi_;
};

}  // namespace parquet::internal
