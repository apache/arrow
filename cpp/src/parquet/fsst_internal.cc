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

#include "parquet/fsst_internal.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <numeric>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/vendored/fsst/fsst.h"

#include "parquet/exception.h"

namespace parquet::internal {
namespace {

constexpr uint8_t kFsstEscape = 0xFF;

}  // namespace

struct FsstSymbolTable::CwiState {
  CwiState() { cwi_to_parquet_code.fill(kFsstEscape); }

  ~CwiState() {
    if (encoder != nullptr) {
      fsst_destroy(encoder);
    }
  }

  fsst_encoder_t* encoder = nullptr;
  fsst_decoder_t decoder{};
  std::array<uint8_t, 255> cwi_to_parquet_code;
};

FsstSymbolTable::~FsstSymbolTable() = default;

FsstSymbolTable::FsstSymbolTable(std::vector<std::string> symbols)
    : symbols_(std::move(symbols)), cwi_(std::make_unique<CwiState>()) {
  InitializeCwiDecoder();
}

std::shared_ptr<FsstSymbolTable> FsstSymbolTable::Train(
    const std::vector<std::string>& values) {
  std::vector<size_t> lengths;
  std::vector<const unsigned char*> inputs;
  lengths.reserve(values.size());
  inputs.reserve(values.size());
  for (const std::string& value : values) {
    lengths.push_back(value.size());
    inputs.push_back(reinterpret_cast<const unsigned char*>(value.data()));
  }
  return TrainFromInputs(std::move(lengths), std::move(inputs));
}

std::shared_ptr<FsstSymbolTable> FsstSymbolTable::TrainBatches(
    const std::vector<std::vector<std::string>>& value_batches) {
  size_t num_values = 0;
  for (const auto& batch : value_batches) {
    if (batch.size() > std::numeric_limits<size_t>::max() - num_values) {
      throw ParquetException("FSST training corpus contains too many values");
    }
    num_values += batch.size();
  }
  std::vector<size_t> lengths;
  std::vector<const unsigned char*> inputs;
  lengths.reserve(num_values);
  inputs.reserve(num_values);
  for (const auto& batch : value_batches) {
    for (const std::string& value : batch) {
      lengths.push_back(value.size());
      inputs.push_back(reinterpret_cast<const unsigned char*>(value.data()));
    }
  }
  return TrainFromInputs(std::move(lengths), std::move(inputs));
}

std::shared_ptr<FsstSymbolTable> FsstSymbolTable::TrainFromInputs(
    std::vector<size_t> lengths, std::vector<const unsigned char*> inputs) {
  if (inputs.empty()) {
    // CWI expects at least one input slot. A zero-length sample produces a
    // fully usable table for encoding values on later pages.
    static constexpr unsigned char kEmptyInput = 0;
    lengths.push_back(0);
    inputs.push_back(&kEmptyInput);
  }

  auto trained = std::make_unique<CwiState>();
  trained->encoder = fsst_create(lengths.size(), lengths.data(), inputs.data(),
                                 /*zeroTerminated=*/0);
  if (trained->encoder == nullptr) {
    throw ParquetException("CWI FSST failed to train a symbol table");
  }
  const fsst_decoder_t native_decoder = fsst_decoder(trained->encoder);

  // The pinned CWI format stores nSymbols in byte 1 of its native export.
  // We use it only to inspect the trained table; native bytes are never written
  // to Parquet.
  std::array<unsigned char, FSST_MAXHEADER> native_table{};
  const unsigned int native_size = fsst_export(trained->encoder, native_table.data());
  if (native_size < 17 || native_table[0] != 1) {
    throw ParquetException("CWI FSST produced an invalid native symbol table");
  }
  const uint16_t symbol_count = native_table[1];
  uint16_t histogram_count = 0;
  for (int length = 0; length < 8; ++length) {
    histogram_count += native_table[9 + length];
  }
  if (histogram_count != symbol_count) {
    throw ParquetException("CWI FSST produced an inconsistent native symbol table");
  }

  std::vector<uint16_t> native_codes(symbol_count);
  std::iota(native_codes.begin(), native_codes.end(), 0);
  std::stable_sort(native_codes.begin(), native_codes.end(),
                   [&](uint16_t left, uint16_t right) {
                     return native_decoder.len[left] < native_decoder.len[right];
                   });

  std::vector<std::string> symbols;
  symbols.reserve(symbol_count);
  for (uint16_t parquet_code = 0; parquet_code < symbol_count; ++parquet_code) {
    const uint16_t native_code = native_codes[parquet_code];
    const uint8_t length = native_decoder.len[native_code];
    if (length == 0 || length > 8) {
      throw ParquetException("CWI FSST produced an invalid symbol length");
    }
    trained->cwi_to_parquet_code[native_code] = static_cast<uint8_t>(parquet_code);
    symbols.emplace_back(
        reinterpret_cast<const char*>(&native_decoder.symbol[native_code]), length);
  }

  auto table = std::shared_ptr<FsstSymbolTable>(new FsstSymbolTable(std::move(symbols)));
  table->cwi_->encoder = trained->encoder;
  trained->encoder = nullptr;
  table->cwi_->cwi_to_parquet_code = trained->cwi_to_parquet_code;
  return table;
}

std::shared_ptr<FsstSymbolTable> FsstSymbolTable::Deserialize(
    const std::shared_ptr<::arrow::Buffer>& body) {
  constexpr int max_length = 8;
  constexpr int64_t fixed_size = 9;
  if (body == nullptr || body->size() < fixed_size || body->size() > 2049) {
    throw ParquetException("Invalid FSST symbol table body size: ",
                           body == nullptr ? -1 : body->size());
  }

  const uint8_t* data = body->data();
  const uint32_t symbol_count = data[0];

  std::vector<uint32_t> histogram(max_length);
  uint64_t histogram_sum = 0;
  uint64_t expected_symbol_bytes = 0;
  const uint8_t* histogram_data = data + 1;
  for (int i = 0; i < max_length; ++i) {
    histogram[i] = histogram_data[i];
    histogram_sum += histogram[i];
    expected_symbol_bytes += static_cast<uint64_t>(histogram[i]) * (i + 1);
  }
  if (histogram_sum != symbol_count) {
    throw ParquetException("FSST length histogram does not match symbol count");
  }
  if (expected_symbol_bytes != static_cast<uint64_t>(body->size() - fixed_size)) {
    throw ParquetException("FSST symbol data size does not match length histogram");
  }

  std::vector<std::string> symbols;
  symbols.reserve(symbol_count);
  const char* symbol_data = reinterpret_cast<const char*>(data + fixed_size);
  int64_t offset = 0;
  for (int length = 1; length <= max_length; ++length) {
    for (uint32_t i = 0; i < histogram[length - 1]; ++i) {
      symbols.emplace_back(symbol_data + offset, length);
      offset += length;
    }
  }
  return std::shared_ptr<FsstSymbolTable>(new FsstSymbolTable(std::move(symbols)));
}

std::shared_ptr<::arrow::Buffer> FsstSymbolTable::Serialize(
    ::arrow::MemoryPool* pool) const {
  constexpr int max_length = 8;
  constexpr int64_t fixed_size = 9;
  int64_t symbol_bytes = 0;
  std::vector<uint32_t> histogram(max_length, 0);
  for (const std::string& symbol : symbols_) {
    ++histogram[symbol.size() - 1];
    symbol_bytes += static_cast<int64_t>(symbol.size());
  }

  auto buffer = ::arrow::AllocateBuffer(fixed_size + symbol_bytes, pool).ValueOrDie();
  uint8_t* output = buffer->mutable_data();
  output[0] = static_cast<uint8_t>(symbols_.size());
  uint8_t* histogram_output = output + 1;
  for (int i = 0; i < max_length; ++i) {
    histogram_output[i] = static_cast<uint8_t>(histogram[i]);
  }
  uint8_t* symbol_output = output + fixed_size;
  for (const std::string& symbol : symbols_) {
    std::copy(symbol.begin(), symbol.end(), symbol_output);
    symbol_output += symbol.size();
  }
  return std::shared_ptr<::arrow::Buffer>(std::move(buffer));
}

void FsstSymbolTable::InitializeCwiDecoder() {
  std::memset(&cwi_->decoder, 0, sizeof(cwi_->decoder));
  cwi_->decoder.zeroTerminated = 0;
  for (size_t code = 0; code < symbols_.size(); ++code) {
    const std::string& symbol = symbols_[code];
    if (symbol.empty() || symbol.size() > 8) {
      throw ParquetException("Invalid FSST8 symbol length");
    }
    cwi_->decoder.len[code] = static_cast<uint8_t>(symbol.size());
    std::memcpy(reinterpret_cast<uint8_t*>(&cwi_->decoder.symbol[code]), symbol.data(),
                symbol.size());
  }
}

bool FsstSymbolTable::CompressBatch(const std::vector<std::string>& input,
                                    size_t max_output_size,
                                    std::vector<int32_t>* end_offsets,
                                    std::vector<uint8_t>* output) const {
  end_offsets->clear();
  output->clear();
  if (input.empty()) {
    return true;
  }
  if (cwi_->encoder == nullptr) {
    throw ParquetException("FSST compression requires a CWI-trained FSST8 table");
  }

  std::vector<size_t> input_lengths;
  std::vector<const unsigned char*> input_pointers;
  input_lengths.reserve(input.size());
  input_pointers.reserve(input.size());
  size_t scratch_size = 0;
  for (const std::string& value : input) {
    if (scratch_size > std::numeric_limits<size_t>::max() - 7 ||
        value.size() > (std::numeric_limits<size_t>::max() - scratch_size - 7) / 2) {
      throw ParquetException("FSST compression scratch size overflow");
    }
    scratch_size += 7 + 2 * value.size();
    input_lengths.push_back(value.size());
    input_pointers.push_back(reinterpret_cast<const unsigned char*>(value.data()));
  }

  std::vector<uint8_t> scratch(scratch_size);
  std::vector<size_t> compressed_lengths(input.size());
  std::vector<unsigned char*> compressed_pointers(input.size());
  const size_t compressed_count =
      fsst_compress(cwi_->encoder, input.size(), input_lengths.data(),
                    input_pointers.data(), scratch.size(), scratch.data(),
                    compressed_lengths.data(), compressed_pointers.data());
  if (compressed_count != input.size()) {
    throw ParquetException("CWI FSST did not compress the complete input batch");
  }

  end_offsets->reserve(input.size());
  output->reserve(std::min(max_output_size, scratch_size));
  const uint8_t* scratch_end = scratch.data() + scratch.size();
  for (size_t value_index = 0; value_index < input.size(); ++value_index) {
    const uint8_t* compressed = compressed_pointers[value_index];
    const size_t compressed_size = compressed_lengths[value_index];
    if (compressed == nullptr || compressed < scratch.data() ||
        compressed > scratch_end ||
        compressed_size > static_cast<size_t>(scratch_end - compressed)) {
      throw ParquetException("CWI FSST returned an invalid output range");
    }

    size_t position = 0;
    while (position < compressed_size) {
      const uint8_t native_code = compressed[position++];
      if (native_code == kFsstEscape) {
        if (position == compressed_size) {
          throw ParquetException("CWI FSST produced a truncated escape sequence");
        }
        if (output->size() > max_output_size || max_output_size - output->size() < 2) {
          end_offsets->clear();
          output->clear();
          return false;
        }
        output->push_back(kFsstEscape);
        output->push_back(compressed[position++]);
      } else {
        const uint8_t parquet_code = cwi_->cwi_to_parquet_code[native_code];
        if (parquet_code == kFsstEscape) {
          throw ParquetException("CWI FSST emitted an unknown symbol code");
        }
        if (output->size() == max_output_size) {
          end_offsets->clear();
          output->clear();
          return false;
        }
        output->push_back(parquet_code);
      }
    }
    if (output->size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      throw ParquetException("FSST compressed data exceeds INT32_MAX");
    }
    end_offsets->push_back(static_cast<int32_t>(output->size()));
  }
  return true;
}

bool FsstSymbolTable::Compress(std::string_view input, size_t max_output_size,
                               std::vector<uint8_t>* output) const {
  std::vector<int32_t> end_offsets;
  return CompressBatch({std::string(input)}, max_output_size, &end_offsets, output);
}

void FsstSymbolTable::Decompress(const uint8_t* input, int64_t input_size,
                                 std::vector<uint8_t>* output) const {
  output->clear();
  if (input_size < 0 || (input == nullptr && input_size != 0)) {
    throw ParquetException("Invalid FSST compressed value buffer");
  }

  size_t decoded_size = 0;
  int64_t position = 0;
  while (position < input_size) {
    const uint8_t code = input[position++];
    size_t append_size;
    if (code == kFsstEscape) {
      if (position == input_size) {
        throw ParquetException("FSST value ends with a truncated escape");
      }
      ++position;
      append_size = 1;
    } else {
      if (code >= symbols_.size()) {
        throw ParquetException("FSST value references invalid symbol code ", code);
      }
      append_size = symbols_[code].size();
    }
    if (decoded_size >
        static_cast<size_t>(std::numeric_limits<int32_t>::max()) - append_size) {
      throw ParquetException("FSST decoded value exceeds INT32_MAX");
    }
    decoded_size += append_size;
  }

  output->resize(decoded_size);
  const size_t actual_size =
      fsst_decompress(&cwi_->decoder, static_cast<size_t>(input_size), input,
                      output->size(), output->data());
  if (actual_size != decoded_size) {
    throw ParquetException("CWI FSST decompression size mismatch");
  }
}

}  // namespace parquet::internal
