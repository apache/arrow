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

#include "benchmark/benchmark.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow::util {

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE

std::vector<uint8_t> MakeCompressibleData(int data_size) {
  // XXX This isn't a real-world corpus so doesn't really represent the
  // comparative qualities of the algorithms

  // First make highly compressible data
  std::string base_data =
      "Apache Arrow is a cross-language development platform for in-memory data";
  int nrepeats = static_cast<int>(1 + data_size / base_data.size());

  std::vector<uint8_t> data(base_data.size() * nrepeats);
  for (int i = 0; i < nrepeats; ++i) {
    std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
  }
  data.resize(data_size);

  // Then randomly mutate some bytes so as to make things harder
  std::mt19937 engine(42);
  std::exponential_distribution<> offsets(0.05);
  std::uniform_int_distribution<> values(0, 255);

  int64_t pos = 0;
  while (pos < data_size) {
    data[pos] = static_cast<uint8_t>(values(engine));
    pos += static_cast<int64_t>(offsets(engine));
  }

  return data;
}

int64_t StreamingCompress(Codec* codec, const std::vector<uint8_t>& data,
                          std::vector<uint8_t>* compressed_data = nullptr) {
  if (compressed_data != nullptr) {
    compressed_data->clear();
    compressed_data->shrink_to_fit();
  }
  auto compressor = *codec->MakeCompressor();

  const uint8_t* input = data.data();
  int64_t input_len = data.size();
  int64_t compressed_size = 0;

  std::vector<uint8_t> output_buffer(1 << 20);  // 1 MB

  while (input_len > 0) {
    auto result = *compressor->Compress(input_len, input, output_buffer.size(),
                                        output_buffer.data());
    input += result.bytes_read;
    input_len -= result.bytes_read;
    compressed_size += result.bytes_written;
    if (compressed_data != nullptr && result.bytes_written > 0) {
      compressed_data->resize(compressed_data->size() + result.bytes_written);
      memcpy(compressed_data->data() + compressed_data->size() - result.bytes_written,
             output_buffer.data(), result.bytes_written);
    }
    if (result.bytes_read == 0) {
      // Need to enlarge output buffer
      output_buffer.resize(output_buffer.size() * 2);
    }
  }
  while (true) {
    auto result = *compressor->End(output_buffer.size(), output_buffer.data());
    compressed_size += result.bytes_written;
    if (compressed_data != nullptr && result.bytes_written > 0) {
      compressed_data->resize(compressed_data->size() + result.bytes_written);
      memcpy(compressed_data->data() + compressed_data->size() - result.bytes_written,
             output_buffer.data(), result.bytes_written);
    }
    if (result.should_retry) {
      // Need to enlarge output buffer
      output_buffer.resize(output_buffer.size() * 2);
    } else {
      break;
    }
  }
  return compressed_size;
}

static void StreamingCompression(Compression::type compression,
                                 const std::vector<uint8_t>& data,
                                 benchmark::State& state) {  // NOLINT non-const reference
  auto codec = *Codec::Create(compression);

  while (state.KeepRunning()) {
    int64_t compressed_size = StreamingCompress(codec.get(), data);
    state.counters["ratio"] =
        static_cast<double>(data.size()) / static_cast<double>(compressed_size);
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

template <Compression::type COMPRESSION>
static void ReferenceStreamingCompression(
    benchmark::State& state) {                        // NOLINT non-const reference
  auto data = MakeCompressibleData(8 * 1024 * 1024);  // 8 MB

  StreamingCompression(COMPRESSION, data, state);
}

int64_t Compress(Codec* codec, const std::vector<uint8_t>& data,
                 std::vector<uint8_t>* compressed_data) {
  const uint8_t* input = data.data();
  int64_t input_len = data.size();
  int64_t compressed_size = 0;
  int64_t max_compressed_len = codec->MaxCompressedLen(input_len, input);
  compressed_data->resize(max_compressed_len);

  if (input_len > 0) {
    compressed_size = *codec->Compress(input_len, input, compressed_data->size(),
                                       compressed_data->data());
    compressed_data->resize(compressed_size);
  }
  return compressed_size;
}

template <Compression::type COMPRESSION>
static void ReferenceCompression(benchmark::State& state) {  // NOLINT non-const reference
  auto data = MakeCompressibleData(8 * 1024 * 1024);         // 8 MB

  auto codec = *Codec::Create(COMPRESSION);

  while (state.KeepRunning()) {
    std::vector<uint8_t> compressed_data;
    auto compressed_size = Compress(codec.get(), data, &compressed_data);
    state.counters["ratio"] =
        static_cast<double>(data.size()) / static_cast<double>(compressed_size);
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

static void StreamingDecompression(
    Compression::type compression, const std::vector<uint8_t>& data,
    benchmark::State& state) {  // NOLINT non-const reference
  auto codec = *Codec::Create(compression);

  std::vector<uint8_t> compressed_data;
  ARROW_UNUSED(StreamingCompress(codec.get(), data, &compressed_data));
  state.counters["ratio"] =
      static_cast<double>(data.size()) / static_cast<double>(compressed_data.size());

  while (state.KeepRunning()) {
    auto decompressor = *codec->MakeDecompressor();

    const uint8_t* input = compressed_data.data();
    int64_t input_len = compressed_data.size();
    int64_t decompressed_size = 0;

    std::vector<uint8_t> output_buffer(1 << 20);  // 1 MB
    while (!decompressor->IsFinished()) {
      auto result = *decompressor->Decompress(input_len, input, output_buffer.size(),
                                              output_buffer.data());
      input += result.bytes_read;
      input_len -= result.bytes_read;
      decompressed_size += result.bytes_written;
      if (result.need_more_output) {
        // Enlarge output buffer
        output_buffer.resize(output_buffer.size() * 2);
      }
    }
    ARROW_CHECK(decompressed_size == static_cast<int64_t>(data.size()));
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

template <Compression::type COMPRESSION>
static void ReferenceStreamingDecompression(
    benchmark::State& state) {                        // NOLINT non-const reference
  auto data = MakeCompressibleData(8 * 1024 * 1024);  // 8 MB

  StreamingDecompression(COMPRESSION, data, state);
}

template <Compression::type COMPRESSION>
static void ReferenceDecompression(
    benchmark::State& state) {                        // NOLINT non-const reference
  auto data = MakeCompressibleData(8 * 1024 * 1024);  // 8 MB

  auto codec = *Codec::Create(COMPRESSION);

  std::vector<uint8_t> compressed_data;
  ARROW_UNUSED(Compress(codec.get(), data, &compressed_data));
  state.counters["ratio"] =
      static_cast<double>(data.size()) / static_cast<double>(compressed_data.size());

  std::vector<uint8_t> decompressed_data(data);
  while (state.KeepRunning()) {
    auto result = codec->Decompress(compressed_data.size(), compressed_data.data(),
                                    decompressed_data.size(), decompressed_data.data());
    ARROW_CHECK(result.ok());
    ARROW_CHECK(*result == static_cast<int64_t>(decompressed_data.size()));
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

#ifdef ARROW_WITH_ZLIB
BENCHMARK_TEMPLATE(ReferenceStreamingCompression, Compression::GZIP);
BENCHMARK_TEMPLATE(ReferenceCompression, Compression::GZIP);
BENCHMARK_TEMPLATE(ReferenceStreamingDecompression, Compression::GZIP);
BENCHMARK_TEMPLATE(ReferenceDecompression, Compression::GZIP);
#endif

#ifdef ARROW_WITH_BROTLI
BENCHMARK_TEMPLATE(ReferenceStreamingCompression, Compression::BROTLI);
BENCHMARK_TEMPLATE(ReferenceCompression, Compression::BROTLI);
BENCHMARK_TEMPLATE(ReferenceStreamingDecompression, Compression::BROTLI);
BENCHMARK_TEMPLATE(ReferenceDecompression, Compression::BROTLI);
#endif

#ifdef ARROW_WITH_ZSTD
BENCHMARK_TEMPLATE(ReferenceStreamingCompression, Compression::ZSTD);
BENCHMARK_TEMPLATE(ReferenceCompression, Compression::ZSTD);
BENCHMARK_TEMPLATE(ReferenceStreamingDecompression, Compression::ZSTD);
BENCHMARK_TEMPLATE(ReferenceDecompression, Compression::ZSTD);
#endif

#ifdef ARROW_WITH_LZ4
BENCHMARK_TEMPLATE(ReferenceStreamingCompression, Compression::LZ4_FRAME);
BENCHMARK_TEMPLATE(ReferenceCompression, Compression::LZ4_FRAME);
BENCHMARK_TEMPLATE(ReferenceStreamingDecompression, Compression::LZ4_FRAME);
BENCHMARK_TEMPLATE(ReferenceDecompression, Compression::LZ4_FRAME);

BENCHMARK_TEMPLATE(ReferenceCompression, Compression::LZ4);
BENCHMARK_TEMPLATE(ReferenceDecompression, Compression::LZ4);
#endif

#ifdef ARROW_WITH_SNAPPY
BENCHMARK_TEMPLATE(ReferenceCompression, Compression::SNAPPY);
BENCHMARK_TEMPLATE(ReferenceDecompression, Compression::SNAPPY);
#endif

#endif

}  // namespace arrow::util
