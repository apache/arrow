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

// Comparison benchmark: PFOR vs DeltaBitPack vs ZSTD vs RleBitPackHybrid
//                       vs ByteStreamSplit+ZSTD vs ByteStreamSplit+LZ4
//
// All throughput is reported as uncompressed_size / time (MB/s).
// Data generators mimic ClickBench and TPC-DS column distributions.

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/util/compression.h"
#include "arrow/util/fastlanes/fastlanes_for.h"
#include "arrow/util/logging.h"
#include "arrow/util/pfor/pfor_wrapper.h"
#include "arrow/util/rle_encoding_internal.h"

#include "parquet/encoding.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using ::arrow::Compression;
using ::arrow::util::Codec;

namespace parquet {
namespace {

// ============================================================================
// Data Generators — ClickBench-inspired
// ============================================================================

using Gen32 = std::vector<int32_t> (*)(int64_t);

std::vector<int32_t> GenClientIP(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(101);
  std::uniform_int_distribution<uint32_t> dist(0x0A000000, 0xDFFFFFFF);
  for (auto& x : v) x = static_cast<int32_t>(dist(rng));
  return v;
}

std::vector<int32_t> GenUrlRegionID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(102);
  // Zipf-like over ~1000 values
  std::uniform_real_distribution<double> uni(0.0, 1.0);
  for (auto& x : v) {
    double u = uni(rng);
    x = static_cast<int32_t>(std::pow(u, 2.0) * 1000) + 1;
  }
  return v;
}

std::vector<int32_t> GenCounterID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(103);
  std::uniform_int_distribution<int32_t> jitter(0, 3);
  int32_t counter = 100000;
  for (auto& x : v) {
    counter += 1 + jitter(rng);
    x = counter;
  }
  return v;
}

std::vector<int32_t> GenEventDate(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(104);
  const int32_t dates[] = {19691, 19692, 19693, 19694, 19695};
  std::uniform_int_distribution<int> idx(0, 4);
  for (auto& x : v) x = dates[idx(rng)];
  return v;
}

std::vector<int32_t> GenEventTime(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(105);
  const int32_t base = 1704067200;  // 2024-01-01
  std::uniform_int_distribution<int32_t> offset(0, 86399);
  for (auto& x : v) x = base + offset(rng);
  return v;
}

std::vector<int32_t> GenGoodEvent(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(106);
  std::uniform_int_distribution<int> dist(0, 99);
  for (auto& x : v) x = (dist(rng) < 95) ? 1 : 0;
  return v;
}

std::vector<int32_t> GenHID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(107);
  std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                              std::numeric_limits<int32_t>::max());
  for (auto& x : v) x = dist(rng);
  return v;
}

std::vector<int32_t> GenHitColor(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(108);
  const int32_t colors[] = {1, 2, 3, 4, 5};
  std::uniform_int_distribution<int> idx(0, 4);
  for (auto& x : v) x = colors[idx(rng)];
  return v;
}

std::vector<int32_t> GenIPNetworkID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(109);
  std::uniform_int_distribution<int32_t> dist(1, 10000);
  for (auto& x : v) x = dist(rng);
  return v;
}

std::vector<int32_t> GenJavaEnable(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(110);
  std::uniform_int_distribution<int> dist(0, 99);
  for (auto& x : v) x = (dist(rng) < 85) ? 1 : 0;
  return v;
}

std::vector<int32_t> GenOS(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(111);
  std::uniform_int_distribution<int32_t> dist(1, 20);
  for (auto& x : v) x = dist(rng);
  return v;
}

std::vector<int32_t> GenResolution(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(112);
  const int32_t resolutions[] = {360,  480,  600,  720,  768,  800,  900,
                                 1024, 1050, 1080, 1200, 1440, 1600, 2160};
  std::uniform_int_distribution<int> idx(0, 13);
  for (auto& x : v) x = resolutions[idx(rng)];
  return v;
}

std::vector<int32_t> GenTrafficSourceID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(113);
  std::uniform_int_distribution<int32_t> dist(0, 10);
  for (auto& x : v) x = dist(rng);
  return v;
}

std::vector<int32_t> GenUserAgent(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(114);
  // Zipf-like over ~100 user agents
  std::uniform_real_distribution<double> uni(0.0, 1.0);
  for (auto& x : v) {
    double u = uni(rng);
    x = static_cast<int32_t>(std::pow(u, 1.5) * 100) + 1;
  }
  return v;
}

// ============================================================================
// Data Generators — TPC-DS (4 most queried columns from store_sales)
// ============================================================================

std::vector<int32_t> GenTpcdsSoldDateSk(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 2450815;
  std::mt19937 rng(201);
  std::uniform_int_distribution<int32_t> dist(0, 1820);
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

std::vector<int32_t> GenTpcdsStoreSk(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(202);
  std::uniform_int_distribution<int32_t> dist(1, 1000);
  for (auto& x : v) x = dist(rng);
  return v;
}

std::vector<int32_t> GenTpcdsItemSk(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kMax = 100000;
  std::mt19937 rng(203);
  std::exponential_distribution<double> exp_dist(0.00005);
  for (auto& x : v) {
    int32_t val = static_cast<int32_t>(exp_dist(rng));
    x = std::min(val + 1, kMax);
  }
  return v;
}

std::vector<int32_t> GenTpcdsQuantity(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(204);
  std::uniform_int_distribution<int32_t> small_dist(1, 10);
  std::uniform_int_distribution<int32_t> large_dist(11, 100);
  std::uniform_int_distribution<int> chance(0, 99);
  for (auto& x : v) {
    x = (chance(rng) < 90) ? small_dist(rng) : large_dist(rng);
  }
  return v;
}

// ============================================================================
// Helpers
// ============================================================================

static int32_t ComputeBitWidth(const std::vector<int32_t>& values) {
  uint32_t max_val = 0;
  for (int32_t v : values) {
    max_val = std::max(max_val, static_cast<uint32_t>(v));
  }
  if (max_val == 0) return 1;
  return static_cast<int32_t>(32 - __builtin_clz(max_val));
}

static std::shared_ptr<ColumnDescriptor> MakeInt32Descriptor() {
  auto node =
      schema::PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32);
  return std::make_shared<ColumnDescriptor>(node, /*max_def_level=*/0,
                                            /*max_rep_level=*/0);
}

// ============================================================================
// PFOR Encode/Decode
// ============================================================================

static void BM_PforEncode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int64_t max_size =
      ::arrow::util::pfor::PforWrapper<int32_t>::GetMaxCompressedSize(
          static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);

  // Compute comp_size once for the counter
  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(), &comp_size);

  for (auto _ : state) {
    int64_t sz = max_size;
    ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
        values.data(), static_cast<int32_t>(num_values), compressed.data(), &sz);
    benchmark::DoNotOptimize(sz);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_PforDecode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int64_t max_size =
      ::arrow::util::pfor::PforWrapper<int32_t>::GetMaxCompressedSize(
          static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(),
      &comp_size);

  std::vector<int32_t> decoded(num_values);
  for (auto _ : state) {
    auto status = ::arrow::util::pfor::PforWrapper<int32_t>::Decode(
        decoded.data(), static_cast<int32_t>(num_values), compressed.data(),
        comp_size);
    ARROW_CHECK_OK(status);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// PFOR with FastLanes-mode bit packing (per-vector flag in PFOR header)
// ============================================================================
//
// Same PFOR pipeline (FOR + exceptions) but the bit-packing payload uses
// FastLanes lane-interleaved layout instead of the legacy sequential bit
// stream. Output is still flat (PFOR contract preserved); the decoder
// scatters via FL_ORDER inside DecodeVector.

static void BM_PforFastLanesEncode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int64_t max_size =
      ::arrow::util::pfor::PforWrapper<int32_t>::GetMaxCompressedSize(
          static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);

  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(), &comp_size,
      ::arrow::util::pfor::PackingMode::FastLanes);

  for (auto _ : state) {
    int64_t sz = max_size;
    ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
        values.data(), static_cast<int32_t>(num_values), compressed.data(), &sz,
        ::arrow::util::pfor::PackingMode::FastLanes);
    benchmark::DoNotOptimize(sz);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_PforFastLanesDecode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int64_t max_size =
      ::arrow::util::pfor::PforWrapper<int32_t>::GetMaxCompressedSize(
          static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(),
      &comp_size, ::arrow::util::pfor::PackingMode::FastLanes);

  std::vector<int32_t> decoded(num_values);
  for (auto _ : state) {
    auto status = ::arrow::util::pfor::PforWrapper<int32_t>::Decode(
        decoded.data(), static_cast<int32_t>(num_values), compressed.data(),
        comp_size);
    ARROW_CHECK_OK(status);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// PFOR with FastLanes bit-packing, decoded in TRANSPOSED order: skips the
// per-vector FL_ORDER gather, output is in FastLanes stream order. The
// downstream consumer must be permutation-aware. Apples-to-apples this
// against BM_PforFastLanesDecode (flat-order, with the scatter cost) to see
// how much of the gap to pfor+bitpack closes.
static void BM_PforFastLanesDecodeTransposed(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int64_t max_size =
      ::arrow::util::pfor::PforWrapper<int32_t>::GetMaxCompressedSize(
          static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<int32_t>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(),
      &comp_size, ::arrow::util::pfor::PackingMode::FastLanes);

  std::vector<int32_t> decoded(num_values);
  for (auto _ : state) {
    auto status = ::arrow::util::pfor::PforWrapper<int32_t>::Decode(
        decoded.data(), static_cast<int32_t>(num_values), compressed.data(),
        comp_size, ::arrow::util::pfor::OutputOrder::Transposed);
    ARROW_CHECK_OK(status);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// DeltaBitPack Encode/Decode
// ============================================================================

static void BM_DeltaBitPackEncode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::DELTA_BINARY_PACKED);

  // Compute comp_size once for the counter
  encoder->Put(values.data(), static_cast<int>(num_values));
  auto pre_buf = encoder->FlushValues();
  int64_t comp_size = pre_buf->size();

  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(num_values));
    auto buf = encoder->FlushValues();
    benchmark::DoNotOptimize(buf);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_DeltaBitPackDecode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::DELTA_BINARY_PACKED);
  encoder->Put(values.data(), static_cast<int>(num_values));
  auto buf = encoder->FlushValues();
  int64_t comp_size = buf->size();

  std::vector<int32_t> decoded(num_values);
  auto decoder = MakeTypedDecoder<Int32Type>(Encoding::DELTA_BINARY_PACKED);

  for (auto _ : state) {
    decoder->SetData(static_cast<int>(num_values), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(decoded.data(), static_cast<int>(num_values));
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// FastLanes + Frame-of-Reference Encode/Decode
// ============================================================================
//
// Round-trip is NOT flat: decoder produces output in transposed FL_ORDER
// (per 1024-block, output[t] == input[fromTransposed32(t)] + min). Chunked
// at 2048 values; per-chunk header stores [min, bit_width].

static void BM_FastLanesEncode(benchmark::State& state, Gen32 gen) {
  using ::arrow::util::fastlanes::FastLanesForCodec;
  // Round num_values up to a multiple of kChunkSize (2048) for this codec.
  int64_t num_values = state.range(0);
  num_values -= num_values % FastLanesForCodec::kChunkSize;
  if (num_values == 0) num_values = FastLanesForCodec::kChunkSize;

  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  std::vector<uint8_t> compressed(FastLanesForCodec::MaxEncodedSize(num_values));

  auto first = FastLanesForCodec::Encode(values.data(), num_values, compressed.data());
  ARROW_CHECK_OK(first.status());
  const int64_t comp_size = *first;

  for (auto _ : state) {
    auto sz = FastLanesForCodec::Encode(values.data(), num_values, compressed.data());
    benchmark::DoNotOptimize(sz);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_FastLanesDecode(benchmark::State& state, Gen32 gen) {
  using ::arrow::util::fastlanes::FastLanesForCodec;
  int64_t num_values = state.range(0);
  num_values -= num_values % FastLanesForCodec::kChunkSize;
  if (num_values == 0) num_values = FastLanesForCodec::kChunkSize;

  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  std::vector<uint8_t> compressed(FastLanesForCodec::MaxEncodedSize(num_values));
  auto comp = FastLanesForCodec::Encode(values.data(), num_values, compressed.data());
  ARROW_CHECK_OK(comp.status());
  const int64_t comp_size = *comp;

  std::vector<int32_t> decoded(num_values);
  for (auto _ : state) {
    auto status = FastLanesForCodec::Decode(decoded.data(), num_values,
                                             compressed.data(), comp_size);
    ARROW_CHECK_OK(status);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// Decode + FL_ORDER scatter: produces flat output in original input
// order (apples-to-apples vs PFOR/DeltaBitPack which also produce flat).
static void BM_FastLanesDecodeFlat(benchmark::State& state, Gen32 gen) {
  using ::arrow::util::fastlanes::FastLanesForCodec;
  int64_t num_values = state.range(0);
  num_values -= num_values % FastLanesForCodec::kChunkSize;
  if (num_values == 0) num_values = FastLanesForCodec::kChunkSize;

  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  std::vector<uint8_t> compressed(FastLanesForCodec::MaxEncodedSize(num_values));
  auto comp = FastLanesForCodec::Encode(values.data(), num_values, compressed.data());
  ARROW_CHECK_OK(comp.status());
  const int64_t comp_size = *comp;

  std::vector<int32_t> decoded(num_values);
  for (auto _ : state) {
    auto status = FastLanesForCodec::DecodeFlat(decoded.data(), num_values,
                                                 compressed.data(), comp_size);
    ARROW_CHECK_OK(status);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// Plain + ZSTD Encode/Decode
// ============================================================================

static void BM_PlainZstdEncode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(values.data());

  auto codec = *Codec::Create(Compression::ZSTD);
  int64_t max_comp = codec->MaxCompressedLen(uncompressed_size, raw);
  std::vector<uint8_t> compressed(max_comp);

  // Compute comp_size once for the counter
  int64_t comp_size =
      *codec->Compress(uncompressed_size, raw, max_comp, compressed.data());

  for (auto _ : state) {
    auto sz = *codec->Compress(uncompressed_size, raw, max_comp, compressed.data());
    benchmark::DoNotOptimize(sz);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_PlainZstdDecode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(values.data());

  auto codec = *Codec::Create(Compression::ZSTD);
  int64_t max_comp = codec->MaxCompressedLen(uncompressed_size, raw);
  std::vector<uint8_t> compressed(max_comp);
  int64_t comp_size =
      *codec->Compress(uncompressed_size, raw, max_comp, compressed.data());

  std::vector<uint8_t> decompressed(uncompressed_size);
  for (auto _ : state) {
    auto result = codec->Decompress(comp_size, compressed.data(), uncompressed_size,
                                    decompressed.data());
    ARROW_CHECK_OK(result.status());
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// Plain + LZ4 Encode/Decode
// ============================================================================

static void BM_PlainLz4Encode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(values.data());

  auto codec = *Codec::Create(Compression::LZ4_FRAME);
  int64_t max_comp = codec->MaxCompressedLen(uncompressed_size, raw);
  std::vector<uint8_t> compressed(max_comp);

  int64_t comp_size =
      *codec->Compress(uncompressed_size, raw, max_comp, compressed.data());

  for (auto _ : state) {
    auto sz = *codec->Compress(uncompressed_size, raw, max_comp, compressed.data());
    benchmark::DoNotOptimize(sz);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_PlainLz4Decode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(values.data());

  auto codec = *Codec::Create(Compression::LZ4_FRAME);
  int64_t max_comp = codec->MaxCompressedLen(uncompressed_size, raw);
  std::vector<uint8_t> compressed(max_comp);
  int64_t comp_size =
      *codec->Compress(uncompressed_size, raw, max_comp, compressed.data());

  std::vector<uint8_t> decompressed(uncompressed_size);
  for (auto _ : state) {
    auto result = codec->Decompress(comp_size, compressed.data(), uncompressed_size,
                                    decompressed.data());
    ARROW_CHECK_OK(result.status());
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// RleBitPackHybrid Encode/Decode
// ============================================================================

static void BM_RleBitPackEncode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int32_t bit_width = ComputeBitWidth(values);
  int64_t max_buf =
      ::arrow::util::RleBitPackedEncoder::MaxBufferSize(bit_width, num_values) +
      ::arrow::util::RleBitPackedEncoder::MinBufferSize(bit_width);
  std::vector<uint8_t> buffer(max_buf);

  // Compute comp_size once for the counter
  int64_t comp_size;
  {
    ::arrow::util::RleBitPackedEncoder enc(buffer.data(),
                                           static_cast<int>(max_buf), bit_width);
    for (int64_t i = 0; i < num_values; ++i) {
      enc.Put(static_cast<uint64_t>(static_cast<uint32_t>(values[i])));
    }
    comp_size = enc.Flush();
  }

  for (auto _ : state) {
    ::arrow::util::RleBitPackedEncoder encoder(buffer.data(),
                                             static_cast<int>(max_buf), bit_width);
    for (int64_t i = 0; i < num_values; ++i) {
      encoder.Put(static_cast<uint64_t>(static_cast<uint32_t>(values[i])));
    }
    auto sz = encoder.Flush();
    benchmark::DoNotOptimize(sz);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_RleBitPackDecode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int32_t bit_width = ComputeBitWidth(values);
  int64_t max_buf =
      ::arrow::util::RleBitPackedEncoder::MaxBufferSize(bit_width, num_values) +
      ::arrow::util::RleBitPackedEncoder::MinBufferSize(bit_width);
  std::vector<uint8_t> buffer(max_buf);

  ::arrow::util::RleBitPackedEncoder encoder(buffer.data(),
                                           static_cast<int>(max_buf), bit_width);
  for (int64_t i = 0; i < num_values; ++i) {
    encoder.Put(static_cast<uint64_t>(static_cast<uint32_t>(values[i])));
  }
  int comp_size = encoder.Flush();

  std::vector<int32_t> decoded(num_values);
  for (auto _ : state) {
    ::arrow::util::RleBitPackedParser parser(buffer.data(), comp_size, bit_width);
    int64_t out_idx = 0;
    struct Handler {
      int32_t* output;
      int64_t* idx;
      int64_t max_values;
      int32_t bw;
      ::arrow::util::RleBitPackedParser::ControlFlow OnRleRun(
          ::arrow::util::RleRun run) {
        ::arrow::util::RleRunDecoder<int32_t> dec(run, bw);
        auto want = static_cast<int32_t>(
            std::min(static_cast<int64_t>(run.values_count()), max_values - *idx));
        auto count = dec.GetBatch(output + *idx, want, bw);
        *idx += count;
        return *idx >= max_values
                   ? ::arrow::util::RleBitPackedParser::ControlFlow::Break
                   : ::arrow::util::RleBitPackedParser::ControlFlow::Continue;
      }
      ::arrow::util::RleBitPackedParser::ControlFlow OnBitPackedRun(
          ::arrow::util::BitPackedRun run) {
        ::arrow::util::BitPackedRunDecoder<int32_t> dec(run, bw);
        auto want = static_cast<int32_t>(
            std::min(static_cast<int64_t>(dec.remaining()), max_values - *idx));
        auto count = dec.GetBatch(output + *idx, want, bw);
        *idx += count;
        return *idx >= max_values
                   ? ::arrow::util::RleBitPackedParser::ControlFlow::Break
                   : ::arrow::util::RleBitPackedParser::ControlFlow::Continue;
      }
    };
    Handler handler{decoded.data(), &out_idx, num_values, bit_width};
    parser.Parse(handler);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// ByteStreamSplit + Codec (ZSTD or LZ4)
// ============================================================================

static void BM_BssCodecEncode(benchmark::State& state, Gen32 gen,
                              Compression::type codec_type) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  auto descr = MakeInt32Descriptor();
  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::BYTE_STREAM_SPLIT,
                                            /*use_dictionary=*/false, descr.get());
  auto codec = *Codec::Create(codec_type);

  encoder->Put(values.data(), static_cast<int>(num_values));
  auto encoded_buf = encoder->FlushValues();
  int64_t encoded_size = encoded_buf->size();

  int64_t max_comp = codec->MaxCompressedLen(encoded_size, encoded_buf->data());
  std::vector<uint8_t> compressed(max_comp);

  // Compute comp_size once for the counter
  int64_t comp_size =
      *codec->Compress(encoded_size, encoded_buf->data(), max_comp, compressed.data());

  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(num_values));
    auto buf = encoder->FlushValues();
    auto sz =
        *codec->Compress(buf->size(), buf->data(), max_comp, compressed.data());
    benchmark::DoNotOptimize(sz);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_BssCodecDecode(benchmark::State& state, Gen32 gen,
                              Compression::type codec_type) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  auto descr = MakeInt32Descriptor();
  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::BYTE_STREAM_SPLIT,
                                            /*use_dictionary=*/false, descr.get());
  auto codec = *Codec::Create(codec_type);

  encoder->Put(values.data(), static_cast<int>(num_values));
  auto encoded_buf = encoder->FlushValues();
  int64_t encoded_size = encoded_buf->size();

  int64_t max_comp = codec->MaxCompressedLen(encoded_size, encoded_buf->data());
  std::vector<uint8_t> compressed(max_comp);
  int64_t comp_size =
      *codec->Compress(encoded_size, encoded_buf->data(), max_comp, compressed.data());

  std::vector<uint8_t> decompressed(encoded_size);
  std::vector<int32_t> decoded(num_values);
  auto decoder = MakeTypedDecoder<Int32Type>(Encoding::BYTE_STREAM_SPLIT, descr.get());

  for (auto _ : state) {
    auto result = codec->Decompress(comp_size, compressed.data(), encoded_size,
                                    decompressed.data());
    ARROW_CHECK_OK(result.status());
    decoder->SetData(static_cast<int>(num_values), decompressed.data(),
                     static_cast<int>(encoded_size));
    decoder->Decode(decoded.data(), static_cast<int>(num_values));
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// Wrappers for BSS+ZSTD
static void BM_BssZstdEncode(benchmark::State& state, Gen32 gen) {
  BM_BssCodecEncode(state, gen, Compression::ZSTD);
}
static void BM_BssZstdDecode(benchmark::State& state, Gen32 gen) {
  BM_BssCodecDecode(state, gen, Compression::ZSTD);
}

// Wrappers for BSS+LZ4
static void BM_BssLz4Encode(benchmark::State& state, Gen32 gen) {
  BM_BssCodecEncode(state, gen, Compression::LZ4_FRAME);
}
static void BM_BssLz4Decode(benchmark::State& state, Gen32 gen) {
  BM_BssCodecDecode(state, gen, Compression::LZ4_FRAME);
}

// ============================================================================
// Benchmark Registration
// ============================================================================

static void CustomArgs(benchmark::internal::Benchmark* b) { b->Arg(102400); }

// Macro to register all algorithms for a given dataset
#define REGISTER_DATASET(Name, GenFunc)                                          \
  BENCHMARK_CAPTURE(BM_PforEncode, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_PforDecode, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_PforFastLanesEncode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_PforFastLanesDecode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_PforFastLanesDecodeTransposed, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_DeltaBitPackEncode, Name, &GenFunc)->Apply(CustomArgs);  \
  BENCHMARK_CAPTURE(BM_DeltaBitPackDecode, Name, &GenFunc)->Apply(CustomArgs);  \
  BENCHMARK_CAPTURE(BM_FastLanesEncode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_FastLanesDecode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_FastLanesDecodeFlat, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_PlainZstdEncode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_PlainZstdDecode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_PlainLz4Encode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_PlainLz4Decode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_RleBitPackEncode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_RleBitPackDecode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_BssZstdEncode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_BssZstdDecode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_BssLz4Encode, Name, &GenFunc)->Apply(CustomArgs);        \
  BENCHMARK_CAPTURE(BM_BssLz4Decode, Name, &GenFunc)->Apply(CustomArgs);

// ClickBench datasets
REGISTER_DATASET(ClientIP, GenClientIP)
REGISTER_DATASET(UrlRegionID, GenUrlRegionID)
REGISTER_DATASET(CounterID, GenCounterID)
REGISTER_DATASET(EventDate, GenEventDate)
REGISTER_DATASET(EventTime, GenEventTime)
REGISTER_DATASET(GoodEvent, GenGoodEvent)
REGISTER_DATASET(HID, GenHID)
REGISTER_DATASET(HitColor, GenHitColor)
REGISTER_DATASET(IPNetworkID, GenIPNetworkID)
REGISTER_DATASET(JavaEnable, GenJavaEnable)
REGISTER_DATASET(OS, GenOS)
REGISTER_DATASET(Resolution, GenResolution)
REGISTER_DATASET(TrafficSourceID, GenTrafficSourceID)
REGISTER_DATASET(UserAgent, GenUserAgent)

// TPC-DS datasets
REGISTER_DATASET(TpcdsSoldDateSk, GenTpcdsSoldDateSk)
REGISTER_DATASET(TpcdsStoreSk, GenTpcdsStoreSk)
REGISTER_DATASET(TpcdsItemSk, GenTpcdsItemSk)
REGISTER_DATASET(TpcdsQuantity, GenTpcdsQuantity)

}  // namespace
}  // namespace parquet
