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
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/util/compression.h"
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

// Generator pointer type, parameterized on the column value type.
template <typename T>
using GenT = std::vector<T> (*)(int64_t);
using Gen32 = GenT<int32_t>;
using Gen64 = GenT<int64_t>;

// Map the C++ value type to its Parquet physical type + descriptor type.
template <typename T>
struct PqTraits;
template <>
struct PqTraits<int32_t> {
  using PType = Int32Type;
  static constexpr Type::type kPhysicalType = Type::INT32;
};
template <>
struct PqTraits<int64_t> {
  using PType = Int64Type;
  static constexpr Type::type kPhysicalType = Type::INT64;
};

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

// --- TPC-H (lineitem) top-queried numeric columns (Q1/Q3/Q5/Q6) ------------
// l_quantity: integer [1, 50], uniform. Small range, min 1.
std::vector<int32_t> GenTpchLQuantity(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(301);
  std::uniform_int_distribution<int32_t> dist(1, 50);
  for (auto& x : v) x = dist(rng);
  return v;
}

// l_extendedprice (cents): l_quantity * p_retailprice. p_retailprice spans
// ~$900.00..$2099.00, so cents in [90000, 10495000]. Wide range, nonzero min.
std::vector<int32_t> GenTpchLExtendedPrice(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(302);
  std::uniform_int_distribution<int32_t> qty(1, 50);
  std::uniform_int_distribution<int32_t> retail_cents(90000, 209900);
  for (auto& x : v) x = qty(rng) * retail_cents(rng);
  return v;
}

// l_discount (x100): integer [0, 10] i.e. 0.00..0.10. Genuinely includes 0
// (0% discount is a real value), so this one legitimately starts at 0.
std::vector<int32_t> GenTpchLDiscount(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(303);
  std::uniform_int_distribution<int32_t> dist(0, 10);
  for (auto& x : v) x = dist(rng);
  return v;
}

// l_shipdate (days since 1970-01-01): 1992-01-01..1998-12 span. Base 8036,
// range ~7 years. Large nonzero min -> exercises frame-of-reference.
std::vector<int32_t> GenTpchLShipDate(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 8036;  // days since epoch for 1992-01-01
  std::mt19937 rng(304);
  std::uniform_int_distribution<int32_t> dist(0, 2557);
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

// --- TPC-DS (store_sales / date_dim) further top-queried numeric columns ---
// ss_customer_sk: surrogate key, uniform [1, 2,000,000]. Big range, min 1.
std::vector<int32_t> GenTpcdsCustomerSk(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(311);
  std::uniform_int_distribution<int32_t> dist(1, 2000000);
  for (auto& x : v) x = dist(rng);
  return v;
}

// ss_ext_sales_price (cents): skewed price, exponential mean ~$50, floored at
// $1.00 (a sale has a nonzero price), capped at $20,000. Long tail -> patches.
std::vector<int32_t> GenTpcdsExtSalesPrice(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kMin = 100, kMax = 2000000;
  std::mt19937 rng(312);
  std::exponential_distribution<double> exp_dist(1.0 / 5000.0);
  for (auto& x : v) {
    int32_t val = kMin + static_cast<int32_t>(exp_dist(rng));
    x = std::min(val, kMax);
  }
  return v;
}

// ss_net_profit (cents): usually a small profit, sometimes a loss -> negative
// values, so the frame of reference is negative (not zero).
std::vector<int32_t> GenTpcdsNetProfit(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(313);
  std::uniform_int_distribution<int32_t> dist(-10000, 300000);
  for (auto& x : v) x = dist(rng);
  return v;
}

// d_year: queried date_dim year range [1998, 2003]. Low cardinality, min 1998.
std::vector<int32_t> GenTpcdsDYear(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(314);
  std::uniform_int_distribution<int32_t> dist(1998, 2003);
  for (auto& x : v) x = dist(rng);
  return v;
}

// --- NYC yellow-taxi trip numeric columns ----------------------------------
// pickup timestamp (unix seconds): 2015-01, base 1,420,070,400 + ~31 days.
// Very large min -> frame-of-reference is essential.
std::vector<int32_t> GenTaxiPickupUnixTime(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 1420070400;  // 2015-01-01 UTC
  std::mt19937 rng(321);
  std::uniform_int_distribution<int32_t> dist(0, 2678400);  // ~31 days
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

// trip_distance (x100 miles): exponential mean ~1.8 mi, floored at 0.10 mi
// (no zero-distance trips), capped at 100 mi. Long tail -> patches.
std::vector<int32_t> GenTaxiTripDistanceX100(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kMin = 10, kMax = 10000;
  std::mt19937 rng(322);
  std::exponential_distribution<double> exp_dist(1.0 / 180.0);
  for (auto& x : v) {
    int32_t val = kMin + static_cast<int32_t>(exp_dist(rng));
    x = std::min(val, kMax);
  }
  return v;
}

// fare_amount (cents): $2.50 base + exponential mean ~$10, capped at $150.
// Nonzero floor at the base fare, skewed with a long tail.
std::vector<int32_t> GenTaxiFareCents(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 250, kMax = 15000;
  std::mt19937 rng(323);
  std::exponential_distribution<double> exp_dist(1.0 / 1000.0);
  for (auto& x : v) {
    int32_t val = kBase + static_cast<int32_t>(exp_dist(rng));
    x = std::min(val, kMax);
  }
  return v;
}

// ============================================================================
// Data Generators — int64 / BIGINT columns (values that require 8 bytes,
// i.e. exceed the int32 range). Covers the common 64-bit analytic cases:
// nanosecond timestamps, large surrogate keys, scaled-decimal money, monotone
// IDs, and wide counters.
// ============================================================================

// Nanosecond epoch timestamp (Parquet TIMESTAMP(NANOS)): 2024-01-01 base plus
// up to ~1 day of jitter. Min ~1.70e18 -> very large frame of reference; raw
// values need ~61 bits.
std::vector<int64_t> GenTsNanos(int64_t n) {
  std::vector<int64_t> v(n);
  const int64_t kBase = 1704067200000000000LL;  // 2024-01-01T00:00:00Z in ns
  std::mt19937_64 rng(401);
  std::uniform_int_distribution<int64_t> off(0, 86399999999999LL);  // ~1 day
  for (auto& x : v) x = kBase + off(rng);
  return v;
}

// BIGINT surrogate / order key, uniform over [1, 10 billion] (exceeds 2^32).
std::vector<int64_t> GenOrderKey(int64_t n) {
  std::vector<int64_t> v(n);
  std::mt19937_64 rng(402);
  std::uniform_int_distribution<int64_t> dist(1, 10000000000LL);
  for (auto& x : v) x = dist(rng);
  return v;
}

// Money as int64 scaled decimal (micro-units): $0.01 .. ~$100k, skewed, with a
// nonzero floor at one cent.
std::vector<int64_t> GenPriceMicros(int64_t n) {
  std::vector<int64_t> v(n);
  const int64_t kMin = 10000, kMax = 100000000000LL;  // $0.01 .. $100,000
  std::mt19937_64 rng(403);
  std::exponential_distribution<double> exp_dist(1.0 / 5000000.0);  // mean ~$5
  for (auto& x : v) {
    int64_t val = kMin + static_cast<int64_t>(exp_dist(rng));
    x = std::min(val, kMax);
  }
  return v;
}

// Snowflake-style monotone ID: large base + jittered per-row increments.
// Monotone -> ideal for DELTA_BINARY_PACKED; huge min -> ideal for PFOR's FOR.
std::vector<int64_t> GenSnowflakeId(int64_t n) {
  std::vector<int64_t> v(n);
  std::mt19937_64 rng(404);
  std::uniform_int_distribution<int64_t> step(1, 4096);
  int64_t id = 1500000000000000000LL;
  for (auto& x : v) {
    id += step(rng);
    x = id;
  }
  return v;
}

// Wide cumulative byte counts, uniform [1024, 5 trillion] (wide 64-bit range).
std::vector<int64_t> GenByteCount(int64_t n) {
  std::vector<int64_t> v(n);
  std::mt19937_64 rng(405);
  std::uniform_int_distribution<int64_t> dist(1024, 5000000000000LL);
  for (auto& x : v) x = dist(rng);
  return v;
}

// ============================================================================
// Helpers
// ============================================================================

template <typename T>
static int32_t ComputeBitWidth(const std::vector<T>& values) {
  using U = std::make_unsigned_t<T>;
  U max_val = 0;
  for (T v : values) {
    max_val = std::max(max_val, static_cast<U>(v));
  }
  if (max_val == 0) return 1;
  if constexpr (sizeof(T) == 8) {
    return static_cast<int32_t>(std::bit_width(static_cast<uint64_t>(max_val)));
  } else {
    return static_cast<int32_t>(std::bit_width(static_cast<uint32_t>(max_val)));
  }
}

template <typename T>
static std::shared_ptr<ColumnDescriptor> MakeDescriptor() {
  auto node = schema::PrimitiveNode::Make("col", Repetition::REQUIRED,
                                          PqTraits<T>::kPhysicalType);
  return std::make_shared<ColumnDescriptor>(node, /*max_def_level=*/0,
                                            /*max_rep_level=*/0);
}

// ============================================================================
// PFOR Encode/Decode
// ============================================================================

template <typename T>
static void PforEncodeImpl(benchmark::State& state, GenT<T> gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  int64_t max_size = ::arrow::util::pfor::PforWrapper<T>::GetMaxCompressedSize(
      static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);

  // Compute comp_size once for the counter
  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<T>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(), &comp_size);

  for (auto _ : state) {
    int64_t sz = max_size;
    ::arrow::util::pfor::PforWrapper<T>::Encode(
        values.data(), static_cast<int32_t>(num_values), compressed.data(), &sz);
    benchmark::DoNotOptimize(sz);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}
static void BM_PforEncode(benchmark::State& state, Gen32 gen) {
  PforEncodeImpl<int32_t>(state, gen);
}
static void BM_Pfor64Encode(benchmark::State& state, Gen64 gen) {
  PforEncodeImpl<int64_t>(state, gen);
}

template <typename T>
static void PforDecodeImpl(benchmark::State& state, GenT<T> gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  int64_t max_size = ::arrow::util::pfor::PforWrapper<T>::GetMaxCompressedSize(
      static_cast<int32_t>(num_values));
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;
  ::arrow::util::pfor::PforWrapper<T>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(), &comp_size);

  std::vector<T> decoded(num_values);
  for (auto _ : state) {
    auto status = ::arrow::util::pfor::PforWrapper<T>::Decode(
        decoded.data(), static_cast<int32_t>(num_values), compressed.data(), comp_size);
    ARROW_CHECK_OK(status);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}
static void BM_PforDecode(benchmark::State& state, Gen32 gen) {
  PforDecodeImpl<int32_t>(state, gen);
}
static void BM_Pfor64Decode(benchmark::State& state, Gen64 gen) {
  PforDecodeImpl<int64_t>(state, gen);
}

// ============================================================================
// DeltaBitPack Encode/Decode
// ============================================================================

template <typename T>
static void DeltaBitPackEncodeImpl(benchmark::State& state, GenT<T> gen) {
  using PType = typename PqTraits<T>::PType;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  auto encoder = MakeTypedEncoder<PType>(Encoding::DELTA_BINARY_PACKED);

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
static void BM_DeltaBitPackEncode(benchmark::State& state, Gen32 gen) {
  DeltaBitPackEncodeImpl<int32_t>(state, gen);
}
static void BM_DeltaBitPack64Encode(benchmark::State& state, Gen64 gen) {
  DeltaBitPackEncodeImpl<int64_t>(state, gen);
}

template <typename T>
static void DeltaBitPackDecodeImpl(benchmark::State& state, GenT<T> gen) {
  using PType = typename PqTraits<T>::PType;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  auto encoder = MakeTypedEncoder<PType>(Encoding::DELTA_BINARY_PACKED);
  encoder->Put(values.data(), static_cast<int>(num_values));
  auto buf = encoder->FlushValues();
  int64_t comp_size = buf->size();

  std::vector<T> decoded(num_values);
  auto decoder = MakeTypedDecoder<PType>(Encoding::DELTA_BINARY_PACKED);

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
static void BM_DeltaBitPackDecode(benchmark::State& state, Gen32 gen) {
  DeltaBitPackDecodeImpl<int32_t>(state, gen);
}
static void BM_DeltaBitPack64Decode(benchmark::State& state, Gen64 gen) {
  DeltaBitPackDecodeImpl<int64_t>(state, gen);
}

// ============================================================================
// Plain + ZSTD Encode/Decode
// ============================================================================

template <typename T>
static void PlainCodecEncodeImpl(benchmark::State& state, GenT<T> gen,
                                 Compression::type codec_type) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(values.data());

  auto codec = *Codec::Create(codec_type);
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

template <typename T>
static void PlainCodecDecodeImpl(benchmark::State& state, GenT<T> gen,
                                 Compression::type codec_type) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(values.data());

  auto codec = *Codec::Create(codec_type);
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
static void BM_PlainZstdEncode(benchmark::State& state, Gen32 gen) {
  PlainCodecEncodeImpl<int32_t>(state, gen, Compression::ZSTD);
}
static void BM_PlainZstd64Encode(benchmark::State& state, Gen64 gen) {
  PlainCodecEncodeImpl<int64_t>(state, gen, Compression::ZSTD);
}
static void BM_PlainZstdDecode(benchmark::State& state, Gen32 gen) {
  PlainCodecDecodeImpl<int32_t>(state, gen, Compression::ZSTD);
}
static void BM_PlainZstd64Decode(benchmark::State& state, Gen64 gen) {
  PlainCodecDecodeImpl<int64_t>(state, gen, Compression::ZSTD);
}

// ============================================================================
// Plain + LZ4 Encode/Decode
// ============================================================================

static void BM_PlainLz4Encode(benchmark::State& state, Gen32 gen) {
  PlainCodecEncodeImpl<int32_t>(state, gen, Compression::LZ4_FRAME);
}
static void BM_PlainLz464Encode(benchmark::State& state, Gen64 gen) {
  PlainCodecEncodeImpl<int64_t>(state, gen, Compression::LZ4_FRAME);
}
static void BM_PlainLz4Decode(benchmark::State& state, Gen32 gen) {
  PlainCodecDecodeImpl<int32_t>(state, gen, Compression::LZ4_FRAME);
}
static void BM_PlainLz464Decode(benchmark::State& state, Gen64 gen) {
  PlainCodecDecodeImpl<int64_t>(state, gen, Compression::LZ4_FRAME);
}

// ============================================================================
// RleBitPackHybrid Encode/Decode
// ============================================================================

template <typename T>
static void RleBitPackEncodeImpl(benchmark::State& state, GenT<T> gen) {
  using U = std::make_unsigned_t<T>;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  int32_t bit_width = ComputeBitWidth<T>(values);
  int64_t max_buf =
      ::arrow::util::RleBitPackedEncoder::MaxBufferSize(bit_width, num_values) +
      ::arrow::util::RleBitPackedEncoder::MinBufferSize(bit_width);
  std::vector<uint8_t> buffer(max_buf);

  // Compute comp_size once for the counter
  int64_t comp_size;
  {
    ::arrow::util::RleBitPackedEncoder enc(buffer.data(), static_cast<int>(max_buf),
                                           bit_width);
    for (int64_t i = 0; i < num_values; ++i) {
      enc.Put(static_cast<uint64_t>(static_cast<U>(values[i])));
    }
    comp_size = enc.Flush();
  }

  for (auto _ : state) {
    ::arrow::util::RleBitPackedEncoder encoder(buffer.data(), static_cast<int>(max_buf),
                                               bit_width);
    for (int64_t i = 0; i < num_values; ++i) {
      encoder.Put(static_cast<uint64_t>(static_cast<U>(values[i])));
    }
    auto sz = encoder.Flush();
    benchmark::DoNotOptimize(sz);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}
static void BM_RleBitPackEncode(benchmark::State& state, Gen32 gen) {
  RleBitPackEncodeImpl<int32_t>(state, gen);
}
static void BM_RleBitPack64Encode(benchmark::State& state, Gen64 gen) {
  RleBitPackEncodeImpl<int64_t>(state, gen);
}

template <typename T>
static void RleBitPackDecodeImpl(benchmark::State& state, GenT<T> gen) {
  using U = std::make_unsigned_t<T>;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  int32_t bit_width = ComputeBitWidth<T>(values);
  int64_t max_buf =
      ::arrow::util::RleBitPackedEncoder::MaxBufferSize(bit_width, num_values) +
      ::arrow::util::RleBitPackedEncoder::MinBufferSize(bit_width);
  std::vector<uint8_t> buffer(max_buf);

  ::arrow::util::RleBitPackedEncoder encoder(buffer.data(), static_cast<int>(max_buf),
                                             bit_width);
  for (int64_t i = 0; i < num_values; ++i) {
    encoder.Put(static_cast<uint64_t>(static_cast<U>(values[i])));
  }
  int comp_size = encoder.Flush();

  std::vector<T> decoded(num_values);
  for (auto _ : state) {
    ::arrow::util::RleBitPackedParser parser(buffer.data(), comp_size, bit_width);
    int64_t out_idx = 0;
    struct Handler {
      T* output;
      int64_t* idx;
      int64_t max_values;
      int32_t bw;
      ::arrow::util::RleBitPackedParser::ControlFlow OnRleRun(::arrow::util::RleRun run) {
        ::arrow::util::RleRunDecoder<T> dec(run, bw);
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
        ::arrow::util::BitPackedRunDecoder<T> dec(run, bw);
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
static void BM_RleBitPackDecode(benchmark::State& state, Gen32 gen) {
  RleBitPackDecodeImpl<int32_t>(state, gen);
}
static void BM_RleBitPack64Decode(benchmark::State& state, Gen64 gen) {
  RleBitPackDecodeImpl<int64_t>(state, gen);
}

// ============================================================================
// ByteStreamSplit + Codec (ZSTD or LZ4)
// ============================================================================

template <typename T>
static void BssCodecEncodeImpl(benchmark::State& state, GenT<T> gen,
                               Compression::type codec_type) {
  using PType = typename PqTraits<T>::PType;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  auto descr = MakeDescriptor<T>();
  auto encoder = MakeTypedEncoder<PType>(Encoding::BYTE_STREAM_SPLIT,
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
    auto sz = *codec->Compress(buf->size(), buf->data(), max_comp, compressed.data());
    benchmark::DoNotOptimize(sz);
  }

  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

template <typename T>
static void BssCodecDecodeImpl(benchmark::State& state, GenT<T> gen,
                               Compression::type codec_type) {
  using PType = typename PqTraits<T>::PType;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);

  auto descr = MakeDescriptor<T>();
  auto encoder = MakeTypedEncoder<PType>(Encoding::BYTE_STREAM_SPLIT,
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
  std::vector<T> decoded(num_values);
  auto decoder = MakeTypedDecoder<PType>(Encoding::BYTE_STREAM_SPLIT, descr.get());

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
  BssCodecEncodeImpl<int32_t>(state, gen, Compression::ZSTD);
}
static void BM_BssZstd64Encode(benchmark::State& state, Gen64 gen) {
  BssCodecEncodeImpl<int64_t>(state, gen, Compression::ZSTD);
}
static void BM_BssZstdDecode(benchmark::State& state, Gen32 gen) {
  BssCodecDecodeImpl<int32_t>(state, gen, Compression::ZSTD);
}
static void BM_BssZstd64Decode(benchmark::State& state, Gen64 gen) {
  BssCodecDecodeImpl<int64_t>(state, gen, Compression::ZSTD);
}

// Wrappers for BSS+LZ4
static void BM_BssLz4Encode(benchmark::State& state, Gen32 gen) {
  BssCodecEncodeImpl<int32_t>(state, gen, Compression::LZ4_FRAME);
}
static void BM_BssLz464Encode(benchmark::State& state, Gen64 gen) {
  BssCodecEncodeImpl<int64_t>(state, gen, Compression::LZ4_FRAME);
}
static void BM_BssLz4Decode(benchmark::State& state, Gen32 gen) {
  BssCodecDecodeImpl<int32_t>(state, gen, Compression::LZ4_FRAME);
}
static void BM_BssLz464Decode(benchmark::State& state, Gen64 gen) {
  BssCodecDecodeImpl<int64_t>(state, gen, Compression::LZ4_FRAME);
}

// ============================================================================
// Benchmark Registration
// ============================================================================

static void CustomArgs(benchmark::internal::Benchmark* b) { b->Arg(102400); }

// Macro to register all algorithms for a given dataset
#define REGISTER_DATASET(Name, GenFunc)                                        \
  BENCHMARK_CAPTURE(BM_PforEncode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_PforDecode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_DeltaBitPackEncode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_DeltaBitPackDecode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_PlainZstdEncode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_PlainZstdDecode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_PlainLz4Encode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_PlainLz4Decode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_RleBitPackEncode, Name, &GenFunc)->Apply(CustomArgs);   \
  BENCHMARK_CAPTURE(BM_RleBitPackDecode, Name, &GenFunc)->Apply(CustomArgs);   \
  BENCHMARK_CAPTURE(BM_BssZstdEncode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_BssZstdDecode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_BssLz4Encode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_BssLz4Decode, Name, &GenFunc)->Apply(CustomArgs);

// Same as REGISTER_DATASET but for int64 (BIGINT) columns; benchmark names get
// the "64" codec suffix (e.g. BM_Pfor64Encode) to distinguish them.
#define REGISTER_DATASET64(Name, GenFunc)                                        \
  BENCHMARK_CAPTURE(BM_Pfor64Encode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_Pfor64Decode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_DeltaBitPack64Encode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_DeltaBitPack64Decode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_PlainZstd64Encode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_PlainZstd64Decode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_PlainLz464Encode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_PlainLz464Decode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_RleBitPack64Encode, Name, &GenFunc)->Apply(CustomArgs);   \
  BENCHMARK_CAPTURE(BM_RleBitPack64Decode, Name, &GenFunc)->Apply(CustomArgs);   \
  BENCHMARK_CAPTURE(BM_BssZstd64Encode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_BssZstd64Decode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_BssLz464Encode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_BssLz464Decode, Name, &GenFunc)->Apply(CustomArgs);

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
REGISTER_DATASET(TpcdsCustomerSk, GenTpcdsCustomerSk)
REGISTER_DATASET(TpcdsExtSalesPrice, GenTpcdsExtSalesPrice)
REGISTER_DATASET(TpcdsNetProfit, GenTpcdsNetProfit)
REGISTER_DATASET(TpcdsDYear, GenTpcdsDYear)
// TPC-H datasets
REGISTER_DATASET(TpchLQuantity, GenTpchLQuantity)
REGISTER_DATASET(TpchLExtendedPrice, GenTpchLExtendedPrice)
REGISTER_DATASET(TpchLDiscount, GenTpchLDiscount)
REGISTER_DATASET(TpchLShipDate, GenTpchLShipDate)
// NYC taxi datasets
REGISTER_DATASET(TaxiPickupUnixTime, GenTaxiPickupUnixTime)
REGISTER_DATASET(TaxiTripDistanceX100, GenTaxiTripDistanceX100)
REGISTER_DATASET(TaxiFareCents, GenTaxiFareCents)
// int64 / BIGINT datasets (8-byte signed values)
REGISTER_DATASET64(TsNanos, GenTsNanos)
REGISTER_DATASET64(OrderKey, GenOrderKey)
REGISTER_DATASET64(PriceMicros, GenPriceMicros)
REGISTER_DATASET64(SnowflakeId, GenSnowflakeId)
REGISTER_DATASET64(ByteCount, GenByteCount)

}  // namespace
}  // namespace parquet
