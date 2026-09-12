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
// Data generators mimic ClickBench and TPC-DS column distributions. A second
// group covers columns with structure between neighbouring values, or with a
// cluster the minimum is not part of; those are the ones on which PFOR's delta
// mode and DELTA_BINARY_PACKED make different choices.

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

#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/fastlanes/interleaved_pfor.h"
#include "arrow/util/fastlanes/lane_delta.h"
#include "arrow/util/fastlanes/transposed_delta.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/pfor/pfor_wrapper_internal.h"
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

// ============================================================================
// Data Generators — sorted and near-sorted
//
// The generators above draw independently around a base, which is the case
// delta encoding is not for: subtracting two i.i.d. values spans the whole
// range whichever two you pick, so the distance between them does not matter
// and every delta variant lands within a bit or two of frame of reference.
// Comparing delta schemes needs columns whose value depends on the previous
// one. These cover the shapes that occur in practice: a clustered timestamp
// column, a sorted key with duplicates, an exact counter, and a column that
// is sorted apart from a few late arrivals.
// ============================================================================

// Event timestamps in seconds, arriving a few seconds apart. The common case
// for a table clustered or sorted on time.
std::vector<int32_t> GenSortedUnixTime(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(401);
  std::uniform_int_distribution<int32_t> gap(0, 7);
  int32_t t = 1700000000;
  for (auto& x : v) {
    t += gap(rng);
    x = t;
  }
  return v;
}

// A sorted surrogate key with runs of duplicates, as produced by a join key or
// a dictionary-sorted column: most steps are 0, some are 1.
std::vector<int32_t> GenSortedKeyDups(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(402);
  std::uniform_int_distribution<int32_t> step(0, 1);
  int32_t k = 5000000;
  for (auto& x : v) {
    k += step(rng);
    x = k;
  }
  return v;
}

// An exact +1 row id. The best case for any delta scheme, and the case where
// stride-1 and lane-parallel delta differ most.
std::vector<int32_t> GenMonotoneRowId(int64_t n) {
  std::vector<int32_t> v(n);
  int32_t k = 1;
  for (auto& x : v) x = k++;
  return v;
}

// Sorted except that 2% of rows arrive late, so a few deltas are large and
// negative. Tests whether one bit width per block survives outliers.
std::vector<int32_t> GenNearSortedUnixTime(int64_t n) {
  std::vector<int32_t> v = GenSortedUnixTime(n);
  std::mt19937 rng(403);
  std::uniform_int_distribution<int> pick(0, 49);
  std::uniform_int_distribution<int32_t> late(1, 3600);
  for (auto& x : v) {
    if (pick(rng) == 0) x -= late(rng);
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
// Data Generators — structure between neighbouring values
// ============================================================================

// The generators above are all either unordered or perfectly regular, so none
// of them separates an encoding that differences neighbouring values from one
// that packs them, and none of them puts a frame of reference anywhere but the
// minimum. These do. They are the columns on which PFOR's delta mode and
// DELTA_BINARY_PACKED make different choices, so they are what this file needs
// in order to compare the two.
//
// Unlike the generators above, these are templates rather than a pair of
// per-width functions: it keeps the int32 and int64 arms of every REGISTER
// below on provably the same distribution, which two hand-written copies would
// not. They live in their own namespace because two of them build on base
// distributions whose names are already taken in this file by columns drawn
// from different seeds -- keeping them separate is what makes these figures
// comparable with pfor_benchmark.cc, which uses the same shapes.
namespace delta_shapes {

template <typename T>
std::vector<T> GenSmallRange(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<T> dist(100000, 200000);
  for (auto& x : v) x = dist(rng);
  return v;
}

template <typename T>
std::vector<T> GenTpcdsItemSk(int64_t n) {
  std::vector<T> v(n);
  const T kMax = 100000;
  std::mt19937_64 rng(12345);
  std::exponential_distribution<double> exp_dist(0.00005);
  for (auto& x : v) {
    T val = static_cast<T>(exp_dist(rng));
    x = std::min(static_cast<T>(val + 1), kMax);
  }
  return v;
}

/// A timestamp column with a fixed sampling interval and jitter: the
/// differences cluster tightly around the interval, the values do not.
template <typename T>
std::vector<T> GenTrendJitter(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(7);
  std::normal_distribution<double> jitter(0.0, 40.0);
  double t = 1704067200.0;
  for (auto& x : v) {
    t += 3.0;
    x = static_cast<T>(t + jitter(rng));
  }
  return v;
}

/// A TCP congestion window: a linear climb cut by a sharp halving. Almost
/// every difference is the same small number and a few are large and
/// negative, which is the shape a two-sided frame plus patching handles at
/// close to no cost and a one-sided one cannot.
template <typename T>
std::vector<T> GenSawtooth(int64_t n) {
  std::vector<T> v(n);
  T cur = 1000;
  for (int64_t i = 0; i < n; ++i) {
    if (i % 200 == 199) {
      cur /= 2;
    } else {
      cur += 12;
    }
    v[i] = cur;
  }
  return v;
}

/// A continuous quantity sampled over time, so the rate of change is bounded
/// but the level wanders: differencing wins, though not by much.
template <typename T>
std::vector<T> GenMeasurement(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(11);
  std::normal_distribution<double> noise(0.0, 1.0);
  double level = 0.0;
  double rate = 0.0;
  for (int64_t i = 0; i < n; ++i) {
    rate = 0.95 * rate + noise(rng);
    level += rate;
    v[i] = static_cast<T>(4000.0 * level / 50.0) + static_cast<T>(500000);
  }
  return v;
}

/// A sorted surrogate key column, which is what a clustered index or a
/// sorted-by-key file produces.
template <typename T>
std::vector<T> GenSortedKeys(int64_t n) {
  auto v = GenTpcdsItemSk<T>(n);
  std::sort(v.begin(), v.end());
  return v;
}

/// A measurement series punctuated by a low sentinel standing in for "no
/// reading". The sentinel sits far below the cluster, so a frame pinned to
/// the minimum has to widen to reach it and no value can ever be patched.
template <typename T>
std::vector<T> GenSensorDropouts(int64_t n) {
  auto v = GenMeasurement<T>(n);
  std::mt19937_64 rng(3);
  std::uniform_int_distribution<int64_t> pos(0, n - 1);
  for (int64_t i = 0; i < std::max<int64_t>(1, n / 200); ++i) {
    v[pos(rng)] = static_cast<T>(-999999);
  }
  return v;
}

/// Monotonic ids with occasional large jumps, as a sequence shared between
/// writers or restarted produces.
template <typename T>
std::vector<T> GenIdsWithGaps(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(5);
  std::uniform_int_distribution<int> step(1, 6);
  std::uniform_int_distribution<int> jump(0, 199);
  T cur = 1000000;
  for (auto& x : v) {
    cur += static_cast<T>(step(rng));
    if (jump(rng) == 0) cur += 100000;
    x = cur;
  }
  return v;
}

/// Prices on a random walk: small signed steps around a high base, and no
/// trend for the differences to line up on.
template <typename T>
std::vector<T> GenRandomWalk(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(13);
  std::uniform_int_distribution<int> step(-25, 25);
  T cur = 1500000;
  for (auto& x : v) {
    cur += static_cast<T>(step(rng));
    x = cur;
  }
  return v;
}

/// Event arrival times in milliseconds since the epoch, bursty, so the gaps
/// are skewed rather than clustered.
template <typename T>
std::vector<T> GenEventMillis(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(17);
  std::exponential_distribution<double> gap(0.4);
  double t = 1704067200.0;
  for (auto& x : v) {
    t += gap(rng);
    x = static_cast<T>(t);
  }
  return v;
}

/// A tight unordered cluster with a low "missing" sentinel: the frame cannot
/// sit at the minimum and differencing has nothing to exploit either, so
/// this is the column that has to come out no worse than before.
template <typename T>
std::vector<T> GenLowSentinel(int64_t n) {
  auto v = GenSmallRange<T>(n);
  std::mt19937_64 rng(23);
  std::uniform_int_distribution<int64_t> pos(0, n - 1);
  for (int64_t i = 0; i < std::max<int64_t>(1, n / 200); ++i) {
    v[pos(rng)] = static_cast<T>(-1000);
  }
  return v;
}

/// Two interleaved clusters far apart, so no single window covers the vector
/// and the cheaper cluster is worth framing on its own.
template <typename T>
std::vector<T> GenBimodal(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(19);
  std::uniform_int_distribution<T> lo(1000, 1100);
  std::uniform_int_distribution<T> hi(900000, 900100);
  std::uniform_int_distribution<int> pick(0, 9);
  for (auto& x : v) {
    x = pick(rng) < 8 ? lo(rng) : hi(rng);
  }
  return v;
}

}  // namespace delta_shapes

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
                         static_cast<int32_t>(num_values))
                         .ValueOrDie();
  std::vector<uint8_t> compressed(max_size);

  // Compute comp_size once for the counter
  int64_t comp_size = max_size;
  ARROW_CHECK_OK(::arrow::util::pfor::PforWrapper<T>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(), &comp_size));

  for (auto _ : state) {
    int64_t sz = max_size;
    ARROW_CHECK_OK(::arrow::util::pfor::PforWrapper<T>::Encode(
        values.data(), static_cast<int32_t>(num_values), compressed.data(), &sz));
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
                         static_cast<int32_t>(num_values))
                         .ValueOrDie();
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;
  ARROW_CHECK_OK(::arrow::util::pfor::PforWrapper<T>::Encode(
      values.data(), static_cast<int32_t>(num_values), compressed.data(), &comp_size));

  std::vector<T> decoded(num_values);
  for (auto _ : state) {
    auto status = ::arrow::util::pfor::PforWrapper<T>::Decode(
        compressed.data(), comp_size, static_cast<int32_t>(num_values), decoded.data());
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
// DELTA_BINARY_PACKED cost decomposition
// ============================================================================
//
// Splits the cost of decoding a DELTA_BINARY_PACKED page into three parts:
// per-block header parsing, the call into the bit unpacker, and the serial
// prefix sum. Each ablated arm reads the same stream and calls the same
// out-of-line `arrow::internal::unpack` in libarrow, so only the surrounding
// loop is recompiled here. `kFull` exists to be cross-checked against the
// in-tree BM_DeltaBitPackDecode arm; if the two disagree, none of the ablated
// arms mean anything.

enum class DbpAblate {
  kFull,        // headers + unpack + prefix sum
  kNoSum,       // headers + unpack
  kNoUnpack,    // headers + prefix sum, bit reader advanced instead of unpacked
  kHeaderOnly,  // headers only
  // Reader-side variants. Both read a byte-identical stream and are available
  // to any conforming reader with no format change.
  kCoalesce,  // one unpack call per run of miniblocks sharing a bit width
  kBlockSum,  // one prefix-sum loop per block instead of one per miniblock
  kBoth,      // both of the above
  kDirect,    // kBoth, plus unpack_bias called directly with min_delta as bias
};

constexpr bool DbpIsReaderSide(DbpAblate a) {
  return a == DbpAblate::kCoalesce || a == DbpAblate::kBlockSum ||
         a == DbpAblate::kBoth || a == DbpAblate::kDirect;
}
constexpr bool DbpDoesCoalesce(DbpAblate a) {
  return a == DbpAblate::kCoalesce || a == DbpAblate::kBoth ||
         a == DbpAblate::kDirect;
}
constexpr bool DbpDoesBlockSum(DbpAblate a) {
  return a == DbpAblate::kBlockSum || a == DbpAblate::kBoth ||
         a == DbpAblate::kDirect;
}
// kDirect skips BitReader::GetBatch and calls the unpacker itself. Legal here
// because every miniblock boundary is byte-aligned: the block header is read
// with byte-aligned primitives, and a miniblock of v values at width w occupies
// exactly v*w/8 bytes with v a multiple of 32, so w*v is always a multiple of 8.
// Folding min_delta in as the unpacker's bias also removes one add per value
// from the prefix-sum loop body.
constexpr bool DbpDoesDirectUnpack(DbpAblate a) { return a == DbpAblate::kDirect; }

// A DELTA_BINARY_PACKED writer with configurable block geometry. The format
// carries values_per_block and mini_blocks_per_block as varints at the front of
// every stream, constrained only to multiples of 128 and 32 respectively.
// Arrow's own encoder takes both as constructor arguments but is declared in an
// anonymous namespace, so sweeping the geometry needs a writer here.
template <typename T>
std::shared_ptr<::arrow::Buffer> WriteDbpStream(const std::vector<T>& values,
                                               uint32_t values_per_block,
                                               uint32_t mini_blocks_per_block) {
  using UT = std::make_unsigned_t<T>;
  const uint32_t values_per_mini_block = values_per_block / mini_blocks_per_block;
  ARROW_CHECK_EQ(values_per_block % 128, 0u);
  ARROW_CHECK_EQ(values_per_mini_block % 32, 0u);

  const size_t n = values.size();
  // Deltas are v[i] - v[i-1]; the first value travels in the header.
  std::vector<UT> deltas(n > 0 ? n - 1 : 0);
  for (size_t i = 1; i < n; ++i) {
    deltas[i - 1] = static_cast<UT>(values[i]) - static_cast<UT>(values[i - 1]);
  }

  auto buf = AllocateBuffer(::arrow::default_memory_pool(),
                            static_cast<int64_t>(n * sizeof(T) * 2 + 1024));
  ::arrow::bit_util::BitWriter writer(buf->mutable_data(),
                                      static_cast<int>(buf->size()));
  ARROW_CHECK(writer.PutVlqInt(values_per_block));
  ARROW_CHECK(writer.PutVlqInt(mini_blocks_per_block));
  ARROW_CHECK(writer.PutVlqInt(static_cast<uint32_t>(n)));
  ARROW_CHECK(writer.PutZigZagVlqInt(n > 0 ? values[0] : T{0}));

  std::vector<UT> residuals(values_per_block);
  std::vector<uint8_t> widths(mini_blocks_per_block);
  for (size_t base = 0; base < deltas.size(); base += values_per_block) {
    const size_t present = std::min<size_t>(values_per_block, deltas.size() - base);
    // min_delta is signed-minimum over the deltas actually present.
    T min_delta = static_cast<T>(deltas[base]);
    for (size_t k = 1; k < present; ++k) {
      min_delta = std::min(min_delta, static_cast<T>(deltas[base + k]));
    }
    for (size_t k = 0; k < present; ++k) {
      residuals[k] = deltas[base + k] - static_cast<UT>(min_delta);
    }
    // Pad the block out with zero residuals, i.e. with a delta of min_delta.
    std::fill(residuals.begin() + present, residuals.end(), UT{0});

    for (uint32_t m = 0; m < mini_blocks_per_block; ++m) {
      UT max_res = 0;
      for (uint32_t k = 0; k < values_per_mini_block; ++k) {
        max_res = std::max(max_res, residuals[m * values_per_mini_block + k]);
      }
      widths[m] = static_cast<uint8_t>(std::bit_width(max_res));
    }
    ARROW_CHECK(writer.PutZigZagVlqInt(min_delta));
    uint8_t* wp = writer.GetNextBytePtr(static_cast<int>(mini_blocks_per_block));
    ARROW_CHECK(wp != nullptr);
    memcpy(wp, widths.data(), mini_blocks_per_block);
    for (uint32_t m = 0; m < mini_blocks_per_block; ++m) {
      if (widths[m] == 0) continue;
      for (uint32_t k = 0; k < values_per_mini_block; ++k) {
        ARROW_CHECK(writer.PutValue(residuals[m * values_per_mini_block + k], widths[m]));
      }
    }
  }
  writer.Flush();
  return SliceBuffer(buf, 0, writer.bytes_written());
}

// Shadow of DeltaBitPackDecoder::GetInternal with the same stream handling and
// the same call into libarrow's unpacker, minus whichever part is ablated.
template <typename T, DbpAblate kAblate, bool kHeapReader = false>
int DbpAblatedDecode(const uint8_t* data, int len, T* out) {
  using UT = std::make_unsigned_t<T>;
  // The shipping decoder holds its BitReader behind a shared_ptr, so its bit
  // position lives in heap memory reached through a load rather than in a
  // stack slot the compiler can keep in registers. kHeapReader reproduces that
  // placement so the shadow can be compared with the real thing.
  std::shared_ptr<::arrow::bit_util::BitReader> heap_reader;
  if constexpr (kHeapReader) {
    heap_reader = std::make_shared<::arrow::bit_util::BitReader>(data, len);
  }
  ::arrow::bit_util::BitReader stack_reader(data, len);
  ::arrow::bit_util::BitReader& reader =
      kHeapReader ? *heap_reader : stack_reader;
  uint32_t values_per_block = 0, mini_blocks_per_block = 0, total_count = 0;
  T first_value = 0;
  ARROW_CHECK(reader.GetVlqInt(&values_per_block));
  ARROW_CHECK(reader.GetVlqInt(&mini_blocks_per_block));
  ARROW_CHECK(reader.GetVlqInt(&total_count));
  ARROW_CHECK(reader.GetZigZagVlqInt(&first_value));
  const uint32_t values_per_mini_block = values_per_block / mini_blocks_per_block;

  int i = 0;
  out[i++] = first_value;
  UT last = static_cast<UT>(first_value);
  uint32_t remaining = total_count - 1;
  std::vector<uint8_t> widths(mini_blocks_per_block);

  while (remaining > 0) {
    T min_delta = 0;
    ARROW_CHECK(reader.GetZigZagVlqInt(&min_delta));
    for (uint32_t m = 0; m < mini_blocks_per_block; ++m) {
      ARROW_CHECK(reader.GetAligned<uint8_t>(1, widths.data() + m));
    }
    for (uint32_t m = 0; m < mini_blocks_per_block && remaining > 0; ++m) {
      const uint32_t count = std::min(values_per_mini_block, remaining);
      const int w = widths[m];
      if (w == 0) {
        // Same zero-width shortcut the in-tree decoder takes.
        if constexpr (kAblate == DbpAblate::kFull || kAblate == DbpAblate::kNoUnpack) {
          for (uint32_t k = 0; k < count; ++k) {
            out[i + k] = static_cast<T>(last + static_cast<UT>(k + 1) *
                                                   static_cast<UT>(min_delta));
          }
          last += static_cast<UT>(count) * static_cast<UT>(min_delta);
        }
      } else {
        if constexpr (kAblate == DbpAblate::kFull || kAblate == DbpAblate::kNoSum) {
          ARROW_CHECK_EQ(reader.GetBatch(w, out + i, static_cast<int>(count)),
                         static_cast<int>(count));
        } else {
          ARROW_CHECK(reader.Advance(static_cast<int64_t>(w) * count));
        }
        if constexpr (kAblate == DbpAblate::kFull || kAblate == DbpAblate::kNoUnpack) {
          const UT md = static_cast<UT>(min_delta);
          for (uint32_t k = 0; k < count; ++k) {
            last += md + static_cast<UT>(out[i + k]);
            out[i + k] = static_cast<T>(last);
          }
        }
      }
      i += static_cast<int>(count);
      remaining -= count;
    }
  }
  return i;
}

// Reader-side variants, reading a byte-identical stream.
//
// Coalescing: the payload is one sequential LSB-first bit run with no padding
// between miniblocks, so two adjacent miniblocks that share a bit width are
// bit-identical to a single run of twice the length at that width. Unpacking
// them in one call raises the values-per-call the unpacker sees without
// touching the stream. Nothing in the format has to change; the reader just
// stops asking for 32 values at a time when it could ask for 128.
//
// Block sum: min_delta is a per-block quantity, so the serial prefix sum need
// not restart at every miniblock boundary.
template <typename T, DbpAblate kAblate>
int DbpReaderSideDecode(const uint8_t* data, int len, T* out) {
  using UT = std::make_unsigned_t<T>;
  ::arrow::bit_util::BitReader reader(data, len);
  uint32_t values_per_block = 0, mini_blocks_per_block = 0, total_count = 0;
  T first_value = 0;
  ARROW_CHECK(reader.GetVlqInt(&values_per_block));
  ARROW_CHECK(reader.GetVlqInt(&mini_blocks_per_block));
  ARROW_CHECK(reader.GetVlqInt(&total_count));
  ARROW_CHECK(reader.GetZigZagVlqInt(&first_value));
  const uint32_t values_per_mini_block = values_per_block / mini_blocks_per_block;

  int i = 0;
  out[i++] = first_value;
  UT last = static_cast<UT>(first_value);
  uint32_t remaining = total_count - 1;
  std::vector<uint8_t> widths(mini_blocks_per_block);

  while (remaining > 0) {
    T min_delta = 0;
    ARROW_CHECK(reader.GetZigZagVlqInt(&min_delta));
    for (uint32_t m = 0; m < mini_blocks_per_block; ++m) {
      ARROW_CHECK(reader.GetAligned<uint8_t>(1, widths.data() + m));
    }
    const UT md = static_cast<UT>(min_delta);
    const int block_start = i;
    uint32_t block_values = 0;

    for (uint32_t m = 0; m < mini_blocks_per_block && remaining > 0;) {
      uint32_t run = 1;
      if constexpr (DbpDoesCoalesce(kAblate)) {
        // Extend the run while the next miniblock shares this width and is
        // fully present, so the coalesced call never crosses the tail.
        while (m + run < mini_blocks_per_block && widths[m + run] == widths[m] &&
               remaining > run * values_per_mini_block) {
          ++run;
        }
      }
      const uint32_t count = std::min(run * values_per_mini_block, remaining);
      const int w = widths[m];
      if (w == 0) {
        if constexpr (DbpDoesBlockSum(kAblate)) {
          // Leave a residual of zero for the block-wide pass below.
          std::fill(out + i, out + i + count, T{0});
        } else {
          for (uint32_t k = 0; k < count; ++k) {
            out[i + k] = static_cast<T>(last + static_cast<UT>(k + 1) * md);
          }
          last += static_cast<UT>(count) * md;
        }
      } else {
        ARROW_CHECK_EQ(reader.GetBatch(w, out + i, static_cast<int>(count)),
                       static_cast<int>(count));
        if constexpr (!DbpDoesBlockSum(kAblate)) {
          for (uint32_t k = 0; k < count; ++k) {
            last += md + static_cast<UT>(out[i + k]);
            out[i + k] = static_cast<T>(last);
          }
        }
      }
      i += static_cast<int>(count);
      remaining -= count;
      block_values += count;
      m += run;
    }

    if constexpr (DbpDoesBlockSum(kAblate)) {
      // One serial pass over the whole block, once every miniblock in it has
      // been unpacked. With a direct biased unpack min_delta is already folded
      // into each residual, so the chain is a bare running sum.
      for (uint32_t k = 0; k < block_values; ++k) {
        last += md + static_cast<UT>(out[block_start + k]);
        out[block_start + k] = static_cast<T>(last);
      }
    }
  }
  return i;
}

// The most optimized reader this format admits, as a lower bound on what is
// available without a spec change. Three things beyond the shipping decoder:
// coalescing equal-width miniblocks into one unpack call, one prefix-sum pass
// per block instead of per miniblock, and calling unpack_bias directly with
// min_delta as the bias, which both skips BitReader::GetBatch's per-call
// bookkeeping and removes one add per value from the sum loop.
//
// A byte cursor is enough because every position this walks is byte-aligned:
// the stream header and block headers are byte-aligned varints and width bytes,
// and a miniblock of v values at width w occupies exactly v*w/8 bytes with v a
// multiple of 32, so v*w is always a multiple of 8.
template <typename T>
int DbpDirectDecode(const uint8_t* data, int len, T* out) {
  using UT = std::make_unsigned_t<T>;
  int pos = 0;
  auto get_vlq = [&](uint32_t* v) {
    uint32_t r = 0;
    int shift = 0;
    while (true) {
      ARROW_CHECK_LT(pos, len);
      const uint8_t b = data[pos++];
      r |= static_cast<uint32_t>(b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
      ARROW_CHECK_LE(shift, 28);
    }
    *v = r;
  };
  auto get_zigzag = [&](T* v) {
    uint32_t u = 0;
    get_vlq(&u);
    *v = static_cast<T>((u >> 1) ^ (~(u & 1) + 1));
  };

  uint32_t values_per_block = 0, mini_blocks_per_block = 0, total_count = 0;
  T first_value = 0;
  get_vlq(&values_per_block);
  get_vlq(&mini_blocks_per_block);
  get_vlq(&total_count);
  get_zigzag(&first_value);
  const uint32_t values_per_mini_block = values_per_block / mini_blocks_per_block;

  int i = 0;
  out[i++] = first_value;
  UT last = static_cast<UT>(first_value);
  uint32_t remaining = total_count - 1;

  while (remaining > 0) {
    T min_delta = 0;
    get_zigzag(&min_delta);
    const uint8_t* widths = data + pos;
    pos += static_cast<int>(mini_blocks_per_block);
    ARROW_CHECK_LE(pos, len);
    const UT md = static_cast<UT>(min_delta);
    const int block_start = i;
    uint32_t block_values = 0;

    for (uint32_t m = 0; m < mini_blocks_per_block && remaining > 0;) {
      uint32_t run = 1;
      while (m + run < mini_blocks_per_block && widths[m + run] == widths[m] &&
             remaining > run * values_per_mini_block) {
        ++run;
      }
      const uint32_t count = std::min(run * values_per_mini_block, remaining);
      const int w = widths[m];
      if (w == 0) {
        // No payload; the residual is min_delta itself, which the biased
        // unpack would otherwise have supplied.
        std::fill(out + i, out + i + count, static_cast<T>(md));
      } else {
        const ::arrow::internal::UnpackOptions opts{
            .batch_size = static_cast<int>(count),
            .bit_width = w,
            .bit_offset = 0,
            .max_read_bytes = len - pos,
        };
        ::arrow::internal::unpack_bias(data + pos, reinterpret_cast<UT*>(out + i),
                                       opts, md);
        pos += static_cast<int>((static_cast<int64_t>(w) * count) / 8);
        ARROW_CHECK_LE(pos, len);
      }
      i += static_cast<int>(count);
      remaining -= count;
      block_values += count;
      m += run;
    }
    // min_delta is already folded into every residual, so this is a bare
    // running sum: one dependent add per value and nothing else.
    for (uint32_t k = 0; k < block_values; ++k) {
      last += static_cast<UT>(out[block_start + k]);
      out[block_start + k] = static_cast<T>(last);
    }
  }
  return i;
}

// Mean values per unpack call a coalescing reader achieves on this stream.
// Without coalescing it is always values_per_mini_block, so the ratio of the two
// is the granularity gain available on this column, and it bounds what the
// kCoalesce arm can show.
template <typename T>
double DbpMeanValuesPerUnpackCall(const uint8_t* data, int len) {
  ::arrow::bit_util::BitReader reader(data, len);
  uint32_t values_per_block = 0, mini_blocks_per_block = 0, total_count = 0;
  T first_value = 0;
  ARROW_CHECK(reader.GetVlqInt(&values_per_block));
  ARROW_CHECK(reader.GetVlqInt(&mini_blocks_per_block));
  ARROW_CHECK(reader.GetVlqInt(&total_count));
  ARROW_CHECK(reader.GetZigZagVlqInt(&first_value));
  const uint32_t values_per_mini_block = values_per_block / mini_blocks_per_block;

  uint32_t remaining = total_count - 1;
  int64_t calls = 0, values_unpacked = 0;
  std::vector<uint8_t> widths(mini_blocks_per_block);
  while (remaining > 0) {
    T min_delta = 0;
    ARROW_CHECK(reader.GetZigZagVlqInt(&min_delta));
    for (uint32_t m = 0; m < mini_blocks_per_block; ++m) {
      ARROW_CHECK(reader.GetAligned<uint8_t>(1, widths.data() + m));
    }
    for (uint32_t m = 0; m < mini_blocks_per_block && remaining > 0;) {
      uint32_t run = 1;
      while (m + run < mini_blocks_per_block && widths[m + run] == widths[m] &&
             remaining > run * values_per_mini_block) {
        ++run;
      }
      const uint32_t count = std::min(run * values_per_mini_block, remaining);
      if (widths[m] != 0) {
        ++calls;
        values_unpacked += count;
        ARROW_CHECK(reader.Advance(static_cast<int64_t>(widths[m]) * count));
      }
      remaining -= count;
      m += run;
    }
  }
  return calls == 0 ? 0.0 : static_cast<double>(values_unpacked) / static_cast<double>(calls);
}

template <typename T, DbpAblate kAblate, bool kHeapReader = false>
static void DbpAblateImpl(benchmark::State& state, GenT<T> gen, uint32_t values_per_block,
                          uint32_t mini_blocks_per_block) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(T);
  auto buf = WriteDbpStream<T>(values, values_per_block, mini_blocks_per_block);

  std::vector<T> decoded(num_values);
  // Correctness gate: the unablated path must round-trip this stream exactly.
  {
    std::vector<T> check(num_values);
    const int n = DbpAblatedDecode<T, DbpAblate::kFull>(
        buf->data(), static_cast<int>(buf->size()), check.data());
    ARROW_CHECK_EQ(n, static_cast<int>(num_values));
    ARROW_CHECK(check == values) << "hand-built DBP stream does not round-trip";
    // A reader-side variant must produce an identical result, or its speed
    // means nothing.
    if constexpr (DbpIsReaderSide(kAblate)) {
      std::vector<T> rs(num_values);
      const int m = kAblate == DbpAblate::kDirect
                        ? DbpDirectDecode<T>(buf->data(), static_cast<int>(buf->size()),
                                             rs.data())
                        : DbpReaderSideDecode<T, kAblate>(
                              buf->data(), static_cast<int>(buf->size()), rs.data());
      ARROW_CHECK_EQ(m, static_cast<int>(num_values));
      ARROW_CHECK(rs == values) << "reader-side DBP variant disagrees";
    }
  }

  for (auto _ : state) {
    if constexpr (kAblate == DbpAblate::kDirect) {
      DbpDirectDecode<T>(buf->data(), static_cast<int>(buf->size()), decoded.data());
    } else if constexpr (DbpIsReaderSide(kAblate)) {
      DbpReaderSideDecode<T, kAblate>(buf->data(), static_cast<int>(buf->size()),
                                      decoded.data());
    } else {
      DbpAblatedDecode<T, kAblate, kHeapReader>(
          buf->data(), static_cast<int>(buf->size()), decoded.data());
    }
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(buf->size());
  if constexpr (DbpDoesCoalesce(kAblate)) {
    state.counters["values_per_unpack_call"] =
        DbpMeanValuesPerUnpackCall<T>(buf->data(), static_cast<int>(buf->size()));
    state.counters["values_per_unpack_call_plain"] =
        static_cast<double>(values_per_block / mini_blocks_per_block);
  }
}

// Ablation arms, all at Arrow's default int32 geometry (128 values per block,
// 4 miniblocks, so 32 values per unpack call).
static void BM_DbpAbFull(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull>(state, gen, 128, 4);
}
static void BM_DbpAbNoSum(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kNoSum>(state, gen, 128, 4);
}
static void BM_DbpAbNoUnpack(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kNoUnpack>(state, gen, 128, 4);
}
static void BM_DbpAbHeaderOnly(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kHeaderOnly>(state, gen, 128, 4);
}

// Same work as BM_DbpAbFull with the bit reader placed on the heap, isolating
// how much of the shadow-versus-shipping gap that placement accounts for.
static void BM_DbpAbFullHeapReader(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull, /*kHeapReader=*/true>(state, gen, 128, 4);
}

// Reader-side arms, at Arrow's default 128/4 geometry so they are directly
// comparable with BM_DbpAbFull.
static void BM_DbpRsCoalesce(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kCoalesce>(state, gen, 128, 4);
}
static void BM_DbpRsBlockSum(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kBlockSum>(state, gen, 128, 4);
}
static void BM_DbpRsBoth(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kBoth>(state, gen, 128, 4);
}
static void BM_DbpRsDirect(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kDirect>(state, gen, 128, 4);
}

// Geometry sweep, full decode each time. 128/4 is Arrow's default and the
// control; the rest vary how many values one unpack call covers and how often a
// block header is parsed.
static void BM_DbpGeom128x4(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull>(state, gen, 128, 4);
}
static void BM_DbpGeom128x1(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull>(state, gen, 128, 1);
}
static void BM_DbpGeom1024x32(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull>(state, gen, 1024, 32);
}
static void BM_DbpGeom1024x8(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull>(state, gen, 1024, 8);
}
static void BM_DbpGeom1024x1(benchmark::State& state, Gen32 gen) {
  DbpAblateImpl<int32_t, DbpAblate::kFull>(state, gen, 1024, 1);
}

// ============================================================================
// Lane-parallel delta: the +32 stride against the paper's lane assignment
// ============================================================================
//
// Both replace DELTA_BINARY_PACKED's 1024-long dependency chain with 32
// independent chains of 32. They differ only in which value a lane's chain
// steps back to, and that is what decides the compression ratio:
//
//   LaneDelta   lane l holds l, 32+l, ..., 992+l -- the in-lane predecessor is
//               32 positions back, so the differences are wider than the
//               format's. Container order is file order, so no permutation.
//   Tpose       lane l holds the contiguous run [32l, 32l+32) -- the in-lane
//               predecessor is the immediately preceding value, so the
//               differences are exactly the format's. Decode ends transposed.
//
// Tpose comes in two base codings (32 entry points per block stored raw, or
// delta-coded across lanes) and, for the decode side, with and without the
// transpose back to file order. Only the repairing arm is a conforming decoder;
// the other one isolates what the transpose costs.

using ::arrow::util::fastlanes::TransposedBaseCoding;
using ::arrow::util::fastlanes::TransposedRepair;

static void BM_LaneDeltaDecode(benchmark::State& state, Gen32 gen) {
  namespace fl = ::arrow::util::fastlanes;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  std::vector<uint8_t> buf(fl::LaneDeltaMaxEncodedSize(num_values));
  const size_t comp_size = fl::LaneDeltaEncode<fl::LaneDeltaOrder::kInterleaved>(
      values.data(), num_values, buf.data());
  ARROW_CHECK_GT(comp_size, 0);

  std::vector<int32_t> decoded(num_values);
  fl::LaneDeltaDecode<fl::LaneDeltaOrder::kInterleaved>(buf.data(), num_values,
                                                        decoded.data());
  ARROW_CHECK(decoded == values) << "lane delta round trip failed";

  for (auto _ : state) {
    fl::LaneDeltaDecode<fl::LaneDeltaOrder::kInterleaved>(buf.data(), num_values,
                                                          decoded.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// ============================================================================
// Lane-parallel delta through the decoder interface
// ============================================================================
//
// BM_TposeFusedDecode below measures the kernel: it is handed a payload and told
// how many values it holds. These two arms go through the encoder and decoder
// the library registers for the encoding, which is the same layout, base coding
// and repair that arm uses, so they pay for page framing, for SetData, and for
// reading the value count out of the page. That makes them comparable with
// BM_DeltaBitPackEncode / BM_DeltaBitPackDecode, which is the pair libparquet
// ships, and it is the comparison a margin should be quoted over -- decoder
// against decoder rather than kernel against decoder. Dividing one by the other
// prices the framing.

static void BM_TposeApiDecode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::LANE_DELTA);
  encoder->Put(values.data(), static_cast<int>(num_values));
  auto buf = encoder->FlushValues();
  const int64_t comp_size = buf->size();

  std::vector<int32_t> decoded(num_values);
  auto decoder = MakeTypedDecoder<Int32Type>(Encoding::LANE_DELTA);
  decoder->SetData(static_cast<int>(num_values), buf->data(),
                   static_cast<int>(buf->size()));
  ARROW_CHECK_EQ(decoder->Decode(decoded.data(), static_cast<int>(num_values)),
                 static_cast<int>(num_values));
  ARROW_CHECK(decoded == values) << "lane delta decoder round trip failed";

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

static void BM_TposeApiEncode(benchmark::State& state, Gen32 gen) {
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  int64_t comp_size = 0;
  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::LANE_DELTA);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(num_values));
    auto buf = encoder->FlushValues();
    comp_size = buf->size();
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

// The values a decoder that stops in transposed order should produce.
static std::vector<int32_t> TransposeReference(const std::vector<int32_t>& in) {
  namespace fl = ::arrow::util::fastlanes;
  std::vector<int32_t> out(in.size());
  const size_t nblocks = in.size() / fl::kBlockSize;
  for (size_t b = 0; b < nblocks; ++b) {
    for (size_t t = 0; t < fl::kBlockSize; ++t) {
      out[b * fl::kBlockSize + t] = in[b * fl::kBlockSize + fl::TransposedSource(t)];
    }
  }
  return out;
}

template <TransposedBaseCoding kBases, TransposedRepair kRepair>
static void TposeDecodeImpl(benchmark::State& state, Gen32 gen) {
  namespace fl = ::arrow::util::fastlanes;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  std::vector<uint8_t> buf(fl::TransposedMaxEncodedSize(num_values));
  const size_t comp_size =
      fl::TransposedDeltaEncode<kBases>(values.data(), num_values, buf.data());
  ARROW_CHECK_GT(comp_size, 0);

  std::vector<int32_t> decoded(num_values);
  fl::TransposedDeltaDecode<kBases, kRepair>(buf.data(), num_values, decoded.data());
  if constexpr (kRepair != TransposedRepair::kNone) {
    ARROW_CHECK(decoded == values) << "transposed delta round trip failed";
  } else {
    ARROW_CHECK(decoded == TransposeReference(values))
        << "transposed delta round trip failed in transposed order";
  }

  for (auto _ : state) {
    fl::TransposedDeltaDecode<kBases, kRepair>(buf.data(), num_values, decoded.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}

static void BM_TposeRawDecode(benchmark::State& state, Gen32 gen) {
  TposeDecodeImpl<TransposedBaseCoding::kRaw, TransposedRepair::kSeparate>(state, gen);
}
static void BM_TposePackedDecode(benchmark::State& state, Gen32 gen) {
  TposeDecodeImpl<TransposedBaseCoding::kPacked, TransposedRepair::kSeparate>(state, gen);
}
// The conforming decoder: adjacent deltas, file order out, one pass over the block.
static void BM_TposeFusedDecode(benchmark::State& state, Gen32 gen) {
  TposeDecodeImpl<TransposedBaseCoding::kPacked, TransposedRepair::kFused>(state, gen);
}
// Neither of these is a conforming decoder: they leave the block transposed.
// Raw-vs-Packed at fixed repair isolates the base stream, and NoRepair-vs-not at
// fixed base coding isolates the 32x32 transpose.
static void BM_TposeNoRepairDecode(benchmark::State& state, Gen32 gen) {
  TposeDecodeImpl<TransposedBaseCoding::kPacked, TransposedRepair::kNone>(state, gen);
}
static void BM_TposeRawNoRepairDecode(benchmark::State& state, Gen32 gen) {
  TposeDecodeImpl<TransposedBaseCoding::kRaw, TransposedRepair::kNone>(state, gen);
}

// ============================================================================
// Interleaved-layout PFOR (no delta chain to break)
// ============================================================================
//
// BM_PforDecode is Arrow's shipped sequential decoder -- the layout the
// format specifies today. These two swap only the bit-unpack kernel for the
// FastLanes container, keeping the same per-block frame-of-reference and the
// same real dataset columns, so the margin against BM_PforDecode isolates
// the layout question on data instead of on synthetic per-width residuals.
// The second arm adds the paper's lane assignment and the gather needed to
// undo it, which fastlanes_kernels_internal.h's header comment argues plain
// PFOR has nothing to buy with -- this prices that argument on real columns.

using ::arrow::util::fastlanes::InterleavedPforOrder;

template <InterleavedPforOrder kOrder>
static void InterleavedPforDecodeImpl(benchmark::State& state, Gen32 gen) {
  namespace fl = ::arrow::util::fastlanes;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  const int64_t uncompressed_size = num_values * sizeof(int32_t);

  std::vector<uint8_t> buf(fl::InterleavedPforMaxEncodedSize(num_values));
  const size_t comp_size =
      fl::InterleavedPforEncode<kOrder>(values.data(), num_values, buf.data());
  ARROW_CHECK_GT(comp_size, 0);

  std::vector<int32_t> decoded(num_values);
  fl::InterleavedPforDecode<kOrder>(buf.data(), num_values, decoded.data());
  ARROW_CHECK(decoded == values) << "interleaved pfor round trip failed";

  for (auto _ : state) {
    fl::InterleavedPforDecode<kOrder>(buf.data(), num_values, decoded.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * uncompressed_size);
  state.SetItemsProcessed(state.iterations() * num_values);
  state.counters["compression_ratio"] =
      static_cast<double>(uncompressed_size) / static_cast<double>(comp_size);
}
static void BM_InterleavedPforDecode(benchmark::State& state, Gen32 gen) {
  InterleavedPforDecodeImpl<InterleavedPforOrder::kFileOrder>(state, gen);
}
static void BM_InterleavedPforFlOrderDecode(benchmark::State& state, Gen32 gen) {
  InterleavedPforDecodeImpl<InterleavedPforOrder::kFlOrder>(state, gen);
}

static void BM_LaneDeltaEncode(benchmark::State& state, Gen32 gen) {
  namespace fl = ::arrow::util::fastlanes;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  std::vector<uint8_t> buf(fl::LaneDeltaMaxEncodedSize(num_values));
  size_t comp_size = 0;
  for (auto _ : state) {
    comp_size = fl::LaneDeltaEncode<fl::LaneDeltaOrder::kInterleaved>(
        values.data(), num_values, buf.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * num_values * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * num_values);
  const double raw = static_cast<double>(num_values * sizeof(int32_t));
  state.counters["compression_ratio"] = raw / static_cast<double>(comp_size);
}

static void BM_TposePackedEncode(benchmark::State& state, Gen32 gen) {
  namespace fl = ::arrow::util::fastlanes;
  const int64_t num_values = state.range(0);
  auto values = gen(num_values);
  std::vector<uint8_t> buf(fl::TransposedMaxEncodedSize(num_values));
  size_t comp_size = 0;
  for (auto _ : state) {
    comp_size = fl::TransposedDeltaEncode<TransposedBaseCoding::kPacked>(
        values.data(), num_values, buf.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * num_values * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * num_values);
  const double raw = static_cast<double>(num_values * sizeof(int32_t));
  state.counters["compression_ratio"] = raw / static_cast<double>(comp_size);
}

// ============================================================================
// Benchmark Registration
// ============================================================================

static void CustomArgs(benchmark::internal::Benchmark* b) { b->Arg(102400); }

// Macro to register all algorithms for a given dataset
#define REGISTER_DATASET(Name, GenFunc)                                            \
  BENCHMARK_CAPTURE(BM_PforEncode, Name, &GenFunc)->Apply(CustomArgs);             \
  BENCHMARK_CAPTURE(BM_PforDecode, Name, &GenFunc)->Apply(CustomArgs);             \
  BENCHMARK_CAPTURE(BM_DeltaBitPackEncode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_DeltaBitPackDecode, Name, &GenFunc)->Apply(CustomArgs);     \
  BENCHMARK_CAPTURE(BM_DbpAbFull, Name, &GenFunc)->Apply(CustomArgs);              \
  BENCHMARK_CAPTURE(BM_DbpAbNoSum, Name, &GenFunc)->Apply(CustomArgs);             \
  BENCHMARK_CAPTURE(BM_DbpAbNoUnpack, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_DbpAbHeaderOnly, Name, &GenFunc)->Apply(CustomArgs);        \
  BENCHMARK_CAPTURE(BM_DbpAbFullHeapReader, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_DbpRsCoalesce, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_DbpRsBlockSum, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_DbpRsBoth, Name, &GenFunc)->Apply(CustomArgs);              \
  BENCHMARK_CAPTURE(BM_DbpRsDirect, Name, &GenFunc)->Apply(CustomArgs);            \
  BENCHMARK_CAPTURE(BM_DbpGeom128x1, Name, &GenFunc)->Apply(CustomArgs);           \
  BENCHMARK_CAPTURE(BM_DbpGeom1024x32, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_DbpGeom1024x8, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_DbpGeom1024x1, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_InterleavedPforDecode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_InterleavedPforFlOrderDecode, Name, &GenFunc)             \
      ->Apply(CustomArgs);                                                      \
  BENCHMARK_CAPTURE(BM_LaneDeltaDecode, Name, &GenFunc)->Apply(CustomArgs);        \
  BENCHMARK_CAPTURE(BM_TposeApiDecode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_TposeApiEncode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_TposeRawDecode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_TposePackedDecode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_TposeFusedDecode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_TposeNoRepairDecode, Name, &GenFunc)->Apply(CustomArgs);    \
  BENCHMARK_CAPTURE(BM_TposeRawNoRepairDecode, Name, &GenFunc)->Apply(CustomArgs); \
  BENCHMARK_CAPTURE(BM_LaneDeltaEncode, Name, &GenFunc)->Apply(CustomArgs);        \
  BENCHMARK_CAPTURE(BM_TposePackedEncode, Name, &GenFunc)->Apply(CustomArgs);      \
  BENCHMARK_CAPTURE(BM_PlainZstdEncode, Name, &GenFunc)->Apply(CustomArgs);        \
  BENCHMARK_CAPTURE(BM_PlainZstdDecode, Name, &GenFunc)->Apply(CustomArgs);        \
  BENCHMARK_CAPTURE(BM_PlainLz4Encode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_PlainLz4Decode, Name, &GenFunc)->Apply(CustomArgs);         \
  BENCHMARK_CAPTURE(BM_RleBitPackEncode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_RleBitPackDecode, Name, &GenFunc)->Apply(CustomArgs);       \
  BENCHMARK_CAPTURE(BM_BssZstdEncode, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_BssZstdDecode, Name, &GenFunc)->Apply(CustomArgs);          \
  BENCHMARK_CAPTURE(BM_BssLz4Encode, Name, &GenFunc)->Apply(CustomArgs);           \
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

// Correlated columns. DELTA_BINARY_PACKED is only used on data of this shape,
// so any claim about its decode cost has to be measured here and not only on
// the independently-drawn columns above.
REGISTER_DATASET(SortedUnixTime, GenSortedUnixTime)
REGISTER_DATASET(SortedKeyDups, GenSortedKeyDups)
REGISTER_DATASET(MonotoneRowId, GenMonotoneRowId)
REGISTER_DATASET(NearSortedUnixTime, GenNearSortedUnixTime)
// int64 / BIGINT datasets (8-byte signed values)
REGISTER_DATASET64(TsNanos, GenTsNanos)
REGISTER_DATASET64(OrderKey, GenOrderKey)
REGISTER_DATASET64(PriceMicros, GenPriceMicros)
REGISTER_DATASET64(SnowflakeId, GenSnowflakeId)
REGISTER_DATASET64(ByteCount, GenByteCount)

// Columns with structure between neighbouring values (see delta_shapes above),
// registered at both widths from the same distribution.
#define REGISTER_DELTA_SHAPE(Name, GenFunc) \
  REGISTER_DATASET(Name, GenFunc<int32_t>)  \
  REGISTER_DATASET64(Name, GenFunc<int64_t>)

REGISTER_DELTA_SHAPE(TrendJitter, delta_shapes::GenTrendJitter)
REGISTER_DELTA_SHAPE(Sawtooth, delta_shapes::GenSawtooth)
REGISTER_DELTA_SHAPE(MeasurementSeries, delta_shapes::GenMeasurement)
REGISTER_DELTA_SHAPE(SortedKeys, delta_shapes::GenSortedKeys)
REGISTER_DELTA_SHAPE(SensorDropouts, delta_shapes::GenSensorDropouts)
REGISTER_DELTA_SHAPE(IdsWithGaps, delta_shapes::GenIdsWithGaps)
REGISTER_DELTA_SHAPE(RandomWalk, delta_shapes::GenRandomWalk)
REGISTER_DELTA_SHAPE(EventMillis, delta_shapes::GenEventMillis)
REGISTER_DELTA_SHAPE(LowSentinel, delta_shapes::GenLowSentinel)
REGISTER_DELTA_SHAPE(Bimodal, delta_shapes::GenBimodal)

}  // namespace
}  // namespace parquet
