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

// The integer column corpus the PFOR comparison benchmarks generate: 43 int32
// columns, a few int64 ones, and a group whose neighbouring values carry
// structure. It lives in its own header because two harnesses generate the same
// columns -- pfor_comparison_benchmark.cc in this directory, and the standalone
// layout harness under fl5_corpus/ -- and a figure from one is only comparable
// with a figure from the other if both read from one definition. Nothing here
// depends on anything but the standard library, so a harness that wants the
// corpus does not have to pull in Parquet.
//
// The columns are synthetic. Each generator is shaped after a distribution seen
// in ClickBench, TPC-DS, TPC-H or the NYC taxi set; none of them loads a record
// from those datasets. Bit-unpacking throughput is data-independent at a fixed
// width, so that substitution is harmless for a timing figure. It is not
// harmless for any claim about how wide a column packs, since the generator's
// autocorrelation is chosen rather than observed.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

namespace parquet {
namespace corpus {

inline std::vector<int32_t> GenClientIP(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(101);
  std::uniform_int_distribution<uint32_t> dist(0x0A000000, 0xDFFFFFFF);
  for (auto& x : v) x = static_cast<int32_t>(dist(rng));
  return v;
}

inline std::vector<int32_t> GenUrlRegionID(int64_t n) {
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

inline std::vector<int32_t> GenCounterID(int64_t n) {
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

inline std::vector<int32_t> GenEventDate(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(104);
  const int32_t dates[] = {19691, 19692, 19693, 19694, 19695};
  std::uniform_int_distribution<int> idx(0, 4);
  for (auto& x : v) x = dates[idx(rng)];
  return v;
}

inline std::vector<int32_t> GenEventTime(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(105);
  const int32_t base = 1704067200;  // 2024-01-01
  std::uniform_int_distribution<int32_t> offset(0, 86399);
  for (auto& x : v) x = base + offset(rng);
  return v;
}

inline std::vector<int32_t> GenGoodEvent(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(106);
  std::uniform_int_distribution<int> dist(0, 99);
  for (auto& x : v) x = (dist(rng) < 95) ? 1 : 0;
  return v;
}

inline std::vector<int32_t> GenHID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(107);
  std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                              std::numeric_limits<int32_t>::max());
  for (auto& x : v) x = dist(rng);
  return v;
}

inline std::vector<int32_t> GenHitColor(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(108);
  const int32_t colors[] = {1, 2, 3, 4, 5};
  std::uniform_int_distribution<int> idx(0, 4);
  for (auto& x : v) x = colors[idx(rng)];
  return v;
}

inline std::vector<int32_t> GenIPNetworkID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(109);
  std::uniform_int_distribution<int32_t> dist(1, 10000);
  for (auto& x : v) x = dist(rng);
  return v;
}

inline std::vector<int32_t> GenJavaEnable(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(110);
  std::uniform_int_distribution<int> dist(0, 99);
  for (auto& x : v) x = (dist(rng) < 85) ? 1 : 0;
  return v;
}

inline std::vector<int32_t> GenOS(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(111);
  std::uniform_int_distribution<int32_t> dist(1, 20);
  for (auto& x : v) x = dist(rng);
  return v;
}

inline std::vector<int32_t> GenResolution(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(112);
  const int32_t resolutions[] = {360,  480,  600,  720,  768,  800,  900,
                                 1024, 1050, 1080, 1200, 1440, 1600, 2160};
  std::uniform_int_distribution<int> idx(0, 13);
  for (auto& x : v) x = resolutions[idx(rng)];
  return v;
}

inline std::vector<int32_t> GenTrafficSourceID(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(113);
  std::uniform_int_distribution<int32_t> dist(0, 10);
  for (auto& x : v) x = dist(rng);
  return v;
}

inline std::vector<int32_t> GenUserAgent(int64_t n) {
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

// ----------------------------------------------------------------------
// Data Generators — TPC-DS (4 most queried columns from store_sales)

inline std::vector<int32_t> GenTpcdsSoldDateSk(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 2450815;
  std::mt19937 rng(201);
  std::uniform_int_distribution<int32_t> dist(0, 1820);
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

inline std::vector<int32_t> GenTpcdsStoreSk(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(202);
  std::uniform_int_distribution<int32_t> dist(1, 1000);
  for (auto& x : v) x = dist(rng);
  return v;
}

inline std::vector<int32_t> GenTpcdsItemSk(int64_t n) {
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

inline std::vector<int32_t> GenTpcdsQuantity(int64_t n) {
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
inline std::vector<int32_t> GenTpchLQuantity(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(301);
  std::uniform_int_distribution<int32_t> dist(1, 50);
  for (auto& x : v) x = dist(rng);
  return v;
}

// l_extendedprice (cents): l_quantity * p_retailprice. p_retailprice spans
// ~$900.00..$2099.00, so cents in [90000, 10495000]. Wide range, nonzero min.
inline std::vector<int32_t> GenTpchLExtendedPrice(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(302);
  std::uniform_int_distribution<int32_t> qty(1, 50);
  std::uniform_int_distribution<int32_t> retail_cents(90000, 209900);
  for (auto& x : v) x = qty(rng) * retail_cents(rng);
  return v;
}

// l_discount (x100): integer [0, 10] i.e. 0.00..0.10. Genuinely includes 0
// (0% discount is a real value), so this one legitimately starts at 0.
inline std::vector<int32_t> GenTpchLDiscount(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(303);
  std::uniform_int_distribution<int32_t> dist(0, 10);
  for (auto& x : v) x = dist(rng);
  return v;
}

// l_shipdate (days since 1970-01-01): 1992-01-01..1998-12 span. Base 8036,
// range ~7 years. Large nonzero min -> exercises frame-of-reference.
inline std::vector<int32_t> GenTpchLShipDate(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 8036;  // days since epoch for 1992-01-01
  std::mt19937 rng(304);
  std::uniform_int_distribution<int32_t> dist(0, 2557);
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

// --- TPC-DS (store_sales / date_dim) further top-queried numeric columns ---
// ss_customer_sk: surrogate key, uniform [1, 2,000,000]. Big range, min 1.
inline std::vector<int32_t> GenTpcdsCustomerSk(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(311);
  std::uniform_int_distribution<int32_t> dist(1, 2000000);
  for (auto& x : v) x = dist(rng);
  return v;
}

// ss_ext_sales_price (cents): skewed price, exponential mean ~$50, floored at
// $1.00 (a sale has a nonzero price), capped at $20,000. Long tail -> patches.
inline std::vector<int32_t> GenTpcdsExtSalesPrice(int64_t n) {
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
inline std::vector<int32_t> GenTpcdsNetProfit(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(313);
  std::uniform_int_distribution<int32_t> dist(-10000, 300000);
  for (auto& x : v) x = dist(rng);
  return v;
}

// d_year: queried date_dim year range [1998, 2003]. Low cardinality, min 1998.
inline std::vector<int32_t> GenTpcdsDYear(int64_t n) {
  std::vector<int32_t> v(n);
  std::mt19937 rng(314);
  std::uniform_int_distribution<int32_t> dist(1998, 2003);
  for (auto& x : v) x = dist(rng);
  return v;
}

// --- NYC yellow-taxi trip numeric columns ----------------------------------
// pickup timestamp (unix seconds): 2015-01, base 1,420,070,400 + ~31 days.
// Very large min -> frame-of-reference is essential.
inline std::vector<int32_t> GenTaxiPickupUnixTime(int64_t n) {
  std::vector<int32_t> v(n);
  const int32_t kBase = 1420070400;  // 2015-01-01 UTC
  std::mt19937 rng(321);
  std::uniform_int_distribution<int32_t> dist(0, 2678400);  // ~31 days
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

// trip_distance (x100 miles): exponential mean ~1.8 mi, floored at 0.10 mi
// (no zero-distance trips), capped at 100 mi. Long tail -> patches.
inline std::vector<int32_t> GenTaxiTripDistanceX100(int64_t n) {
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
inline std::vector<int32_t> GenTaxiFareCents(int64_t n) {
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

// ----------------------------------------------------------------------
// Data Generators — int64 / BIGINT columns (values that require 8 bytes,
// i.e. exceed the int32 range). Covers the common 64-bit analytic cases:
// nanosecond timestamps, large surrogate keys, scaled-decimal money, monotone
// IDs, and wide counters.

// Nanosecond epoch timestamp (Parquet TIMESTAMP(NANOS)): 2024-01-01 base plus
// up to ~1 day of jitter. Min ~1.70e18 -> very large frame of reference; raw
// values need ~61 bits.
inline std::vector<int64_t> GenTsNanos(int64_t n) {
  std::vector<int64_t> v(n);
  const int64_t kBase = 1704067200000000000LL;  // 2024-01-01T00:00:00Z in ns
  std::mt19937_64 rng(401);
  std::uniform_int_distribution<int64_t> off(0, 86399999999999LL);  // ~1 day
  for (auto& x : v) x = kBase + off(rng);
  return v;
}

// BIGINT surrogate / order key, uniform over [1, 10 billion] (exceeds 2^32).
inline std::vector<int64_t> GenOrderKey(int64_t n) {
  std::vector<int64_t> v(n);
  std::mt19937_64 rng(402);
  std::uniform_int_distribution<int64_t> dist(1, 10000000000LL);
  for (auto& x : v) x = dist(rng);
  return v;
}

// Money as int64 scaled decimal (micro-units): $0.01 .. ~$100k, skewed, with a
// nonzero floor at one cent.
inline std::vector<int64_t> GenPriceMicros(int64_t n) {
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

// ----------------------------------------------------------------------
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

// Event timestamps in seconds, arriving a few seconds apart. The common case
// for a table clustered or sorted on time.
inline std::vector<int32_t> GenSortedUnixTime(int64_t n) {
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
inline std::vector<int32_t> GenSortedKeyDups(int64_t n) {
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
inline std::vector<int32_t> GenMonotoneRowId(int64_t n) {
  std::vector<int32_t> v(n);
  int32_t k = 1;
  for (auto& x : v) x = k++;
  return v;
}

// Sorted except that 2% of rows arrive late, so a few deltas are large and
// negative. Tests whether one bit width per block survives outliers.
inline std::vector<int32_t> GenNearSortedUnixTime(int64_t n) {
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
inline std::vector<int64_t> GenSnowflakeId(int64_t n) {
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
inline std::vector<int64_t> GenByteCount(int64_t n) {
  std::vector<int64_t> v(n);
  std::mt19937_64 rng(405);
  std::uniform_int_distribution<int64_t> dist(1024, 5000000000000LL);
  for (auto& x : v) x = dist(rng);
  return v;
}

// ----------------------------------------------------------------------
// Data Generators — structure between neighbouring values

// The generators above are all either unordered or perfectly regular, so none
// of them separates an encoding that differences neighbouring values from one
// that packs them, and none of them puts a frame of reference anywhere but the
// minimum. These do: they are the columns on which PFOR's delta mode and
// DELTA_BINARY_PACKED make different choices.
//
// They are templates rather than a pair of per-width functions, which keeps a
// column's int32 and int64 registration on provably the same distribution. They
// live in their own namespace because two of them build on base distributions
// whose names are already taken above by columns drawn from different seeds --
// keeping them separate is what makes these figures comparable with
// pfor_benchmark.cc, which uses the same shapes.
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

}  // namespace corpus
}  // namespace parquet
