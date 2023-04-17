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
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/formatting.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace internal {

template <typename c_int>
static std::vector<std::string> MakeIntStrings(int32_t num_items) {
  using c_int_limits = std::numeric_limits<c_int>;
  std::vector<std::string> base_strings = {"0",
                                           "5",
                                           c_int_limits::is_signed ? "-12" : "12",
                                           "34",
                                           "99",
                                           c_int_limits::is_signed ? "-111" : "111",
                                           std::to_string(c_int_limits::min()),
                                           std::to_string(c_int_limits::max())};
  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

template <typename c_int>
static std::vector<std::string> MakeHexStrings(int32_t num_items) {
  int32_t num_bytes = sizeof(c_int);
  const char* kAsciiTable = "0123456789ABCDEF";
  std::vector<char> large_hex_chars(num_bytes * 2 + 2);
  large_hex_chars[0] = '0';
  large_hex_chars[1] = 'x';
  for (int32_t i = 0; i < num_bytes * 2; ++i) {
    large_hex_chars[i + 2] = kAsciiTable[i];
  }
  std::string large_hex(&large_hex_chars[0], large_hex_chars.size());

  std::vector<std::string> base_strings = {"0x0", "0xA5", "0x5E", large_hex};
  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

static std::vector<std::string> MakeFloatStrings(int32_t num_items) {
  std::vector<std::string> base_strings = {"0.0",         "5",        "-12.3",
                                           "98765430000", "3456.789", "0.0012345",
                                           "2.34567e8",   "-5.67e-8"};
  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

static std::vector<std::string> MakeTimestampStrings(int32_t num_items) {
  std::vector<std::string> base_strings = {"2018-11-13 17:11:10", "2018-11-13 11:22:33",
                                           "2016-02-29 11:22:33"};

  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

template <typename c_int, typename c_int_limits = std::numeric_limits<c_int>>
static typename std::enable_if<c_int_limits::is_signed, std::vector<c_int>>::type
MakeInts(int32_t num_items) {
  std::vector<c_int> out;
  // C++ doesn't guarantee that all integer types support std::uniform_int_distribution,
  // so use a known type (int64_t)
  randint<int64_t, c_int>(num_items, c_int_limits::min(), c_int_limits::max(), &out);
  return out;
}

template <typename c_int, typename c_int_limits = std::numeric_limits<c_int>>
static typename std::enable_if<!c_int_limits::is_signed, std::vector<c_int>>::type
MakeInts(int32_t num_items) {
  std::vector<c_int> out;
  // See above.
  randint<uint64_t, c_int>(num_items, c_int_limits::min(), c_int_limits::max(), &out);
  return out;
}

template <typename c_float>
static std::vector<c_float> MakeFloats(int32_t num_items) {
  std::vector<c_float> out;
  random_real<double, c_float>(num_items, /*seed =*/42, -1e10, 1e10, &out);
  return out;
}

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void IntegerParsing(benchmark::State& state) {  // NOLINT non-const reference
  auto strings = MakeIntStrings<C_TYPE>(1000);

  while (state.KeepRunning()) {
    C_TYPE total = 0;
    for (const auto& s : strings) {
      C_TYPE value;
      if (!ParseValue<ARROW_TYPE>(s.data(), s.length(), &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total = static_cast<C_TYPE>(total + value);
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void HexParsing(benchmark::State& state) {  // NOLINT non-const reference
  auto strings = MakeHexStrings<C_TYPE>(1000);

  while (state.KeepRunning()) {
    C_TYPE total = 0;
    for (const auto& s : strings) {
      C_TYPE value;
      if (!ParseValue<ARROW_TYPE>(s.data(), s.length(), &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total = static_cast<C_TYPE>(total + value);
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void FloatParsing(benchmark::State& state) {  // NOLINT non-const reference
  auto strings = MakeFloatStrings(1000);

  while (state.KeepRunning()) {
    C_TYPE total = 0;
    for (const auto& s : strings) {
      C_TYPE value;
      if (!ParseValue<ARROW_TYPE>(s.data(), s.length(), &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total += value;
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

static void BenchTimestampParsing(
    benchmark::State& state, TimeUnit::type unit,
    const TimestampParser& parser) {  // NOLINT non-const reference
  using c_type = TimestampType::c_type;

  auto strings = MakeTimestampStrings(1000);

  for (auto _ : state) {
    c_type total = 0;
    for (const auto& s : strings) {
      c_type value;
      if (!parser(s.data(), s.length(), unit, &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total += value;
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

template <TimeUnit::type UNIT>
static void TimestampParsingISO8601(
    benchmark::State& state) {  // NOLINT non-const reference
  auto parser = TimestampParser::MakeISO8601();
  BenchTimestampParsing(state, UNIT, *parser);
}

template <TimeUnit::type UNIT>
static void TimestampParsingStrptime(
    benchmark::State& state) {  // NOLINT non-const reference
  auto parser = TimestampParser::MakeStrptime("%Y-%m-%d %H:%M:%S");
  BenchTimestampParsing(state, UNIT, *parser);
}

struct DummyAppender {
  Status operator()(std::string_view v) {
    if (pos_ >= static_cast<int32_t>(v.size())) {
      pos_ = 0;
    }
    total_ += v[pos_++];
    return Status::OK();
  }

  int64_t total_ = 0;
  int32_t pos_ = 0;
};

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void IntegerFormatting(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<C_TYPE> values = MakeInts<C_TYPE>(1000);
  StringFormatter<ARROW_TYPE> formatter;

  while (state.KeepRunning()) {
    DummyAppender appender;
    for (const auto value : values) {
      ABORT_NOT_OK(formatter(value, appender));
    }
    benchmark::DoNotOptimize(appender.total_);
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void FloatFormatting(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<C_TYPE> values = MakeFloats<C_TYPE>(1000);
  StringFormatter<ARROW_TYPE> formatter;

  while (state.KeepRunning()) {
    DummyAppender appender;
    for (const auto value : values) {
      ABORT_NOT_OK(formatter(value, appender));
    }
    benchmark::DoNotOptimize(appender.total_);
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

BENCHMARK_TEMPLATE(IntegerParsing, Int8Type);
BENCHMARK_TEMPLATE(IntegerParsing, Int16Type);
BENCHMARK_TEMPLATE(IntegerParsing, Int32Type);
BENCHMARK_TEMPLATE(IntegerParsing, Int64Type);
BENCHMARK_TEMPLATE(IntegerParsing, UInt8Type);
BENCHMARK_TEMPLATE(IntegerParsing, UInt16Type);
BENCHMARK_TEMPLATE(IntegerParsing, UInt32Type);
BENCHMARK_TEMPLATE(IntegerParsing, UInt64Type);

BENCHMARK_TEMPLATE(HexParsing, Int8Type);
BENCHMARK_TEMPLATE(HexParsing, Int16Type);
BENCHMARK_TEMPLATE(HexParsing, Int32Type);
BENCHMARK_TEMPLATE(HexParsing, Int64Type);
BENCHMARK_TEMPLATE(HexParsing, UInt8Type);
BENCHMARK_TEMPLATE(HexParsing, UInt16Type);
BENCHMARK_TEMPLATE(HexParsing, UInt32Type);
BENCHMARK_TEMPLATE(HexParsing, UInt64Type);

BENCHMARK_TEMPLATE(FloatParsing, FloatType);
BENCHMARK_TEMPLATE(FloatParsing, DoubleType);

BENCHMARK_TEMPLATE(TimestampParsingISO8601, TimeUnit::SECOND);
BENCHMARK_TEMPLATE(TimestampParsingISO8601, TimeUnit::MILLI);
BENCHMARK_TEMPLATE(TimestampParsingISO8601, TimeUnit::MICRO);
BENCHMARK_TEMPLATE(TimestampParsingISO8601, TimeUnit::NANO);
BENCHMARK_TEMPLATE(TimestampParsingStrptime, TimeUnit::MILLI);

BENCHMARK_TEMPLATE(IntegerFormatting, Int8Type);
BENCHMARK_TEMPLATE(IntegerFormatting, Int16Type);
BENCHMARK_TEMPLATE(IntegerFormatting, Int32Type);
BENCHMARK_TEMPLATE(IntegerFormatting, Int64Type);
BENCHMARK_TEMPLATE(IntegerFormatting, UInt8Type);
BENCHMARK_TEMPLATE(IntegerFormatting, UInt16Type);
BENCHMARK_TEMPLATE(IntegerFormatting, UInt32Type);
BENCHMARK_TEMPLATE(IntegerFormatting, UInt64Type);

BENCHMARK_TEMPLATE(FloatFormatting, FloatType);
BENCHMARK_TEMPLATE(FloatFormatting, DoubleType);

}  // namespace internal
}  // namespace arrow
