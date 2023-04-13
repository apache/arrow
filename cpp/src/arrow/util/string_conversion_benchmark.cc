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

#include "arrow/util/unreachable.h"
#include "arrow/visit_data_inline.h"
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

#include "arrow/array/array_binary.h"
#include "arrow/array/builder_binary.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/formatting.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/value_parsing.h"

namespace arrow::internal {
namespace {

// Matrix of benchmarks:
//
// Direction:
// - STRING <-> RAW VIEWS
// - STRING <-> IO VIEWS
// - IO VIEWS <-> RAW VIEWS
//
// View length:
// - pure inline
// - pure non-inline
// - mixed with small mean length
// - mixed with large mean length
//
// Character buffer count:
// - ensure there are multiple 1MB buffers
// - baseline with only a single character buffer?
constexpr int kCharacterCount = (1 << 20) * 16;

// Null counts?
// Scrambled ordering?

enum {
  kStrings,
  kRawPointerViews,
  kIndexOffsetViews,
};
std::shared_ptr<DataType> DataTypeFor(decltype(kStrings) enm) {
  switch (enm) {
    case kStrings:
      return utf8();
    case kIndexOffsetViews:
      return utf8_view();
    case kRawPointerViews:
      return utf8_view(/*has_raw_pointers=*/true);
  }
  Unreachable();
}

enum {
  kAlwaysInlineable,
  kUsuallyInlineable,
  kShortButNeverInlineable,
  kLongAndSeldomInlineable,
  kLongAndNeverInlineable,
};

StringViewArray ToStringViewArray(const StringArray& arr) {
  auto header_buffer = AllocateBuffer(arr.length() * sizeof(StringHeader)).ValueOrDie();

  StringHeadersFromStrings(*arr.data(), header_buffer->mutable_data_as<StringHeader>());

  return {arr.length(),
          std::move(header_buffer),
          {arr.value_data()},
          arr.null_bitmap(),
          arr.null_count()};
}

StringArray ToStringArray(const StringViewArray& arr) {
  int64_t char_count = 0;
  for (size_t i = 2; i < arr.data()->buffers.size(); ++i) {
    char_count += arr.data()->buffers[i]->size();
  }

  auto offset_buffer = AllocateBuffer((arr.length() + 1) * sizeof(int32_t)).ValueOrDie();
  auto* offset = offset_buffer->mutable_data_as<int32_t>();
  offset[0] = 0;

  BufferBuilder char_buffer_builder;
  ABORT_NOT_OK(char_buffer_builder.Reserve(char_count));

  ABORT_NOT_OK(VisitArraySpanInline<StringViewType>(
      *arr.data(),
      [&](std::string_view v) {
        offset[1] = offset[0] + v.size();
        ++offset;
        return char_buffer_builder.Append(v);
      },
      [&] {
        offset[1] = offset[0];
        ++offset;
        return Status::OK();
      }));

  auto char_buffer = char_buffer_builder.Finish().ValueOrDie();

  return {arr.length(), std::move(offset_buffer), std::move(char_buffer),
          arr.null_bitmap(), arr.null_count()};
}

std::shared_ptr<Buffer> ToRawPointers(const StringViewArray& io) {
  auto raw_buf = AllocateBuffer(io.length() * sizeof(StringHeader)).ValueOrDie();
  auto st =
      SwapStringHeaderPointers(*io.data(), raw_buf->mutable_data_as<StringHeader>());
  ABORT_NOT_OK(st);
  return raw_buf;
}

std::shared_ptr<Buffer> ToIndexOffsets(const StringViewArray& raw) {
  auto io_buf = AllocateBuffer(raw.length() * sizeof(StringHeader)).ValueOrDie();
  auto st =
      SwapStringHeaderPointers(*raw.data(), io_buf->mutable_data_as<StringHeader>());
  ABORT_NOT_OK(st);
  return io_buf;
}

template <auto From, auto To, auto StringLengthsAre>
static void ConvertViews(benchmark::State& state) {  // NOLINT non-const reference
  auto [min_length, max_length] = [] {
    switch (StringLengthsAre) {
      case kAlwaysInlineable:
        return std::pair{0, 12};
      case kUsuallyInlineable:
        return std::pair{0, 16};
      case kShortButNeverInlineable:
        return std::pair{13, 30};
      case kLongAndSeldomInlineable:
        return std::pair{0, 256};
      case kLongAndNeverInlineable:
        return std::pair{13, 256};
    }
  }();

  auto num_items = kCharacterCount / max_length;

  auto from_type = DataTypeFor(From);
  auto to_type = DataTypeFor(To);

  auto from = random::GenerateArray(*field("", from_type,
                                           key_value_metadata({
                                               {"null_probability", "0"},
                                               {"min_length", std::to_string(min_length)},
                                               {"max_length", std::to_string(max_length)},
                                           })),
                                    num_items, 0xdeadbeef);

  uint64_t dummy = 0;
  for (auto _ : state) {
    if constexpr (From == kStrings && To == kIndexOffsetViews) {
      dummy += ToStringViewArray(checked_cast<const StringArray&>(*from)).length();
    }

    if constexpr (From == kIndexOffsetViews && To == kStrings) {
      dummy += ToStringArray(checked_cast<const StringViewArray&>(*from)).length();
    }

    if constexpr (From == kIndexOffsetViews && To == kRawPointerViews) {
      dummy += ToRawPointers(checked_cast<const StringViewArray&>(*from))->size();
    }

    if constexpr (From == kRawPointerViews && To == kIndexOffsetViews) {
      dummy += ToIndexOffsets(checked_cast<const StringViewArray&>(*from))->size();
    }

    benchmark::DoNotOptimize(dummy);
  }
  state.SetItemsProcessed(state.iterations() * num_items);
}

BENCHMARK_TEMPLATE(ConvertViews, kStrings, kIndexOffsetViews, kAlwaysInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kIndexOffsetViews, kUsuallyInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kIndexOffsetViews, kShortButNeverInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kIndexOffsetViews, kLongAndSeldomInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kIndexOffsetViews, kLongAndNeverInlineable);

BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kStrings, kAlwaysInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kStrings, kUsuallyInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kStrings, kShortButNeverInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kStrings, kLongAndSeldomInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kStrings, kLongAndNeverInlineable);

/*
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kRawPointerViews, kAlwaysInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kRawPointerViews, kUsuallyInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kRawPointerViews, kShortButNeverInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kRawPointerViews, kLongAndSeldomInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kStrings, kRawPointerViews, kLongAndNeverInlineable);

BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kStrings, kAlwaysInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kStrings, kUsuallyInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kStrings, kShortButNeverInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kStrings, kLongAndSeldomInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kStrings, kLongAndNeverInlineable);
  */

BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kIndexOffsetViews, kAlwaysInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kIndexOffsetViews, kUsuallyInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kIndexOffsetViews,
                   kShortButNeverInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kIndexOffsetViews,
                   kLongAndSeldomInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kRawPointerViews, kIndexOffsetViews,
                   kLongAndNeverInlineable);

BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kRawPointerViews, kAlwaysInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kRawPointerViews, kUsuallyInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kRawPointerViews,
                   kShortButNeverInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kRawPointerViews,
                   kLongAndSeldomInlineable);
BENCHMARK_TEMPLATE(ConvertViews, kIndexOffsetViews, kRawPointerViews,
                   kLongAndNeverInlineable);

}  // namespace
}  // namespace arrow::internal
