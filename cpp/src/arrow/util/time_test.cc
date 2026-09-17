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

#include <gtest/gtest.h>

#include <sstream>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/date_internal.h"
#include "arrow/util/time.h"

namespace arrow {
namespace util {

TEST(TimeTest, ChronoFormats) {
  namespace chrono = arrow::internal::chrono;
  using std::chrono::milliseconds;
  using std::chrono::minutes;
  EXPECT_EQ(chrono::format("%F", chrono::sys_days{}), "1970-01-01");
  EXPECT_EQ(chrono::format("%F %T", chrono::sys_time<milliseconds>{milliseconds{-1}}),
            "1969-12-31 23:59:59.999");
  EXPECT_EQ(chrono::format("%T", milliseconds{5400123}), "01:30:00.123");
  EXPECT_EQ(chrono::format("%T", milliseconds{-5400123}), "-01:30:00.123");
  EXPECT_EQ(chrono::format("%H%M", minutes{90}), "0130");
  EXPECT_EQ(chrono::format("%H%M", minutes{-90}), "-0130");
}

TEST(TimeTest, ReuseZonedFormat) {
  namespace chrono = arrow::internal::chrono;
  using std::chrono::microseconds;
  const arrow::internal::OffsetZone zone{std::chrono::minutes{60}};
  const chrono::ZonedFormat<microseconds> format{"{%F %T} %Q %q %J"};
  for (const auto& [count, expected] :
       {std::pair{1, "{1970-01-01 01:00:00.000001} 3600000001 \xC2\xB5s %J"},
        std::pair{-1, "{1970-01-01 00:59:59.999999} 3599999999 \xC2\xB5s %J"}}) {
    std::ostringstream out;
    out.imbue(std::locale::classic());
    const chrono::zoned_time<microseconds, arrow::internal::OffsetZone> value{
        zone, chrono::sys_time<microseconds>{microseconds{count}}};
    chrono::to_stream(out, format, value);
    EXPECT_EQ(out.str(), expected);
  }
}

TEST(TimeTest, ZonedFormatStreamState) {
  namespace chrono = arrow::internal::chrono;
  using std::chrono::seconds;
  const chrono::ZonedFormat<seconds> format{"%F %T"};
  const chrono::zoned_time<seconds, arrow::internal::OffsetZone> value{
      arrow::internal::OffsetZone{std::chrono::minutes{0}},
      chrono::sys_time<seconds>{seconds{0}}};
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << std::hex << std::showbase;
  out.precision(3);
  out.width(30);
  out.fill('*');
  const auto flags = out.flags();
  EXPECT_EQ(&chrono::to_stream(out, format, value), &out);
  EXPECT_EQ(out.str(), "1970-01-01 00:00:00");
  EXPECT_EQ(out.flags(), flags);
  EXPECT_EQ(out.precision(), 3);
  EXPECT_EQ(out.width(), 30);
  EXPECT_EQ(out.fill(), '*');

  // A streambuf with no put area rejects every write.
  class FailingBuffer : public std::streambuf {
  } buffer;
  std::ostream failing(&buffer);
  failing.exceptions(std::ios::badbit | std::ios::failbit);
  EXPECT_THROW(chrono::to_stream(failing, format, value), std::ios_base::failure);
}

TEST(TimeTest, ConvertTimestampValue) {
  auto convert = [](TimeUnit::type in, TimeUnit::type out, int64_t value) {
    return ConvertTimestampValue(timestamp(in), timestamp(out), value).ValueOrDie();
  };

  auto units = {
      TimeUnit::SECOND,
      TimeUnit::MILLI,
      TimeUnit::MICRO,
      TimeUnit::NANO,
  };

  // Test for identity
  for (auto unit : units) {
    EXPECT_EQ(convert(unit, unit, 0), 0);
    EXPECT_EQ(convert(unit, unit, INT64_MAX), INT64_MAX);
    EXPECT_EQ(convert(unit, unit, INT64_MIN), INT64_MIN);
  }

  EXPECT_EQ(convert(TimeUnit::SECOND, TimeUnit::MILLI, 2), 2000);
  EXPECT_EQ(convert(TimeUnit::SECOND, TimeUnit::MICRO, 2), 2000000);
  EXPECT_EQ(convert(TimeUnit::SECOND, TimeUnit::NANO, 2), 2000000000);

  EXPECT_EQ(convert(TimeUnit::MILLI, TimeUnit::SECOND, 7000), 7);
  EXPECT_EQ(convert(TimeUnit::MILLI, TimeUnit::MICRO, 7), 7000);
  EXPECT_EQ(convert(TimeUnit::MILLI, TimeUnit::NANO, 7), 7000000);

  EXPECT_EQ(convert(TimeUnit::MICRO, TimeUnit::SECOND, 4000000), 4);
  EXPECT_EQ(convert(TimeUnit::MICRO, TimeUnit::MILLI, 4000), 4);
  EXPECT_EQ(convert(TimeUnit::MICRO, TimeUnit::SECOND, 4000000), 4);

  EXPECT_EQ(convert(TimeUnit::NANO, TimeUnit::SECOND, 6000000000), 6);
  EXPECT_EQ(convert(TimeUnit::NANO, TimeUnit::MILLI, 6000000), 6);
  EXPECT_EQ(convert(TimeUnit::NANO, TimeUnit::MICRO, 6000), 6);
}

}  // namespace util
}  // namespace arrow
