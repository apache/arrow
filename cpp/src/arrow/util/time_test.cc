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

#include "arrow/testing/gtest_util.h"
#include "arrow/util/time.h"

namespace arrow {
namespace util {

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
