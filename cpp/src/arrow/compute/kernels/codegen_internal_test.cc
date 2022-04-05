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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {
namespace internal {

TEST(TestDispatchBest, CastBinaryDecimalArgs) {
  std::vector<ValueDescr> args;
  std::vector<DecimalPromotion> modes = {
      DecimalPromotion::kAdd, DecimalPromotion::kMultiply, DecimalPromotion::kDivide};

  // Any float -> all float
  for (auto mode : modes) {
    args = {decimal128(3, 2), float64()};
    ASSERT_OK(CastBinaryDecimalArgs(mode, &args));
    AssertTypeEqual(args[0].type, float64());
    AssertTypeEqual(args[1].type, float64());
  }

  // Integer -> decimal with common scale
  args = {decimal128(1, 0), int64()};
  ASSERT_OK(CastBinaryDecimalArgs(DecimalPromotion::kAdd, &args));
  AssertTypeEqual(args[0].type, decimal128(1, 0));
  AssertTypeEqual(args[1].type, decimal128(19, 0));

  // Add: rescale so all have common scale
  args = {decimal128(3, 2), decimal128(3, -2)};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, ::testing::HasSubstr("Decimals with negative scales not supported"),
      CastBinaryDecimalArgs(DecimalPromotion::kAdd, &args));
}

TEST(TestDispatchBest, CastDecimalArgs) {
  std::vector<ValueDescr> args;

  // Any float -> all float
  args = {decimal128(3, 2), float64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, float64());
  AssertTypeEqual(args[1].type, float64());

  args = {float32(), float64(), decimal128(3, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, float64());
  AssertTypeEqual(args[1].type, float64());
  AssertTypeEqual(args[2].type, float64());

  // Promote to common decimal width
  args = {decimal128(3, 2), decimal256(3, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal256(3, 2));
  AssertTypeEqual(args[1].type, decimal256(3, 2));

  // Rescale so all have common scale/precision
  args = {decimal128(3, 2), decimal128(3, 0)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal128(5, 2));
  AssertTypeEqual(args[1].type, decimal128(5, 2));

  args = {decimal128(3, 2), decimal128(3, -2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal128(7, 2));
  AssertTypeEqual(args[1].type, decimal128(7, 2));

  args = {decimal128(3, 0), decimal128(3, 1), decimal128(3, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal128(5, 2));
  AssertTypeEqual(args[1].type, decimal128(5, 2));
  AssertTypeEqual(args[2].type, decimal128(5, 2));

  // Integer -> decimal with appropriate precision
  args = {decimal128(3, 0), int64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal128(19, 0));
  AssertTypeEqual(args[1].type, decimal128(19, 0));

  args = {decimal128(3, 1), int64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal128(20, 1));
  AssertTypeEqual(args[1].type, decimal128(20, 1));

  args = {decimal128(3, -1), int64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal128(19, 0));
  AssertTypeEqual(args[1].type, decimal128(19, 0));

  // Overflow decimal128 max precision -> promote to decimal256
  args = {decimal128(38, 0), decimal128(37, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal256(40, 2));
  AssertTypeEqual(args[1].type, decimal256(40, 2));

  // Overflow decimal256 max precision
  args = {decimal256(76, 0), decimal256(75, 1)};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Result precision (77) exceeds max precision of Decimal256 (76)"),
      CastDecimalArgs(args.data(), args.size()));

  // Incompatible, no cast
  args = {decimal256(3, 2), float64(), utf8()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(args[0].type, decimal256(3, 2));
  AssertTypeEqual(args[1].type, float64());
  AssertTypeEqual(args[2].type, utf8());
}

TEST(TestDispatchBest, CommonTemporal) {
  std::vector<ValueDescr> args;

  args = {timestamp(TimeUnit::SECOND), timestamp(TimeUnit::NANO)};
  AssertTypeEqual(timestamp(TimeUnit::NANO), CommonTemporal(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND, "UTC"), timestamp(TimeUnit::NANO, "UTC")};
  AssertTypeEqual(timestamp(TimeUnit::NANO, "UTC"),
                  CommonTemporal(args.data(), args.size()));
  args = {date32(), timestamp(TimeUnit::NANO)};
  AssertTypeEqual(timestamp(TimeUnit::NANO), CommonTemporal(args.data(), args.size()));
  args = {date64(), timestamp(TimeUnit::SECOND)};
  AssertTypeEqual(timestamp(TimeUnit::MILLI), CommonTemporal(args.data(), args.size()));
  args = {date32(), date32()};
  AssertTypeEqual(date32(), CommonTemporal(args.data(), args.size()));
  args = {date64(), date64()};
  AssertTypeEqual(date64(), CommonTemporal(args.data(), args.size()));
  args = {date32(), date64()};
  AssertTypeEqual(date64(), CommonTemporal(args.data(), args.size()));
  args = {};
  ASSERT_EQ(nullptr, CommonTemporal(args.data(), args.size()));
  args = {float64(), int32()};
  ASSERT_EQ(nullptr, CommonTemporal(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND), timestamp(TimeUnit::SECOND, "UTC")};
  ASSERT_EQ(nullptr, CommonTemporal(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND, "America/Phoenix"),
          timestamp(TimeUnit::SECOND, "UTC")};
  ASSERT_EQ(nullptr, CommonTemporal(args.data(), args.size()));
}

TEST(TestDispatchBest, CommonTemporalResolution) {
  std::vector<ValueDescr> args;
  std::string tz = "Pacific/Marquesas";

  args = {date32(), date32()};
  ASSERT_EQ(TimeUnit::SECOND, CommonTemporalResolution(args.data(), args.size()));
  args = {date32(), date64()};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {time32(TimeUnit::MILLI), date32()};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {time32(TimeUnit::MILLI), time32(TimeUnit::SECOND)};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {time32(TimeUnit::MILLI), time64(TimeUnit::MICRO)};
  ASSERT_EQ(TimeUnit::MICRO, CommonTemporalResolution(args.data(), args.size()));
  args = {time64(TimeUnit::NANO), time64(TimeUnit::MICRO)};
  ASSERT_EQ(TimeUnit::NANO, CommonTemporalResolution(args.data(), args.size()));
  args = {duration(TimeUnit::MILLI), duration(TimeUnit::MICRO)};
  ASSERT_EQ(TimeUnit::MICRO, CommonTemporalResolution(args.data(), args.size()));
  args = {duration(TimeUnit::MILLI), date32()};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {date64(), duration(TimeUnit::SECOND)};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {duration(TimeUnit::SECOND), time32(TimeUnit::SECOND)};
  ASSERT_EQ(TimeUnit::SECOND, CommonTemporalResolution(args.data(), args.size()));
  args = {duration(TimeUnit::SECOND), time64(TimeUnit::NANO)};
  ASSERT_EQ(TimeUnit::NANO, CommonTemporalResolution(args.data(), args.size()));
  args = {time64(TimeUnit::MICRO), duration(TimeUnit::NANO)};
  ASSERT_EQ(TimeUnit::NANO, CommonTemporalResolution(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND, tz), timestamp(TimeUnit::MICRO)};
  ASSERT_EQ(TimeUnit::MICRO, CommonTemporalResolution(args.data(), args.size()));
  args = {date32(), timestamp(TimeUnit::MICRO, tz)};
  ASSERT_EQ(TimeUnit::MICRO, CommonTemporalResolution(args.data(), args.size()));
  args = {timestamp(TimeUnit::MICRO, tz), date64()};
  ASSERT_EQ(TimeUnit::MICRO, CommonTemporalResolution(args.data(), args.size()));
  args = {time32(TimeUnit::MILLI), timestamp(TimeUnit::MICRO, tz)};
  ASSERT_EQ(TimeUnit::MICRO, CommonTemporalResolution(args.data(), args.size()));
  args = {timestamp(TimeUnit::MICRO, tz), time64(TimeUnit::NANO)};
  ASSERT_EQ(TimeUnit::NANO, CommonTemporalResolution(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND, tz), duration(TimeUnit::MILLI)};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND, "UTC"), timestamp(TimeUnit::SECOND, tz)};
  ASSERT_EQ(TimeUnit::SECOND, CommonTemporalResolution(args.data(), args.size()));
  args = {time32(TimeUnit::MILLI), duration(TimeUnit::SECOND)};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
  args = {time64(TimeUnit::MICRO), duration(TimeUnit::NANO)};
  ASSERT_EQ(TimeUnit::NANO, CommonTemporalResolution(args.data(), args.size()));
  args = {duration(TimeUnit::SECOND), int64()};
  ASSERT_EQ(TimeUnit::SECOND, CommonTemporalResolution(args.data(), args.size()));
  args = {duration(TimeUnit::MILLI), timestamp(TimeUnit::SECOND, tz)};
  ASSERT_EQ(TimeUnit::MILLI, CommonTemporalResolution(args.data(), args.size()));
}

TEST(TestDispatchBest, ReplaceTemporalTypes) {
  std::vector<ValueDescr> args;
  std::string tz = "Pacific/Marquesas";
  TimeUnit::type ty;

  args = {date32(), date32()};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, timestamp(TimeUnit::SECOND));
  AssertTypeEqual(args[1].type, timestamp(TimeUnit::SECOND));

  args = {date64(), time32(TimeUnit::SECOND)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, timestamp(TimeUnit::MILLI));
  AssertTypeEqual(args[1].type, time32(TimeUnit::MILLI));

  args = {duration(TimeUnit::SECOND), date64()};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, duration(TimeUnit::MILLI));
  AssertTypeEqual(args[1].type, timestamp(TimeUnit::MILLI));

  args = {timestamp(TimeUnit::MICRO, tz), timestamp(TimeUnit::NANO)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, timestamp(TimeUnit::NANO, tz));
  AssertTypeEqual(args[1].type, timestamp(TimeUnit::NANO));

  args = {timestamp(TimeUnit::MICRO, tz), time64(TimeUnit::NANO)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, timestamp(TimeUnit::NANO, tz));
  AssertTypeEqual(args[1].type, time64(TimeUnit::NANO));

  args = {timestamp(TimeUnit::SECOND, tz), date64()};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, timestamp(TimeUnit::MILLI, tz));
  AssertTypeEqual(args[1].type, timestamp(TimeUnit::MILLI));

  args = {timestamp(TimeUnit::SECOND, "UTC"), timestamp(TimeUnit::SECOND, tz)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, timestamp(TimeUnit::SECOND, "UTC"));
  AssertTypeEqual(args[1].type, timestamp(TimeUnit::SECOND, tz));

  args = {time32(TimeUnit::SECOND), duration(TimeUnit::SECOND)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, time32(TimeUnit::SECOND));
  AssertTypeEqual(args[1].type, duration(TimeUnit::SECOND));

  args = {time64(TimeUnit::MICRO), duration(TimeUnit::SECOND)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, time64(TimeUnit::MICRO));
  AssertTypeEqual(args[1].type, duration(TimeUnit::MICRO));

  args = {time32(TimeUnit::SECOND), duration(TimeUnit::NANO)};
  ty = CommonTemporalResolution(args.data(), args.size());
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(args[0].type, time64(TimeUnit::NANO));
  AssertTypeEqual(args[1].type, duration(TimeUnit::NANO));

  args = {duration(TimeUnit::SECOND), int64()};
  ReplaceTemporalTypes(CommonTemporalResolution(args.data(), args.size()), &args);
  AssertTypeEqual(args[0].type, duration(TimeUnit::SECOND));
  AssertTypeEqual(args[1].type, int64());
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
