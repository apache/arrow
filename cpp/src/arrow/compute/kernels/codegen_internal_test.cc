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

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/array_binary.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/datum.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/binary_view_util.h"

namespace arrow {
namespace compute {
namespace internal {

TEST(TestDispatchBest, CastBinaryDecimalArgs) {
  std::vector<TypeHolder> args;
  std::vector<DecimalPromotion> modes = {
      DecimalPromotion::kAdd, DecimalPromotion::kMultiply, DecimalPromotion::kDivide};

  // Any float -> all float
  for (auto mode : modes) {
    args = {decimal128(3, 2), float32(), float64()};
    ASSERT_OK(CastBinaryDecimalArgs(mode, &args));
    AssertTypeEqual(*args[0], *float64());
    AssertTypeEqual(*args[1], *float64());
  }

  // Integer -> decimal with common scale
  args = {decimal128(1, 0), int64()};
  ASSERT_OK(CastBinaryDecimalArgs(DecimalPromotion::kAdd, &args));
  AssertTypeEqual(*args[0], *decimal128(1, 0));
  AssertTypeEqual(*args[1], *decimal128(19, 0));

  // Add: rescale so all have common scale
  args = {decimal128(3, 2), decimal128(3, -2)};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, ::testing::HasSubstr("Decimals with negative scales not supported"),
      CastBinaryDecimalArgs(DecimalPromotion::kAdd, &args));

  // Non-castable -> unchanged
  for (const auto promotion :
       {DecimalPromotion::kAdd, DecimalPromotion::kMultiply, DecimalPromotion::kDivide}) {
    for (const auto& args : std::vector<std::vector<TypeHolder>>{
             {decimal128(3, 2), boolean()},
             {boolean(), decimal128(3, 2)},
             {decimal128(3, 2), utf8()},
             {utf8(), decimal128(3, 2)},
         }) {
      auto args_copy = args;
      ASSERT_OK(CastBinaryDecimalArgs(promotion, &args_copy));
      AssertTypeEqual(*args_copy[0], *args[0]);
      AssertTypeEqual(*args_copy[1], *args[1]);
    }
  }
}

TEST(TestDispatchBest, CastDecimalArgs) {
  std::vector<TypeHolder> args;

  // Any float -> all float
  args = {decimal128(3, 2), float64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *float64());
  AssertTypeEqual(*args[1], *float64());

  args = {float32(), float64(), decimal128(3, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *float64());
  AssertTypeEqual(*args[1], *float64());
  AssertTypeEqual(*args[2], *float64());

  // Promote to common decimal width
  args = {decimal128(3, 2), decimal256(3, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal256(3, 2));
  AssertTypeEqual(*args[1], *decimal256(3, 2));

  // Rescale so all have common scale/precision
  args = {decimal128(3, 2), decimal128(3, 0)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal128(5, 2));
  AssertTypeEqual(*args[1], *decimal128(5, 2));

  args = {decimal128(3, 2), decimal128(3, -2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal128(7, 2));
  AssertTypeEqual(*args[1], *decimal128(7, 2));

  args = {decimal128(3, 0), decimal128(3, 1), decimal128(3, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal128(5, 2));
  AssertTypeEqual(*args[1], *decimal128(5, 2));
  AssertTypeEqual(*args[2], *decimal128(5, 2));

  // Integer -> decimal with appropriate precision
  args = {decimal128(3, 0), int64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal128(19, 0));
  AssertTypeEqual(*args[1], *decimal128(19, 0));

  args = {decimal128(3, 1), int64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal128(20, 1));
  AssertTypeEqual(*args[1], *decimal128(20, 1));

  args = {decimal128(3, -1), int64()};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal128(19, 0));
  AssertTypeEqual(*args[1], *decimal128(19, 0));

  // Overflow decimal128 max precision -> promote to decimal256
  args = {decimal128(38, 0), decimal128(37, 2)};
  ASSERT_OK(CastDecimalArgs(args.data(), args.size()));
  AssertTypeEqual(*args[0], *decimal256(40, 2));
  AssertTypeEqual(*args[1], *decimal256(40, 2));

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
  AssertTypeEqual(*args[0], *decimal256(3, 2));
  AssertTypeEqual(*args[1], *float64());
  AssertTypeEqual(*args[2], *utf8());
}

TEST(TestDecimalPromotion, WidenDecimalToMaxPrecision) {
  std::shared_ptr<DataType> arg;
  std::shared_ptr<DataType> expected;
  std::shared_ptr<DataType> unwrapped;

  arg = decimal32(3, 2);
  expected = decimal32(9, 2);
  ASSERT_OK_AND_ASSIGN(unwrapped, WidenDecimalToMaxPrecision(arg));
  AssertTypeEqual(*unwrapped, *expected);

  arg = decimal64(3, 2);
  expected = decimal64(18, 2);
  ASSERT_OK_AND_ASSIGN(unwrapped, WidenDecimalToMaxPrecision(arg));
  AssertTypeEqual(*unwrapped, *expected);

  arg = decimal128(3, 2);
  expected = decimal128(38, 2);
  ASSERT_OK_AND_ASSIGN(unwrapped, WidenDecimalToMaxPrecision(arg));
  AssertTypeEqual(*unwrapped, *expected);

  arg = decimal256(3, 2);
  expected = decimal256(76, 2);
  ASSERT_OK_AND_ASSIGN(unwrapped, WidenDecimalToMaxPrecision(arg));
  AssertTypeEqual(*unwrapped, *expected);
}

TEST(TestDispatchBest, CommonTemporal) {
  std::vector<TypeHolder> args;

  args = {timestamp(TimeUnit::SECOND), timestamp(TimeUnit::NANO)};
  AssertTypeEqual(*timestamp(TimeUnit::NANO), *CommonTemporal(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND, "UTC"), timestamp(TimeUnit::NANO, "UTC")};
  AssertTypeEqual(*timestamp(TimeUnit::NANO, "UTC"),
                  *CommonTemporal(args.data(), args.size()));
  args = {timestamp(TimeUnit::SECOND), date32()};
  AssertTypeEqual(*timestamp(TimeUnit::SECOND),
                  *CommonTemporal(args.data(), args.size()));
  args = {date32(), timestamp(TimeUnit::NANO)};
  AssertTypeEqual(*timestamp(TimeUnit::NANO), *CommonTemporal(args.data(), args.size()));
  args = {date64(), timestamp(TimeUnit::SECOND)};
  AssertTypeEqual(*timestamp(TimeUnit::MILLI), *CommonTemporal(args.data(), args.size()));
  args = {date32(), date32()};
  AssertTypeEqual(*date32(), *CommonTemporal(args.data(), args.size()));
  args = {date64(), date64()};
  AssertTypeEqual(*date64(), *CommonTemporal(args.data(), args.size()));
  args = {date32(), date64()};
  AssertTypeEqual(*date64(), *CommonTemporal(args.data(), args.size()));
  args = {};
  ASSERT_EQ(CommonTemporal(args.data(), args.size()), nullptr);
  args = {float64(), int32()};
  ASSERT_EQ(CommonTemporal(args.data(), args.size()), nullptr);
  args = {timestamp(TimeUnit::SECOND), timestamp(TimeUnit::SECOND, "UTC")};
  ASSERT_EQ(CommonTemporal(args.data(), args.size()), nullptr);
  args = {timestamp(TimeUnit::SECOND, "America/Phoenix"),
          timestamp(TimeUnit::SECOND, "UTC")};
  ASSERT_EQ(CommonTemporal(args.data(), args.size()), nullptr);

  args = {time32(TimeUnit::SECOND), time32(TimeUnit::MILLI)};
  AssertTypeEqual(*time32(TimeUnit::MILLI), *CommonTemporal(args.data(), args.size()));

  args = {time32(TimeUnit::SECOND), time64(TimeUnit::NANO)};
  AssertTypeEqual(*time64(TimeUnit::NANO), *CommonTemporal(args.data(), args.size()));

  args = {date32(), time32(TimeUnit::SECOND)};
  ASSERT_EQ(CommonTemporal(args.data(), args.size()), nullptr);

  args = {timestamp(TimeUnit::SECOND), time32(TimeUnit::SECOND)};
  ASSERT_EQ(CommonTemporal(args.data(), args.size()), nullptr);
}

TEST(TestDispatchBest, CommonTemporalResolution) {
  std::vector<TypeHolder> args;
  std::string tz = "Pacific/Marquesas";
  TimeUnit::type ty;

  args = {date32(), date32()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::SECOND, ty);
  args = {date32(), date64()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {date32(), timestamp(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::SECOND, ty);
  args = {time32(TimeUnit::MILLI), date32()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {time32(TimeUnit::MILLI), time32(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {time32(TimeUnit::MILLI), time64(TimeUnit::MICRO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MICRO, ty);
  args = {time64(TimeUnit::NANO), time64(TimeUnit::MICRO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::NANO, ty);
  args = {duration(TimeUnit::MILLI), duration(TimeUnit::MICRO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MICRO, ty);
  args = {duration(TimeUnit::MILLI), date32()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {date64(), duration(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {duration(TimeUnit::SECOND), time32(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::SECOND, ty);
  args = {duration(TimeUnit::SECOND), time64(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::NANO, ty);
  args = {time64(TimeUnit::MICRO), duration(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::NANO, ty);
  args = {timestamp(TimeUnit::SECOND, tz), timestamp(TimeUnit::MICRO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MICRO, ty);
  args = {date32(), timestamp(TimeUnit::MICRO, tz)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MICRO, ty);
  args = {timestamp(TimeUnit::MICRO, tz), date64()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MICRO, ty);
  args = {time32(TimeUnit::MILLI), timestamp(TimeUnit::MICRO, tz)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MICRO, ty);
  args = {timestamp(TimeUnit::MICRO, tz), time64(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::NANO, ty);
  args = {timestamp(TimeUnit::SECOND, tz), duration(TimeUnit::MILLI)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {timestamp(TimeUnit::SECOND, "UTC"), timestamp(TimeUnit::SECOND, tz)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::SECOND, ty);
  args = {time32(TimeUnit::MILLI), duration(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
  args = {time64(TimeUnit::MICRO), duration(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::NANO, ty);
  args = {duration(TimeUnit::SECOND), int64()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::SECOND, ty);
  args = {duration(TimeUnit::MILLI), timestamp(TimeUnit::SECOND, tz)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ASSERT_EQ(TimeUnit::MILLI, ty);
}

TEST(TestDispatchBest, ReplaceTemporalTypes) {
  std::vector<TypeHolder> args;
  std::string tz = "Pacific/Marquesas";
  TimeUnit::type ty;

  args = {date32(), date32()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *timestamp(TimeUnit::SECOND));
  AssertTypeEqual(*args[1], *timestamp(TimeUnit::SECOND));

  args = {date64(), time32(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *timestamp(TimeUnit::MILLI));
  AssertTypeEqual(*args[1], *time32(TimeUnit::MILLI));

  args = {duration(TimeUnit::SECOND), date64()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *duration(TimeUnit::MILLI));
  AssertTypeEqual(*args[1], *timestamp(TimeUnit::MILLI));

  args = {timestamp(TimeUnit::MICRO, tz), timestamp(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *timestamp(TimeUnit::NANO, tz));
  AssertTypeEqual(*args[1], *timestamp(TimeUnit::NANO));

  args = {timestamp(TimeUnit::MICRO, tz), time64(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *timestamp(TimeUnit::NANO, tz));
  AssertTypeEqual(*args[1], *time64(TimeUnit::NANO));

  args = {timestamp(TimeUnit::SECOND, tz), date64()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *timestamp(TimeUnit::MILLI, tz));
  AssertTypeEqual(*args[1], *timestamp(TimeUnit::MILLI));

  args = {timestamp(TimeUnit::SECOND, "UTC"), timestamp(TimeUnit::SECOND, tz)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *timestamp(TimeUnit::SECOND, "UTC"));
  AssertTypeEqual(*args[1], *timestamp(TimeUnit::SECOND, tz));

  args = {time32(TimeUnit::SECOND), duration(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *time32(TimeUnit::SECOND));
  AssertTypeEqual(*args[1], *duration(TimeUnit::SECOND));

  args = {time64(TimeUnit::MICRO), duration(TimeUnit::SECOND)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *time64(TimeUnit::MICRO));
  AssertTypeEqual(*args[1], *duration(TimeUnit::MICRO));

  args = {time32(TimeUnit::SECOND), duration(TimeUnit::NANO)};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *time64(TimeUnit::NANO));
  AssertTypeEqual(*args[1], *duration(TimeUnit::NANO));

  args = {duration(TimeUnit::SECOND), int64()};
  ASSERT_TRUE(CommonTemporalResolution(args.data(), args.size(), &ty));
  ReplaceTemporalTypes(ty, &args);
  AssertTypeEqual(*args[0], *duration(TimeUnit::SECOND));
  AssertTypeEqual(*args[1], *int64());
}

namespace {

struct LegacyUnaryLength {
  template <typename OutValue, typename Arg0Value>
  static OutValue Call(KernelContext*, Arg0Value val, Status*) {
    if constexpr (std::is_same_v<OutValue, bool>) {
      return val.size() > 3;
    } else {
      return static_cast<OutValue>(val.size());
    }
  }
};

struct LegacyBinaryEqual {
  template <typename OutValue, typename Arg0Value, typename Arg1Value>
  static OutValue Call(KernelContext*, Arg0Value left, Arg1Value right, Status*) {
    return static_cast<OutValue>(left == right);
  }
};

struct DescriptorUnaryOp {
  template <typename OutValue, typename Arg0Value>
  static OutValue Call(KernelContext*, Arg0Value, Status*) {
    ADD_FAILURE() << "Call should not be invoked when CallView is available";
    return OutValue{};
  }

  template <typename OutValue>
  static OutValue CallView(KernelContext*, const util::BinaryViewAccessor& view,
                           Status*) {
    if constexpr (std::is_same_v<OutValue, bool>) {
      return !view.is_inline() && view.size() == 14;
    } else {
      return static_cast<OutValue>(view.size());
    }
  }
};

struct DescriptorBinaryEqualOp {
  template <typename OutValue, typename Arg0Value, typename Arg1Value>
  static OutValue Call(KernelContext*, Arg0Value, Arg1Value, Status*) {
    ADD_FAILURE() << "Call should not be invoked when CallView is available";
    return OutValue{};
  }

  template <typename OutValue, typename Arg0, typename Arg1>
    requires util::IsBinaryViewOperand<Arg0> || util::IsBinaryViewOperand<Arg1>
  static OutValue CallView(KernelContext*, const Arg0& left, const Arg1& right, Status*) {
    return static_cast<OutValue>(util::EqualBinaryViewValue(left, right));
  }
};

struct ErrorCallViewOp {
  template <typename OutValue, typename Arg0Value>
  static OutValue Call(KernelContext*, Arg0Value, Status*) {
    return OutValue{};
  }

  template <typename OutValue>
  static OutValue CallView(KernelContext*, const util::BinaryViewAccessor&, Status* st) {
    *st = Status::Invalid("view error");
    return OutValue{};
  }

  template <typename OutValue, typename Arg0Value, typename Arg1Value>
  static OutValue Call(KernelContext*, Arg0Value, Arg1Value, Status*) {
    return OutValue{};
  }

  template <typename OutValue, typename Arg0, typename Arg1>
    requires util::IsBinaryViewOperand<Arg0> || util::IsBinaryViewOperand<Arg1>
  static OutValue CallView(KernelContext*, const Arg0&, const Arg1&, Status* st) {
    *st = Status::Invalid("binary view error");
    return OutValue{};
  }
};

ExecResult MakeBoolResult(int64_t length, std::shared_ptr<ArrayData>* out_data) {
  auto buf = AllocateBitmap(length).ValueOrDie();
  std::memset(buf->mutable_data(), 0, static_cast<size_t>(buf->size()));
  *out_data = ArrayData::Make(boolean(), length, {nullptr, std::move(buf)});
  ExecResult out;
  out.value = ArraySpan(**out_data);
  return out;
}

ExecResult MakeInt32Result(int64_t length, std::shared_ptr<ArrayData>* out_data) {
  auto buf = AllocateBuffer(length * sizeof(int32_t)).ValueOrDie();
  *out_data = ArrayData::Make(int32(), length, {nullptr, std::move(buf)});
  ExecResult out;
  out.value = ArraySpan(**out_data);
  return out;
}

std::shared_ptr<Array> MakeSentinelStringViewArray() {
  // Out-of-line view with an out-of-range buffer_index (999) and empty variadic buffers:
  // any eager call to FromBinaryView() would crash by indexing data_buffers[999].
  std::vector<BinaryViewType::c_type> views = {
      util::ToInlineBinaryView("abc"),
      util::ToNonInlineBinaryView("abcd1234567890", 14, /*buffer_index=*/999,
                                  /*offset=*/0),
      util::ToInlineBinaryView("prefix_match"),
  };
  return std::make_shared<StringViewArray>(utf8_view(),
                                           static_cast<int64_t>(views.size()),
                                           Buffer::FromVector(views), BufferVector{});
}

}  // namespace

TEST(TestApplicator, BinaryViewCallViewDispatch) {
  ExecContext exec_ctx;
  KernelContext ctx(&exec_ctx);

  // 1. Legacy operators without CallView continue using Op::Call(..., std::string_view).
  {
    auto view_arr = ArrayFromJSON(utf8_view(), R"(["a", "abcd", "abcdefghijklm", null])");
    auto view_rhs = ArrayFromJSON(utf8_view(), R"(["a", "wxyz", "abcdefghijklm", "q"])");
    auto scalar = std::make_shared<StringViewScalar>("abcd");

    {
      std::shared_ptr<ArrayData> out_data;
      auto out = MakeBoolResult(view_arr->length(), &out_data);
      ExecSpan batch({ExecValue(*view_arr->data())}, view_arr->length());
      ASSERT_OK(
          (applicator::ScalarUnary<BooleanType, StringViewType, LegacyUnaryLength>::Exec(
              &ctx, batch, &out)));
      AssertArraysEqual(*ArrayFromJSON(boolean(), R"([false, true, true, false])"),
                        *MakeArray(out_data));
    }
    {
      std::shared_ptr<ArrayData> out_data;
      auto out = MakeInt32Result(view_arr->length(), &out_data);
      ExecSpan batch({ExecValue(*view_arr->data())}, view_arr->length());
      applicator::ScalarUnaryNotNullStateful<Int32Type, StringViewType, LegacyUnaryLength>
          kernel(LegacyUnaryLength{});
      ASSERT_OK(kernel.Exec(&ctx, batch, &out));
      AssertArraysEqual(*ArrayFromJSON(int32(), R"([1, 4, 13, 0])"),
                        *MakeArray(out_data));
    }
    {
      std::shared_ptr<ArrayData> out_data;
      auto out = MakeBoolResult(view_arr->length(), &out_data);
      ExecSpan batch({ExecValue(*view_arr->data()), ExecValue(*view_rhs->data())},
                     view_arr->length());
      ASSERT_OK((applicator::ScalarBinary<BooleanType, StringViewType, StringViewType,
                                          LegacyBinaryEqual>::Exec(&ctx, batch, &out)));
      AssertArraysEqual(*ArrayFromJSON(boolean(), R"([true, false, true, false])"),
                        *MakeArray(out_data));
    }
    {
      std::shared_ptr<ArrayData> out_data;
      auto out = MakeInt32Result(view_arr->length(), &out_data);
      ExecSpan batch({ExecValue(*view_arr->data()), ExecValue(scalar.get())},
                     view_arr->length());
      applicator::ScalarBinaryNotNullStateful<Int32Type, StringViewType, StringViewType,
                                              LegacyBinaryEqual>
          kernel(LegacyBinaryEqual{});
      ASSERT_OK(kernel.Exec(&ctx, batch, &out));
      AssertArraysEqual(*ArrayFromJSON(int32(), R"([0, 1, 0, 0])"), *MakeArray(out_data));
    }
  }

  // 2. CallView dispatches descriptor-level access without touching out-of-range buffers.
  auto sentinel_arr = MakeSentinelStringViewArray();
  {
    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(sentinel_arr->length(), &out_data);
    ExecSpan batch({ExecValue(*sentinel_arr->data())}, sentinel_arr->length());
    ASSERT_OK(
        (applicator::ScalarUnary<BooleanType, StringViewType, DescriptorUnaryOp>::Exec(
            &ctx, batch, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([false, true, false])"),
                      *MakeArray(out_data));
  }
  {
    std::shared_ptr<ArrayData> out_data;
    auto out = MakeInt32Result(sentinel_arr->length(), &out_data);
    ExecSpan batch({ExecValue(*sentinel_arr->data())}, sentinel_arr->length());
    applicator::ScalarUnaryNotNullStateful<Int32Type, StringViewType, DescriptorUnaryOp>
        kernel(DescriptorUnaryOp{});
    ASSERT_OK(kernel.Exec(&ctx, batch, &out));
    AssertArraysEqual(*ArrayFromJSON(int32(), R"([3, 14, 12])"), *MakeArray(out_data));
  }
  {
    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(sentinel_arr->length(), &out_data);
    ExecSpan batch({ExecValue(*sentinel_arr->data())}, sentinel_arr->length());
    applicator::ScalarUnaryNotNullStateful<BooleanType, StringViewType, DescriptorUnaryOp>
        kernel(DescriptorUnaryOp{});
    ASSERT_OK(kernel.Exec(&ctx, batch, &out));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([false, true, false])"),
                      *MakeArray(out_data));
  }

  // ScalarBinary ArrayArray / ArrayScalar / ScalarArray with mismatched prefix/length
  // rejects early without dereferencing sentinel buffer_index = 999.
  {
    auto other_view = ArrayFromJSON(utf8_view(), R"(["abc", "wxyz1234567890", "other"])");
    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(sentinel_arr->length(), &out_data);
    ExecSpan batch({ExecValue(*sentinel_arr->data()), ExecValue(*other_view->data())},
                   sentinel_arr->length());
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, StringViewType, StringViewType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, batch, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([true, false, false])"),
                      *MakeArray(out_data));
  }
  {
    auto other_str =
        ArrayFromJSON(utf8(), R"(["abc", "wxyz1234567890", "prefix_match"])");
    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(sentinel_arr->length(), &out_data);
    ExecSpan batch({ExecValue(*sentinel_arr->data()), ExecValue(*other_str->data())},
                   sentinel_arr->length());
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, StringViewType, StringType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, batch, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([true, false, true])"),
                      *MakeArray(out_data));

    ExecSpan reversed({ExecValue(*other_str->data()), ExecValue(*sentinel_arr->data())},
                      sentinel_arr->length());
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, StringType, StringViewType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, reversed, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([true, false, true])"),
                      *MakeArray(out_data));
  }
  {
    auto scalar = std::make_shared<LargeStringScalar>("wxyz1234567890");
    auto view_scalar = std::make_shared<StringViewScalar>("prefix_match");
    auto str_arr = ArrayFromJSON(large_utf8(), R"(["abc", "prefix_match", "other"])");

    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(sentinel_arr->length(), &out_data);
    ExecSpan batch_as({ExecValue(*sentinel_arr->data()), ExecValue(scalar.get())},
                      sentinel_arr->length());
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, StringViewType, LargeStringType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, batch_as, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([false, false, false])"),
                      *MakeArray(out_data));

    ExecSpan batch_sa({ExecValue(scalar.get()), ExecValue(*sentinel_arr->data())},
                      sentinel_arr->length());
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, LargeStringType, StringViewType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, batch_sa, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([false, false, false])"),
                      *MakeArray(out_data));

    ExecSpan batch_sa_mixed({ExecValue(view_scalar.get()), ExecValue(*str_arr->data())},
                            str_arr->length());
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, StringViewType, LargeStringType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, batch_sa_mixed,
                                                                 &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([false, true, false])"),
                      *MakeArray(out_data));
  }

  // ScalarBinaryNotNullStateful with nulls + sentinel buffer_index = 999
  {
    std::vector<BinaryViewType::c_type> views = {
        util::ToNonInlineBinaryView("abcd1234567890", 14, /*buffer_index=*/999,
                                    /*offset=*/0),
        util::ToNonInlineBinaryView("null_slot_long_value", 20, /*buffer_index=*/999,
                                    /*offset=*/0),
        util::ToInlineBinaryView("abc"),
    };
    auto valid_buf = ArrayFromJSON(boolean(), "[true, false, true]")->data()->buffers[1];
    auto arr_with_null = std::make_shared<StringViewArray>(
        utf8_view(), 3, Buffer::FromVector(views), BufferVector{}, valid_buf, 1);
    auto rhs_scalar = std::make_shared<StringScalar>("wxyz1234567890");

    std::shared_ptr<ArrayData> out_data;
    auto out = MakeInt32Result(3, &out_data);
    ExecSpan batch_as({ExecValue(*arr_with_null->data()), ExecValue(rhs_scalar.get())},
                      3);
    applicator::ScalarBinaryNotNullStateful<Int32Type, StringViewType, StringType,
                                            DescriptorBinaryEqualOp>
        kernel(DescriptorBinaryEqualOp{});
    ASSERT_OK(kernel.Exec(&ctx, batch_as, &out));
    AssertArraysEqual(*ArrayFromJSON(int32(), R"([0, 0, 0])"), *MakeArray(out_data));

    ExecSpan batch_sa({ExecValue(rhs_scalar.get()), ExecValue(*arr_with_null->data())},
                      3);
    applicator::ScalarBinaryNotNullStateful<Int32Type, StringType, StringViewType,
                                            DescriptorBinaryEqualOp>
        kernel_sa(DescriptorBinaryEqualOp{});
    ASSERT_OK(kernel_sa.Exec(&ctx, batch_sa, &out));
    AssertArraysEqual(*ArrayFromJSON(int32(), R"([0, 0, 0])"), *MakeArray(out_data));

    std::vector<BinaryViewType::c_type> rhs_views = {
        util::ToNonInlineBinaryView("wxyz1234567890", 14, /*buffer_index=*/999,
                                    /*offset=*/0),
        util::ToNonInlineBinaryView("null_slot_long_value", 20, /*buffer_index=*/999,
                                    /*offset=*/0),
        util::ToInlineBinaryView("abc"),
    };
    auto rhs_with_null = std::make_shared<StringViewArray>(
        utf8_view(), 3, Buffer::FromVector(rhs_views), BufferVector{}, valid_buf, 1);
    ExecSpan batch_aa(
        {ExecValue(*arr_with_null->data()), ExecValue(*rhs_with_null->data())}, 3);
    applicator::ScalarBinaryNotNullStateful<Int32Type, StringViewType, StringViewType,
                                            DescriptorBinaryEqualOp>
        kernel_aa(DescriptorBinaryEqualOp{});
    ASSERT_OK(kernel_aa.Exec(&ctx, batch_aa, &out));
    AssertArraysEqual(*ArrayFromJSON(int32(), R"([0, 0, 1])"), *MakeArray(out_data));
  }

  // Sliced StringViewArray + null scalar safety
  {
    auto full_arr =
        ArrayFromJSON(utf8_view(), R"(["drop", "alpha_long_string", "beta"])");
    auto sliced = full_arr->Slice(1, 2);
    auto match_arr = ArrayFromJSON(utf8_view(), R"(["z", "alpha_long_string", "other"])");
    auto match_sliced = match_arr->Slice(1, 2);

    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(2, &out_data);
    ExecSpan batch({ExecValue(*sliced->data()), ExecValue(*match_sliced->data())}, 2);
    ASSERT_OK(
        (applicator::ScalarBinary<BooleanType, StringViewType, StringViewType,
                                  DescriptorBinaryEqualOp>::Exec(&ctx, batch, &out)));
    AssertArraysEqual(*ArrayFromJSON(boolean(), R"([true, false])"),
                      *MakeArray(out_data));

    StringViewScalar null_view_scalar;
    ASSERT_FALSE(null_view_scalar.is_valid);
    ASSERT_EQ(null_view_scalar.value, nullptr);
    ExecSpan batch_null({ExecValue(*sliced->data()), ExecValue(&null_view_scalar)}, 2);
    ASSERT_OK((
        applicator::ScalarBinary<BooleanType, StringViewType, StringViewType,
                                 DescriptorBinaryEqualOp>::Exec(&ctx, batch_null, &out)));
  }

  // Status propagation from CallView
  {
    std::shared_ptr<ArrayData> out_data;
    auto out = MakeBoolResult(sentinel_arr->length(), &out_data);
    ExecSpan unary_batch({ExecValue(*sentinel_arr->data())}, sentinel_arr->length());
    ASSERT_RAISES(
        Invalid,
        (applicator::ScalarUnary<BooleanType, StringViewType, ErrorCallViewOp>::Exec(
            &ctx, unary_batch, &out)));

    ExecSpan binary_batch(
        {ExecValue(*sentinel_arr->data()), ExecValue(*sentinel_arr->data())},
        sentinel_arr->length());
    ASSERT_RAISES(
        Invalid,
        (applicator::ScalarBinary<BooleanType, StringViewType, StringViewType,
                                  ErrorCallViewOp>::Exec(&ctx, binary_batch, &out)));
  }
}

TEST(TestApplicator, PinnedBinaryScalarAndEqualValueEdgeCases) {
  // Null scalar unboxing must not call BinaryViewScalar::view() on nullptr value.
  StringViewScalar null_view_scalar;
  auto unboxed_null = ViewScalarUnboxer<StringViewType>::Unbox(null_view_scalar);
  EXPECT_EQ(unboxed_null.len, 0);
  EXPECT_TRUE(unboxed_null.fits_in_binary_view);

  // Empty string_view with nullptr data must be UBSan-clean.
  auto empty_pinned =
      util::PinnedBinaryScalar::FromString(/*is_valid=*/true, std::string_view{});
  auto empty_view_raw = util::ToInlineBinaryView("");
  util::BinaryViewAccessor empty_acc{empty_view_raw, nullptr};
  EXPECT_TRUE(util::EqualBinaryViewValue(empty_acc, empty_pinned));
  EXPECT_TRUE(util::EqualBinaryViewValue(empty_pinned, empty_acc));
  EXPECT_TRUE(util::EqualBinaryViewValue(std::string_view{}, empty_pinned));
  EXPECT_TRUE(util::EqualBinaryViewValue(empty_pinned, std::string_view{}));

  // Lengths < kPrefixSize (1..3), == kPrefixSize (4), inline (5..12), out-of-line (> 12),
  // and embedded NUL bytes.
  auto payload_buf = Buffer::FromString(std::string("long_prefix_\0tail", 17));
  auto payload_diff = Buffer::FromString(std::string("long_prefix_\0diff", 17));
  std::shared_ptr<Buffer> buffers_a[] = {payload_buf};
  std::shared_ptr<Buffer> buffers_b[] = {payload_diff};

  for (const std::string& sample :
       {std::string("a"), std::string("ab"), std::string("abc"), std::string("abcd"),
        std::string("abcdefgh"), std::string("abcdefghijkl"), std::string("a\0b", 3),
        std::string("abcd\0ef", 7)}) {
    auto pinned = util::PinnedBinaryScalar::FromString(/*is_valid=*/true, sample);
    auto raw_view = util::ToInlineBinaryView(sample);
    util::BinaryViewAccessor acc{raw_view, nullptr};
    EXPECT_TRUE(util::EqualBinaryViewValue(acc, pinned));
    EXPECT_TRUE(util::EqualBinaryViewValue(pinned, acc));
    EXPECT_TRUE(util::EqualBinaryViewValue(std::string_view{sample}, pinned));
    EXPECT_TRUE(util::EqualBinaryViewValue(pinned, std::string_view{sample}));
    EXPECT_TRUE(util::EqualBinaryViewValue(acc, std::string_view{sample}));
    EXPECT_TRUE(util::EqualBinaryViewValue(std::string_view{sample}, acc));
    EXPECT_TRUE(util::EqualBinaryViewValue(acc, acc));

    std::string mutated = sample;
    mutated.back() ^= 1;
    auto mutated_pinned =
        util::PinnedBinaryScalar::FromString(/*is_valid=*/true, mutated);
    EXPECT_FALSE(util::EqualBinaryViewValue(acc, mutated_pinned));
    EXPECT_FALSE(util::EqualBinaryViewValue(std::string_view{sample}, mutated_pinned));
  }

  auto long_raw_a = util::ToBinaryView(std::string_view{*payload_buf}, 0, 0);
  auto long_raw_b = util::ToBinaryView(std::string_view{*payload_diff}, 0, 0);
  util::BinaryViewAccessor long_acc_a{long_raw_a, buffers_a};
  util::BinaryViewAccessor long_acc_b{long_raw_b, buffers_b};
  auto long_pinned_a = util::PinnedBinaryScalar::FromString(
      /*is_valid=*/true, std::string_view{*payload_buf});
  auto long_pinned_b = util::PinnedBinaryScalar::FromString(
      /*is_valid=*/true, std::string_view{*payload_diff});
  EXPECT_TRUE(util::EqualBinaryViewValue(long_acc_a, long_pinned_a));
  EXPECT_FALSE(util::EqualBinaryViewValue(long_acc_a, long_pinned_b));
  EXPECT_FALSE(util::EqualBinaryViewValue(long_acc_a, long_acc_b));
  EXPECT_TRUE(util::EqualBinaryViewValue(std::string_view{*payload_buf}, long_pinned_a));
  EXPECT_FALSE(util::EqualBinaryViewValue(std::string_view{*payload_buf}, long_pinned_b));

  // Large scalar exceeding INT32_MAX must never truncate to match a BinaryView.
  static constexpr char kOversizedPrefix[] = "long_prefix_data";
  std::string_view oversized_view(
      kOversizedPrefix, static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1);
  auto oversized_pinned =
      util::PinnedBinaryScalar::FromString(/*is_valid=*/true, oversized_view);
  EXPECT_FALSE(oversized_pinned.fits_in_binary_view);
  EXPECT_FALSE(util::EqualBinaryViewValue(long_acc_a, oversized_pinned));
  EXPECT_FALSE(util::EqualBinaryViewValue(oversized_pinned, long_acc_a));
  EXPECT_FALSE(util::EqualBinaryViewValue(std::string_view{"long"}, oversized_pinned));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
