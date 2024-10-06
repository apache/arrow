
#include <gtest/gtest.h>

#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow::compute {

TEST(ReverseIndex, Invalid) {
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    ReverseIndexOptions options{0, nullptr};
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_index must be integer, got null",
        CallFunction("reverse_index", {indices}, &options));
  }
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    ReverseIndexOptions options{0, utf8()};
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_index must be integer, got string",
        CallFunction("reverse_index", {indices}, &options));
  }
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    ReverseIndexOptions options{-1, int8()};
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output length of reverse_index must be non-negative, got -1",
        CallFunction("reverse_index", {indices}, &options));
  }
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    ReverseIndexOptions options{0, int8(), MakeScalar(int16_t(-1))};
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Output non-taken of reverse_index must be of "
                               "the same type with the output type, got int16",
                               CallFunction("reverse_index", {indices}, &options));
  }
}

TEST(ReverseIndex, Basic) {
  {
    auto indices = ArrayFromJSON(int32(), "[9, 7, 5, 3, 1, 0, 2, 4, 6, 8]");
    auto expected = ArrayFromJSON(int8(), "[5, 4, 6, 3, 7, 2, 8, 1, 9, 0]");
    ReverseIndexOptions options{10, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[null, 0, 1, null, null, null, null]");
    ReverseIndexOptions options{7, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[null, 0, 1, null, null, null, null]");
    ReverseIndexOptions options{7, int8(), MakeNullScalar(int8())};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[-1, 0, 1, -1, -1, -1, -1]");
    ReverseIndexOptions options{7, int8(), MakeScalar(int8_t(-1))};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
    ASSERT_FALSE(result.array()->HasValidityBitmap());
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[]");
    ReverseIndexOptions options{0, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 0]");
    auto expected = ArrayFromJSON(int8(), "[1]");
    ReverseIndexOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[null]");
    ReverseIndexOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    auto expected = ArrayFromJSON(int8(), "[null, null, null, null, null, null, null]");
    ReverseIndexOptions options{7, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
}

TEST(ReverseIndex, Overflow) {
  {
    auto indices = ConstantArrayGenerator::Zeroes(
        std::numeric_limits<int8_t>::max() + int64_t(1), int8());
    auto expected = ArrayFromJSON(
        int8(), "[" + std::to_string(std::numeric_limits<int8_t>::max()) + "]");
    ReverseIndexOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ConstantArrayGenerator::Zeroes(
        std::numeric_limits<int8_t>::max() + int64_t(2), int8());
    ReverseIndexOptions options{1, int8()};
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Overflow in reverse_index, got " +
            std::to_string(std::numeric_limits<int8_t>::max() + int64_t(1)) + " for int8",
        CallFunction("reverse_index", {indices}, &options));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto indices,
        MakeArrayOfNull(int8(), std::numeric_limits<int8_t>::max() + int64_t(2)));
    auto expected = ArrayFromJSON(int8(), "[null]");
    ReverseIndexOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
}

};  // namespace arrow::compute
