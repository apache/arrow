
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
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Output type of reverse_index must not be null",
                               CallFunction("reverse_index", {indices}, &options));
  }
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    ReverseIndexOptions options{0, utf8()};
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_index must be integer, got string",
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
    auto indices = ConstantArrayGenerator::Zeroes(127, int8());
    auto expected = ArrayFromJSON(int8(), "[126]");
    ReverseIndexOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_index", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ConstantArrayGenerator::Zeroes(128, int8());
    ReverseIndexOptions options{1, int8()};
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Output type int8 of reverse_index is "
                               "insufficient to store indices of length 128",
                               CallFunction("reverse_index", {indices}, &options));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto indices, MakeArrayOfNull(int8(), 128));
    auto expected = ArrayFromJSON(int8(), "[null]");
    ReverseIndexOptions options{1, int8()};
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Output type int8 of reverse_index is "
                               "insufficient to store indices of length 128",
                               CallFunction("reverse_index", {indices}, &options));
  }
}

TEST(Permute, Basic) {
  {
    auto values = ArrayFromJSON(int64(), "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]");
    auto indices = ArrayFromJSON(int64(), "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]");
    auto expected = ArrayFromJSON(int64(), "[19, 18, 17, 16, 15, 14, 13, 12, 11, 10]");
    PermuteOptions options{10};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("permute", {values, indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto values = ArrayFromJSON(int64(), "[0, 0, 0, 1, 1, 1]");
    auto indices = ArrayFromJSON(int64(), "[0, 3, 6, 1, 4, 7]");
    auto expected = ArrayFromJSON(int64(), "[0, 1, null, 0, 1, null, 0, 1, null]");
    PermuteOptions options{9};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("permute", {values, indices}, &options));
    AssertDatumsEqual(expected, result);
  }
}

};  // namespace arrow::compute
