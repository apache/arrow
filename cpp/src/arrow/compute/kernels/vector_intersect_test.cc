#include <gtest/gtest.h>

#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

void AssertIntersect(const std::shared_ptr<Array>& array1,
                     const std::shared_ptr<Array>& array2,
                     const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(auto actual, CallFunction("intersect", {array1, array2}));
  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

void AssertIntersectEmpty(std::shared_ptr<DataType> type) {
  AssertIntersect(ArrayFromJSON(type, "[]"), ArrayFromJSON(type, "[]"),
                  ArrayFromJSON(type, "[]"));
  // AssertIntersect(ArrayFromJSON(type, "[null]"), ArrayFromJSON(type, "[null]"),
  // ArrayFromJSON(type, "[]"));
}

TEST(TestIntersect, IntersectReal) {
  for (auto real_type : ::arrow::FloatingPointTypes()) {
    AssertIntersectEmpty(real_type);
    auto array1 = ArrayFromJSON(real_type, "[1.1, 2.2, 3.2, 4.2, 5.4]");
    auto array2 = ArrayFromJSON(real_type, "[2.2, 5.4, 3.2]");
    auto expected = ArrayFromJSON(real_type, "[2.2, 3.2, 5.4]");

    AssertIntersect(array1, array2, expected);
  }
}

TEST(TestIntersect, IntersectIntegral) {
  for (auto integer_type : ::arrow::IntTypes()) {
    AssertIntersectEmpty(integer_type);
    auto array1 = ArrayFromJSON(integer_type, "[1, 2, 3, 4, 5]");
    auto array2 = ArrayFromJSON(integer_type, "[2, 5, 3]");
    auto expected = ArrayFromJSON(integer_type, "[2, 3, 5]");

    AssertIntersect(array1, array2, expected);
  }
}

/*
TEST(TestIntersect, IntersectBool) {
  for (auto integer_type : ::arrow::IntTypes()) {
    AssertIntersectEmpty(boolean());
    auto array1 = ArrayFromJSON(boolean(), "[false, true]");
    auto array2 = ArrayFromJSON(boolean(), "[false]");
    auto expected = ArrayFromJSON(boolean(), "[false]");

    AssertIntersect(array1, array2, expected);
  }
}

TEST(TestIntersect, IntersectTemporal) {
  for (auto temporal_type : ::arrow::TemporalTypes()) {
    AssertIntersectEmpty(temporal_type);
    auto array1 = ArrayFromJSON(temporal_type, "[1, 2, 3, 4, 5]");
    auto array2 = ArrayFromJSON(temporal_type, "[2, 5, 3]");
    auto expected = ArrayFromJSON(temporal_type, "[2, 3, 5]");

    AssertIntersect(array1, array2, expected);
  }
}
*/

TEST(TestIntersect, IntersectStrings) {
  for (auto string_type : ::arrow::StringTypes()) {
    AssertIntersectEmpty(string_type);
    auto array1 = ArrayFromJSON(string_type, R"(["b", "c", "a", "", "d"])");
    auto array2 = ArrayFromJSON(string_type, R"(["c", "d", "a"])");
    auto expected = ArrayFromJSON(string_type, R"(["a", "c", "d"])");

    AssertIntersect(array1, array2, expected);
  }
}

TEST(TestIntersect, IntersectFixedSizeBinary) {
  auto binary_type = fixed_size_binary(3);
  AssertIntersectEmpty(binary_type);

  auto array1 = ArrayFromJSON(binary_type, R"(["bbb", "ccc", "aaa", "   ", "dddd"])");
  auto array2 = ArrayFromJSON(binary_type, R"(["ccc", "ddd", "aaa"])");
  auto expected = ArrayFromJSON(binary_type, R"(["aaa", "ccc", "ddd"])");

  AssertIntersect(array1, array2, expected);
}

}  // namespace compute
}  // namespace arrow
