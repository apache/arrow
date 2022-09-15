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

#include <algorithm>
#include <cstdint>

#include <gtest/gtest.h>

#include "arrow/stl.h"
#include "arrow/stl_iterator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using std::nullopt;
using std::optional;

namespace stl {

TEST(ArrayIterator, Basics) {
  auto array =
      checked_pointer_cast<Int32Array>(ArrayFromJSON(int32(), "[4, 5, null, 6]"));

  ArrayIterator<Int32Array> it(*array);
  optional<int32_t> v = *it;
  ASSERT_EQ(v, 4);
  ASSERT_EQ(it[0], 4);
  ++it;
  ASSERT_EQ(it[0], 5);
  ASSERT_EQ(*it, 5);
  ASSERT_EQ(it[1], nullopt);
  ASSERT_EQ(it[2], 6);
}

TEST(ArrayIterator, Arithmetic) {
  auto array = checked_pointer_cast<Int32Array>(
      ArrayFromJSON(int32(), "[4, 5, null, 6, null, 7]"));

  ArrayIterator<Int32Array> it(*array);
  auto it2 = it + 2;
  ASSERT_EQ(*it, 4);
  ASSERT_EQ(*it2, nullopt);
  ASSERT_EQ(it2 - it, 2);
  ASSERT_EQ(it - it2, -2);
  auto it3 = it++;
  ASSERT_EQ(it2 - it, 1);
  ASSERT_EQ(it2 - it3, 2);
  ASSERT_EQ(*it3, 4);
  ASSERT_EQ(*it, 5);
  auto it4 = ++it;
  ASSERT_EQ(*it, nullopt);
  ASSERT_EQ(*it4, nullopt);
  ASSERT_EQ(it2 - it, 0);
  ASSERT_EQ(it2 - it4, 0);
  auto it5 = it + 3;
  ASSERT_EQ(*it5, 7);
  ASSERT_EQ(*(it5 - 2), 6);
  ASSERT_EQ(*(it5 + (-2)), 6);
  auto it6 = (--it5)--;
  ASSERT_EQ(*it6, nullopt);
  ASSERT_EQ(*it5, 6);
  ASSERT_EQ(it6 - it5, 1);
}

TEST(ArrayIterator, Comparison) {
  auto array = checked_pointer_cast<Int32Array>(
      ArrayFromJSON(int32(), "[4, 5, null, 6, null, 7]"));

  auto it = ArrayIterator<Int32Array>(*array) + 2;
  auto it2 = ArrayIterator<Int32Array>(*array) + 2;
  auto it3 = ArrayIterator<Int32Array>(*array) + 4;

  ASSERT_TRUE(it == it2);
  ASSERT_TRUE(it <= it2);
  ASSERT_TRUE(it >= it2);
  ASSERT_FALSE(it != it2);
  ASSERT_FALSE(it < it2);
  ASSERT_FALSE(it > it2);

  ASSERT_FALSE(it == it3);
  ASSERT_TRUE(it <= it3);
  ASSERT_FALSE(it >= it3);
  ASSERT_TRUE(it != it3);
  ASSERT_TRUE(it < it3);
  ASSERT_FALSE(it > it3);
}

TEST(ArrayIterator, BeginEnd) {
  auto array =
      checked_pointer_cast<Int32Array>(ArrayFromJSON(int32(), "[4, 5, null, 6]"));
  std::vector<optional<int32_t>> values;
  for (auto it = array->begin(); it != array->end(); ++it) {
    values.push_back(*it);
  }
  std::vector<optional<int32_t>> expected{4, 5, {}, 6};
  ASSERT_EQ(values, expected);
}

TEST(ArrayIterator, RangeFor) {
  auto array =
      checked_pointer_cast<Int32Array>(ArrayFromJSON(int32(), "[4, 5, null, 6]"));
  std::vector<optional<int32_t>> values;
  for (const auto v : *array) {
    values.push_back(v);
  }
  std::vector<optional<int32_t>> expected{4, 5, {}, 6};
  ASSERT_EQ(values, expected);
}

TEST(ArrayIterator, String) {
  auto array = checked_pointer_cast<StringArray>(
      ArrayFromJSON(utf8(), R"(["foo", "bar", null, "quux"])"));
  std::vector<optional<util::string_view>> values;
  for (const auto v : *array) {
    values.push_back(v);
  }
  std::vector<optional<util::string_view>> expected{"foo", "bar", {}, "quux"};
  ASSERT_EQ(values, expected);
}

TEST(ArrayIterator, Boolean) {
  auto array = checked_pointer_cast<BooleanArray>(
      ArrayFromJSON(boolean(), "[true, null, null, false]"));
  std::vector<optional<bool>> values;
  for (const auto v : *array) {
    values.push_back(v);
  }
  std::vector<optional<bool>> expected{true, {}, {}, false};
  ASSERT_EQ(values, expected);
}

TEST(ArrayIterator, FixedSizeBinary) {
  auto array = checked_pointer_cast<FixedSizeBinaryArray>(
      ArrayFromJSON(fixed_size_binary(3), R"(["foo", "bar", null, "quu"])"));
  std::vector<optional<util::string_view>> values;
  for (const auto v : *array) {
    values.push_back(v);
  }
  std::vector<optional<util::string_view>> expected{"foo", "bar", {}, "quu"};
  ASSERT_EQ(values, expected);
}

// Test compatibility with various STL algorithms

TEST(ArrayIterator, StdFind) {
  auto array = checked_pointer_cast<StringArray>(
      ArrayFromJSON(utf8(), R"(["foo", "bar", null, "quux"])"));

  auto it = std::find(array->begin(), array->end(), "bar");
  ASSERT_EQ(it.index(), 1);
  it = std::find(array->begin(), array->end(), nullopt);
  ASSERT_EQ(it.index(), 2);
  it = std::find(array->begin(), array->end(), "zzz");
  ASSERT_EQ(it, array->end());
}

TEST(ArrayIterator, StdCountIf) {
  auto array = checked_pointer_cast<BooleanArray>(
      ArrayFromJSON(boolean(), "[true, null, null, false]"));

  auto n = std::count_if(array->begin(), array->end(),
                         [](optional<bool> v) { return !v.has_value(); });
  ASSERT_EQ(n, 2);
}

TEST(ArrayIterator, StdCopy) {
  auto array = checked_pointer_cast<BooleanArray>(
      ArrayFromJSON(boolean(), "[true, null, null, false]"));
  std::vector<optional<bool>> values;
  std::copy(array->begin() + 1, array->end(), std::back_inserter(values));
  std::vector<optional<bool>> expected{{}, {}, false};
  ASSERT_EQ(values, expected);
}

TEST(ArrayIterator, StdPartitionPoint) {
  auto array = checked_pointer_cast<DoubleArray>(
      ArrayFromJSON(float64(), "[4.5, 2.5, 1e100, 3, null, null, null, null, null]"));
  auto it = std::partition_point(array->begin(), array->end(),
                                 [](optional<double> v) { return v.has_value(); });
  ASSERT_EQ(it.index(), 4);
  ASSERT_EQ(*it, nullopt);

  array =
      checked_pointer_cast<DoubleArray>(ArrayFromJSON(float64(), "[null, null, null]"));
  it = std::partition_point(array->begin(), array->end(),
                            [](optional<double> v) { return v.has_value(); });
  ASSERT_EQ(it, array->begin());
  it = std::partition_point(array->begin(), array->end(),
                            [](optional<double> v) { return !v.has_value(); });
  ASSERT_EQ(it, array->end());
}

TEST(ArrayIterator, StdEqualRange) {
  auto array = checked_pointer_cast<Int8Array>(
      ArrayFromJSON(int8(), "[1, 4, 5, 5, 5, 7, 8, null, null, null]"));
  auto cmp_lt = [](optional<int8_t> u, optional<int8_t> v) {
    return u.has_value() && (!v.has_value() || *u < *v);
  };

  auto pair = std::equal_range(array->begin(), array->end(), nullopt, cmp_lt);
  ASSERT_EQ(pair.first, array->end() - 3);
  ASSERT_EQ(pair.second, array->end());
  pair = std::equal_range(array->begin(), array->end(), 6, cmp_lt);
  ASSERT_EQ(pair.first, array->begin() + 5);
  ASSERT_EQ(pair.second, pair.first);
  pair = std::equal_range(array->begin(), array->end(), 5, cmp_lt);
  ASSERT_EQ(pair.first, array->begin() + 2);
  ASSERT_EQ(pair.second, array->begin() + 5);
  pair = std::equal_range(array->begin(), array->end(), 1, cmp_lt);
  ASSERT_EQ(pair.first, array->begin());
  ASSERT_EQ(pair.second, array->begin() + 1);
  pair = std::equal_range(array->begin(), array->end(), 0, cmp_lt);
  ASSERT_EQ(pair.first, array->begin());
  ASSERT_EQ(pair.second, pair.first);
}

TEST(ArrayIterator, StdMerge) {
  auto array1 = checked_pointer_cast<Int8Array>(
      ArrayFromJSON(int8(), "[1, 4, 5, 5, 7, null, null, null]"));
  auto array2 =
      checked_pointer_cast<Int8Array>(ArrayFromJSON(int8(), "[-1, 3, 3, 6, 42]"));
  auto cmp_lt = [](optional<int8_t> u, optional<int8_t> v) {
    return u.has_value() && (!v.has_value() || *u < *v);
  };

  std::vector<optional<int8_t>> values;
  std::merge(array1->begin(), array1->end(), array2->begin(), array2->end(),
             std::back_inserter(values), cmp_lt);
  std::vector<optional<int8_t>> expected{-1, 1, 3, 3, 4, 5, 5, 6, 7, 42, {}, {}, {}};
  ASSERT_EQ(values, expected);
}

TEST(ChunkedArrayIterator, Basics) {
  auto result = ChunkedArrayFromJSON(int32(), {R"([4, 5, null])", R"([6])"});
  auto it = Begin<Int32Type>(*result);
  optional<int32_t> v = *it;
  ASSERT_EQ(v, 4);
  ASSERT_EQ(it[0], 4);
  ++it;
  ASSERT_EQ(it[0], 5);
  ASSERT_EQ(*it, 5);
  ASSERT_EQ(it[1], nullopt);
  ASSERT_EQ(it[2], 6);
}

TEST(ChunkedArrayIterator, Arithmetic) {
  auto result = ChunkedArrayFromJSON(int32(), {R"([4, 5, null])", R"([6, null, 7])"});

  auto it = Begin<Int32Type>(*result);
  auto it2 = it + 2;
  auto end = End<Int32Type>(*result);

  ASSERT_EQ(*it, 4);
  ASSERT_EQ(*it2, nullopt);
  ASSERT_EQ(it2 - it, 2);
  ASSERT_EQ(it - it2, -2);
  ASSERT_EQ(end - it2, 4);
  ASSERT_EQ(it2 - end, -4);
  auto it3 = it++;
  ASSERT_EQ(it2 - it, 1);
  ASSERT_EQ(it2 - it3, 2);
  ASSERT_EQ(*it3, 4);
  ASSERT_EQ(*it, 5);
  auto it4 = ++it;
  ASSERT_EQ(*it, nullopt);
  ASSERT_EQ(*it4, nullopt);
  ASSERT_EQ(it2 - it, 0);
  ASSERT_EQ(it2 - it4, 0);
  auto it5 = it + 3;
  ASSERT_EQ(*it5, 7);
  ASSERT_EQ(*(it5 - 2), 6);
  ASSERT_EQ(*(it5 + (-2)), 6);
  auto it6 = (--it5)--;
  ASSERT_EQ(*it6, nullopt);
  ASSERT_EQ(*it5, 6);
  ASSERT_EQ(it6 - it5, 1);
}

TEST(ChunkedArrayIterator, Comparison) {
  auto result = ChunkedArrayFromJSON(int32(), {R"([4, 5, null])", R"([6, null, 7])"});

  auto it = Begin<Int32Type>(*result) + 2;
  auto it2 = Begin<Int32Type>(*result) + 2;
  auto it3 = Begin<Int32Type>(*result) + 4;
  auto it4 = Begin<Int32Type>(*result) + 6;
  auto end = End<Int32Type>(*result);

  ASSERT_TRUE(it == it2);
  ASSERT_TRUE(it <= it2);
  ASSERT_TRUE(it >= it2);
  ASSERT_FALSE(it != it2);
  ASSERT_FALSE(it < it2);
  ASSERT_FALSE(it > it2);

  ASSERT_FALSE(it == it3);
  ASSERT_TRUE(it <= it3);
  ASSERT_FALSE(it >= it3);
  ASSERT_TRUE(it != it3);
  ASSERT_TRUE(it < it3);
  ASSERT_FALSE(it > it3);

  ASSERT_FALSE(it3 == end);
  ASSERT_TRUE(it3 <= end);
  ASSERT_TRUE(it3 < end);
  ASSERT_TRUE(it4 == end);
  ASSERT_TRUE(it4 <= end);
  ASSERT_FALSE(it4 < end);
}

TEST(ChunkedArrayIterator, MultipleChunks) {
  auto result =
      ChunkedArrayFromJSON(int32(), {R"([4, 5, null])", R"([6])", R"([7, 9, 10, 8])",
                                     R"([11, 13])", R"([14])", R"([15])", R"([16])"});

  auto it = Begin<Int32Type>(*result);
  auto end = End<Int32Type>(*result);

  ASSERT_EQ(it[8], 11);
  ASSERT_EQ(it[9], 13);
  it += 3;
  ASSERT_EQ(it[0], 6);
  ++it;
  ASSERT_EQ(it[0], 7);
  ASSERT_EQ(*it, 7);
  ASSERT_EQ(it[1], 9);
  ASSERT_EQ(it[2], 10);
  it -= 4;
  ASSERT_EQ(it[0], 4);
  ASSERT_EQ(it[1], 5);
  ASSERT_EQ(it[2], nullopt);
  ASSERT_EQ(it[3], 6);
  ASSERT_EQ(it[4], 7);
  it += 9;
  ASSERT_EQ(*it, 13);
  --it;
  ASSERT_EQ(*it, 11);
  --it;
  ASSERT_EQ(*it, 8);
  ASSERT_EQ(it[0], 8);
  ASSERT_EQ(it[1], 11);
  ASSERT_EQ(it[2], 13);
  ASSERT_EQ(it[3], 14);
  ASSERT_EQ(it[4], 15);
  ASSERT_EQ(it[5], 16);
  ++it;
  ASSERT_EQ(*it, 11);
  ASSERT_EQ(it[0], 11);
  ASSERT_EQ(it[1], 13);
  ASSERT_EQ(it[2], 14);
  ASSERT_EQ(it[3], 15);
  ASSERT_EQ(it[4], 16);
  ++it;
  ASSERT_EQ(*it, 13);
  ASSERT_EQ(it[0], 13);
  ASSERT_EQ(it[1], 14);
  ASSERT_EQ(it[2], 15);
  ASSERT_EQ(it[3], 16);
  ++it;
  ++it;
  ASSERT_EQ(*it, 15);
  ASSERT_NE(it, end);
  ASSERT_NE(it + 1, end);
  ASSERT_EQ(it + 2, end);

  ASSERT_EQ(it[0], 15);
  ASSERT_EQ(it[1], 16);
  --it;
  ASSERT_EQ(*it, 14);
  ASSERT_EQ(it[0], 14);
  ASSERT_EQ(it[1], 15);
  ASSERT_EQ(it[2], 16);
  --it;
  ASSERT_EQ(*it, 13);
  ASSERT_EQ(it[0], 13);
  ASSERT_EQ(it[1], 14);
  ASSERT_EQ(it[2], 15);
  it -= 2;
  ASSERT_EQ(*it, 8);
  ASSERT_EQ(it[0], 8);
  ASSERT_EQ(it[1], 11);
  ASSERT_EQ(it[2], 13);
}

TEST(ChunkedArrayIterator, EmptyChunks) {
  auto result = ChunkedArrayFromJSON(int32(), {});
  auto it = Begin<Int32Type>(*result);
  auto end = End<Int32Type>(*result);
  ASSERT_EQ(it, end);

  result = ChunkedArrayFromJSON(int32(), {R"([4, 5, null])", R"([])", R"([7, 9, 10, 8])",
                                          R"([11, 13])", R"([])", R"([])", R"([16])"});

  it = Begin<Int32Type>(*result);
  end = End<Int32Type>(*result);

  ASSERT_NE(it, end);
  ASSERT_EQ(it[8], 13);
  ASSERT_EQ(it[9], 16);
  it += 3;
  ASSERT_EQ(it[0], 7);
  ++it;
  ASSERT_EQ(it[0], 9);
  ASSERT_EQ(*it, 9);
  ASSERT_EQ(it[1], 10);
  ASSERT_EQ(it[2], 8);
  it -= 4;
  ASSERT_EQ(it[0], 4);
  ASSERT_EQ(it[1], 5);
  ASSERT_EQ(it[2], nullopt);
  ASSERT_EQ(it[3], 7);
  ASSERT_EQ(it[4], 9);
  it += 9;
  ASSERT_EQ(*it, 16);
  --it;
  ASSERT_EQ(*it, 13);
  --it;
  ASSERT_EQ(*it, 11);
  ASSERT_EQ(it[0], 11);
  ASSERT_EQ(it[1], 13);
  ASSERT_EQ(it[2], 16);
  ++it;
  ASSERT_EQ(*it, 13);
  ASSERT_EQ(it[0], 13);
  ASSERT_EQ(it[1], 16);
  ++it;
  ASSERT_EQ(*it, 16);
  ASSERT_EQ(it[0], 16);
  ASSERT_NE(it, end);
  ASSERT_EQ(it + 1, end);
  --it;
  ASSERT_EQ(*it, 13);
  ASSERT_EQ(it[0], 13);
  ASSERT_EQ(it[1], 16);
  --it;
  ASSERT_EQ(*it, 11);
  ASSERT_EQ(it[0], 11);
  ASSERT_EQ(it[1], 13);
  ASSERT_EQ(it[2], 16);
  it += 2;
  ASSERT_EQ(*it, 16);
  ASSERT_EQ(it[0], 16);
  it -= 3;
  ASSERT_EQ(*it, 8);
  ASSERT_EQ(it[0], 8);
  ASSERT_EQ(it[1], 11);
  ASSERT_EQ(it[2], 13);
}

// Test compatibility with various STL algorithms

TEST(ChunkedArrayIterator, StdFind) {
  auto chunked_array1 =
      ChunkedArrayFromJSON(int32(), {R"([5, 10])", R"([null])", R"([16])"});
  auto it_begin = Begin<Int32Type>(*chunked_array1);
  auto it_end = End<Int32Type>(*chunked_array1);

  auto it = std::find(it_begin, it_end, 10);
  ASSERT_EQ(it.index(), 1);
  it = std::find(it_begin, it_end, nullopt);
  ASSERT_EQ(it.index(), 2);
  it = std::find(it_begin, it_end, 20);
  ASSERT_EQ(it, it_end);
}

TEST(ChunkedArrayIterator, StdFindIf) {
  auto chunked_array =
      ChunkedArrayFromJSON(int32(), {R"([5, 10])", R"([null])", R"([16])"});
  auto it_begin = Begin<Int32Type>(*chunked_array);
  auto it_end = End<Int32Type>(*chunked_array);

  auto it = std::find_if(it_begin, it_end, [](optional<int32_t> item) {
    return item.has_value() && *item >= 10;
  });
  ASSERT_EQ(it.index(), 1);
}

TEST(ChunkedArrayIterator, StdCountIf) {
  auto chunked_array1 =
      ChunkedArrayFromJSON(boolean(), {R"([true, null])", R"([null])", R"([false])"});

  auto n = std::count_if(Begin<BooleanType>(*chunked_array1),
                         End<BooleanType>(*chunked_array1),
                         [](optional<bool> v) { return !v.has_value(); });
  ASSERT_EQ(n, 2);
}

TEST(ChunkedArrayIterator, StdCopy) {
  auto chunked_array1 =
      ChunkedArrayFromJSON(int8(), {R"([4, 5])", R"([6])", R"([null, 8])"});
  auto it1 = Begin<Int8Type>(*chunked_array1);

  std::vector<optional<int8_t>> values;
  std::copy(it1 + 1, End<Int8Type>(*chunked_array1), std::back_inserter(values));
  std::vector<optional<int8_t>> expected{5, 6, {}, 8};
  ASSERT_EQ(values, expected);
}

TEST(ChunkedArrayIterator, StdMerge) {
  auto chunked_array1 =
      ChunkedArrayFromJSON(int8(), {R"([4, 5])", R"([6])", R"([7, 11, 13, 15, null])"});
  auto chunked_array2 =
      ChunkedArrayFromJSON(int8(), {R"([8, 9])", R"([10])", R"([14])", R"([16])"});

  auto cmp_lt = [](optional<int8_t> u, optional<int8_t> v) {
    return u.has_value() && (!v.has_value() || *u < *v);
  };

  std::vector<optional<int8_t>> values;

  std::merge(Begin<Int8Type>(*chunked_array1), End<Int8Type>(*chunked_array1),
             Begin<Int8Type>(*chunked_array2), End<Int8Type>(*chunked_array2),
             std::back_inserter(values), cmp_lt);
  std::vector<optional<int8_t>> expected{4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, {}};
  ASSERT_EQ(values, expected);
}

TEST(ChunkedArrayIterator, ForEachIterator) {
  auto chunked_array =
      ChunkedArrayFromJSON(int8(), {R"([4, 5])", R"([6])", R"([null, 8])"});
  std::vector<int8_t> expected = {4, 5, 6, 8};
  std::vector<int8_t> values;
  for (auto elem : Iterate<Int8Type>(*chunked_array)) {
    if (elem.has_value()) {
      values.push_back(*elem);
    }
  }
  ASSERT_EQ(values, expected);
}

}  // namespace stl
}  // namespace arrow
