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
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include "arrow/compute/kernels/test_util.h"

#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/heap_internal.h"

namespace arrow {
namespace internal {

using SortOrder = arrow::compute::SortOrder;

template <typename T, SortOrder sort_order>
struct CustomComparator {
  using HeapCompare = std::function<bool(const T&, const T&)>;
  using SortCompare = std::function<bool(const T&, const T&)>;

  static int64_t compare(const T&, const T&);
};

template <typename T>
struct CustomComparator<T, SortOrder::Ascending> {
  using HeapCompare = std::less<T>;
  using SortCompare = std::greater<T>;

  static int64_t compare(const T& lhs, const T& rhs) { return rhs - lhs; }
};

template <typename T>
struct CustomComparator<T, SortOrder::Descending> {
  using HeapCompare = std::greater<T>;
  using SortCompare = std::less<T>;

  static int64_t compare(const T& lhs, const T& rhs) { return lhs - rhs; }
};

template <typename ArrowType, SortOrder order>
void CheckHeapSort() {
  using T = typename TypeTraits<ArrowType>::CType;
  auto type = arrow::compute::default_type_instance<ArrowType>();

  const int64_t length = 200;
  auto rand = random::RandomArrayGenerator(/*seed=*/0);
  auto array = rand.ArrayOf(type, length, /*null_probability=*/0);
  ArrayData* array_data = array->data().get();
  const T* values_ptr = array_data->GetValues<T>(1);

  std::vector<T> values(values_ptr, values_ptr + length);
  using HeapCompare = typename CustomComparator<T, order>::HeapCompare;
  using SortCompare = typename CustomComparator<T, order>::SortCompare;
  Heap<T, HeapCompare> heap;
  for (auto&& v : values) {
    heap.Push(v);
  }
  std::vector<T> sorted_values_from_heap;
  while (!heap.empty()) {
    sorted_values_from_heap.push_back(heap.top());
    heap.Pop();
  }
  ASSERT_TRUE(heap.empty());

  std::vector<T> expected_sorted_values = values;
  std::sort(expected_sorted_values.begin(), expected_sorted_values.end(), SortCompare());

  ASSERT_EQ(sorted_values_from_heap, expected_sorted_values);
}

template <typename T>
class TestHeap : public ::testing::Test {};

using NumericBasedTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Date32Type, Date64Type>;

TYPED_TEST_SUITE(TestHeap, NumericBasedTypes);

TYPED_TEST(TestHeap, Ascending) { CheckHeapSort<UInt16Type, SortOrder::Ascending>(); }

TYPED_TEST(TestHeap, Descending) { CheckHeapSort<UInt16Type, SortOrder::Descending>(); }

template <typename ArrowType, SortOrder order>
void CheckStableHeapSort() {
  using T = typename TypeTraits<ArrowType>::CType;
  auto type = arrow::compute::default_type_instance<ArrowType>();

  const int64_t length = 10;
  auto rand = random::RandomArrayGenerator(/*seed=*/0);
  auto array = rand.ArrayOf(type, length, /*null_probability=*/0);
  ArrayData* array_data = array->data().get();
  const T* values_ptr = array_data->GetValues<T>(1);
  using HeapItem = std::pair<T, uint64_t>;
  std::vector<HeapItem> values;
  std::for_each(values_ptr, values_ptr + length, [&values](T val) {
    values.emplace_back(std::make_pair(val, values.size()));
  });
  auto compare_fn = CustomComparator<T, order>::compare;

  StableHeap<HeapItem> heap(
      [compare_fn](const HeapItem& lhs, const HeapItem& rhs) -> int64_t {
        return compare_fn(lhs.first, rhs.first);
      });
  for (auto&& v : values) {
    heap.Push(v);
  }
  std::vector<HeapItem> sorted_values_from_heap;
  while (!heap.empty()) {
    sorted_values_from_heap.push_back(heap.top());
    heap.Pop();
  }
  ASSERT_TRUE(heap.empty());

  std::vector<HeapItem> expected_sorted_values = values;
  std::stable_sort(expected_sorted_values.begin(), expected_sorted_values.end(),
                   [compare_fn](const HeapItem& lhs, const HeapItem& rhs) -> bool {
                     return compare_fn(lhs.first, rhs.first) > 0;
                   });

  ASSERT_EQ(sorted_values_from_heap, expected_sorted_values);
}

template <typename T>
class TestStableHeap : public ::testing::Test {};

TYPED_TEST_SUITE(TestStableHeap, NumericBasedTypes);

TYPED_TEST(TestStableHeap, Ascending) {
  CheckStableHeapSort<UInt16Type, SortOrder::Ascending>();
}
TYPED_TEST(TestStableHeap, Descending) {
  CheckStableHeapSort<UInt16Type, SortOrder::Descending>();
}

}  // namespace internal
}  // namespace arrow
