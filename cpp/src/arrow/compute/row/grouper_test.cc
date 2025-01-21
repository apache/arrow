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

#include <numeric>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/compute/row/grouper_internal.h"
#include "arrow/compute/test_util_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string.h"

namespace arrow::compute {

using ::arrow::internal::checked_pointer_cast;
using ::arrow::internal::ToChars;
using ::testing::Eq;
using ::testing::HasSubstr;

// Specialized case for GH-40997
TEST(Grouper, ResortedColumnsWithLargeNullRows) {
  const uint64_t num_rows = 1024;

  // construct random array with plenty of null values
  const int32_t kSeed = 42;
  const int32_t min = 0;
  const int32_t max = 100;
  const double null_probability = 0.3;
  const double true_probability = 0.5;
  auto rng = random::RandomArrayGenerator(kSeed);
  auto b_arr = rng.Boolean(num_rows, true_probability, null_probability);
  auto i32_arr = rng.Int32(num_rows, min, max, null_probability);
  auto i64_arr = rng.Int64(num_rows, min, max * 10, null_probability);

  // construct batches with columns which will be resorted in the grouper make
  std::vector<ExecBatch> exec_batches = {ExecBatch({i64_arr, i32_arr, b_arr}, num_rows),
                                         ExecBatch({i32_arr, i64_arr, b_arr}, num_rows),
                                         ExecBatch({i64_arr, b_arr, i32_arr}, num_rows),
                                         ExecBatch({i32_arr, b_arr, i64_arr}, num_rows),
                                         ExecBatch({b_arr, i32_arr, i64_arr}, num_rows),
                                         ExecBatch({b_arr, i64_arr, i32_arr}, num_rows)};

  const int num_batches = static_cast<int>(exec_batches.size());
  std::vector<uint32_t> group_num_vec;
  group_num_vec.reserve(num_batches);

  for (const auto& exec_batch : exec_batches) {
    ExecSpan span(exec_batch);
    ASSERT_OK_AND_ASSIGN(auto grouper, Grouper::Make(span.GetTypes()));
    ASSERT_OK_AND_ASSIGN(Datum group_ids, grouper->Consume(span));
    group_num_vec.emplace_back(grouper->num_groups());
  }

  for (int i = 1; i < num_batches; i++) {
    ASSERT_EQ(group_num_vec[i - 1], group_num_vec[i]);
  }
}

// Reproduction of GH-43124: Provoke var length buffer size if a grouper produces zero
// groups.
TEST(Grouper, EmptyGroups) {
  ASSERT_OK_AND_ASSIGN(auto grouper, Grouper::Make({int32(), utf8()}));
  ASSERT_OK_AND_ASSIGN(auto groups, grouper->GetUniques());

  ASSERT_TRUE(groups[0].is_array());
  ASSERT_EQ(groups[0].array()->buffers.size(), 2);
  ASSERT_EQ(groups[0].array()->buffers[0], nullptr);
  ASSERT_NE(groups[0].array()->buffers[1], nullptr);
  ASSERT_EQ(groups[0].array()->buffers[1]->size(), 0);

  ASSERT_TRUE(groups[1].is_array());
  ASSERT_EQ(groups[1].array()->buffers.size(), 3);
  ASSERT_EQ(groups[1].array()->buffers[0], nullptr);
  ASSERT_NE(groups[1].array()->buffers[1], nullptr);
  ASSERT_EQ(groups[1].array()->buffers[1]->size(), 4);
  ASSERT_EQ(groups[1].array()->buffers[1]->data_as<const uint32_t>()[0], 0);
  ASSERT_NE(groups[1].array()->buffers[2], nullptr);
  ASSERT_EQ(groups[1].array()->buffers[2]->size(), 0);
}

template <typename GroupClass>
void TestGroupClassSupportedKeys(
    std::function<Result<std::unique_ptr<GroupClass>>(const std::vector<TypeHolder>&)>
        make_func) {
  ASSERT_OK(make_func({boolean()}));

  ASSERT_OK(make_func({int8(), uint16(), int32(), uint64()}));

  ASSERT_OK(make_func({dictionary(int64(), utf8())}));

  ASSERT_OK(make_func({float16(), float32(), float64()}));

  ASSERT_OK(make_func({utf8(), binary(), large_utf8(), large_binary()}));

  ASSERT_OK(make_func({fixed_size_binary(16), fixed_size_binary(32)}));

  ASSERT_OK(make_func({decimal128(32, 10), decimal256(76, 20)}));

  ASSERT_OK(make_func({date32(), date64()}));

  for (auto unit : {
           TimeUnit::SECOND,
           TimeUnit::MILLI,
           TimeUnit::MICRO,
           TimeUnit::NANO,
       }) {
    ASSERT_OK(make_func({timestamp(unit), duration(unit)}));
  }

  ASSERT_OK(
      make_func({day_time_interval(), month_interval(), month_day_nano_interval()}));

  ASSERT_OK(make_func({null()}));

  ASSERT_RAISES(NotImplemented, make_func({struct_({field("", int64())})}));

  ASSERT_RAISES(NotImplemented, make_func({struct_({})}));

  ASSERT_RAISES(NotImplemented, make_func({list(int32())}));

  ASSERT_RAISES(NotImplemented, make_func({fixed_size_list(int32(), 5)}));

  ASSERT_RAISES(NotImplemented, make_func({dense_union({field("", int32())})}));
}

void TestSegments(std::unique_ptr<RowSegmenter>& segmenter, const ExecSpan& batch,
                  std::vector<Segment> expected_segments) {
  ASSERT_OK_AND_ASSIGN(auto actual_segments, segmenter->GetSegments(batch));
  ASSERT_EQ(actual_segments.size(), expected_segments.size());
  for (size_t i = 0; i < actual_segments.size(); ++i) {
    SCOPED_TRACE("segment #" + ToChars(i));
    ASSERT_EQ(actual_segments[i], expected_segments[i]);
  }
}

Result<std::unique_ptr<Grouper>> MakeGrouper(const std::vector<TypeHolder>& key_types) {
  return Grouper::Make(key_types, default_exec_context());
}

Result<std::unique_ptr<RowSegmenter>> MakeRowSegmenter(
    const std::vector<TypeHolder>& key_types) {
  return RowSegmenter::Make(key_types, /*nullable_leys=*/false, default_exec_context());
}

Result<std::unique_ptr<RowSegmenter>> MakeGenericSegmenter(
    const std::vector<TypeHolder>& key_types) {
  return MakeAnyKeysSegmenter(key_types, default_exec_context());
}

TEST(RowSegmenter, SupportedKeys) {
  TestGroupClassSupportedKeys<RowSegmenter>(MakeRowSegmenter);
}

TEST(RowSegmenter, Basics) {
  std::vector<TypeHolder> bad_types2 = {int32(), float32()};
  std::vector<TypeHolder> types2 = {int32(), int32()};
  std::vector<TypeHolder> bad_types1 = {float32()};
  std::vector<TypeHolder> types1 = {int32()};
  std::vector<TypeHolder> types0 = {};
  auto batch2 = ExecBatchFromJSON(types2, "[[1, 1], [1, 2], [2, 2]]");
  auto batch1 = ExecBatchFromJSON(types1, "[[1], [1], [2]]");
  ExecBatch batch0({}, 3);
  {
    SCOPED_TRACE("types0 segmenting of batch2");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types0));
    ExecSpan span2(batch2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch size 0 "),
                                    segmenter->GetSegments(span2));
    ExecSpan span0(batch0);
    TestSegments(segmenter, span0, {{0, 3, true, true}});
  }
  {
    SCOPED_TRACE("bad_types1 segmenting of batch1");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(bad_types1));
    ExecSpan span1(batch1);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch value 0 of type "),
                                    segmenter->GetSegments(span1));
  }
  {
    SCOPED_TRACE("types1 segmenting of batch2");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types1));
    ExecSpan span2(batch2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch size 1 "),
                                    segmenter->GetSegments(span2));
    ExecSpan span1(batch1);
    TestSegments(segmenter, span1, {{0, 2, false, true}, {2, 1, true, false}});
  }
  {
    SCOPED_TRACE("bad_types2 segmenting of batch2");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(bad_types2));
    ExecSpan span2(batch2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch value 1 of type "),
                                    segmenter->GetSegments(span2));
  }
  {
    SCOPED_TRACE("types2 segmenting of batch1");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types2));
    ExecSpan span1(batch1);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch size 2 "),
                                    segmenter->GetSegments(span1));
    ExecSpan span2(batch2);
    TestSegments(segmenter, span2,
                 {{0, 1, false, true}, {1, 1, false, false}, {2, 1, true, false}});
  }
}

TEST(RowSegmenter, NonOrdered) {
  for (int num_keys = 1; num_keys <= 2; ++num_keys) {
    SCOPED_TRACE("non-ordered " + ToChars(num_keys) + " int32(s)");
    std::vector<TypeHolder> types(num_keys, int32());
    std::vector<Datum> values(num_keys, ArrayFromJSON(int32(), "[1, 1, 2, 1, 2]"));
    ExecBatch batch(std::move(values), 5);
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
    TestSegments(segmenter, ExecSpan(batch),
                 {{0, 2, false, true},
                  {2, 1, false, false},
                  {3, 1, false, false},
                  {4, 1, true, false}});
  }
}

TEST(RowSegmenter, EmptyBatches) {
  {
    SCOPED_TRACE("empty batches {int32}");
    std::vector<TypeHolder> types = {int32()};
    std::vector<ExecBatch> batches = {
        ExecBatchFromJSON(types, "[]"),         ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[[1]]"),      ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[[1]]"),      ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[[2], [2]]"), ExecBatchFromJSON(types, "[]"),
    };
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
    TestSegments(segmenter, ExecSpan(batches[0]), {});
    TestSegments(segmenter, ExecSpan(batches[1]), {});
    TestSegments(segmenter, ExecSpan(batches[2]), {{0, 1, true, true}});
    TestSegments(segmenter, ExecSpan(batches[3]), {});
    TestSegments(segmenter, ExecSpan(batches[4]), {{0, 1, true, true}});
    TestSegments(segmenter, ExecSpan(batches[5]), {});
    TestSegments(segmenter, ExecSpan(batches[6]), {{0, 2, true, false}});
    TestSegments(segmenter, ExecSpan(batches[7]), {});
  }
  {
    SCOPED_TRACE("empty batches {int32, int32}");
    std::vector<TypeHolder> types = {int32(), int32()};
    std::vector<ExecBatch> batches = {
        ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[[1, 1]]"),
        ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[[1, 1]]"),
        ExecBatchFromJSON(types, "[]"),
        ExecBatchFromJSON(types, "[[2, 2], [2, 2]]"),
        ExecBatchFromJSON(types, "[]"),
    };
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
    TestSegments(segmenter, ExecSpan(batches[0]), {});
    TestSegments(segmenter, ExecSpan(batches[1]), {});
    TestSegments(segmenter, ExecSpan(batches[2]), {{0, 1, true, true}});
    TestSegments(segmenter, ExecSpan(batches[3]), {});
    TestSegments(segmenter, ExecSpan(batches[4]), {{0, 1, true, true}});
    TestSegments(segmenter, ExecSpan(batches[5]), {});
    TestSegments(segmenter, ExecSpan(batches[6]), {{0, 2, true, false}});
    TestSegments(segmenter, ExecSpan(batches[7]), {});
  }
}

TEST(RowSegmenter, MultipleSegments) {
  auto test_with_keys = [](int num_keys, const std::shared_ptr<Array>& key) {
    SCOPED_TRACE("multiple segments " + ToChars(num_keys) + " " +
                 key->type()->ToString());
    std::vector<TypeHolder> types(num_keys, key->type());
    std::vector<Datum> values(num_keys, key);
    ExecBatch batch(std::move(values), key->length());
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
    TestSegments(segmenter, ExecSpan(batch),
                 {{0, 2, false, true},
                  {2, 1, false, false},
                  {3, 1, false, false},
                  {4, 2, false, false},
                  {6, 2, false, false},
                  {8, 1, true, false}});
  };
  for (int num_keys = 1; num_keys <= 2; ++num_keys) {
    test_with_keys(num_keys, ArrayFromJSON(int32(), "[1, 1, 2, 5, 3, 3, 5, 5, 4]"));
    test_with_keys(
        num_keys,
        ArrayFromJSON(fixed_size_binary(2),
                      R"(["aa", "aa", "bb", "ee", "cc", "cc", "ee", "ee", "dd"])"));
    test_with_keys(num_keys, DictArrayFromJSON(dictionary(int8(), utf8()),
                                               "[0, 0, 1, 4, 2, 2, 4, 4, 3]",
                                               R"(["a", "b", "c", "d", "e"])"));
  }
}

TEST(RowSegmenter, MultipleSegmentsMultipleBatches) {
  {
    SCOPED_TRACE("multiple segments multiple batches {int32}");
    std::vector<TypeHolder> types = {int32()};
    std::vector<ExecBatch> batches = {
        ExecBatchFromJSON(types, "[[1]]"), ExecBatchFromJSON(types, "[[1], [2]]"),
        ExecBatchFromJSON(types, "[[5], [3]]"),
        ExecBatchFromJSON(types, "[[3], [5], [5]]"), ExecBatchFromJSON(types, "[[4]]")};

    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
    TestSegments(segmenter, ExecSpan(batches[0]), {{0, 1, true, true}});
    TestSegments(segmenter, ExecSpan(batches[1]),
                 {{0, 1, false, true}, {1, 1, true, false}});
    TestSegments(segmenter, ExecSpan(batches[2]),
                 {{0, 1, false, false}, {1, 1, true, false}});
    TestSegments(segmenter, ExecSpan(batches[3]),
                 {{0, 1, false, true}, {1, 2, true, false}});
    TestSegments(segmenter, ExecSpan(batches[4]), {{0, 1, true, false}});
  }
  {
    SCOPED_TRACE("multiple segments multiple batches {int32, int32}");
    std::vector<TypeHolder> types = {int32(), int32()};
    std::vector<ExecBatch> batches = {
        ExecBatchFromJSON(types, "[[1, 1]]"),
        ExecBatchFromJSON(types, "[[1, 1], [2, 2]]"),
        ExecBatchFromJSON(types, "[[5, 5], [3, 3]]"),
        ExecBatchFromJSON(types, "[[3, 3], [5, 5], [5, 5]]"),
        ExecBatchFromJSON(types, "[[4, 4]]")};

    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
    TestSegments(segmenter, ExecSpan(batches[0]), {{0, 1, true, true}});
    TestSegments(segmenter, ExecSpan(batches[1]),
                 {{0, 1, false, true}, {1, 1, true, false}});
    TestSegments(segmenter, ExecSpan(batches[2]),
                 {{0, 1, false, false}, {1, 1, true, false}});
    TestSegments(segmenter, ExecSpan(batches[3]),
                 {{0, 1, false, true}, {1, 2, true, false}});
    TestSegments(segmenter, ExecSpan(batches[4]), {{0, 1, true, false}});
  }
}

void TestRowSegmenterConstantBatch(
    const std::shared_ptr<DataType>& type,
    std::function<ArgShape(int64_t key)> shape_func,
    std::function<Result<std::shared_ptr<Scalar>>(int64_t key)> value_func,
    std::function<Result<std::unique_ptr<RowSegmenter>>(const std::vector<TypeHolder>&)>
        make_segmenter) {
  constexpr int64_t n_keys = 3, n_rows = 3, repetitions = 3;
  std::vector<TypeHolder> types(n_keys, type);
  std::vector<Datum> full_values(n_keys);
  for (int64_t i = 0; i < n_keys; i++) {
    auto shape = shape_func(i);
    ASSERT_OK_AND_ASSIGN(auto scalar, value_func(i));
    if (shape == ArgShape::SCALAR) {
      full_values[i] = std::move(scalar);
    } else {
      ASSERT_OK_AND_ASSIGN(full_values[i], MakeArrayFromScalar(*scalar, n_rows));
    }
  }
  auto test_with_keys = [&](int64_t keys) -> Status {
    SCOPED_TRACE("constant-batch with " + ToChars(keys) + " key(s)");
    std::vector<Datum> values(full_values.begin(), full_values.begin() + keys);
    ExecBatch batch(values, n_rows);
    std::vector<TypeHolder> key_types(types.begin(), types.begin() + keys);
    ARROW_ASSIGN_OR_RAISE(auto segmenter, make_segmenter(key_types));
    for (int64_t i = 0; i < repetitions; i++) {
      TestSegments(segmenter, ExecSpan(batch), {{0, n_rows, true, true}});
      ARROW_RETURN_NOT_OK(segmenter->Reset());
    }
    return Status::OK();
  };
  for (int64_t i = 0; i <= n_keys; i++) {
    ASSERT_OK(test_with_keys(i));
  }
}

TEST(RowSegmenter, ConstantArrayBatch) {
  TestRowSegmenterConstantBatch(
      int32(), [](int64_t key) { return ArgShape::ARRAY; },
      [](int64_t key) { return MakeScalar(1); }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantScalarBatch) {
  TestRowSegmenterConstantBatch(
      int32(), [](int64_t key) { return ArgShape::SCALAR; },
      [](int64_t key) { return MakeScalar(1); }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantMixedBatch) {
  TestRowSegmenterConstantBatch(
      int32(),
      [](int64_t key) { return key % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      [](int64_t key) { return MakeScalar(1); }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantArrayBatchWithAnyKeysSegmenter) {
  TestRowSegmenterConstantBatch(
      int32(), [](int64_t key) { return ArgShape::ARRAY; },
      [](int64_t key) { return MakeScalar(1); }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantScalarBatchWithAnyKeysSegmenter) {
  TestRowSegmenterConstantBatch(
      int32(), [](int64_t key) { return ArgShape::SCALAR; },
      [](int64_t key) { return MakeScalar(1); }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantMixedBatchWithAnyKeysSegmenter) {
  TestRowSegmenterConstantBatch(
      int32(),
      [](int64_t key) { return key % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      [](int64_t key) { return MakeScalar(1); }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantFixedSizeBinaryArrayBatch) {
  constexpr int fsb = 8;
  auto type = fixed_size_binary(fsb);
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(type, std::string(fsb, 'X')));
  TestRowSegmenterConstantBatch(
      type, [](int64_t key) { return ArgShape::ARRAY; },
      [&](int64_t key) { return value; }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantFixedSizeBinaryScalarBatch) {
  constexpr int fsb = 8;
  auto type = fixed_size_binary(fsb);
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(type, std::string(fsb, 'X')));
  TestRowSegmenterConstantBatch(
      fixed_size_binary(8), [](int64_t key) { return ArgShape::SCALAR; },
      [&](int64_t key) { return value; }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantFixedSizeBinaryMixedBatch) {
  constexpr int fsb = 8;
  auto type = fixed_size_binary(fsb);
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(type, std::string(fsb, 'X')));
  TestRowSegmenterConstantBatch(
      fixed_size_binary(8),
      [](int64_t key) { return key % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      [&](int64_t key) { return value; }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantFixedSizeBinaryArrayBatchWithAnyKeysSegmenter) {
  constexpr int fsb = 8;
  auto type = fixed_size_binary(fsb);
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(type, std::string(fsb, 'X')));
  TestRowSegmenterConstantBatch(
      type, [](int64_t key) { return ArgShape::ARRAY; },
      [&](int64_t key) { return value; }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantFixedSizeBinaryScalarBatchWithAnyKeysSegmenter) {
  constexpr int fsb = 8;
  auto type = fixed_size_binary(fsb);
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(type, std::string(fsb, 'X')));
  TestRowSegmenterConstantBatch(
      fixed_size_binary(8), [](int64_t key) { return ArgShape::SCALAR; },
      [&](int64_t key) { return value; }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantFixedSizeBinaryMixedBatchWithAnyKeysSegmenter) {
  constexpr int fsb = 8;
  auto type = fixed_size_binary(fsb);
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(type, std::string(fsb, 'X')));
  TestRowSegmenterConstantBatch(
      fixed_size_binary(8),
      [](int64_t key) { return key % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      [&](int64_t key) { return value; }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantDictionaryArrayBatch) {
  auto index_type = int32();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto dict = ArrayFromJSON(value_type, R"(["alpha", null, "gamma"])");
  ASSERT_OK_AND_ASSIGN(auto index_value, MakeScalar(index_type, 0));
  auto dict_value = DictionaryScalar::Make(std::move(index_value), dict);
  TestRowSegmenterConstantBatch(
      dict_type, [](int64_t key) { return ArgShape::ARRAY; },
      [&](int64_t key) { return dict_value; }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantDictionaryScalarBatch) {
  auto index_type = int32();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto dict = ArrayFromJSON(value_type, R"(["alpha", null, "gamma"])");
  ASSERT_OK_AND_ASSIGN(auto index_value, MakeScalar(index_type, 0));
  auto dict_value = DictionaryScalar::Make(std::move(index_value), dict);
  TestRowSegmenterConstantBatch(
      dict_type, [](int64_t key) { return ArgShape::SCALAR; },
      [&](int64_t key) { return dict_value; }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantDictionaryMixedBatch) {
  auto index_type = int32();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto dict = ArrayFromJSON(value_type, R"(["alpha", null, "gamma"])");
  ASSERT_OK_AND_ASSIGN(auto index_value, MakeScalar(index_type, 0));
  auto dict_value = DictionaryScalar::Make(std::move(index_value), dict);
  TestRowSegmenterConstantBatch(
      dict_type,
      [](int64_t key) { return key % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      [&](int64_t key) { return dict_value; }, MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantDictionaryArrayBatchWithAnyKeysSegmenter) {
  auto index_type = int32();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto dict = ArrayFromJSON(value_type, R"(["alpha", null, "gamma"])");
  ASSERT_OK_AND_ASSIGN(auto index_value, MakeScalar(index_type, 0));
  auto dict_value = DictionaryScalar::Make(std::move(index_value), dict);
  TestRowSegmenterConstantBatch(
      dict_type, [](int64_t key) { return ArgShape::ARRAY; },
      [&](int64_t key) { return dict_value; }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantDictionaryScalarBatchWithAnyKeysSegmenter) {
  auto index_type = int32();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto dict = ArrayFromJSON(value_type, R"(["alpha", null, "gamma"])");
  ASSERT_OK_AND_ASSIGN(auto index_value, MakeScalar(index_type, 0));
  auto dict_value = DictionaryScalar::Make(std::move(index_value), dict);
  TestRowSegmenterConstantBatch(
      dict_type, [](int64_t key) { return ArgShape::SCALAR; },
      [&](int64_t key) { return dict_value; }, MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantDictionaryMixedBatchWithAnyKeysSegmenter) {
  auto index_type = int32();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto dict = ArrayFromJSON(value_type, R"(["alpha", null, "gamma"])");
  ASSERT_OK_AND_ASSIGN(auto index_value, MakeScalar(index_type, 0));
  auto dict_value = DictionaryScalar::Make(std::move(index_value), dict);
  TestRowSegmenterConstantBatch(
      dict_type,
      [](int64_t key) { return key % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      [&](int64_t key) { return dict_value; }, MakeGenericSegmenter);
}

TEST(RowSegmenter, RowConstantBatch) {
  constexpr size_t n = 3;
  std::vector<TypeHolder> types = {int32(), int32(), int32()};
  auto full_batch = ExecBatchFromJSON(types, "[[1, 1, 1], [2, 2, 2], [3, 3, 3]]");
  std::vector<Segment> expected_segments_for_size_0 = {{0, 3, true, true}};
  std::vector<Segment> expected_segments = {
      {0, 1, false, true}, {1, 1, false, false}, {2, 1, true, false}};
  auto test_by_size = [&](size_t size) -> Status {
    SCOPED_TRACE("constant-batch with " + ToChars(size) + " key(s)");
    std::vector<Datum> values(full_batch.values.begin(),
                              full_batch.values.begin() + size);
    ExecBatch batch(values, full_batch.length);
    std::vector<TypeHolder> key_types(types.begin(), types.begin() + size);
    ARROW_ASSIGN_OR_RAISE(auto segmenter, MakeRowSegmenter(key_types));
    TestSegments(segmenter, ExecSpan(batch),
                 size == 0 ? expected_segments_for_size_0 : expected_segments);
    return Status::OK();
  };
  for (size_t i = 0; i <= n; i++) {
    ASSERT_OK(test_by_size(i));
  }
}

TEST(Grouper, SupportedKeys) { TestGroupClassSupportedKeys<Grouper>(MakeGrouper); }

struct TestGrouper {
  explicit TestGrouper(std::vector<TypeHolder> types, std::vector<ArgShape> shapes = {})
      : types_(std::move(types)), shapes_(std::move(shapes)) {
    grouper_ = Grouper::Make(types_).ValueOrDie();

    FieldVector fields;
    for (const auto& type : types_) {
      fields.push_back(field("", type.GetSharedPtr()));
    }
    key_schema_ = schema(std::move(fields));
  }

  void ExpectConsume(const std::string& key_json, const std::string& expected) {
    auto expected_arr = ArrayFromJSON(uint32(), expected);
    if (shapes_.size() > 0) {
      ExpectConsume(ExecBatchFromJSON(types_, shapes_, key_json), expected_arr);
    } else {
      ExpectConsume(ExecBatchFromJSON(types_, key_json), expected_arr);
    }
  }

  void ExpectConsume(const std::vector<Datum>& key_values, Datum expected) {
    ASSERT_OK_AND_ASSIGN(auto key_batch, ExecBatch::Make(key_values));
    ExpectConsume(key_batch, expected);
  }

  void ExpectConsume(const ExecBatch& key_batch, Datum expected) {
    Datum ids;
    ConsumeAndValidate(key_batch, &ids);
    AssertEquivalentIds(expected, ids);
  }

  void ExpectUniques(const ExecBatch& uniques) {
    EXPECT_THAT(grouper_->GetUniques(), ResultWith(Eq(uniques)));
  }

  void ExpectUniques(const std::string& uniques_json) {
    if (shapes_.size() > 0) {
      ExpectUniques(ExecBatchFromJSON(types_, shapes_, uniques_json));
    } else {
      ExpectUniques(ExecBatchFromJSON(types_, uniques_json));
    }
  }

  void AssertEquivalentIds(const Datum& expected, const Datum& actual) {
    auto left = expected.make_array();
    auto right = actual.make_array();
    ASSERT_EQ(left->length(), right->length()) << "#ids unequal";
    int64_t num_ids = left->length();
    auto left_data = left->data();
    auto right_data = right->data();
    auto left_ids = reinterpret_cast<const uint32_t*>(left_data->buffers[1]->data());
    auto right_ids = reinterpret_cast<const uint32_t*>(right_data->buffers[1]->data());
    uint32_t max_left_id = 0;
    uint32_t max_right_id = 0;
    for (int64_t i = 0; i < num_ids; ++i) {
      if (left_ids[i] > max_left_id) {
        max_left_id = left_ids[i];
      }
      if (right_ids[i] > max_right_id) {
        max_right_id = right_ids[i];
      }
    }
    std::vector<bool> right_to_left_present(max_right_id + 1, false);
    std::vector<bool> left_to_right_present(max_left_id + 1, false);
    std::vector<uint32_t> right_to_left(max_right_id + 1);
    std::vector<uint32_t> left_to_right(max_left_id + 1);
    for (int64_t i = 0; i < num_ids; ++i) {
      uint32_t left_id = left_ids[i];
      uint32_t right_id = right_ids[i];
      if (!left_to_right_present[left_id]) {
        left_to_right[left_id] = right_id;
        left_to_right_present[left_id] = true;
      }
      if (!right_to_left_present[right_id]) {
        right_to_left[right_id] = left_id;
        right_to_left_present[right_id] = true;
      }
      ASSERT_EQ(left_id, right_to_left[right_id]);
      ASSERT_EQ(right_id, left_to_right[left_id]);
    }
  }

  void ConsumeAndValidate(const ExecBatch& key_batch, Datum* ids = nullptr) {
    ASSERT_OK_AND_ASSIGN(Datum id_batch, grouper_->Consume(ExecSpan(key_batch)));

    ValidateConsume(key_batch, id_batch);

    if (ids) {
      *ids = std::move(id_batch);
    }
  }

  void ValidateConsume(const ExecBatch& key_batch, const Datum& id_batch) {
    if (uniques_.length == -1) {
      ASSERT_OK_AND_ASSIGN(uniques_, grouper_->GetUniques());
    } else if (static_cast<int64_t>(grouper_->num_groups()) > uniques_.length) {
      ASSERT_OK_AND_ASSIGN(ExecBatch new_uniques, grouper_->GetUniques());

      // check that uniques_ are prefixes of new_uniques
      for (int i = 0; i < uniques_.num_values(); ++i) {
        auto new_unique = new_uniques[i].make_array();
        ValidateOutput(*new_unique);

        AssertDatumsEqual(uniques_[i], new_unique->Slice(0, uniques_.length),
                          /*verbose=*/true);
      }

      uniques_ = std::move(new_uniques);
    }

    // check that the ids encode an equivalent key sequence
    auto ids = id_batch.make_array();
    ValidateOutput(*ids);

    for (int i = 0; i < key_batch.num_values(); ++i) {
      SCOPED_TRACE(ToChars(i) + "th key array");
      auto original =
          key_batch[i].is_array()
              ? key_batch[i].make_array()
              : *MakeArrayFromScalar(*key_batch[i].scalar(), key_batch.length);
      ASSERT_OK_AND_ASSIGN(auto encoded, Take(*uniques_[i].make_array(), *ids));
      AssertArraysEqual(*original, *encoded, /*verbose=*/true,
                        EqualOptions().nans_equal(true));
    }
  }

  std::vector<TypeHolder> types_;
  std::vector<ArgShape> shapes_;
  std::shared_ptr<Schema> key_schema_;
  std::unique_ptr<Grouper> grouper_;
  ExecBatch uniques_ = ExecBatch({}, -1);
};

TEST(Grouper, BooleanKey) {
  TestGrouper g({boolean()});

  g.ExpectConsume("[[true], [true]]", "[0, 0]");

  g.ExpectConsume("[[true], [true]]", "[0, 0]");

  g.ExpectConsume("[[false], [null]]", "[1, 2]");

  g.ExpectConsume("[[true], [false], [true], [false], [null], [false], [null]]",
                  "[0, 1, 0, 1, 2, 1, 2]");
}

TEST(Grouper, NumericKey) {
  for (auto ty : {
           uint8(),
           int8(),
           uint16(),
           int16(),
           uint32(),
           int32(),
           uint64(),
           int64(),
           float16(),
           float32(),
           float64(),
       }) {
    SCOPED_TRACE("key type: " + ty->ToString());

    TestGrouper g({ty});

    g.ExpectConsume("[[3], [3]]", "[0, 0]");
    g.ExpectUniques("[[3]]");

    g.ExpectConsume("[[3], [3]]", "[0, 0]");
    g.ExpectUniques("[[3]]");

    g.ExpectConsume("[[27], [81], [81]]", "[1, 2, 2]");
    g.ExpectUniques("[[3], [27], [81]]");

    g.ExpectConsume("[[3], [27], [3], [27], [null], [81], [27], [81]]",
                    "[0, 1, 0, 1, 3, 2, 1, 2]");
    g.ExpectUniques("[[3], [27], [81], [null]]");
  }
}

TEST(Grouper, FloatingPointKey) {
  TestGrouper g({float32()});

  // -0.0 hashes differently from 0.0
  g.ExpectConsume("[[0.0], [-0.0]]", "[0, 1]");

  g.ExpectConsume("[[Inf], [-Inf]]", "[2, 3]");

  // assert(!(NaN == NaN)) does not cause spurious new groups
  g.ExpectConsume("[[NaN], [NaN]]", "[4, 4]");

  // TODO(bkietz) test denormal numbers, more NaNs
}

TEST(Grouper, StringKey) {
  for (auto ty : {utf8(), large_utf8(), fixed_size_binary(2)}) {
    SCOPED_TRACE("key type: " + ty->ToString());

    TestGrouper g({ty});

    g.ExpectConsume(R"([["eh"], ["eh"]])", "[0, 0]");

    g.ExpectConsume(R"([["eh"], ["eh"]])", "[0, 0]");

    g.ExpectConsume(R"([["be"], [null]])", "[1, 2]");
  }
}

TEST(Grouper, DictKey) {
  TestGrouper g({dictionary(int32(), utf8())});

  // For dictionary keys, all batches must share a single dictionary.
  // Eventually, differing dictionaries will be unified and indices transposed
  // during encoding to relieve this restriction.
  const auto dict = ArrayFromJSON(utf8(), R"(["ex", "why", "zee", null])");

  auto WithIndices = [&](const std::string& indices) {
    return Datum(*DictionaryArray::FromArrays(ArrayFromJSON(int32(), indices), dict));
  };

  // NB: null index is not considered equivalent to index=3 (which encodes null in dict)
  g.ExpectConsume({WithIndices("           [3, 1, null, 0, 2]")},
                  ArrayFromJSON(uint32(), "[0, 1, 2, 3, 4]"));

  g = TestGrouper({dictionary(int32(), utf8())});

  g.ExpectConsume({WithIndices("           [0, 1, 2, 3, null]")},
                  ArrayFromJSON(uint32(), "[0, 1, 2, 3, 4]"));

  g.ExpectConsume({WithIndices("           [3, 1, null, 0, 2]")},
                  ArrayFromJSON(uint32(), "[3, 1, 4,    0, 2]"));

  auto dict_arr = *DictionaryArray::FromArrays(
      ArrayFromJSON(int32(), "[0, 1]"),
      ArrayFromJSON(utf8(), R"(["different", "dictionary"])"));
  ExecSpan dict_span({*dict_arr->data()}, 2);
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  HasSubstr("Unifying differing dictionaries"),
                                  g.grouper_->Consume(dict_span));
}

TEST(Grouper, StringInt64Key) {
  TestGrouper g({utf8(), int64()});

  g.ExpectConsume(R"([["eh", 0], ["eh", 0]])", "[0, 0]");

  g.ExpectConsume(R"([["eh", 0], ["eh", null]])", "[0, 1]");

  g.ExpectConsume(R"([["eh", 1], ["bee", 1]])", "[2, 3]");

  g.ExpectConsume(R"([["eh", null], ["bee", 1]])", "[1, 3]");

  g = TestGrouper({utf8(), int64()});

  g.ExpectConsume(R"([
    ["ex",  0],
    ["ex",  0],
    ["why", 0],
    ["ex",  1],
    ["why", 0],
    ["ex",  1],
    ["ex",  0],
    ["why", 1]
  ])",
                  "[0, 0, 1, 2, 1, 2, 0, 3]");

  g.ExpectConsume(R"([
    ["ex",  0],
    [null,  0],
    [null,  0],
    ["ex",  1],
    [null,  null],
    ["ex",  1],
    ["ex",  0],
    ["why", null]
  ])",
                  "[0, 4, 4, 2, 5, 2, 0, 6]");
}

TEST(Grouper, DoubleStringInt64Key) {
  TestGrouper g({float64(), utf8(), int64()});

  g.ExpectConsume(R"([[1.5, "eh", 0], [1.5, "eh", 0]])", "[0, 0]");

  g.ExpectConsume(R"([[1.5, "eh", 0], [1.5, "eh", 0]])", "[0, 0]");

  g.ExpectConsume(R"([[1.0, "eh", 0], [1.0, "be", null]])", "[1, 2]");

  // note: -0 and +0 hash differently
  g.ExpectConsume(R"([[-0.0, "be", 7], [0.0, "be", 7]])", "[3, 4]");
}

TEST(Grouper, RandomInt64Keys) {
  TestGrouper g({int64()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(ToChars(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, RandomStringInt64Keys) {
  TestGrouper g({utf8(), int64()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(ToChars(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, RandomStringInt64DoubleInt32Keys) {
  TestGrouper g({utf8(), int64(), float64(), int32()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(ToChars(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, NullKeys) {
  TestGrouper g({null()});
  g.ExpectConsume("[[null], [null]]", "[0, 0]");
}

TEST(Grouper, MultipleNullKeys) {
  TestGrouper g({null(), null(), null(), null()});
  g.ExpectConsume("[[null, null, null, null], [null, null, null, null]]", "[0, 0]");
}

TEST(Grouper, Int64NullKeys) {
  TestGrouper g({int64(), null()});
  g.ExpectConsume("[[1, null], [2, null], [1, null]]", "[0, 1, 0]");
}

TEST(Grouper, StringNullKeys) {
  TestGrouper g({utf8(), null()});
  g.ExpectConsume(R"([["be", null], ["eh", null]])", "[0, 1]");
}

TEST(Grouper, DoubleNullStringKey) {
  TestGrouper g({float64(), null(), utf8()});

  g.ExpectConsume(R"([[1.5, null, "eh"], [1.5, null, "eh"]])", "[0, 0]");
  g.ExpectConsume(R"([[null, null, "eh"], [1.0, null, null]])", "[1, 2]");
  g.ExpectConsume(R"([
    [1.0,  null, "wh"],
    [4.4,  null, null],
    [5.2,  null, "eh"],
    [6.5,  null, "be"],
    [7.3,  null, null],
    [1.0,  null, "wh"],
    [9.1,  null, "eh"],
    [10.2, null, "be"],
    [1.0, null, null]
  ])",
                  "[3, 4, 5, 6, 7, 3, 8, 9, 2]");
}

TEST(Grouper, EmptyNullKeys) {
  TestGrouper g({null()});
  g.ExpectConsume("[]", "[]");
}

TEST(Grouper, MakeGroupings) {
  auto ExpectGroupings = [](std::string ids_json, std::string expected_json) {
    auto ids = checked_pointer_cast<UInt32Array>(ArrayFromJSON(uint32(), ids_json));
    auto expected = ArrayFromJSON(list(int32()), expected_json);

    auto num_groups = static_cast<uint32_t>(expected->length());
    ASSERT_OK_AND_ASSIGN(auto actual, Grouper::MakeGroupings(*ids, num_groups));
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);

    // validate ApplyGroupings
    ASSERT_OK_AND_ASSIGN(auto grouped_ids, Grouper::ApplyGroupings(*actual, *ids));

    for (uint32_t group = 0; group < num_groups; ++group) {
      auto ids_slice = checked_pointer_cast<UInt32Array>(grouped_ids->value_slice(group));
      for (auto slot : *ids_slice) {
        EXPECT_EQ(slot, group);
      }
    }
  };

  ExpectGroupings("[]", "[[]]");

  ExpectGroupings("[0, 0, 0]", "[[0, 1, 2]]");

  ExpectGroupings("[0, 0, 0, 1, 1, 2]", "[[0, 1, 2], [3, 4], [5], []]");

  ExpectGroupings("[2, 1, 2, 1, 1, 2]", "[[], [1, 3, 4], [0, 2, 5], [], []]");

  ExpectGroupings("[2, 2, 5, 5, 2, 3]", "[[], [], [0, 1, 4], [5], [], [2, 3], [], []]");

  auto ids = checked_pointer_cast<UInt32Array>(ArrayFromJSON(uint32(), "[0, null, 1]"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("MakeGroupings with null ids"),
                                  Grouper::MakeGroupings(*ids, 5));
}

TEST(Grouper, ScalarValues) {
  // large_utf8 forces GrouperImpl over GrouperFastImpl
  for (const auto& str_type : {utf8(), large_utf8()}) {
    {
      TestGrouper g(
          {boolean(), int32(), decimal128(3, 2), decimal256(3, 2), fixed_size_binary(2),
           str_type, int32()},
          {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR,
           ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY});
      g.ExpectConsume(
          R"([
[true, 1, "1.00", "2.00", "ab", "foo", 2],
[true, 1, "1.00", "2.00", "ab", "foo", 2],
[true, 1, "1.00", "2.00", "ab", "foo", 3]
])",
          "[0, 0, 1]");
    }
    {
      auto dict_type = dictionary(int32(), utf8());
      TestGrouper g({dict_type, str_type}, {ArgShape::SCALAR, ArgShape::SCALAR});
      const auto dict = R"(["foo", null])";
      g.ExpectConsume(
          {DictScalarFromJSON(dict_type, "0", dict), ScalarFromJSON(str_type, R"("")")},
          ArrayFromJSON(uint32(), "[0]"));
      g.ExpectConsume(
          {DictScalarFromJSON(dict_type, "1", dict), ScalarFromJSON(str_type, R"("")")},
          ArrayFromJSON(uint32(), "[1]"));
    }
  }
}

}  // namespace arrow::compute
