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

#include "parquet/arrow/path_internal.h"

#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "arrow/api.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"

#include "parquet/properties.h"

namespace parquet {
namespace arrow {
namespace {

using ::arrow::default_memory_pool;
using ::arrow::field;
using ::arrow::fixed_size_list;
using ::arrow::Status;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::NotNull;
using ::testing::SizeIs;

struct CapturedResult {
  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  bool null_rep_levels_ = false;
  bool null_def_levels_ = false;

  std::vector<ElementRange> post_list_elements;

  CapturedResult(const int16_t* def_levels, const int16_t* rep_levels,
                 int64_t def_rep_level_count,
                 std::vector<ElementRange> post_list_visited_elements) {
    if (def_levels != nullptr) {
      def_levels_ = std::vector<int16_t>(def_levels, def_levels + def_rep_level_count);
    } else {
      null_def_levels_ = true;
    }
    if (rep_levels != nullptr) {
      rep_levels_ = std::vector<int16_t>(rep_levels, rep_levels + def_rep_level_count);
    } else {
      null_rep_levels_ = true;
    }
    post_list_elements = std::move(post_list_visited_elements);
  }

  explicit CapturedResult(MultipathLevelBuilderResult result)
      : CapturedResult(result.def_levels, result.rep_levels, result.def_rep_level_count,
                       std::move(result.post_list_visited_elements)) {}

  void CheckLevelsWithNullRepLevels(const std::vector<int16_t>& expected_def) {
    EXPECT_TRUE(null_rep_levels_);
    ASSERT_FALSE(null_def_levels_);
    EXPECT_THAT(def_levels_, ElementsAreArray(expected_def));
  }

  void CheckLevels(const std::vector<int16_t>& expected_def,
                   const std::vector<int16_t>& expected_rep) const {
    ASSERT_FALSE(null_def_levels_);
    ASSERT_FALSE(null_rep_levels_);
    EXPECT_THAT(def_levels_, ElementsAreArray(expected_def));
    EXPECT_THAT(rep_levels_, ElementsAreArray(expected_rep));
  }
};

struct Callback {
  Status operator()(const MultipathLevelBuilderResult& result) {
    results->emplace_back(result);
    return Status::OK();
  }
  std::vector<CapturedResult>* results;
};

class MultipathLevelBuilderTest : public testing::Test {
 protected:
  std::vector<CapturedResult> results_;
  Callback callback_{&results_};
  std::shared_ptr<ArrowWriterProperties> arrow_properties_ =
      default_arrow_writer_properties();
  ArrowWriteContext context_ =
      ArrowWriteContext(default_memory_pool(), arrow_properties_.get());
};

TEST_F(MultipathLevelBuilderTest, NonNullableSingleListNonNullableEntries) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/false);
  auto list_type = large_list(entries);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([[1], [2, 3], [4, 5, 6]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/false, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];

  result.CheckLevels(/*def_levels=*/std::vector<int16_t>(/*count=*/6, 1),
                     /*rep_levels=*/{0, 0, 1, 0, 1, 1});

  ASSERT_THAT(result.post_list_elements, SizeIs(1));
  EXPECT_THAT(result.post_list_elements[0].start, Eq(0));
  EXPECT_THAT(result.post_list_elements[0].end, Eq(6));
}

TEST_F(MultipathLevelBuilderTest, NullableSingleListWithAllNullsLists) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/false);
  auto list_type = list(entries);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([null, null, null, null])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>(/*count=*/4, 0),
                     /*rep_levels=*/std::vector<int16_t>(4, 0));
}

TEST_F(MultipathLevelBuilderTest, NullableSingleListWithAllNullEntries) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_type = list(entries);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([[null], [null], [null], [null]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>(/*count=*/4, 2),
                     /*rep_levels=*/std::vector<int16_t>(4, 0));
  ASSERT_THAT(result.post_list_elements, SizeIs(1));
  EXPECT_THAT(result.post_list_elements[0].start, Eq(0));
  EXPECT_THAT(result.post_list_elements[0].end, Eq(4));
}

TEST_F(MultipathLevelBuilderTest, NullableSingleListWithAllPresentEntries) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_type = list(entries);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([[], [], [1], [], [2, 3]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>{1, 1, 3, 1, 3, 3},
                     /*rep_levels=*/std::vector<int16_t>{0, 0, 0, 0, 0, 1});

  ASSERT_THAT(result.post_list_elements, SizeIs(1));
  // JSON construction appears to only create child array lazily
  EXPECT_THAT(result.post_list_elements[0].start, Eq(0));
  EXPECT_THAT(result.post_list_elements[0].end, Eq(3));
}

TEST_F(MultipathLevelBuilderTest, NullableSingleListWithAllEmptyEntries) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_type = list(entries);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([[], [], [], [], []])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>(/*count=*/5, 1),
                     /*rep_levels=*/std::vector<int16_t>(/*count=*/5, 0));
}

TEST_F(MultipathLevelBuilderTest, NullableSingleListWithSomeNullEntriesAndSomeNullLists) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_type = list(entries);
  auto array = ::arrow::ArrayFromJSON(
      list_type, R"([null, [1 , 2, 3], [], [], null,  null, [4, 5], [null]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];

  result.CheckLevels(
      /*def_levels=*/std::vector<int16_t>{0, 3, 3, 3, 1, 1, 0, 0, 3, 3, 2},
      /*rep_levels=*/std::vector<int16_t>{0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0});
}

TEST_F(MultipathLevelBuilderTest, NestedListsWithSomeEntries) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto nested_list_type = list(list_field);

  auto array = ::arrow::ArrayFromJSON(
      nested_list_type, R"([null, [[1 , 2, 3], [4, 5]], [[], [], []], []])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>{0, 5, 5, 5, 5, 5, 3, 3, 3, 1},
                     /*rep_levels=*/std::vector<int16_t>{0, 0, 2, 2, 1, 2, 0, 1, 1, 0});
}

TEST_F(MultipathLevelBuilderTest, NestedListsWithSomeNulls) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto nested_list_type = list(list_field);

  auto array = ::arrow::ArrayFromJSON(nested_list_type,
                                      R"([null, [[1, null, 3], null, null], [[4, 5]]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>{0, 5, 4, 5, 2, 2, 5, 5},
                     /*rep_levels=*/std::vector<int16_t>{0, 0, 2, 2, 1, 1, 0, 2});
}

TEST_F(MultipathLevelBuilderTest, NestedListsWithSomeNullsSomeEmptys) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto nested_list_type = list(list_field);

  auto array = ::arrow::ArrayFromJSON(nested_list_type,
                                      R"([null, [[1 , null, 3], [], []], [[4, 5]]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>{0, 5, 4, 5, 3, 3, 5, 5},
                     /*rep_levels=*/std::vector<int16_t>{0, 0, 2, 2, 1, 1, 0, 2});
}

TEST_F(MultipathLevelBuilderTest, TripleNestedListsAllPresent) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto nested_list_type = list(list_field);
  auto double_nested_list_type = list(nested_list_type);

  auto array = ::arrow::ArrayFromJSON(double_nested_list_type,
                                      R"([ [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9]]] ])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>(9, 7),
                     /*rep_levels=*/std::vector<int16_t>{
                         0, 3, 3, 2, 3, 3, 1, 3, 3  // first row
                     });
}

TEST_F(MultipathLevelBuilderTest, QuadNestedListsAllPresent) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto nested_list_type = list(list_field);
  auto double_nested_list_type = list(nested_list_type);
  auto triple_nested_list_type = list(double_nested_list_type);

  auto array = ::arrow::ArrayFromJSON(triple_nested_list_type,
                                      R"([ [[[[1, 2], [3, 4]], [[5]]], [[[6, 7, 8]]]], 
					   [[[[1, 2], [3, 4]], [[5]]], [[[6, 7, 8]]]] ])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>(16, 9),
                     /*rep_levels=*/std::vector<int16_t>{
                         0, 4, 3, 4, 2, 1, 4, 4,  //
                         0, 4, 3, 4, 2, 1, 4, 4   //
                     });
}

TEST_F(MultipathLevelBuilderTest, TripleNestedListsWithSomeNullsSomeEmptys) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto nested_list_type = list(list_field);
  auto double_nested_list_type = list(nested_list_type);

  auto array = ::arrow::ArrayFromJSON(double_nested_list_type,
                                      R"([
                                           [null, [[1 , null, 3], []], []], 
                                           [[[]], [[], [1, 2]], null, [[3]]],
					   null, 
 					   []
                                         ])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  const CapturedResult& result = results_[0];
  result.CheckLevels(/*def_levels=*/std::vector<int16_t>{2, 7, 6, 7, 5, 3,  // first row
                                                         5, 5, 7, 7, 2, 7,  // second row
                                                         0,                 // third row
                                                         1},
                     /*rep_levels=*/std::vector<int16_t>{0, 1, 3, 3, 2, 1,  // first row
                                                         0, 1, 2, 3, 1, 1,  // second row
                                                         0, 0});
}

TEST_F(MultipathLevelBuilderTest, TestStruct) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/true);
  auto list_field = field("list", list(entries), /*nullable=*/true);
  auto struct_type = ::arrow::struct_({list_field, entries});

  auto array = ::arrow::ArrayFromJSON(struct_type,
                                      R"([{"Entries" : 1, "list": [2, 3]}, 
                                          {"Entries" : 4, "list": [5, 6]},
                                          null])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));
  ASSERT_THAT(results_, SizeIs(2));
  results_[0].CheckLevels(/*def_levels=*/std::vector<int16_t>{4, 4, 4, 4, 0},
                          /*rep_levels=*/std::vector<int16_t>{0, 1, 0, 1, 0});
  results_[1].CheckLevelsWithNullRepLevels(
      /*def_levels=*/std::vector<int16_t>({2, 2, 0}));
}

TEST_F(MultipathLevelBuilderTest, TestFixedSizeList) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/false);
  auto list_type = fixed_size_list(entries, 2);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([null, [2, 3], [4, 5], null])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));
  results_[0].CheckLevels(/*def_levels=*/std::vector<int16_t>{0, 1, 1, 1, 1, 0},
                          /*rep_levels=*/std::vector<int16_t>{0, 0, 1, 0, 1, 0});

  ASSERT_THAT(results_[0].post_list_elements, SizeIs(1));
  EXPECT_THAT(results_[0].post_list_elements[0].start, Eq(2));
  EXPECT_THAT(results_[0].post_list_elements[0].end, Eq(6));
}

TEST_F(MultipathLevelBuilderTest, TestFixedSizeListMissingMiddleHasTwoVisitedRanges) {
  auto entries = field("Entries", ::arrow::int64(), /*nullable=*/false);
  auto list_type = fixed_size_list(entries, 2);
  auto array = ::arrow::ArrayFromJSON(list_type, R"([[0, 1], null, [2, 3]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));

  ASSERT_THAT(results_, SizeIs(1));

  ASSERT_THAT(results_[0].post_list_elements, SizeIs(2));
  EXPECT_THAT(results_[0].post_list_elements[0].start, Eq(0));
  EXPECT_THAT(results_[0].post_list_elements[0].end, Eq(2));

  EXPECT_THAT(results_[0].post_list_elements[1].start, Eq(4));
  EXPECT_THAT(results_[0].post_list_elements[1].end, Eq(6));
}

TEST_F(MultipathLevelBuilderTest, TestMap) {
  auto map_type = ::arrow::map(::arrow::int64(), ::arrow::utf8());

  auto array = ::arrow::ArrayFromJSON(map_type,
                                      R"([[[1, "a"], [2, "b"]], 
                                          [[3, "c"], [4, null]]])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/true, &context_, callback_));
  ASSERT_THAT(results_, SizeIs(2));
  // keys -  Keys are always non-null so only null maps and null empty maps
  // count towards rep levels.
  results_[0].CheckLevels(/*def_levels=*/std::vector<int16_t>{2, 2, 2, 2},
                          /*rep_levels=*/std::vector<int16_t>{0, 1, 0, 1});
  // entries
  results_[1].CheckLevels(/*def_levels=*/std::vector<int16_t>{3, 3, 3, 2},
                          /*rep_levels=*/std::vector<int16_t>{0, 1, 0, 1});
}

TEST_F(MultipathLevelBuilderTest, TestPrimitiveNonNullable) {
  auto array = ::arrow::ArrayFromJSON(::arrow::int64(), R"([1, 2, 3, 4])");

  ASSERT_OK(
      MultipathLevelBuilder::Write(*array, /*nullable=*/false, &context_, callback_));
 
  ASSERT_THAT(results_, SizeIs(1));
  EXPECT_TRUE(results_[0].null_rep_levels_);
  EXPECT_TRUE(results_[0].null_def_levels_);

  ASSERT_THAT(results_[0].post_list_elements, SizeIs(1));
  EXPECT_THAT(results_[0].post_list_elements[0].start, Eq(0));
  EXPECT_THAT(results_[0].post_list_elements[0].end, Eq(4));
}

}  // namespace
}  // namespace arrow
}  // namespace parquet
