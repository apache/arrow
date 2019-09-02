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

#include "gtest/gtest.h"

#include <vector>

#include "arrow/api.h"
#include "arrow/colfmt/api.h"
#include "arrow/colfmt/test-util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace colfmt {

template <typename data_type, typename c_type = typename data_type::c_type>
std::shared_ptr<typename TypeTraits<data_type>::ArrayType> ArrayOf(
    const std::vector<c_type>& values) {
  std::shared_ptr<Array> array;
  ArrayFromVector<data_type>(std::make_shared<data_type>(), values, &array);
  return std::static_pointer_cast<typename TypeTraits<data_type>::ArrayType>(array);
}

class TestColumnarFormat : public ::testing::Test {
 protected:
  // check that stitch(shred(array_in)) == array_in
  void Roundtrip(const std::shared_ptr<Field>& schema,
                 const std::shared_ptr<Array>& array_in) {
    auto pool = default_memory_pool();

    std::shared_ptr<Shredder> shredder = Shredder::Create(schema, pool).ValueOrDie();
    ASSERT_OK(shredder->Shred(*array_in));
    ColumnMap colmap = shredder->Finish().ValueOrDie();

    std::shared_ptr<Stitcher> stitcher = Stitcher::Create(schema, pool).ValueOrDie();
    ABORT_NOT_OK(stitcher->Stitch(colmap));
    std::shared_ptr<arrow::Array> array_out = stitcher->Finish().ValueOrDie();

    ASSERT_ARRAYS_EQUAL(*array_in, *array_out);
  }

  // check that:
  //  - shred(array_in) == colmap_in
  //  - stitch(shred(array_in)) == array_in
  void Roundtrip(const std::shared_ptr<Field>& schema,
                 const std::shared_ptr<Array>& array_in, const ColumnMap& colmap_in) {
    auto pool = default_memory_pool();

    ColumnMap colmap_out;
    {
      Result<std::shared_ptr<Shredder>> shredder = Shredder::Create(schema, pool);
      ASSERT_OK(shredder);
      ASSERT_OK(shredder.ValueOrDie()->Shred(*array_in));
      Result<ColumnMap> res = shredder.ValueOrDie()->Finish();
      ASSERT_OK(res);
      colmap_out = res.ValueOrDie();
    }

    // compare colmap_in with colmap_out
    ASSERT_EQ(colmap_in.size(), colmap_out.size());
    for (int i = 0; i < colmap_in.size(); i++) {
      ColumnMap::Column column_in = colmap_in.Get(i).ValueOrDie();

      Result<ColumnMap::Column> res = colmap_out.Find(column_in.field);
      ASSERT_OK(res);
      ColumnMap::Column column_out = res.ValueOrDie();

      ASSERT_ARRAYS_EQUAL(*column_in.rep_levels, *column_out.rep_levels);
      ASSERT_ARRAYS_EQUAL(*column_in.def_levels, *column_out.def_levels);
      if (column_in.values) {
        ASSERT_ARRAYS_EQUAL(*column_in.values, *column_out.values);
      } else {
        ASSERT_EQ(nullptr, column_out.values);
      }
    }

    std::shared_ptr<Array> array_out;
    {
      Result<std::shared_ptr<Stitcher>> stitcher = Stitcher::Create(schema, pool);
      ASSERT_OK(stitcher);
      ASSERT_OK(stitcher.ValueOrDie()->Stitch(colmap_out));
      Result<std::shared_ptr<Array>> array = stitcher.ValueOrDie()->Finish();
      ASSERT_OK(array);
      array_out = array.ValueOrDie();
    }

    // compare stitched array
    ASSERT_ARRAYS_EQUAL(*array_in, *array_out);
  }

  void AssertStitchError(const std::string& error, std::shared_ptr<Int16Array> rep_levels,
                         std::shared_ptr<Int16Array> def_levels,
                         std::shared_ptr<Array> values) {
    auto f0 = field("f0", uint32(), true);

    Result<std::shared_ptr<Stitcher>> stitcher =
        Stitcher::Create(f0, default_memory_pool());
    ASSERT_OK(stitcher);

    ColumnMap colmap;
    colmap.Put(f0, rep_levels, def_levels, values);
    ASSERT_RAISES_SUBSTR(Invalid, error, stitcher.ValueOrDie()->Stitch(colmap));
  }
};

TEST_F(TestColumnarFormat, ColumnMap) {
  auto f1 = field("a", int64());
  auto f2 = field("a", int64());
  auto f3 = field("b", int64());
  auto f4 = field("a", list(f1));

  ColumnMap map;
  for (int i = 0; i < 100; i++) {
    map.Put(field(std::to_string(i), int64()), nullptr, nullptr, nullptr);
  }
  map.Put(f1, nullptr, nullptr, nullptr);

  EXPECT_EQ(101, map.size());
  EXPECT_TRUE(map.Find(f1).ok());
  EXPECT_TRUE(map.Find(f2).ok());
  EXPECT_FALSE(map.Find(f3).ok());
  EXPECT_FALSE(map.Find(f4).ok());
}

TEST_F(TestColumnarFormat, OptionalFields) {
  /*
   * f0: optional struct {
   *   f1: optional list [
   *     f2: optional int64
   *   ]
   * }
   */
  auto f2 = field("f2", int64(), true);
  auto f1 = field("f1", list(f2), true);
  auto f0 = field("f0", struct_({f1}), true);

  std::string json = R"([null, {"f1":null}, {"f1":[]}, {"f1":[5,null,10]}])";
  std::shared_ptr<Array> array = ArrayFromJSON(f0->type(), json);

  ColumnMap colmap;
  colmap.Put(f0,                                      // NOLINT
             ArrayOf<Int16Type>({0, 0, 0, 0}),        // NOLINT
             ArrayOf<Int16Type>({0, 1, 1, 1}));       // NOLINT
  colmap.Put(f1,                                      // NOLINT
             ArrayOf<Int16Type>({0, 0, 0, 0}),        // NOLINT
             ArrayOf<Int16Type>({0, 1, 2, 3}));       // NOLINT
  colmap.Put(f2,                                      // NOLINT
             ArrayOf<Int16Type>({0, 0, 0, 0, 1, 1}),  // NOLINT
             ArrayOf<Int16Type>({0, 1, 2, 4, 3, 4}),  // NOLINT
             ArrayOf<Int64Type>({5, 10}));            // NOLINT

  Roundtrip(f0, array, colmap);
}

TEST_F(TestColumnarFormat, RequiredFields) {
  /*
   * f0: required struct {
   *   f1: required list [
   *     f2: required int64
   *   ]
   * }
   */
  auto f2 = field("f2", int64(), false);
  auto f1 = field("f1", list(f2), false);
  auto f0 = field("f0", struct_({f1}), false);

  std::string json = R"([{"f1":[]}, {"f1":[5,10,15]}])";
  std::shared_ptr<Array> array = ArrayFromJSON(f0->type(), json);

  ColumnMap colmap;
  colmap.Put(f0,                                // NOLINT
             ArrayOf<Int16Type>({0, 0}),        // NOLINT
             ArrayOf<Int16Type>({0, 0}));       // NOLINT
  colmap.Put(f1,                                // NOLINT
             ArrayOf<Int16Type>({0, 0}),        // NOLINT
             ArrayOf<Int16Type>({0, 1}));       // NOLINT
  colmap.Put(f2,                                // NOLINT
             ArrayOf<Int16Type>({0, 0, 1, 1}),  // NOLINT
             ArrayOf<Int16Type>({0, 1, 1, 1}),  // NOLINT
             ArrayOf<Int64Type>({5, 10, 15}));  // NOLINT

  Roundtrip(f0, array, colmap);
}

TEST_F(TestColumnarFormat, Siblings) {
  /*
   * f0: required struct {
   *   f1: required struct {
   *     f2: required int64,
   *     f3: required int64
   *   },
   *   f4: required struct {
   *     f5: required int64,
   *     f6: required int64
   *   }
   * }
   */
  auto f3 = field("f3", int64(), false);
  auto f2 = field("f2", int64(), false);
  auto f1 = field("f1", struct_({f2, f3}), false);
  auto f5 = field("f5", int64(), false);
  auto f6 = field("f6", int64(), false);
  auto f4 = field("f4", struct_({f5, f6}), false);
  auto f0 = field("f0", struct_({f1, f4}), false);

  std::string json = R"([{"f1":{"f2":5,"f3":10},"f4":{"f5":15,"f6":20}}])";
  std::shared_ptr<Array> array = ArrayFromJSON(f0->type(), json);

  ColumnMap colmap;
  colmap.Put(f0,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}));   // NOLINT
  colmap.Put(f1,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}));   // NOLINT
  colmap.Put(f2,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int64Type>({5}));   // NOLINT
  colmap.Put(f3,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int64Type>({10}));  // NOLINT
  colmap.Put(f4,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}));   // NOLINT
  colmap.Put(f5,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int64Type>({15}));  // NOLINT
  colmap.Put(f6,                         // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int16Type>({0}),    // NOLINT
             ArrayOf<Int64Type>({20}));  // NOLINT

  Roundtrip(f0, array, colmap);
}

TEST_F(TestColumnarFormat, PrimitiveTypes) {
  auto f1 = field("f1", uint32(), true);
  auto f2 = field("f2", utf8(), true);
  auto f3 = field("f3", boolean(), true);
  auto f0 = field("f0", struct_({f1, f2, f3}), false);

  std::string json = R"([{"f1":4294967295},{"f2":"abcde"},{"f3":true}])";
  std::shared_ptr<Array> array = ArrayFromJSON(f0->type(), json);

  ColumnMap colmap;
  colmap.Put(f0,                                            // NOLINT
             ArrayOf<Int16Type>({0, 0, 0}),                 // NOLINT
             ArrayOf<Int16Type>({0, 0, 0}));                // NOLINT
  colmap.Put(f1,                                            // NOLINT
             ArrayOf<Int16Type>({0, 0, 0}),                 // NOLINT
             ArrayOf<Int16Type>({1, 0, 0}),                 // NOLINT
             ArrayOf<UInt32Type>({4294967295}));            // NOLINT
  colmap.Put(f2,                                            // NOLINT
             ArrayOf<Int16Type>({0, 0, 0}),                 // NOLINT
             ArrayOf<Int16Type>({0, 1, 0}),                 // NOLINT
             ArrayOf<StringType, std::string>({"abcde"}));  // NOLINT
  colmap.Put(f3,                                            // NOLINT
             ArrayOf<Int16Type>({0, 0, 0}),                 // NOLINT
             ArrayOf<Int16Type>({0, 0, 1}),                 // NOLINT
             ArrayOf<BooleanType, bool>({true}));           // NOLINT
  Roundtrip(f0, array, colmap);
}

// Childless struct is a strange beast: it shares properties of both struct and primitive
TEST_F(TestColumnarFormat, ChildlessStruct) {
  auto f0 = field("f0", struct_({}), true);

  std::string json = R"([null, {}])";
  std::shared_ptr<Array> array = ArrayFromJSON(f0->type(), json);

  ColumnMap colmap;
  colmap.Put(f0,                           // NOLINT
             ArrayOf<Int16Type>({0, 0}),   // NOLINT
             ArrayOf<Int16Type>({0, 1}));  // NOLINT

  Roundtrip(f0, array, colmap);
}

TEST_F(TestColumnarFormat, ShredderSchemaMismatch) {
  auto schema = field("f0", uint32(), true);
  auto array1 = ArrayOf<UInt32Type>({1, 2, 3});
  auto array2 = ArrayOf<Int32Type>({1, 2, 3});

  Result<std::shared_ptr<Shredder>> shredder =
      Shredder::Create(schema, default_memory_pool());
  ASSERT_OK(shredder);

  ASSERT_OK(shredder.ValueOrDie()->Shred(*array1));
  ASSERT_OK(shredder.ValueOrDie()->Shred(*array1));
  ASSERT_RAISES_SUBSTR(Invalid, "Array schema doesn't match",
                       shredder.ValueOrDie()->Shred(*array2));
  ASSERT_OK(shredder.ValueOrDie()->Shred(*array1));
}

TEST_F(TestColumnarFormat, StitcherNoData) {
  auto f0 = field("f0", uint32(), true);

  Result<std::shared_ptr<Stitcher>> stitcher =
      Stitcher::Create(f0, default_memory_pool());
  ASSERT_OK(stitcher);
  ASSERT_RAISES_SUBSTR(Invalid, "No data for field",
                       stitcher.ValueOrDie()->Stitch(ColumnMap()));
}

TEST_F(TestColumnarFormat, StitcherDifferentNumberOfLevels) {
  AssertStitchError("Different number of",             // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0, 0}),  // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),     // NOLINT
                    ArrayOf<UInt32Type>({}));          // NOLINT
}

TEST_F(TestColumnarFormat, StitcherIncorrectValueType) {
  AssertStitchError("Incorrect value type",         // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),  // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),  // NOLINT
                    ArrayOf<UInt64Type>({}));       // NOLINT
}

TEST_F(TestColumnarFormat, StitcherNotEnoughValues) {
  AssertStitchError("Not enough values",            // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),  // NOLINT
                    ArrayOf<Int16Type>({1, 1, 1}),  // NOLINT
                    ArrayOf<UInt32Type>({5, 13}));  // NOLINT
}

TEST_F(TestColumnarFormat, StitcherRepetitionLevelOutOfRange) {
  AssertStitchError("Invalid repetition level",         // NOLINT
                    ArrayOf<Int16Type>({0, 0, 1}),      // NOLINT
                    ArrayOf<Int16Type>({1, 1, 1}),      // NOLINT
                    ArrayOf<UInt32Type>({5, 13, 25}));  // NOLINT
  AssertStitchError("Invalid repetition level",         // NOLINT
                    ArrayOf<Int16Type>({0, 0, -1}),     // NOLINT
                    ArrayOf<Int16Type>({1, 1, 1}),      // NOLINT
                    ArrayOf<UInt32Type>({5, 13, 25}));  // NOLINT
}

TEST_F(TestColumnarFormat, StitcherDefinitionLevelOutOfRange) {
  AssertStitchError("Invalid definition level",         // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),      // NOLINT
                    ArrayOf<Int16Type>({1, 1, 2}),      // NOLINT
                    ArrayOf<UInt32Type>({5, 13, 25}));  // NOLINT
  AssertStitchError("Invalid definition level",         // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),      // NOLINT
                    ArrayOf<Int16Type>({1, 1, -1}),     // NOLINT
                    ArrayOf<UInt32Type>({5, 13, 25}));  // NOLINT
}

TEST_F(TestColumnarFormat, StitcherNotAllValuesConsumed) {
  AssertStitchError("Not all values were consumed",         // NOLINT
                    ArrayOf<Int16Type>({0, 0, 0}),          // NOLINT
                    ArrayOf<Int16Type>({1, 1, 1}),          // NOLINT
                    ArrayOf<UInt32Type>({5, 13, 25, 40}));  // NOLINT
}

TEST_F(TestColumnarFormat, StitcherNotAllLevelsConsumed) {
  auto f1 = field("f1", uint32(), true);
  auto f2 = field("f2", uint32(), true);
  auto f3 = field("f3", uint32(), true);
  auto f0 = field("f0", struct_({f1, f2, f3}), true);

  Result<std::shared_ptr<Stitcher>> stitcher =
      Stitcher::Create(f0, default_memory_pool());
  ASSERT_OK(stitcher);

  {
    ColumnMap colmap;
    colmap.Put(f1,                                // NOLINT
               ArrayOf<Int16Type>({0, 0, 0}),     // NOLINT
               ArrayOf<Int16Type>({0, 0, 0}),     // NOLINT
               ArrayOf<UInt32Type>({}));          // NOLINT
    colmap.Put(f2,                                // NOLINT
               ArrayOf<Int16Type>({0, 0, 0, 0}),  // NOLINT
               ArrayOf<Int16Type>({0, 0, 0, 0}),  // NOLINT
               ArrayOf<UInt32Type>({}));          // NOLINT
    colmap.Put(f3,                                // NOLINT
               ArrayOf<Int16Type>({0, 0, 0}),     // NOLINT
               ArrayOf<Int16Type>({0, 0, 0}),     // NOLINT
               ArrayOf<UInt32Type>({}));          // NOLINT
    ASSERT_RAISES_SUBSTR(Invalid, "Not all levels were consumed",
                         stitcher.ValueOrDie()->Stitch(colmap));
  }
}

TEST_F(TestColumnarFormat, RandomDeeplyNestedList) {
  DatasetGenerator gen(default_memory_pool(), 0);
  Dataset ds = gen.GenerateNestedList(5, 10, 1000, 0.2);
  Roundtrip(ds.schema(), ds.array());
}

}  // namespace colfmt
}  // namespace arrow
