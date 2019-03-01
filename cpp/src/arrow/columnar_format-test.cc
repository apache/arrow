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

#ifdef _MSC_VER
#pragma warning(push)
// Disable forcing value to bool warnings
#pragma warning(disable : 4800)
#endif

#include "gtest/gtest.h"

#include <cstdint>
#include <functional>
#include <sstream>
#include <vector>

#include "arrow/api.h"
#include "arrow/testing/util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/pretty_print.h"
#include "arrow/columnar_format.h"


using arrow::Array;
using arrow::ListArray;
using arrow::PrimitiveArray;
using arrow::Status;


namespace arrow {


TEST(TestColumnarFormat, ColumnMap) {
  auto f1 = field("a", ::arrow::int64());
  auto f2 = field("a", ::arrow::int64());
  auto f3 = field("b", ::arrow::int64());
  auto f4 = field("a", ::arrow::list(f1));

  ColumnMap map;
  for (int i = 0; i < 100; i++) {
    map.Put(field(std::to_string(i), ::arrow::int64()), nullptr, nullptr, nullptr);
  }
  map.Put(f1, nullptr, nullptr, nullptr);

  EXPECT_TRUE(map.size() == 101);
  EXPECT_TRUE(map.find(f1) >= 0);
  EXPECT_TRUE(map.find(f1) == map.find(f2));
  EXPECT_TRUE(map.find(f3) == -1);
  EXPECT_TRUE(map.find(f4) == -1);
}

void EncodeAndCheckLevels(bool nullable) {
  /*
   * f0: optional_or_required {
   *   f1: optional_or_required list [
   *     f2: optional_or_required int64
   *   ]
   * }
   */
  auto f2 = field("f2", ::arrow::int64(), nullable);
  auto f1 = field("f1", ::arrow::list(f2), nullable);
  auto f0 = field("f0", ::arrow::struct_({f1}), nullable);

  auto pool = ::arrow::default_memory_pool();
  auto f2b = std::make_shared<::arrow::Int64Builder>(f2->type(), pool);
  auto f1b = std::make_shared<::arrow::ListBuilder>(pool, f2b, f1->type());
  auto f0b = std::make_shared<::arrow::StructBuilder>(
    f0->type(), pool, std::vector<std::shared_ptr<::arrow::ArrayBuilder>>{{f1b}});

  if (nullable) {
    // f0: null
    ASSERT_OK(f0b->Append(false));
    ASSERT_OK(f1b->Append(false));

    // f0: { f1: null}
    ASSERT_OK(f0b->Append(true));
    ASSERT_OK(f1b->Append(false));

    // f0: { f1: []}
    ASSERT_OK(f0b->Append(true));
    ASSERT_OK(f1b->Append(true));

    // f0: { f1: [f2:5, f2:null, f2:10]}
    ASSERT_OK(f0b->Append(true));
    ASSERT_OK(f1b->Append(true));
    ASSERT_OK(f2b->Append(5));
    ASSERT_OK(f2b->AppendNull());
    ASSERT_OK(f2b->Append(10));

  } else {
    // f0: { f1: []}
    ASSERT_OK(f0b->Append(true));
    ASSERT_OK(f1b->Append(true));

    // f0: { f1: [f2:5, f2:10, f2:15]}
    ASSERT_OK(f0b->Append(true));
    ASSERT_OK(f1b->Append(true));
    ASSERT_OK(f2b->Append(5));
    ASSERT_OK(f2b->Append(10));
    ASSERT_OK(f2b->Append(15));
  }

  std::shared_ptr<::arrow::Array> array;
  ASSERT_OK(f0b->Finish(&array));

  {
    std::shared_ptr<arrow::Shredder> shredder;
    ASSERT_OK(arrow::Shredder::Create(f0, pool, &shredder));
    ASSERT_OK(shredder->Shred(*array));
    ColumnMap colmap;
    ASSERT_OK(shredder->Finish(&colmap));

    auto CheckLevels = [&colmap](std::shared_ptr<Field> field,
                                 std::initializer_list<int64_t> values,
                                 std::initializer_list<int16_t> def_levels,
                                 std::initializer_list<int16_t> rep_levels) {
      std::shared_ptr<Int16Array> rep_levels_out;
      std::shared_ptr<Int16Array> def_levels_out;
      std::shared_ptr<Array> values_out;
      colmap.get(colmap.find(field), &rep_levels_out, &def_levels_out, &values_out);

      std::shared_ptr<Array> rep_levels_in;
      std::shared_ptr<Array> def_levels_in;
      std::shared_ptr<Array> values_in;
      ArrayFromVector<Int16Type>(int16(), std::vector<int16_t>(def_levels), &rep_levels_in);
      ArrayFromVector<Int16Type>(int16(), std::vector<int16_t>(rep_levels), &def_levels_in);
      ArrayFromVector<Int64Type>(int64(), std::vector<int64_t>(values),     &values_in);

      ASSERT_ARRAYS_EQUAL(*rep_levels_in, *rep_levels_out);
      ASSERT_ARRAYS_EQUAL(*def_levels_in, *def_levels_out);
      if (field->type()->id() == Type::INT64) {
        ASSERT_ARRAYS_EQUAL(*values_in, *values_out);
      } else {
        ASSERT_TRUE(values_out.get() == nullptr);
      }
    };

    if (nullable) {
      CheckLevels(f0, {}, {0,0,0,0},
                          {0,1,1,1});
      CheckLevels(f1, {5,10}, {0,0,0,0},
                              {0,1,2,3});
      CheckLevels(f2, {5,10}, {0,0,0,0,1,1},
                              {0,1,2,4,3,4});
    } else {
      CheckLevels(f0, {}, {0,0},
                          {0,0});
      CheckLevels(f1, {}, {0,0},
                          {0,1});
      CheckLevels(f2, {5,10,15}, {0,0,1,1},
                                 {0,1,1,1});
    }
  }

  {
    std::shared_ptr<arrow::Shredder> shredder;
    ASSERT_OK(arrow::Shredder::Create(f0, pool, &shredder));
    ASSERT_OK(shredder->Shred(*array));

    ColumnMap colmap;
    ASSERT_OK(shredder->Finish(&colmap));

    std::shared_ptr<Stitcher> stitcher;
    ASSERT_OK(Stitcher::Create(f0, pool, &stitcher));
    ASSERT_OK(stitcher->Stitch(colmap));
    stitcher->DebugPrint(std::cout);
    std::shared_ptr<Array> projection;
    ASSERT_OK(stitcher->Finish(&projection));
    ASSERT_OK(PrettyPrint(*projection, 2, &std::cout));
    std::cout << std::endl;
    ASSERT_ARRAYS_EQUAL(*array, *projection);
  }
}

TEST(TestColumnarFormat, RequiredLevels) {
  EncodeAndCheckLevels(false);
}

TEST(TestColumnarFormat, OptionalLevels) {
  EncodeAndCheckLevels(true);
}


TEST(TestColumnarFormat, Demo) {
  /*
   * f0 = required {
   *   f1: optional int64,
   *   f2: optional utf8,
   *   f3: optional list {
   *     f4: optional int64
   *   }
   * }
   */
  /*
  auto f4 = field("f4", ::arrow::uint64(), true);
  auto f3 = field("f3", ::arrow::list(f4), true);
  auto f2 = field("f2", ::arrow::utf8(), true);
  auto f1 = field("f1", ::arrow::uint64(), true);
  auto f0 = field("f0", ::arrow::struct_({f1, f2, f3}), true);

  auto pool = ::arrow::default_memory_pool();
  std::shared_ptr<::arrow::UInt64Builder> f4b = std::make_shared<::arrow::UInt64Builder>(f4->type(), pool);
  std::shared_ptr<::arrow::StringBuilder> f2b = std::make_shared<::arrow::StringBuilder>(f2->type(), pool);
  std::shared_ptr<::arrow::UInt64Builder> f1b = std::make_shared<::arrow::UInt64Builder>(f1->type(), pool);
  std::shared_ptr<::arrow::ListBuilder> f3b = std::make_shared<::arrow::ListBuilder>(pool, f4b, f3->type());
  std::shared_ptr<::arrow::StructBuilder> f0b = std::make_shared<::arrow::StructBuilder>(
    f0->type(), pool, std::vector<std::shared_ptr<::arrow::ArrayBuilder>>{{f1b, f2b, f3b}});
  
  */
  
  /*
   * f0: {
   *   f1: 100500
   *   f2: "abc"
   *   f3: [15, 20, null, 25]
   * }
   */
  /*
  ASSERT_OK(f0b->Append(true));
  ASSERT_OK(f1b->Append(100500));
  ASSERT_OK(f2b->Append("abc", 3));
  ASSERT_OK(f3b->Append());
  ASSERT_OK(f4b->Append(15));
  ASSERT_OK(f4b->Append(20));
  ASSERT_OK(f4b->AppendNull());
  ASSERT_OK(f4b->Append(25));
  */

  /*
   * f0: {
   *   f1: 999
   *   f2: null
   *   f3: []
   * }
   */
  /*
  ASSERT_OK(f0b->Append(true));
  ASSERT_OK(f1b->Append(999));
  ASSERT_OK(f2b->AppendNull());
  ASSERT_OK(f3b->Append());

  std::shared_ptr<::arrow::Array> array;
  ASSERT_OK(f0b->Finish(&array));

  parquet::arrow::LevelEncoder encoder;
  std::vector<parquet::arrow::ColumnData> columns;
  ASSERT_OK(encoder.Encode(*f0, *array, &columns));

  auto ArrayToString = [](const ::arrow::Array& array) -> std::string {
    std::string str;
    ::arrow::PrettyPrintOptions opts(0, std::numeric_limits<int>::max(), 0, "null", true);
    ::arrow::PrettyPrint(array, opts, &str);
    str.erase(std::remove_if(str.begin(), str.end(), [](char c){return c == '\n';}), str.end());
    return str;
  };

  for (size_t i = 0; i < columns.size(); i++) {
    std::cout << '[' << i << "].rep_levels = " << ArrayToString(*(columns[i].rep_levels)) << std::endl;
    std::cout << '[' << i << "].def_levels = " << ArrayToString(*(columns[i].def_levels)) << std::endl;
    std::cout << '[' << i << "].values = "     << ArrayToString(*(columns[i].values))     << std::endl;
  }

  // decoder
  std::shared_ptr<LevelDecoder> decoder;
  ASSERT_OK(LevelDecoder::Create(f0, &decoder));
  for (size_t i = 0; i < columns.size(); i++) {
    ASSERT_OK(decoder->SetColumnData(
      columns[i].path,
      columns[i].rep_levels,
      columns[i].def_levels,
      columns[i].values
    ));
  }
  decoder->DebugInfo(std::cout);
  std::shared_ptr<::arrow::Array> projection;
  ASSERT_OK(decoder->Decode(&projection));
  ASSERT_OK(::arrow::PrettyPrint(*array, 2, &std::cout));
  std::cout << std::endl;
  */
}

}  // namespace arrow

