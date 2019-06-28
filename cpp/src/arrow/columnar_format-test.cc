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

#include <vector>

#include "arrow/api.h"
#include "arrow/testing/util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"
#include "arrow/columnar_format.h"


namespace arrow {


template<typename data_type>
std::shared_ptr<typename TypeTraits<data_type>::ArrayType> ArrayOf(
  const std::vector<typename data_type::c_type>& values) {

  std::shared_ptr<Array> array;
  ArrayFromVector<data_type>(std::make_shared<data_type>(), values, &array);

  return std::static_pointer_cast<typename TypeTraits<data_type>::ArrayType>(array);
}

void Roundtrip(const std::shared_ptr<Field>& schema,
               const std::shared_ptr<Array>& array_in,
               const ColumnMap& colmap_in) {

  auto pool = default_memory_pool();

  ColumnMap colmap_out;
  {
    std::shared_ptr<Shredder> shredder;
    ASSERT_OK(Shredder::Create(schema, pool, &shredder));
    ASSERT_OK(shredder->Shred(*array_in));
    ASSERT_OK(shredder->Finish(&colmap_out));
  }

  // compare colmap_in with colmap_out
  ASSERT_EQ(colmap_in.size(), colmap_out.size());
  for (int i = 0; i < colmap_in.size(); i++) {
    std::shared_ptr<Int16Array> rep_levels_in;
    std::shared_ptr<Int16Array> def_levels_in;
    std::shared_ptr<Array> values_in;
    colmap_in.get(i, &rep_levels_in, &def_levels_in, &values_in);

    int j = colmap_out.find(colmap_in.field(i));
    ASSERT_TRUE(j >= 0);
    std::shared_ptr<Int16Array> rep_levels_out;
    std::shared_ptr<Int16Array> def_levels_out;
    std::shared_ptr<Array> values_out;
    colmap_out.get(j, &rep_levels_out, &def_levels_out, &values_out);

    ASSERT_ARRAYS_EQUAL(*rep_levels_in, *rep_levels_out);
    ASSERT_ARRAYS_EQUAL(*def_levels_in, *def_levels_out);
    if (values_in) {
      ASSERT_ARRAYS_EQUAL(*values_in, *values_out);
    } else {
      ASSERT_TRUE(values_out == nullptr);
    }
  }

  std::shared_ptr<Array> array_out;
  {
    std::shared_ptr<Stitcher> stitcher;
    ASSERT_OK(Stitcher::Create(schema, pool, &stitcher));
    ASSERT_OK(stitcher->Stitch(colmap_out));
    ASSERT_OK(stitcher->Finish(&array_out));
  }

  // compare stitched array
  ASSERT_ARRAYS_EQUAL(*array_in, *array_out);
}

TEST(TestColumnarFormat, ColumnMap) {
  auto f1 = field("a", int64());
  auto f2 = field("a", int64());
  auto f3 = field("b", int64());
  auto f4 = field("a", list(f1));

  ColumnMap map;
  for (int i = 0; i < 100; i++) {
    map.Put(field(std::to_string(i), int64()), nullptr, nullptr, nullptr);
  }
  map.Put(f1, nullptr, nullptr, nullptr);

  EXPECT_TRUE(map.size() == 101);
  EXPECT_TRUE(map.find(f1) >= 0);
  EXPECT_TRUE(map.find(f1) == map.find(f2));
  EXPECT_TRUE(map.find(f3) == -1);
  EXPECT_TRUE(map.find(f4) == -1);
}


TEST(TestColumnarFormat, OptionalFields) {
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

  auto pool = default_memory_pool();

  auto f2b = std::make_shared<Int64Builder>(f2->type(), pool);
  auto f1b = std::make_shared<ListBuilder>(pool, f2b, f1->type());
  auto f0b = std::make_shared<StructBuilder>(
    f0->type(), pool, std::vector<std::shared_ptr<ArrayBuilder>>{{f1b}});

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

  std::shared_ptr<Array> array;
  ASSERT_OK(f0b->Finish(&array));


  ColumnMap colmap;
  colmap.Put(f0, ArrayOf<Int16Type>({0,0,0,0}),
                 ArrayOf<Int16Type>({0,1,1,1}));
  colmap.Put(f1, ArrayOf<Int16Type>({0,0,0,0}),
                 ArrayOf<Int16Type>({0,1,2,3}));
  colmap.Put(f2, ArrayOf<Int16Type>({0,0,0,0,1,1}),
                 ArrayOf<Int16Type>({0,1,2,4,3,4}),
                 ArrayOf<Int64Type>({5,10}));

  Roundtrip(f0, array, colmap);
}

TEST(TestColumnarFormat, RequiredFields) {
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

  auto pool = default_memory_pool();

  auto f2b = std::make_shared<Int64Builder>(f2->type(), pool);
  auto f1b = std::make_shared<ListBuilder>(pool, f2b, f1->type());
  auto f0b = std::make_shared<StructBuilder>(
    f0->type(), pool, std::vector<std::shared_ptr<ArrayBuilder>>{{f1b}});

  // f0: { f1: []}
  ASSERT_OK(f0b->Append(true));
  ASSERT_OK(f1b->Append(true));

  // f0: { f1: [f2:5, f2:10, f2:15]}
  ASSERT_OK(f0b->Append(true));
  ASSERT_OK(f1b->Append(true));
  ASSERT_OK(f2b->Append(5));
  ASSERT_OK(f2b->Append(10));
  ASSERT_OK(f2b->Append(15));

  std::shared_ptr<Array> array;
  ASSERT_OK(f0b->Finish(&array));


  ColumnMap colmap;
  colmap.Put(f0, ArrayOf<Int16Type>({0,0}),
                 ArrayOf<Int16Type>({0,0}));
  colmap.Put(f1, ArrayOf<Int16Type>({0,0}),
                 ArrayOf<Int16Type>({0,1}));
  colmap.Put(f2, ArrayOf<Int16Type>({0,0,1,1}),
                 ArrayOf<Int16Type>({0,1,1,1}),
                 ArrayOf<Int64Type>({5,10,15}));

  Roundtrip(f0, array, colmap);
}

}  // namespace arrow

