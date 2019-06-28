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

#include <string>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {

void CheckView(const std::shared_ptr<Array>& input,
               const std::shared_ptr<DataType>& view_type,
               const std::shared_ptr<Array>& expected) {
  std::shared_ptr<Array> result;
  ASSERT_OK(input->View(view_type, &result));
  ASSERT_OK(ValidateArray(*result));
  AssertArraysEqual(*expected, *result);
}

void CheckView(const std::shared_ptr<Array>& input,
               const std::shared_ptr<Array>& expected_view) {
  CheckView(input, expected_view->type(), expected_view);
}

void CheckViewFails(const std::shared_ptr<Array>& input,
                    const std::shared_ptr<DataType>& view_type) {
  std::shared_ptr<Array> result;
  ASSERT_RAISES(Invalid, input->View(view_type, &result));
}

class IPv4Type : public ExtensionType {
 public:
  IPv4Type() : ExtensionType(fixed_size_binary(4)) {}

  std::string extension_name() const override { return "ipv4"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    return other.extension_name() == this->extension_name();
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    DCHECK_EQ(data->type->id(), Type::EXTENSION);
    DCHECK_EQ("ipv4", static_cast<const ExtensionType&>(*data->type).extension_name());
    return std::make_shared<ExtensionArray>(data);
  }

  Status Deserialize(std::shared_ptr<DataType> storage_type,
                     const std::string& serialized,
                     std::shared_ptr<DataType>* out) const override {
    return Status::NotImplemented("IPv4Type::Deserialize");
  }

  std::string Serialize() const override { return ""; }
};

TEST(TestArrayView, IdentityPrimitive) {
  auto arr = ArrayFromJSON(int16(), "[0, -1, 42]");
  CheckView(arr, arr->type(), arr);
  arr = ArrayFromJSON(int16(), "[0, -1, 42, null]");
  CheckView(arr, arr->type(), arr);
  arr = ArrayFromJSON(boolean(), "[true, false, null]");
  CheckView(arr, arr->type(), arr);
}

TEST(TestArrayView, IdentityNullType) {
  auto arr = ArrayFromJSON(null(), "[null, null, null]");
  CheckView(arr, arr->type(), arr);
}

TEST(TestArrayView, PrimitiveAsPrimitive) {
  auto arr = ArrayFromJSON(int16(), "[0, -1, 42]");
  auto expected = ArrayFromJSON(uint16(), "[0, 65535, 42]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  arr = ArrayFromJSON(int32(), "[0, 1069547520, -1071644672, null]");
  expected = ArrayFromJSON(float32(), "[0.0, 1.5, -2.5, null]");
  CheckView(arr, expected);

  arr = ArrayFromJSON(timestamp(TimeUnit::SECOND),
                      R"(["1970-01-01","2000-02-29","3989-07-14","1900-02-28"])");
  expected = ArrayFromJSON(int64(), "[0, 951782400, 63730281600, -2203977600]");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, PrimitiveAsFixedSizeBinary) {
  auto arr = ArrayFromJSON(int32(), "[2020568934, 2054316386, null]");
  auto expected = ArrayFromJSON(fixed_size_binary(4), R"(["foox", "barz", null])");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, PrimitiveWrongSize) {
  auto arr = ArrayFromJSON(int16(), "[0, -1, 42]");
  CheckViewFails(arr, int8());
  CheckViewFails(arr, fixed_size_binary(3));
  CheckViewFails(arr, null());
}

TEST(TestArrayView, StructAsStructSimple) {
  auto ty1 = struct_({field("a", int8()), field("b", int32())});
  auto ty2 = struct_({field("c", uint8()), field("d", float32())});

  auto arr = ArrayFromJSON(ty1, "[[0, 0], [1, 1069547520], [-1, -1071644672]]");
  auto expected = ArrayFromJSON(ty2, "[[0, 0], [1, 1.5], [255, -2.5]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nulls
  arr = ArrayFromJSON(ty1, "[[0, 0], null, [-1, -1071644672]]");
  expected = ArrayFromJSON(ty2, "[[0, 0], null, [255, -2.5]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nested nulls
  arr = ArrayFromJSON(ty1, "[[0, null], null, [-1, -1071644672]]");
  expected = ArrayFromJSON(ty2, "[[0, null], null, [255, -2.5]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  ty2 = struct_({field("c", uint8()), field("d", fixed_size_binary(4))});
  arr = ArrayFromJSON(ty1, "[[0, null], null, [-1, 2020568934]]");
  expected = ArrayFromJSON(ty2, R"([[0, null], null, [255, "foox"]])");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, StructAsStructNonNullable) {
  auto ty1 = struct_({field("a", int8()), field("b", int32())});
  auto ty2 = struct_({field("c", uint8(), /*nullable=*/false), field("d", float32())});

  auto arr = ArrayFromJSON(ty1, "[[0, 0], [1, 1069547520], [-1, -1071644672]]");
  auto expected = ArrayFromJSON(ty2, "[[0, 0], [1, 1.5], [255, -2.5]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nested nulls
  arr = ArrayFromJSON(ty1, "[[0, null], [-1, -1071644672]]");
  expected = ArrayFromJSON(ty2, "[[0, null], [255, -2.5]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // Nested null cannot be viewed as non-null field
  arr = ArrayFromJSON(ty1, "[[0, null], [null, -1071644672]]");
  CheckViewFails(arr, ty2);
}

TEST(TestArrayView, StructAsStructWrongLayout) {
  auto ty1 = struct_({field("a", int8()), field("b", int32())});
  auto arr = ArrayFromJSON(ty1, "[[0, 0], [1, 1069547520], [-1, -1071644672]]");

  auto ty2 = struct_({field("c", int16()), field("d", int32())});
  CheckViewFails(arr, ty2);
  ty2 = struct_({field("c", int32()), field("d", int8())});
  CheckViewFails(arr, ty2);
  ty2 = struct_({field("c", int8())});
  CheckViewFails(arr, ty2);
  ty2 = struct_({field("c", fixed_size_binary(5))});
  CheckViewFails(arr, ty2);
}

TEST(TestArrayView, StructAsStructWithNullType) {
  auto ty1 = struct_({field("a", int8()), field("b", null())});
  auto ty2 = struct_({field("c", uint8()), field("d", null())});

  auto arr = ArrayFromJSON(ty1, "[[0, null], [1, null], [-1, null]]");
  auto expected = ArrayFromJSON(ty2, "[[0, null], [1, null], [255, null]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nulls and nested nulls
  arr = ArrayFromJSON(ty1, "[null, [null, null], [-1, null]]");
  expected = ArrayFromJSON(ty2, "[null, [null, null], [255, null]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // Moving the null types around
  ty2 = struct_({field("c", null()), field("d", uint8())});
  expected = ArrayFromJSON(ty2, "[null, [null, null], [null, 255]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // Removing the null type
  ty2 = struct_({field("c", uint8())});
  expected = ArrayFromJSON(ty2, "[null, [null], [255]]");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, StructAsFlat) {
  auto ty1 = struct_({field("a", int16())});
  auto arr = ArrayFromJSON(ty1, "[[0], [1], [-1]]");
  auto expected = ArrayFromJSON(uint16(), "[0, 1, 65535]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nulls
  arr = ArrayFromJSON(ty1, "[[0], null, [-1]]");
  expected = ArrayFromJSON(uint16(), "[0, null, 65535]");
  //   CheckView(arr, expected);  // XXX currently fails
  CheckView(expected, arr);

  // With nested nulls => fails
  arr = ArrayFromJSON(ty1, "[[0], [null], [-1]]");
  CheckViewFails(arr, uint16());
}

TEST(TestArrayView, StructAsFlatWithNullType) {
  auto ty1 = struct_({field("a", null()), field("b", int16()), field("c", null())});
  auto arr = ArrayFromJSON(ty1, "[[null, 0, null], [null, -1, null]]");
  auto expected = ArrayFromJSON(uint16(), "[0, 65535]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nulls
  arr = ArrayFromJSON(ty1, "[[null, 0, null], null, [null, -1, null]]");
  expected = ArrayFromJSON(uint16(), "[0, null, 65535]");
  //   CheckView(arr, expected);  // XXX currently fails
  CheckView(expected, arr);

  // With nested nulls => fails
  arr = ArrayFromJSON(ty1, "[[null, null, null]]");
  CheckViewFails(arr, uint16());
}

TEST(TestArrayView, StructAsStructNested) {
  // Nesting tree shape need not be identical
  auto ty1 = struct_({field("a", struct_({field("b", int8())})), field("d", int32())});
  auto ty2 = struct_({field("a", uint8()), field("b", struct_({field("b", float32())}))});
  auto arr = ArrayFromJSON(ty1, "[[[0], 1069547520], [[-1], -1071644672]]");
  auto expected = ArrayFromJSON(ty2, "[[0, [1.5]], [255, [-2.5]]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With null types
  ty1 = struct_({field("a", struct_({field("xx", null()), field("b", int8())})),
                 field("d", int32())});
  ty2 = struct_({field("a", uint8()),
                 field("b", struct_({field("b", float32()), field("xx", null())}))});
  arr = ArrayFromJSON(ty1, "[[[null, 0], 1069547520], [[null, -1], -1071644672]]");
  expected = ArrayFromJSON(ty2, "[[0, [1.5, null]], [255, [-2.5, null]]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // XXX With nulls (currently fails)
}

TEST(TestArrayView, ListAsListSimple) {
  auto arr = ArrayFromJSON(list(int16()), "[[0, -1], [], [42]]");
  auto expected = ArrayFromJSON(list(uint16()), "[[0, 65535], [], [42]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nulls
  arr = ArrayFromJSON(list(int16()), "[[0, -1], null, [42]]");
  expected = ArrayFromJSON(list(uint16()), "[[0, 65535], null, [42]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nested nulls
  arr = ArrayFromJSON(list(int16()), "[[0, -1], null, [null, 42]]");
  expected = ArrayFromJSON(list(uint16()), "[[0, 65535], null, [null, 42]]");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, FixedSizeListAsFixedSizeList) {
  auto ty1 = fixed_size_list(int16(), 3);
  auto ty2 = fixed_size_list(uint16(), 3);
  auto arr = ArrayFromJSON(ty1, "[[0, -1, 42], [5, 6, -16384]]");
  auto expected = ArrayFromJSON(ty2, "[[0, 65535, 42], [5, 6, 49152]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nested nulls
  arr = ArrayFromJSON(ty1, "[[0, -1, null], null, [5, 6, -16384]]");
  expected = ArrayFromJSON(ty2, "[[0, 65535, null], null, [5, 6, 49152]]");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, FixedSizeListAsFlat) {
  auto ty1 = fixed_size_list(int16(), 3);
  auto arr = ArrayFromJSON(ty1, "[[0, -1, 42], [5, 6, -16384]]");
  auto expected = ArrayFromJSON(uint16(), "[0, 65535, 42, 5, 6, 49152]");
  CheckView(arr, expected);
  // CheckView(expected, arr);  // XXX currently fails

  // XXX With nulls (currently fails)
}

TEST(TestArrayView, SparseUnionAsStruct) {
  auto child1 = ArrayFromJSON(int16(), "[0, -1, 42]");
  auto child2 = ArrayFromJSON(int32(), "[0, 1069547520, -1071644672]");
  auto indices = ArrayFromJSON(int8(), "[0, 0, 1]");
  std::shared_ptr<Array> arr;
  ASSERT_OK(UnionArray::MakeSparse(*indices, {child1, child2}, &arr));
  ASSERT_OK(ValidateArray(*arr));

  auto ty1 = struct_({field("a", int8()), field("b", uint16()), field("c", float32())});
  auto expected = ArrayFromJSON(ty1, "[[0, 0, 0], [0, 65535, 1.5], [1, 42, -2.5]]");
  CheckView(arr, expected);
  CheckView(expected, arr);

  // With nulls
  indices = ArrayFromJSON(int8(), "[null, 0, 1]");
  ASSERT_OK(UnionArray::MakeSparse(*indices, {child1, child2}, &arr));
  ASSERT_OK(ValidateArray(*arr));
  expected = ArrayFromJSON(ty1, "[null, [0, 65535, 1.5], [1, 42, -2.5]]");
  CheckView(arr, expected);
  //   CheckView(expected, arr);  // XXX currently fails

  // With nested nulls
  child1 = ArrayFromJSON(int16(), "[0, -1, null]");
  child2 = ArrayFromJSON(int32(), "[0, null, -1071644672]");
  ASSERT_OK(UnionArray::MakeSparse(*indices, {child1, child2}, &arr));
  ASSERT_OK(ValidateArray(*arr));
  expected = ArrayFromJSON(ty1, "[null, [0, 65535, null], [1, null, -2.5]]");
  CheckView(arr, expected);
  //   CheckView(expected, arr);  // XXX currently fails
}

TEST(TestArrayView, DecimalRoundTrip) {
  auto ty1 = decimal(10, 4);
  auto arr = ArrayFromJSON(ty1, R"(["123.4567", "-78.9000", null])");

  auto ty2 = fixed_size_binary(16);
  std::shared_ptr<Array> v, w;
  ASSERT_OK(arr->View(ty2, &v));
  ASSERT_OK(ValidateArray(*v));
  ASSERT_OK(v->View(ty1, &w));
  ASSERT_OK(ValidateArray(*w));
  AssertArraysEqual(*arr, *w);
}

TEST(TestArrayView, Dictionaries) {
  // Can't view dictionaries
  auto ty1 = dictionary(int8(), float32());
  auto indices = ArrayFromJSON(int8(), "[0, 2, null, 1]");
  auto values = ArrayFromJSON(float32(), "[0.0, 1.5, -2.5]");
  std::shared_ptr<Array> arr;
  ASSERT_OK(DictionaryArray::FromArrays(ty1, indices, values, &arr));
  CheckViewFails(arr, int8());
  CheckViewFails(indices, ty1);
}

TEST(TestArrayView, ExtensionType) {
  auto ty1 = std::make_shared<IPv4Type>();
  auto data = ArrayFromJSON(ty1->storage_type(), R"(["ABCD", null])")->data();
  data->type = ty1;
  auto arr = ty1->MakeArray(data);
  auto expected = ArrayFromJSON(uint32(), "[1145258561, null]");
  CheckView(arr, expected);
  CheckView(expected, arr);
}

TEST(TestArrayView, NonZeroOffset) {
  auto arr = ArrayFromJSON(int16(), "[10, 11, 12, 13]")->Slice(1);
  CheckViewFails(arr, uint16());
}

TEST(TestArrayView, NonZeroNestedOffset) {
  auto list_values = ArrayFromJSON(int16(), "[10, 11, 12, 13]")->Slice(1);
  auto list_offsets = ArrayFromJSON(int32(), "[0, 2, 3]");
  std::shared_ptr<Array> list_array;
  ASSERT_OK(ListArray::FromArrays(*list_offsets, *list_values, default_memory_pool(),
                                  &list_array));
  ASSERT_OK(ValidateArray(*list_array));
  CheckViewFails(list_array, list(uint16()));
}

}  // namespace arrow
