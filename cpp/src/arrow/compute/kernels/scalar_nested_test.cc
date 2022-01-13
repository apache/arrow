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

#include <gtest/gtest.h>

#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

static std::shared_ptr<DataType> GetOffsetType(const DataType& type) {
  return type.id() == Type::LIST ? int32() : int64();
}

TEST(TestScalarNested, ListValueLength) {
  for (auto ty : {list(int32()), large_list(int32())}) {
    CheckScalarUnary("list_value_length", ty, "[[0, null, 1], null, [2, 3], []]",
                     GetOffsetType(*ty), "[3, null, 2, 0]");
  }

  CheckScalarUnary("list_value_length", fixed_size_list(int32(), 3),
                   "[[0, null, 1], null, [2, 3, 4], [1, 2, null]]", int32(),
                   "[3, null, 3, 3]");
}

TEST(TestScalarNested, ListElementNonFixedListWithNulls) {
  auto sample = "[[7, 5, 81], [6, null, 4, 7, 8], [3, 12, 2, 0], [1, 9], null]";
  for (auto ty : NumericTypes()) {
    for (auto list_type : {list(ty), large_list(ty)}) {
      auto input = ArrayFromJSON(list_type, sample);
      auto null_input = ArrayFromJSON(list_type, "[null]");
      for (auto index_type : IntTypes()) {
        auto index = ScalarFromJSON(index_type, "1");
        auto expected = ArrayFromJSON(ty, "[5, null, 12, 9, null]");
        auto expected_null = ArrayFromJSON(ty, "[null]");
        CheckScalar("list_element", {input, index}, expected);
        CheckScalar("list_element", {null_input, index}, expected_null);
      }
    }
  }
}

TEST(TestScalarNested, ListElementFixedList) {
  auto sample = "[[7, 5, 81], [6, 4, 8], [3, 12, 2], [1, 43, 87]]";
  for (auto ty : NumericTypes()) {
    auto input = ArrayFromJSON(fixed_size_list(ty, 3), sample);
    for (auto index_type : IntTypes()) {
      auto index = ScalarFromJSON(index_type, "0");
      auto expected = ArrayFromJSON(ty, "[7, 6, 3, 1]");
      CheckScalar("list_element", {input, index}, expected);
    }
  }
}

TEST(TestScalarNested, ListElementInvalid) {
  auto input_array = ArrayFromJSON(list(float32()), "[[0.1, 1.1], [0.2, 1.2]]");
  auto input_scalar = ScalarFromJSON(list(float32()), "[0.1, 0.2]");

  // invalid index: null
  auto index = ScalarFromJSON(int32(), "null");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));

  // invalid index: < 0
  index = ScalarFromJSON(int32(), "-1");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));

  // invalid index: >= list.length
  index = ScalarFromJSON(int32(), "2");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));

  // invalid input
  input_array = ArrayFromJSON(list(float32()), "[[41, 6, 93], [], [2]]");
  input_scalar = ScalarFromJSON(list(float32()), "[]");
  index = ScalarFromJSON(int32(), "0");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));
}

TEST(TestScalarNested, StructField) {
  StructFieldOptions trivial;
  StructFieldOptions extract0({0});
  StructFieldOptions extract20({2, 0});
  StructFieldOptions invalid1({-1});
  StructFieldOptions invalid2({2, 4});
  StructFieldOptions invalid3({3});
  StructFieldOptions invalid4({0, 1});
  FieldVector fields = {field("a", int32()), field("b", utf8()),
                        field("c", struct_({
                                       field("d", int64()),
                                       field("e", float64()),
                                   }))};
  {
    auto arr = ArrayFromJSON(struct_(fields), R"([
      [1, "a", [10, 10.0]],
      [null, "b", [11, 11.0]],
      [3, null, [12, 12.0]],
      null
    ])");
    CheckScalar("struct_field", {arr}, arr, &trivial);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, 3, null]"),
                &extract0);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[10, 11, 12, null]"),
                &extract20);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid1));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(TypeError, ::testing::HasSubstr("cannot subscript"),
                                    CallFunction("struct_field", {arr}, &invalid4));
  }
  {
    auto ty = dense_union(fields, {2, 5, 8});
    auto arr = ArrayFromJSON(ty, R"([
      [2, 1],
      [5, "foo"],
      [8, null],
      [8, [10, 10.0]]
    ])");
    CheckScalar("struct_field", {arr}, arr, &trivial);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[null, null, null, 10]"),
                &extract20);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid1));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(TypeError, ::testing::HasSubstr("cannot subscript"),
                                    CallFunction("struct_field", {arr}, &invalid4));

    // Test edge cases for union representation
    auto ints = ArrayFromJSON(fields[0]->type(), "[null, 2, 3]");
    auto strs = ArrayFromJSON(fields[1]->type(), R"([null, "bar"])");
    auto nested = ArrayFromJSON(fields[2]->type(), R"([null, [10, 10.0]])");
    auto type_ids = ArrayFromJSON(int8(), "[2, 5, 8, 2, 5, 8]")->data()->buffers[1];
    auto offsets = ArrayFromJSON(int32(), "[0, 0, 0, 1, 1, 1]")->data()->buffers[1];

    arr = std::make_shared<DenseUnionArray>(ty, /*length=*/6,
                                            ArrayVector{ints, strs, nested}, type_ids,
                                            offsets, /*offset=*/0);
    // Sliced parent
    CheckScalar("struct_field", {arr->Slice(3, 3)},
                ArrayFromJSON(int32(), "[2, null, null]"), &extract0);
    // Sliced child
    arr = std::make_shared<DenseUnionArray>(ty, /*length=*/6,
                                            ArrayVector{ints->Slice(1, 2), strs, nested},
                                            type_ids, offsets, /*offset=*/0);
    CheckScalar("struct_field", {arr},
                ArrayFromJSON(int32(), "[2, null, null, 3, null, null]"), &extract0);
    // Sliced parent + sliced child
    CheckScalar("struct_field", {arr->Slice(3, 3)},
                ArrayFromJSON(int32(), "[3, null, null]"), &extract0);
  }
  {
    // The underlying implementation is tested directly/more thoroughly in
    // array_union_test.cc.
    auto arr = ArrayFromJSON(sparse_union(fields, {2, 5, 8}), R"([
      [2, 1],
      [5, "foo"],
      [8, null],
      [8, [10, 10.0]]
    ])");
    CheckScalar("struct_field", {arr}, arr, &trivial);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[null, null, null, 10]"),
                &extract20);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid1));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(TypeError, ::testing::HasSubstr("cannot subscript"),
                                    CallFunction("struct_field", {arr}, &invalid4));
  }
  {
    auto arr = ArrayFromJSON(int32(), "[0, 1, 2, 3]");
    ASSERT_RAISES(NotImplemented, CallFunction("struct_field", {arr}, &trivial));
    ASSERT_RAISES(NotImplemented, CallFunction("struct_field", {arr}, &extract0));
  }
}

struct {
  Result<Datum> operator()(std::vector<Datum> args) {
    return CallFunction("make_struct", args);
  }

  template <typename... Options>
  Result<Datum> operator()(std::vector<Datum> args, std::vector<std::string> field_names,
                           Options... options) {
    MakeStructOptions opts{field_names, options...};
    return CallFunction("make_struct", args, &opts);
  }
} MakeStructor;

TEST(MakeStruct, Scalar) {
  auto i32 = MakeScalar(1);
  auto f64 = MakeScalar(2.5);
  auto str = MakeScalar("yo");

  EXPECT_THAT(MakeStructor({i32, f64, str}, {"i", "f", "s"}),
              ResultWith(Datum(*StructScalar::Make({i32, f64, str}, {"i", "f", "s"}))));

  // Names default to field_index
  EXPECT_THAT(MakeStructor({i32, f64, str}),
              ResultWith(Datum(*StructScalar::Make({i32, f64, str}, {"0", "1", "2"}))));

  // No field names or input values is fine
  EXPECT_THAT(MakeStructor({}), ResultWith(Datum(*StructScalar::Make({}, {}))));

  // Three field names but one input value
  EXPECT_THAT(MakeStructor({str}, {"i", "f", "s"}), Raises(StatusCode::Invalid));
}

TEST(MakeStruct, Array) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");

  EXPECT_THAT(MakeStructor({i32, str}, {"i", "s"}),
              ResultWith(Datum(*StructArray::Make({i32, str}, field_names))));

  // Scalars are broadcast to the length of the arrays
  EXPECT_THAT(MakeStructor({i32, MakeScalar("aa")}, {"i", "s"}),
              ResultWith(Datum(*StructArray::Make({i32, str}, field_names))));

  // Array length mismatch
  EXPECT_THAT(MakeStructor({i32->Slice(1), str}, field_names),
              Raises(StatusCode::Invalid));
}

TEST(MakeStruct, NullableMetadataPassedThru) {
  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");

  std::vector<std::string> field_names{"i", "s"};
  std::vector<bool> nullability{true, false};
  std::vector<std::shared_ptr<const KeyValueMetadata>> metadata = {
      key_value_metadata({"a", "b"}, {"ALPHA", "BRAVO"}), nullptr};

  ASSERT_OK_AND_ASSIGN(auto proj,
                       MakeStructor({i32, str}, field_names, nullability, metadata));

  AssertTypeEqual(*proj.type(), StructType({
                                    field("i", int32(), /*nullable=*/true, metadata[0]),
                                    field("s", utf8(), /*nullable=*/false, nullptr),
                                }));

  // error: projecting an array containing nulls with nullable=false
  EXPECT_THAT(MakeStructor({i32, ArrayFromJSON(utf8(), R"(["aa", null, "aa"])")},
                           field_names, nullability, metadata),
              Raises(StatusCode::Invalid));
}

TEST(MakeStruct, ChunkedArray) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32_0 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto i32_1 = ArrayFromJSON(int32(), "[]");
  auto i32_2 = ArrayFromJSON(int32(), "[32, 0]");

  auto str_0 = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");
  auto str_1 = ArrayFromJSON(utf8(), "[]");
  auto str_2 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");

  ASSERT_OK_AND_ASSIGN(auto i32, ChunkedArray::Make({i32_0, i32_1, i32_2}));
  ASSERT_OK_AND_ASSIGN(auto str, ChunkedArray::Make({str_0, str_1, str_2}));

  ASSERT_OK_AND_ASSIGN(auto expected_0, StructArray::Make({i32_0, str_0}, field_names));
  ASSERT_OK_AND_ASSIGN(auto expected_1, StructArray::Make({i32_1, str_1}, field_names));
  ASSERT_OK_AND_ASSIGN(auto expected_2, StructArray::Make({i32_2, str_2}, field_names));
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       ChunkedArray::Make({expected_0, expected_1, expected_2}));

  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, MakeStructor({i32->Slice(1), str}, field_names));
}

TEST(MakeStruct, ChunkedArrayDifferentChunking) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32_0 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto i32_1 = ArrayFromJSON(int32(), "[]");
  auto i32_2 = ArrayFromJSON(int32(), "[32, 0]");

  auto str_0 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");
  auto str_1 = ArrayFromJSON(utf8(), R"(["aa"])");
  auto str_2 = ArrayFromJSON(utf8(), R"([])");
  auto str_3 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");

  ASSERT_OK_AND_ASSIGN(auto i32, ChunkedArray::Make({i32_0, i32_1, i32_2}));
  ASSERT_OK_AND_ASSIGN(auto str, ChunkedArray::Make({str_0, str_1, str_2, str_3}));

  std::vector<ArrayVector> expected_rechunked =
      ::arrow::internal::RechunkArraysConsistently({i32->chunks(), str->chunks()});
  ASSERT_EQ(expected_rechunked[0].size(), expected_rechunked[1].size());

  ArrayVector expected_chunks(expected_rechunked[0].size());
  for (size_t i = 0; i < expected_chunks.size(); ++i) {
    ASSERT_OK_AND_ASSIGN(expected_chunks[i], StructArray::Make({expected_rechunked[0][i],
                                                                expected_rechunked[1][i]},
                                                               field_names));
  }

  ASSERT_OK_AND_ASSIGN(Datum expected, ChunkedArray::Make(expected_chunks));

  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, MakeStructor({i32->Slice(1), str}, field_names));
}

}  // namespace compute
}  // namespace arrow
