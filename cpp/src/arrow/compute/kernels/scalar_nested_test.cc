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
}

struct {
  template <typename... Options>
  Result<Datum> operator()(std::vector<Datum> args, std::vector<std::string> field_names,
                           Options... options) {
    ProjectOptions opts{field_names, options...};
    return CallFunction("project", args, &opts);
  }
} Project;

TEST(Project, Scalar) {
  auto i32 = MakeScalar(1);
  auto f64 = MakeScalar(2.5);
  auto str = MakeScalar("yo");

  ASSERT_OK_AND_ASSIGN(auto expected,
                       StructScalar::Make({i32, f64, str}, {"i", "f", "s"}));
  ASSERT_OK_AND_EQ(Datum(expected), Project({i32, f64, str}, {"i", "f", "s"}));

  // Three field names but one input value
  ASSERT_RAISES(Invalid, Project({str}, {"i", "f", "s"}));

  // No field names or input values is fine
  expected.reset(new StructScalar{{}, struct_({})});
  ASSERT_OK_AND_EQ(Datum(expected), Project(/*args=*/{}, /*field_names=*/{}));
}

TEST(Project, Array) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");
  ASSERT_OK_AND_ASSIGN(Datum expected, StructArray::Make({i32, str}, field_names));

  ASSERT_OK_AND_EQ(expected, Project({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, Project({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, Project({i32->Slice(1), str}, field_names));
}

TEST(Project, NullableMetadataPassedThru) {
  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");

  std::vector<std::string> field_names{"i", "s"};
  std::vector<bool> nullability{true, false};
  std::vector<std::shared_ptr<const KeyValueMetadata>> metadata = {
      key_value_metadata({"a", "b"}, {"ALPHA", "BRAVO"}), nullptr};

  ASSERT_OK_AND_ASSIGN(auto proj,
                       Project({i32, str}, field_names, nullability, metadata));

  AssertTypeEqual(*proj.type(), StructType({
                                    field("i", int32(), /*nullable=*/true, metadata[0]),
                                    field("s", utf8(), /*nullable=*/false, nullptr),
                                }));

  // error: projecting an array containing nulls with nullable=false
  str = ArrayFromJSON(utf8(), R"(["aa", null, "aa"])");
  ASSERT_RAISES(Invalid, Project({i32, str}, field_names, nullability, metadata));
}

TEST(Project, ChunkedArray) {
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

  ASSERT_OK_AND_EQ(expected, Project({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, Project({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, Project({i32->Slice(1), str}, field_names));
}

TEST(Project, ChunkedArrayDifferentChunking) {
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

  ASSERT_OK_AND_EQ(expected, Project({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, Project({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, Project({i32->Slice(1), str}, field_names));
}

}  // namespace compute
}  // namespace arrow
