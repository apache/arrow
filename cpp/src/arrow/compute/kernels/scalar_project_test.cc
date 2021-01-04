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

#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/test_util.h"

namespace arrow {
namespace compute {

struct {
 public:
  Result<Datum> operator()(std::vector<Datum> args) {
    ProjectOptions opts{field_names};
    return CallFunction("project", args, &opts);
  }

  std::vector<std::string> field_names;
} Project;

TEST(Project, Scalar) {
  std::shared_ptr<StructScalar> expected(new StructScalar{{}, struct_({})});
  ASSERT_OK_AND_EQ(Datum(expected), Project({}));

  auto i32 = MakeScalar(1);
  auto f64 = MakeScalar(2.5);
  auto str = MakeScalar("yo");

  expected.reset(new StructScalar{
      {i32, f64, str},
      struct_({field("i", i32->type), field("f", f64->type), field("s", str->type)})});
  Project.field_names = {"i", "f", "s"};
  ASSERT_OK_AND_EQ(Datum(expected), Project({i32, f64, str}));

  // Three field names but one input value
  ASSERT_RAISES(Invalid, Project({str}));
}

TEST(Project, Array) {
  Project.field_names = {"i", "s"};
  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       StructArray::Make({i32, str}, Project.field_names));

  ASSERT_OK_AND_EQ(expected, Project({i32, str}));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, Project({i32, MakeScalar("aa")}));

  // Array length mismatch
  ASSERT_RAISES(Invalid, Project({i32->Slice(1), str}));
}

TEST(Project, ChunkedArray) {
  Project.field_names = {"i", "s"};

  auto i32_0 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto i32_1 = ArrayFromJSON(int32(), "[]");
  auto i32_2 = ArrayFromJSON(int32(), "[32, 0]");

  auto str_0 = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");
  auto str_1 = ArrayFromJSON(utf8(), "[]");
  auto str_2 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");

  ASSERT_OK_AND_ASSIGN(auto i32, ChunkedArray::Make({i32_0, i32_1, i32_2}));
  ASSERT_OK_AND_ASSIGN(auto str, ChunkedArray::Make({str_0, str_1, str_2}));

  ASSERT_OK_AND_ASSIGN(auto expected_0,
                       StructArray::Make({i32_0, str_0}, Project.field_names));
  ASSERT_OK_AND_ASSIGN(auto expected_1,
                       StructArray::Make({i32_1, str_1}, Project.field_names));
  ASSERT_OK_AND_ASSIGN(auto expected_2,
                       StructArray::Make({i32_2, str_2}, Project.field_names));
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       ChunkedArray::Make({expected_0, expected_1, expected_2}));

  ASSERT_OK_AND_EQ(expected, Project({i32, str}));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, Project({i32, MakeScalar("aa")}));

  // Array length mismatch
  ASSERT_RAISES(Invalid, Project({i32->Slice(1), str}));
}

}  // namespace compute
}  // namespace arrow
