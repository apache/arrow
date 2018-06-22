// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

extern "C" int half_or_null_int32(int val, bool in_valid, bool *out_valid);

namespace gandiva {

TEST(TestSample, half_or_null) {
  bool is_valid = false;
  int ret;

  // 4 is a multiple, so expect 2.
  ret = half_or_null_int32(4, true, &is_valid);
  EXPECT_EQ(ret, 2);
  EXPECT_EQ(is_valid, true);

  // if input is not valid, expect null.
  ret = half_or_null_int32(4, false, &is_valid);
  EXPECT_EQ(is_valid, false);

  // -16 is a multiple, so expect 8.
  ret = half_or_null_int32(-16, true, &is_valid);
  EXPECT_EQ(ret, -8);
  EXPECT_EQ(is_valid, true);

  // 5 is not a multiple, so expect null.
  ret = half_or_null_int32(5, true, &is_valid);
  EXPECT_EQ(is_valid, false);

  // -31 is not a multiple, so expect null.
  ret = half_or_null_int32(-31, true, &is_valid);
  EXPECT_EQ(is_valid, false);
}

} // namespace gandiva
