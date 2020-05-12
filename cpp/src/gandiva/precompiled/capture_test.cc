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

#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestCapture, capture) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);

  const std::string source_string = "the ip address is 127.0.0.1.";
  const std::string pattern_string = "(?P<ip>\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
  gdv_int32 source_string_length(source_string.size());
  gdv_int32 pattern_string_length(pattern_string.size());
  gdv_int32 out_len;
  const char* ret =
      capture_utf8_utf8(ctx_ptr, source_string.data(), source_string_length, true,
                        pattern_string.data(), pattern_string_length, true, &out_len);
  EXPECT_EQ(std::string(ret, out_len), "127.0.0.1");
}

}  // namespace gandiva
