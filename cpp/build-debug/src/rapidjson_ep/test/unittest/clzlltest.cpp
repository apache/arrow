// Tencent is pleased to support the open source community by making RapidJSON available.
// 
// Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
//
// Licensed under the MIT License (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://opensource.org/licenses/MIT
//
// Unless required by applicable law or agreed to in writing, software distributed 
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

#include "unittest.h"
#include "rapidjson/internal/clzll.h"

#ifdef __GNUC__
RAPIDJSON_DIAG_PUSH
#endif

using namespace rapidjson::internal;

TEST(clzll, normal) {
    EXPECT_EQ(clzll(1), 63U);
    EXPECT_EQ(clzll(2), 62U);
    EXPECT_EQ(clzll(12), 60U);
    EXPECT_EQ(clzll(0x0000000080000001UL), 32U);
    EXPECT_EQ(clzll(0x8000000000000001UL), 0U);
}

#ifdef __GNUC__
RAPIDJSON_DIAG_POP
#endif
