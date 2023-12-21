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
#include "rapidjson/internal/strfunc.h"

using namespace rapidjson;
using namespace rapidjson::internal;

TEST(StrFunc, CountStringCodePoint) {
    SizeType count;
    EXPECT_TRUE(CountStringCodePoint<UTF8<> >("", 0, &count));
    EXPECT_EQ(0u, count);
    EXPECT_TRUE(CountStringCodePoint<UTF8<> >("Hello", 5, &count));
    EXPECT_EQ(5u, count);
    EXPECT_TRUE(CountStringCodePoint<UTF8<> >("\xC2\xA2\xE2\x82\xAC\xF0\x9D\x84\x9E", 9, &count)); // cents euro G-clef
    EXPECT_EQ(3u, count);
    EXPECT_FALSE(CountStringCodePoint<UTF8<> >("\xC2\xA2\xE2\x82\xAC\xF0\x9D\x84\x9E\x80", 10, &count));
}
