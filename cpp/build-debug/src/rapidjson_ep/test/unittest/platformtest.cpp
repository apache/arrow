// Tencent is pleased to support the open source community by making RapidJSON available.
// 
// Copyright (C) 2021 THL A29 Limited, a Tencent company, and Milo Yip.
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

// see https://github.com/Tencent/rapidjson/issues/1448
// including windows.h on purpose to provoke a compile time problem as GetObject is a 
// macro that gets defined when windows.h is included
#ifdef _WIN32
#include <windows.h>
#endif

#include "rapidjson/document.h"
#undef GetObject

using namespace rapidjson;

TEST(Platform, GetObject) {
    Document doc;
    doc.Parse(" { \"object\" : { \"pi\": 3.1416} } ");
    EXPECT_TRUE(doc.IsObject());
    EXPECT_TRUE(doc.HasMember("object"));
    const Document::ValueType& o = doc["object"];
    EXPECT_TRUE(o.IsObject());
    Value::ConstObject sub = o.GetObject();
    EXPECT_TRUE(sub.HasMember("pi"));
    Value::ConstObject sub2 = o.GetObj();
    EXPECT_TRUE(sub2.HasMember("pi"));
}
