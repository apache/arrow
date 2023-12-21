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
#include "rapidjson/document.h"
#include "rapidjson/cursorstreamwrapper.h"

using namespace rapidjson;

// static const char json[] = "{\"string\"\n\n:\"my string\",\"array\"\n:[\"1\", \"2\", \"3\"]}";

bool testJson(const char *json, size_t &line, size_t &col) {
    StringStream ss(json);
    CursorStreamWrapper<StringStream> csw(ss);
    Document document;
    document.ParseStream(csw);
    bool ret = document.HasParseError();
    if (ret) {
        col = csw.GetColumn();
        line = csw.GetLine();
    }
    return ret;
}

TEST(CursorStreamWrapper, MissingFirstBracket) {
    const char json[] = "\"string\"\n\n:\"my string\",\"array\"\n:[\"1\", \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 3u);
    EXPECT_EQ(col, 0u);
}

TEST(CursorStreamWrapper, MissingQuotes) {
    const char json[] = "{\"string\n\n:\"my string\",\"array\"\n:[\"1\", \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 1u);
    EXPECT_EQ(col, 8u);
}

TEST(CursorStreamWrapper, MissingColon) {
    const char json[] = "{\"string\"\n\n\"my string\",\"array\"\n:[\"1\", \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 3u);
    EXPECT_EQ(col, 0u);
}

TEST(CursorStreamWrapper, MissingSecondQuotes) {
    const char json[] = "{\"string\"\n\n:my string\",\"array\"\n:[\"1\", \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 3u);
    EXPECT_EQ(col, 1u);
}

TEST(CursorStreamWrapper, MissingComma) {
    const char json[] = "{\"string\"\n\n:\"my string\"\"array\"\n:[\"1\", \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 3u);
    EXPECT_EQ(col, 12u);
}

TEST(CursorStreamWrapper, MissingArrayBracket) {
    const char json[] = "{\"string\"\n\n:\"my string\",\"array\"\n:\"1\", \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 4u);
    EXPECT_EQ(col, 9u);
}

TEST(CursorStreamWrapper, MissingArrayComma) {
    const char json[] = "{\"string\"\n\n:\"my string\",\"array\"\n:[\"1\" \"2\", \"3\"]}";
    size_t col, line;
    bool ret = testJson(json, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 4u);
    EXPECT_EQ(col, 6u);
}

TEST(CursorStreamWrapper, MissingLastArrayBracket) {
    const char json8[] = "{\"string\"\n\n:\"my string\",\"array\"\n:[\"1\", \"2\", \"3\"}";
    size_t col, line;
    bool ret = testJson(json8, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 4u);
    EXPECT_EQ(col, 15u);
}

TEST(CursorStreamWrapper, MissingLastBracket) {
    const char json9[] = "{\"string\"\n\n:\"my string\",\"array\"\n:[\"1\", \"2\", \"3\"]";
    size_t col, line;
    bool ret = testJson(json9, line, col);
    EXPECT_TRUE(ret);
    EXPECT_EQ(line, 4u);
    EXPECT_EQ(col, 16u);
}
