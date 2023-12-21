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

// test another instantiation of RapidJSON in a different namespace 

#define RAPIDJSON_NAMESPACE my::rapid::json
#define RAPIDJSON_NAMESPACE_BEGIN namespace my { namespace rapid { namespace json {
#define RAPIDJSON_NAMESPACE_END } } }

// include lots of RapidJSON files

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/encodedstream.h"
#include "rapidjson/stringbuffer.h"

static const char json[] = "{\"hello\":\"world\",\"t\":true,\"f\":false,\"n\":null,\"i\":123,\"pi\":3.1416,\"a\":[1,2,3,4]}";

TEST(NamespaceTest,Using) {
    using namespace RAPIDJSON_NAMESPACE;
    typedef GenericDocument<UTF8<>, CrtAllocator> DocumentType;
    DocumentType doc;

    doc.Parse(json);
    EXPECT_TRUE(!doc.HasParseError());
}

TEST(NamespaceTest,Direct) {
    typedef RAPIDJSON_NAMESPACE::Document Document;
    typedef RAPIDJSON_NAMESPACE::Reader Reader;
    typedef RAPIDJSON_NAMESPACE::StringStream StringStream;
    typedef RAPIDJSON_NAMESPACE::StringBuffer StringBuffer;
    typedef RAPIDJSON_NAMESPACE::Writer<StringBuffer> WriterType;

    StringStream s(json);
    StringBuffer buffer;
    WriterType writer(buffer);
    buffer.ShrinkToFit();
    Reader reader;
    reader.Parse(s, writer);

    EXPECT_STREQ(json, buffer.GetString());
    EXPECT_EQ(sizeof(json)-1, buffer.GetSize());
    EXPECT_TRUE(writer.IsComplete());

    Document doc;
    doc.Parse(buffer.GetString());
    EXPECT_TRUE(!doc.HasParseError());

    buffer.Clear();
    writer.Reset(buffer);
    doc.Accept(writer);
    EXPECT_STREQ(json, buffer.GetString());
    EXPECT_TRUE(writer.IsComplete());
}
