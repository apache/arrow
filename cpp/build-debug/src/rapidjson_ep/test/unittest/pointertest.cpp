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
#include "rapidjson/pointer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/ostreamwrapper.h"
#include <sstream>
#include <map>
#include <algorithm>

using namespace rapidjson;

static const char kJson[] = "{\n"
"    \"foo\":[\"bar\", \"baz\"],\n"
"    \"\" : 0,\n"
"    \"a/b\" : 1,\n"
"    \"c%d\" : 2,\n"
"    \"e^f\" : 3,\n"
"    \"g|h\" : 4,\n"
"    \"i\\\\j\" : 5,\n"
"    \"k\\\"l\" : 6,\n"
"    \" \" : 7,\n"
"    \"m~n\" : 8\n"
"}";

TEST(Pointer, DefaultConstructor) {
    Pointer p;
    EXPECT_TRUE(p.IsValid());
    EXPECT_EQ(0u, p.GetTokenCount());
}

TEST(Pointer, Parse) {
    {
        Pointer p("");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(0u, p.GetTokenCount());
    }

    {
        Pointer p("/");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(0u, p.GetTokens()[0].length);
        EXPECT_STREQ("", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }

    {
        Pointer p("/foo");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("foo", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }

    #if RAPIDJSON_HAS_STDSTRING
    {
        Pointer p(std::string("/foo"));
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("foo", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }
    #endif

    {
        Pointer p("/foo/0");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(2u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("foo", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
        EXPECT_EQ(1u, p.GetTokens()[1].length);
        EXPECT_STREQ("0", p.GetTokens()[1].name);
        EXPECT_EQ(0u, p.GetTokens()[1].index);
    }

    {
        // Unescape ~1
        Pointer p("/a~1b");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("a/b", p.GetTokens()[0].name);
    }

    {
        // Unescape ~0
        Pointer p("/m~0n");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("m~n", p.GetTokens()[0].name);
    }

    {
        // empty name
        Pointer p("/");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(0u, p.GetTokens()[0].length);
        EXPECT_STREQ("", p.GetTokens()[0].name);
    }

    {
        // empty and non-empty name
        Pointer p("//a");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(2u, p.GetTokenCount());
        EXPECT_EQ(0u, p.GetTokens()[0].length);
        EXPECT_STREQ("", p.GetTokens()[0].name);
        EXPECT_EQ(1u, p.GetTokens()[1].length);
        EXPECT_STREQ("a", p.GetTokens()[1].name);
    }

    {
        // Null characters
        Pointer p("/\0\0", 3);
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(2u, p.GetTokens()[0].length);
        EXPECT_EQ('\0', p.GetTokens()[0].name[0]);
        EXPECT_EQ('\0', p.GetTokens()[0].name[1]);
        EXPECT_EQ('\0', p.GetTokens()[0].name[2]);
    }

    {
        // Valid index
        Pointer p("/123");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("123", p.GetTokens()[0].name);
        EXPECT_EQ(123u, p.GetTokens()[0].index);
    }

    {
        // Invalid index (with leading zero)
        Pointer p("/01");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("01", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }

    if (sizeof(SizeType) == 4) {
        // Invalid index (overflow)
        Pointer p("/4294967296");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("4294967296", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }

    {
        // kPointerParseErrorTokenMustBeginWithSolidus
        Pointer p(" ");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorTokenMustBeginWithSolidus, p.GetParseErrorCode());
        EXPECT_EQ(0u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidEscape
        Pointer p("/~");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidEscape, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidEscape
        Pointer p("/~2");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidEscape, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }
}

TEST(Pointer, Parse_URIFragment) {
    {
        Pointer p("#");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(0u, p.GetTokenCount());
    }

    {
        Pointer p("#/foo");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("foo", p.GetTokens()[0].name);
    }

    {
        Pointer p("#/foo/0");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(2u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("foo", p.GetTokens()[0].name);
        EXPECT_EQ(1u, p.GetTokens()[1].length);
        EXPECT_STREQ("0", p.GetTokens()[1].name);
        EXPECT_EQ(0u, p.GetTokens()[1].index);
    }

    {
        // Unescape ~1
        Pointer p("#/a~1b");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("a/b", p.GetTokens()[0].name);
    }

    {
        // Unescape ~0
        Pointer p("#/m~0n");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(3u, p.GetTokens()[0].length);
        EXPECT_STREQ("m~n", p.GetTokens()[0].name);
    }

    {
        // empty name
        Pointer p("#/");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(0u, p.GetTokens()[0].length);
        EXPECT_STREQ("", p.GetTokens()[0].name);
    }

    {
        // empty and non-empty name
        Pointer p("#//a");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(2u, p.GetTokenCount());
        EXPECT_EQ(0u, p.GetTokens()[0].length);
        EXPECT_STREQ("", p.GetTokens()[0].name);
        EXPECT_EQ(1u, p.GetTokens()[1].length);
        EXPECT_STREQ("a", p.GetTokens()[1].name);
    }

    {
        // Null characters
        Pointer p("#/%00%00");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(2u, p.GetTokens()[0].length);
        EXPECT_EQ('\0', p.GetTokens()[0].name[0]);
        EXPECT_EQ('\0', p.GetTokens()[0].name[1]);
        EXPECT_EQ('\0', p.GetTokens()[0].name[2]);
    }

    {
        // Percentage Escapes
        EXPECT_STREQ("c%d", Pointer("#/c%25d").GetTokens()[0].name);
        EXPECT_STREQ("e^f", Pointer("#/e%5Ef").GetTokens()[0].name);
        EXPECT_STREQ("g|h", Pointer("#/g%7Ch").GetTokens()[0].name);
        EXPECT_STREQ("i\\j", Pointer("#/i%5Cj").GetTokens()[0].name);
        EXPECT_STREQ("k\"l", Pointer("#/k%22l").GetTokens()[0].name);
        EXPECT_STREQ(" ", Pointer("#/%20").GetTokens()[0].name);
    }

    {
        // Valid index
        Pointer p("#/123");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("123", p.GetTokens()[0].name);
        EXPECT_EQ(123u, p.GetTokens()[0].index);
    }

    {
        // Invalid index (with leading zero)
        Pointer p("#/01");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("01", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }

    if (sizeof(SizeType) == 4) {
        // Invalid index (overflow)
        Pointer p("#/4294967296");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("4294967296", p.GetTokens()[0].name);
        EXPECT_EQ(kPointerInvalidIndex, p.GetTokens()[0].index);
    }

    {
        // Decode UTF-8 perecent encoding to UTF-8
        Pointer p("#/%C2%A2");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_STREQ("\xC2\xA2", p.GetTokens()[0].name);
    }

    {
        // Decode UTF-8 perecent encoding to UTF-16
        GenericPointer<GenericValue<UTF16<> > > p(L"#/%C2%A2");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(static_cast<UTF16<>::Ch>(0x00A2), p.GetTokens()[0].name[0]);
        EXPECT_EQ(1u, p.GetTokens()[0].length);
    }

    {
        // Decode UTF-8 perecent encoding to UTF-16
        GenericPointer<GenericValue<UTF16<> > > p(L"#/%E2%82%AC");
        EXPECT_TRUE(p.IsValid());
        EXPECT_EQ(1u, p.GetTokenCount());
        EXPECT_EQ(static_cast<UTF16<>::Ch>(0x20AC), p.GetTokens()[0].name[0]);
        EXPECT_EQ(1u, p.GetTokens()[0].length);
    }

    {
        // kPointerParseErrorTokenMustBeginWithSolidus
        Pointer p("# ");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorTokenMustBeginWithSolidus, p.GetParseErrorCode());
        EXPECT_EQ(1u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidEscape
        Pointer p("#/~");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidEscape, p.GetParseErrorCode());
        EXPECT_EQ(3u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidEscape
        Pointer p("#/~2");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidEscape, p.GetParseErrorCode());
        EXPECT_EQ(3u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidPercentEncoding
        Pointer p("#/%");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidPercentEncoding, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidPercentEncoding (invalid hex)
        Pointer p("#/%g0");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidPercentEncoding, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidPercentEncoding (invalid hex)
        Pointer p("#/%0g");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidPercentEncoding, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorInvalidPercentEncoding (incomplete UTF-8 sequence)
        Pointer p("#/%C2");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorInvalidPercentEncoding, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorCharacterMustPercentEncode
        Pointer p("#/ ");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorCharacterMustPercentEncode, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }

    {
        // kPointerParseErrorCharacterMustPercentEncode
        Pointer p("#/\n");
        EXPECT_FALSE(p.IsValid());
        EXPECT_EQ(kPointerParseErrorCharacterMustPercentEncode, p.GetParseErrorCode());
        EXPECT_EQ(2u, p.GetParseErrorOffset());
    }
}

TEST(Pointer, Stringify) {
    // Test by roundtrip
    const char* sources[] = {
        "",
        "/foo",
        "/foo/0",
        "/",
        "/a~1b",
        "/c%d",
        "/e^f",
        "/g|h",
        "/i\\j",
        "/k\"l",
        "/ ",
        "/m~0n",
        "/\xC2\xA2",
        "/\xE2\x82\xAC",
        "/\xF0\x9D\x84\x9E"
    };

    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        Pointer p(sources[i]);
        StringBuffer s;
        EXPECT_TRUE(p.Stringify(s));
        EXPECT_STREQ(sources[i], s.GetString());

        // Stringify to URI fragment
        StringBuffer s2;
        EXPECT_TRUE(p.StringifyUriFragment(s2));
        Pointer p2(s2.GetString(), s2.GetSize());
        EXPECT_TRUE(p2.IsValid());
        EXPECT_TRUE(p == p2);
    }

    {
        // Strigify to URI fragment with an invalid UTF-8 sequence
        Pointer p("/\xC2");
        StringBuffer s;
        EXPECT_FALSE(p.StringifyUriFragment(s));
    }
}

// Construct a Pointer with static tokens, no dynamic allocation involved.
#define NAME(s) { s, static_cast<SizeType>(sizeof(s) / sizeof(s[0]) - 1), kPointerInvalidIndex }
#define INDEX(i) { #i, static_cast<SizeType>(sizeof(#i) - 1), i }

static const Pointer::Token kTokens[] = { NAME("foo"), INDEX(0) }; // equivalent to "/foo/0"

#undef NAME
#undef INDEX

TEST(Pointer, ConstructorWithToken) {
    Pointer p(kTokens, sizeof(kTokens) / sizeof(kTokens[0]));
    EXPECT_TRUE(p.IsValid());
    EXPECT_EQ(2u, p.GetTokenCount());
    EXPECT_EQ(3u, p.GetTokens()[0].length);
    EXPECT_STREQ("foo", p.GetTokens()[0].name);
    EXPECT_EQ(1u, p.GetTokens()[1].length);
    EXPECT_STREQ("0", p.GetTokens()[1].name);
    EXPECT_EQ(0u, p.GetTokens()[1].index);
}

TEST(Pointer, CopyConstructor) {
    {
        CrtAllocator allocator;
        Pointer p("/foo/0", &allocator);
        Pointer q(p);
        EXPECT_TRUE(q.IsValid());
        EXPECT_EQ(2u, q.GetTokenCount());
        EXPECT_EQ(3u, q.GetTokens()[0].length);
        EXPECT_STREQ("foo", q.GetTokens()[0].name);
        EXPECT_EQ(1u, q.GetTokens()[1].length);
        EXPECT_STREQ("0", q.GetTokens()[1].name);
        EXPECT_EQ(0u, q.GetTokens()[1].index);
        
        // Copied pointer needs to have its own allocator
        EXPECT_NE(&p.GetAllocator(), &q.GetAllocator());
    }

    // Static tokens
    {
        Pointer p(kTokens, sizeof(kTokens) / sizeof(kTokens[0]));
        Pointer q(p);
        EXPECT_TRUE(q.IsValid());
        EXPECT_EQ(2u, q.GetTokenCount());
        EXPECT_EQ(3u, q.GetTokens()[0].length);
        EXPECT_STREQ("foo", q.GetTokens()[0].name);
        EXPECT_EQ(1u, q.GetTokens()[1].length);
        EXPECT_STREQ("0", q.GetTokens()[1].name);
        EXPECT_EQ(0u, q.GetTokens()[1].index);
    }
}

TEST(Pointer, Assignment) {
    {
        CrtAllocator allocator;
        Pointer p("/foo/0", &allocator);
        Pointer q;
        q = p;
        EXPECT_TRUE(q.IsValid());
        EXPECT_EQ(2u, q.GetTokenCount());
        EXPECT_EQ(3u, q.GetTokens()[0].length);
        EXPECT_STREQ("foo", q.GetTokens()[0].name);
        EXPECT_EQ(1u, q.GetTokens()[1].length);
        EXPECT_STREQ("0", q.GetTokens()[1].name);
        EXPECT_EQ(0u, q.GetTokens()[1].index);
        EXPECT_NE(&p.GetAllocator(), &q.GetAllocator());
        q = static_cast<const Pointer &>(q);
        EXPECT_TRUE(q.IsValid());
        EXPECT_EQ(2u, q.GetTokenCount());
        EXPECT_EQ(3u, q.GetTokens()[0].length);
        EXPECT_STREQ("foo", q.GetTokens()[0].name);
        EXPECT_EQ(1u, q.GetTokens()[1].length);
        EXPECT_STREQ("0", q.GetTokens()[1].name);
        EXPECT_EQ(0u, q.GetTokens()[1].index);
        EXPECT_NE(&p.GetAllocator(), &q.GetAllocator());
    }

    // Static tokens
    {
        Pointer p(kTokens, sizeof(kTokens) / sizeof(kTokens[0]));
        Pointer q;
        q = p;
        EXPECT_TRUE(q.IsValid());
        EXPECT_EQ(2u, q.GetTokenCount());
        EXPECT_EQ(3u, q.GetTokens()[0].length);
        EXPECT_STREQ("foo", q.GetTokens()[0].name);
        EXPECT_EQ(1u, q.GetTokens()[1].length);
        EXPECT_STREQ("0", q.GetTokens()[1].name);
        EXPECT_EQ(0u, q.GetTokens()[1].index);
    }
}

TEST(Pointer, Swap) {
    Pointer p("/foo/0");
    Pointer q(&p.GetAllocator());

    q.Swap(p);
    EXPECT_EQ(&q.GetAllocator(), &p.GetAllocator());
    EXPECT_TRUE(p.IsValid());
    EXPECT_TRUE(q.IsValid());
    EXPECT_EQ(0u, p.GetTokenCount());
    EXPECT_EQ(2u, q.GetTokenCount());
    EXPECT_EQ(3u, q.GetTokens()[0].length);
    EXPECT_STREQ("foo", q.GetTokens()[0].name);
    EXPECT_EQ(1u, q.GetTokens()[1].length);
    EXPECT_STREQ("0", q.GetTokens()[1].name);
    EXPECT_EQ(0u, q.GetTokens()[1].index);

    // std::swap compatibility
    std::swap(p, q);
    EXPECT_EQ(&p.GetAllocator(), &q.GetAllocator());
    EXPECT_TRUE(q.IsValid());
    EXPECT_TRUE(p.IsValid());
    EXPECT_EQ(0u, q.GetTokenCount());
    EXPECT_EQ(2u, p.GetTokenCount());
    EXPECT_EQ(3u, p.GetTokens()[0].length);
    EXPECT_STREQ("foo", p.GetTokens()[0].name);
    EXPECT_EQ(1u, p.GetTokens()[1].length);
    EXPECT_STREQ("0", p.GetTokens()[1].name);
    EXPECT_EQ(0u, p.GetTokens()[1].index);
}

TEST(Pointer, Append) {
    {
        Pointer p;
        Pointer q = p.Append("foo");
        EXPECT_TRUE(Pointer("/foo") == q);
        q = q.Append(1234);
        EXPECT_TRUE(Pointer("/foo/1234") == q);
        q = q.Append("");
        EXPECT_TRUE(Pointer("/foo/1234/") == q);
    }

    {
        Pointer p;
        Pointer q = p.Append(Value("foo").Move());
        EXPECT_TRUE(Pointer("/foo") == q);
        q = q.Append(Value(1234).Move());
        EXPECT_TRUE(Pointer("/foo/1234") == q);
        q = q.Append(Value(kStringType).Move());
        EXPECT_TRUE(Pointer("/foo/1234/") == q);
    }

#if RAPIDJSON_HAS_STDSTRING
    {
        Pointer p;
        Pointer q = p.Append(std::string("foo"));
        EXPECT_TRUE(Pointer("/foo") == q);
    }
#endif
}

TEST(Pointer, Equality) {
    EXPECT_TRUE(Pointer("/foo/0") == Pointer("/foo/0"));
    EXPECT_FALSE(Pointer("/foo/0") == Pointer("/foo/1"));
    EXPECT_FALSE(Pointer("/foo/0") == Pointer("/foo/0/1"));
    EXPECT_FALSE(Pointer("/foo/0") == Pointer("a"));
    EXPECT_FALSE(Pointer("a") == Pointer("a")); // Invalid always not equal
}

TEST(Pointer, Inequality) {
    EXPECT_FALSE(Pointer("/foo/0") != Pointer("/foo/0"));
    EXPECT_TRUE(Pointer("/foo/0") != Pointer("/foo/1"));
    EXPECT_TRUE(Pointer("/foo/0") != Pointer("/foo/0/1"));
    EXPECT_TRUE(Pointer("/foo/0") != Pointer("a"));
    EXPECT_TRUE(Pointer("a") != Pointer("a")); // Invalid always not equal
}

TEST(Pointer, Create) {
    Document d;
    {
        Value* v = &Pointer("").Create(d, d.GetAllocator());
        EXPECT_EQ(&d, v);
    }
    {
        Value* v = &Pointer("/foo").Create(d, d.GetAllocator());
        EXPECT_EQ(&d["foo"], v);
    }
    {
        Value* v = &Pointer("/foo/0").Create(d, d.GetAllocator());
        EXPECT_EQ(&d["foo"][0], v);
    }
    {
        Value* v = &Pointer("/foo/-").Create(d, d.GetAllocator());
        EXPECT_EQ(&d["foo"][1], v);
    }

    {
        Value* v = &Pointer("/foo/-/-").Create(d, d.GetAllocator());
        // "foo/-" is a newly created null value x.
        // "foo/-/-" finds that x is not an array, it converts x to empty object
        // and treats - as "-" member name
        EXPECT_EQ(&d["foo"][2]["-"], v);
    }

    {
        // Document with no allocator
        Value* v = &Pointer("/foo/-").Create(d);
        EXPECT_EQ(&d["foo"][3], v);
    }

    {
        // Value (not document) must give allocator
        Value* v = &Pointer("/-").Create(d["foo"], d.GetAllocator());
        EXPECT_EQ(&d["foo"][4], v);
    }
}

static const char kJsonIds[] = "{\n"
   "    \"id\": \"/root/\","
   "    \"foo\":[\"bar\", \"baz\", {\"id\": \"inarray\", \"child\": 1}],\n"
   "    \"int\" : 2,\n"
   "    \"str\" : \"val\",\n"
   "    \"obj\": {\"id\": \"inobj\", \"child\": 3},\n"
   "    \"jbo\": {\"id\": true, \"child\": 4}\n"
   "}";


TEST(Pointer, GetUri) {
    CrtAllocator allocator;
    Document d;
    d.Parse(kJsonIds);
    Pointer::UriType doc("http://doc");
    Pointer::UriType root("http://doc/root/");
    Pointer::UriType empty = Pointer::UriType();

    EXPECT_TRUE(Pointer("").GetUri(d, doc) == doc);
    EXPECT_TRUE(Pointer("/foo").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/foo/0").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/foo/2").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/foo/2/child").GetUri(d, doc) == Pointer::UriType("http://doc/root/inarray"));
    EXPECT_TRUE(Pointer("/int").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/str").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/obj").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/obj/child").GetUri(d, doc) == Pointer::UriType("http://doc/root/inobj"));
    EXPECT_TRUE(Pointer("/jbo").GetUri(d, doc) == root);
    EXPECT_TRUE(Pointer("/jbo/child").GetUri(d, doc) == root); // id not string

    size_t unresolvedTokenIndex;
    EXPECT_TRUE(Pointer("/abc").GetUri(d, doc, &unresolvedTokenIndex, &allocator) == empty); // Out of boundary
    EXPECT_EQ(0u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/3").GetUri(d, doc, &unresolvedTokenIndex, &allocator) == empty); // Out of boundary
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/a").GetUri(d, doc, &unresolvedTokenIndex, &allocator) == empty); // "/foo" is an array, cannot query by "a"
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/0/0").GetUri(d, doc, &unresolvedTokenIndex, &allocator) == empty); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/0/a").GetUri(d, doc, &unresolvedTokenIndex, &allocator) == empty); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);

    Pointer::Token tokens[] = { { "foo ...", 3, kPointerInvalidIndex } };
    EXPECT_TRUE(Pointer(tokens, 1).GetUri(d, doc) == root);
}

TEST(Pointer, Get) {
    Document d;
    d.Parse(kJson);

    EXPECT_EQ(&d, Pointer("").Get(d));
    EXPECT_EQ(&d["foo"], Pointer("/foo").Get(d));
    EXPECT_EQ(&d["foo"][0], Pointer("/foo/0").Get(d));
    EXPECT_EQ(&d[""], Pointer("/").Get(d));
    EXPECT_EQ(&d["a/b"], Pointer("/a~1b").Get(d));
    EXPECT_EQ(&d["c%d"], Pointer("/c%d").Get(d));
    EXPECT_EQ(&d["e^f"], Pointer("/e^f").Get(d));
    EXPECT_EQ(&d["g|h"], Pointer("/g|h").Get(d));
    EXPECT_EQ(&d["i\\j"], Pointer("/i\\j").Get(d));
    EXPECT_EQ(&d["k\"l"], Pointer("/k\"l").Get(d));
    EXPECT_EQ(&d[" "], Pointer("/ ").Get(d));
    EXPECT_EQ(&d["m~n"], Pointer("/m~0n").Get(d));

    EXPECT_TRUE(Pointer("/abc").Get(d) == 0);  // Out of boundary
    size_t unresolvedTokenIndex;
    EXPECT_TRUE(Pointer("/foo/2").Get(d, &unresolvedTokenIndex) == 0); // Out of boundary
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/a").Get(d, &unresolvedTokenIndex) == 0); // "/foo" is an array, cannot query by "a"
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/0/0").Get(d, &unresolvedTokenIndex) == 0); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);
    EXPECT_TRUE(Pointer("/foo/0/a").Get(d, &unresolvedTokenIndex) == 0); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);

    Pointer::Token tokens[] = { { "foo ...", 3, kPointerInvalidIndex } };
    EXPECT_EQ(&d["foo"], Pointer(tokens, 1).Get(d));
}

TEST(Pointer, GetWithDefault) {
    Document d;
    d.Parse(kJson);

    // Value version
    Document::AllocatorType& a = d.GetAllocator();
    const Value v("qux");
    EXPECT_TRUE(Value("bar") == Pointer("/foo/0").GetWithDefault(d, v, a));
    EXPECT_TRUE(Value("baz") == Pointer("/foo/1").GetWithDefault(d, v, a));
    EXPECT_TRUE(Value("qux") == Pointer("/foo/2").GetWithDefault(d, v, a));
    EXPECT_TRUE(Value("last") == Pointer("/foo/-").GetWithDefault(d, Value("last").Move(), a));
    EXPECT_STREQ("last", d["foo"][3].GetString());

    EXPECT_TRUE(Pointer("/foo/null").GetWithDefault(d, Value().Move(), a).IsNull());
    EXPECT_TRUE(Pointer("/foo/null").GetWithDefault(d, "x", a).IsNull());

    // Generic version
    EXPECT_EQ(-1, Pointer("/foo/int").GetWithDefault(d, -1, a).GetInt());
    EXPECT_EQ(-1, Pointer("/foo/int").GetWithDefault(d, -2, a).GetInt());
    EXPECT_EQ(0x87654321, Pointer("/foo/uint").GetWithDefault(d, 0x87654321, a).GetUint());
    EXPECT_EQ(0x87654321, Pointer("/foo/uint").GetWithDefault(d, 0x12345678, a).GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    EXPECT_EQ(i64, Pointer("/foo/int64").GetWithDefault(d, i64, a).GetInt64());
    EXPECT_EQ(i64, Pointer("/foo/int64").GetWithDefault(d, i64 + 1, a).GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    EXPECT_EQ(u64, Pointer("/foo/uint64").GetWithDefault(d, u64, a).GetUint64());
    EXPECT_EQ(u64, Pointer("/foo/uint64").GetWithDefault(d, u64 - 1, a).GetUint64());

    EXPECT_TRUE(Pointer("/foo/true").GetWithDefault(d, true, a).IsTrue());
    EXPECT_TRUE(Pointer("/foo/true").GetWithDefault(d, false, a).IsTrue());

    EXPECT_TRUE(Pointer("/foo/false").GetWithDefault(d, false, a).IsFalse());
    EXPECT_TRUE(Pointer("/foo/false").GetWithDefault(d, true, a).IsFalse());

    // StringRef version
    EXPECT_STREQ("Hello", Pointer("/foo/hello").GetWithDefault(d, "Hello", a).GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        EXPECT_STREQ("World", Pointer("/foo/world").GetWithDefault(d, buffer, a).GetString());
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("C++", Pointer("/foo/C++").GetWithDefault(d, std::string("C++"), a).GetString());
#endif
}

TEST(Pointer, GetWithDefault_NoAllocator) {
    Document d;
    d.Parse(kJson);

    // Value version
    const Value v("qux");
    EXPECT_TRUE(Value("bar") == Pointer("/foo/0").GetWithDefault(d, v));
    EXPECT_TRUE(Value("baz") == Pointer("/foo/1").GetWithDefault(d, v));
    EXPECT_TRUE(Value("qux") == Pointer("/foo/2").GetWithDefault(d, v));
    EXPECT_TRUE(Value("last") == Pointer("/foo/-").GetWithDefault(d, Value("last").Move()));
    EXPECT_STREQ("last", d["foo"][3].GetString());

    EXPECT_TRUE(Pointer("/foo/null").GetWithDefault(d, Value().Move()).IsNull());
    EXPECT_TRUE(Pointer("/foo/null").GetWithDefault(d, "x").IsNull());

    // Generic version
    EXPECT_EQ(-1, Pointer("/foo/int").GetWithDefault(d, -1).GetInt());
    EXPECT_EQ(-1, Pointer("/foo/int").GetWithDefault(d, -2).GetInt());
    EXPECT_EQ(0x87654321, Pointer("/foo/uint").GetWithDefault(d, 0x87654321).GetUint());
    EXPECT_EQ(0x87654321, Pointer("/foo/uint").GetWithDefault(d, 0x12345678).GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    EXPECT_EQ(i64, Pointer("/foo/int64").GetWithDefault(d, i64).GetInt64());
    EXPECT_EQ(i64, Pointer("/foo/int64").GetWithDefault(d, i64 + 1).GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    EXPECT_EQ(u64, Pointer("/foo/uint64").GetWithDefault(d, u64).GetUint64());
    EXPECT_EQ(u64, Pointer("/foo/uint64").GetWithDefault(d, u64 - 1).GetUint64());

    EXPECT_TRUE(Pointer("/foo/true").GetWithDefault(d, true).IsTrue());
    EXPECT_TRUE(Pointer("/foo/true").GetWithDefault(d, false).IsTrue());

    EXPECT_TRUE(Pointer("/foo/false").GetWithDefault(d, false).IsFalse());
    EXPECT_TRUE(Pointer("/foo/false").GetWithDefault(d, true).IsFalse());

    // StringRef version
    EXPECT_STREQ("Hello", Pointer("/foo/hello").GetWithDefault(d, "Hello").GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        EXPECT_STREQ("World", Pointer("/foo/world").GetWithDefault(d, buffer).GetString());
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("C++", Pointer("/foo/C++").GetWithDefault(d, std::string("C++")).GetString());
#endif
}

TEST(Pointer, Set) {
    Document d;
    d.Parse(kJson);
    Document::AllocatorType& a = d.GetAllocator();
    
    // Value version
    Pointer("/foo/0").Set(d, Value(123).Move(), a);
    EXPECT_EQ(123, d["foo"][0].GetInt());

    Pointer("/foo/-").Set(d, Value(456).Move(), a);
    EXPECT_EQ(456, d["foo"][2].GetInt());

    Pointer("/foo/null").Set(d, Value().Move(), a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/null")->IsNull());

    // Const Value version
    const Value foo(d["foo"], a);
    Pointer("/clone").Set(d, foo, a);
    EXPECT_EQ(foo, *GetValueByPointer(d, "/clone"));

    // Generic version
    Pointer("/foo/int").Set(d, -1, a);
    EXPECT_EQ(-1, GetValueByPointer(d, "/foo/int")->GetInt());

    Pointer("/foo/uint").Set(d, 0x87654321, a);
    EXPECT_EQ(0x87654321, GetValueByPointer(d, "/foo/uint")->GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    Pointer("/foo/int64").Set(d, i64, a);
    EXPECT_EQ(i64, GetValueByPointer(d, "/foo/int64")->GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    Pointer("/foo/uint64").Set(d, u64, a);
    EXPECT_EQ(u64, GetValueByPointer(d, "/foo/uint64")->GetUint64());

    Pointer("/foo/true").Set(d, true, a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/true")->IsTrue());

    Pointer("/foo/false").Set(d, false, a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/false")->IsFalse());

    // StringRef version
    Pointer("/foo/hello").Set(d, "Hello", a);
    EXPECT_STREQ("Hello", GetValueByPointer(d, "/foo/hello")->GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        Pointer("/foo/world").Set(d, buffer, a);
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    Pointer("/foo/c++").Set(d, std::string("C++"), a);
    EXPECT_STREQ("C++", GetValueByPointer(d, "/foo/c++")->GetString());
#endif
}

TEST(Pointer, Set_NoAllocator) {
    Document d;
    d.Parse(kJson);
    
    // Value version
    Pointer("/foo/0").Set(d, Value(123).Move());
    EXPECT_EQ(123, d["foo"][0].GetInt());

    Pointer("/foo/-").Set(d, Value(456).Move());
    EXPECT_EQ(456, d["foo"][2].GetInt());

    Pointer("/foo/null").Set(d, Value().Move());
    EXPECT_TRUE(GetValueByPointer(d, "/foo/null")->IsNull());

    // Const Value version
    const Value foo(d["foo"], d.GetAllocator());
    Pointer("/clone").Set(d, foo);
    EXPECT_EQ(foo, *GetValueByPointer(d, "/clone"));

    // Generic version
    Pointer("/foo/int").Set(d, -1);
    EXPECT_EQ(-1, GetValueByPointer(d, "/foo/int")->GetInt());

    Pointer("/foo/uint").Set(d, 0x87654321);
    EXPECT_EQ(0x87654321, GetValueByPointer(d, "/foo/uint")->GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    Pointer("/foo/int64").Set(d, i64);
    EXPECT_EQ(i64, GetValueByPointer(d, "/foo/int64")->GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    Pointer("/foo/uint64").Set(d, u64);
    EXPECT_EQ(u64, GetValueByPointer(d, "/foo/uint64")->GetUint64());

    Pointer("/foo/true").Set(d, true);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/true")->IsTrue());

    Pointer("/foo/false").Set(d, false);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/false")->IsFalse());

    // StringRef version
    Pointer("/foo/hello").Set(d, "Hello");
    EXPECT_STREQ("Hello", GetValueByPointer(d, "/foo/hello")->GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        Pointer("/foo/world").Set(d, buffer);
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    Pointer("/foo/c++").Set(d, std::string("C++"));
    EXPECT_STREQ("C++", GetValueByPointer(d, "/foo/c++")->GetString());
#endif
}

TEST(Pointer, Swap_Value) {
    Document d;
    d.Parse(kJson);
    Document::AllocatorType& a = d.GetAllocator();
    Pointer("/foo/0").Swap(d, *Pointer("/foo/1").Get(d), a);
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_STREQ("bar", d["foo"][1].GetString());
}

TEST(Pointer, Swap_Value_NoAllocator) {
    Document d;
    d.Parse(kJson);
    Pointer("/foo/0").Swap(d, *Pointer("/foo/1").Get(d));
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_STREQ("bar", d["foo"][1].GetString());
}

TEST(Pointer, Erase) {
    Document d;
    d.Parse(kJson);

    EXPECT_FALSE(Pointer("").Erase(d));
    EXPECT_FALSE(Pointer("/nonexist").Erase(d));
    EXPECT_FALSE(Pointer("/nonexist/nonexist").Erase(d));
    EXPECT_FALSE(Pointer("/foo/nonexist").Erase(d));
    EXPECT_FALSE(Pointer("/foo/nonexist/nonexist").Erase(d));
    EXPECT_FALSE(Pointer("/foo/0/nonexist").Erase(d));
    EXPECT_FALSE(Pointer("/foo/0/nonexist/nonexist").Erase(d));
    EXPECT_FALSE(Pointer("/foo/2/nonexist").Erase(d));
    EXPECT_TRUE(Pointer("/foo/0").Erase(d));
    EXPECT_EQ(1u, d["foo"].Size());
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_TRUE(Pointer("/foo/0").Erase(d));
    EXPECT_TRUE(d["foo"].Empty());
    EXPECT_TRUE(Pointer("/foo").Erase(d));
    EXPECT_TRUE(Pointer("/foo").Get(d) == 0);

    Pointer("/a/0/b/0").Create(d);

    EXPECT_TRUE(Pointer("/a/0/b/0").Get(d) != 0);
    EXPECT_TRUE(Pointer("/a/0/b/0").Erase(d));
    EXPECT_TRUE(Pointer("/a/0/b/0").Get(d) == 0);

    EXPECT_TRUE(Pointer("/a/0/b").Get(d) != 0);
    EXPECT_TRUE(Pointer("/a/0/b").Erase(d));
    EXPECT_TRUE(Pointer("/a/0/b").Get(d) == 0);

    EXPECT_TRUE(Pointer("/a/0").Get(d) != 0);
    EXPECT_TRUE(Pointer("/a/0").Erase(d));
    EXPECT_TRUE(Pointer("/a/0").Get(d) == 0);

    EXPECT_TRUE(Pointer("/a").Get(d) != 0);
    EXPECT_TRUE(Pointer("/a").Erase(d));
    EXPECT_TRUE(Pointer("/a").Get(d) == 0);
}

TEST(Pointer, CreateValueByPointer) {
    Document d;
    Document::AllocatorType& a = d.GetAllocator();

    {
        Value& v = CreateValueByPointer(d, Pointer("/foo/0"), a);
        EXPECT_EQ(&d["foo"][0], &v);
    }
    {
        Value& v = CreateValueByPointer(d, "/foo/1", a);
        EXPECT_EQ(&d["foo"][1], &v);
    }
}

TEST(Pointer, CreateValueByPointer_NoAllocator) {
    Document d;

    {
        Value& v = CreateValueByPointer(d, Pointer("/foo/0"));
        EXPECT_EQ(&d["foo"][0], &v);
    }
    {
        Value& v = CreateValueByPointer(d, "/foo/1");
        EXPECT_EQ(&d["foo"][1], &v);
    }
}

TEST(Pointer, GetValueByPointer) {
    Document d;
    d.Parse(kJson);

    EXPECT_EQ(&d["foo"][0], GetValueByPointer(d, Pointer("/foo/0")));
    EXPECT_EQ(&d["foo"][0], GetValueByPointer(d, "/foo/0"));

    size_t unresolvedTokenIndex;
    EXPECT_TRUE(GetValueByPointer(d, "/foo/2", &unresolvedTokenIndex) == 0); // Out of boundary
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/a", &unresolvedTokenIndex) == 0); // "/foo" is an array, cannot query by "a"
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/0/0", &unresolvedTokenIndex) == 0); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/0/a", &unresolvedTokenIndex) == 0); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);

    // const version
    const Value& v = d;
    EXPECT_EQ(&d["foo"][0], GetValueByPointer(v, Pointer("/foo/0")));
    EXPECT_EQ(&d["foo"][0], GetValueByPointer(v, "/foo/0"));

    EXPECT_TRUE(GetValueByPointer(v, "/foo/2", &unresolvedTokenIndex) == 0); // Out of boundary
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(GetValueByPointer(v, "/foo/a", &unresolvedTokenIndex) == 0); // "/foo" is an array, cannot query by "a"
    EXPECT_EQ(1u, unresolvedTokenIndex);
    EXPECT_TRUE(GetValueByPointer(v, "/foo/0/0", &unresolvedTokenIndex) == 0); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);
    EXPECT_TRUE(GetValueByPointer(v, "/foo/0/a", &unresolvedTokenIndex) == 0); // "/foo/0" is an string, cannot further query
    EXPECT_EQ(2u, unresolvedTokenIndex);

}

TEST(Pointer, GetValueByPointerWithDefault_Pointer) {
    Document d;
    d.Parse(kJson);

    Document::AllocatorType& a = d.GetAllocator();
    const Value v("qux");
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, Pointer("/foo/0"), v, a));
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, Pointer("/foo/0"), v, a));
    EXPECT_TRUE(Value("baz") == GetValueByPointerWithDefault(d, Pointer("/foo/1"), v, a));
    EXPECT_TRUE(Value("qux") == GetValueByPointerWithDefault(d, Pointer("/foo/2"), v, a));
    EXPECT_TRUE(Value("last") == GetValueByPointerWithDefault(d, Pointer("/foo/-"), Value("last").Move(), a));
    EXPECT_STREQ("last", d["foo"][3].GetString());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/null"), Value().Move(), a).IsNull());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/null"), "x", a).IsNull());

    // Generic version
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, Pointer("/foo/int"), -1, a).GetInt());
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, Pointer("/foo/int"), -2, a).GetInt());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, Pointer("/foo/uint"), 0x87654321, a).GetUint());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, Pointer("/foo/uint"), 0x12345678, a).GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, Pointer("/foo/int64"), i64, a).GetInt64());
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, Pointer("/foo/int64"), i64 + 1, a).GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, Pointer("/foo/uint64"), u64, a).GetUint64());
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, Pointer("/foo/uint64"), u64 - 1, a).GetUint64());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/true"), true, a).IsTrue());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/true"), false, a).IsTrue());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/false"), false, a).IsFalse());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/false"), true, a).IsFalse());

    // StringRef version
    EXPECT_STREQ("Hello", GetValueByPointerWithDefault(d, Pointer("/foo/hello"), "Hello", a).GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        EXPECT_STREQ("World", GetValueByPointerWithDefault(d, Pointer("/foo/world"), buffer, a).GetString());
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, Pointer("/foo/world"))->GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("C++", GetValueByPointerWithDefault(d, Pointer("/foo/C++"), std::string("C++"), a).GetString());
#endif
}

TEST(Pointer, GetValueByPointerWithDefault_String) {
    Document d;
    d.Parse(kJson);

    Document::AllocatorType& a = d.GetAllocator();
    const Value v("qux");
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, "/foo/0", v, a));
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, "/foo/0", v, a));
    EXPECT_TRUE(Value("baz") == GetValueByPointerWithDefault(d, "/foo/1", v, a));
    EXPECT_TRUE(Value("qux") == GetValueByPointerWithDefault(d, "/foo/2", v, a));
    EXPECT_TRUE(Value("last") == GetValueByPointerWithDefault(d, "/foo/-", Value("last").Move(), a));
    EXPECT_STREQ("last", d["foo"][3].GetString());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/null", Value().Move(), a).IsNull());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/null", "x", a).IsNull());

    // Generic version
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, "/foo/int", -1, a).GetInt());
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, "/foo/int", -2, a).GetInt());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, "/foo/uint", 0x87654321, a).GetUint());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, "/foo/uint", 0x12345678, a).GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, "/foo/int64", i64, a).GetInt64());
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, "/foo/int64", i64 + 1, a).GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, "/foo/uint64", u64, a).GetUint64());
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, "/foo/uint64", u64 - 1, a).GetUint64());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/true", true, a).IsTrue());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/true", false, a).IsTrue());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/false", false, a).IsFalse());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/false", true, a).IsFalse());

    // StringRef version
    EXPECT_STREQ("Hello", GetValueByPointerWithDefault(d, "/foo/hello", "Hello", a).GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        EXPECT_STREQ("World", GetValueByPointerWithDefault(d, "/foo/world", buffer, a).GetString());
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("C++", GetValueByPointerWithDefault(d, "/foo/C++", std::string("C++"), a).GetString());
#endif
}

TEST(Pointer, GetValueByPointerWithDefault_Pointer_NoAllocator) {
    Document d;
    d.Parse(kJson);

    const Value v("qux");
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, Pointer("/foo/0"), v));
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, Pointer("/foo/0"), v));
    EXPECT_TRUE(Value("baz") == GetValueByPointerWithDefault(d, Pointer("/foo/1"), v));
    EXPECT_TRUE(Value("qux") == GetValueByPointerWithDefault(d, Pointer("/foo/2"), v));
    EXPECT_TRUE(Value("last") == GetValueByPointerWithDefault(d, Pointer("/foo/-"), Value("last").Move()));
    EXPECT_STREQ("last", d["foo"][3].GetString());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/null"), Value().Move()).IsNull());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/null"), "x").IsNull());

    // Generic version
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, Pointer("/foo/int"), -1).GetInt());
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, Pointer("/foo/int"), -2).GetInt());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, Pointer("/foo/uint"), 0x87654321).GetUint());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, Pointer("/foo/uint"), 0x12345678).GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, Pointer("/foo/int64"), i64).GetInt64());
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, Pointer("/foo/int64"), i64 + 1).GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, Pointer("/foo/uint64"), u64).GetUint64());
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, Pointer("/foo/uint64"), u64 - 1).GetUint64());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/true"), true).IsTrue());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/true"), false).IsTrue());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/false"), false).IsFalse());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, Pointer("/foo/false"), true).IsFalse());

    // StringRef version
    EXPECT_STREQ("Hello", GetValueByPointerWithDefault(d, Pointer("/foo/hello"), "Hello").GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        EXPECT_STREQ("World", GetValueByPointerWithDefault(d, Pointer("/foo/world"), buffer).GetString());
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, Pointer("/foo/world"))->GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("C++", GetValueByPointerWithDefault(d, Pointer("/foo/C++"), std::string("C++")).GetString());
#endif
}

TEST(Pointer, GetValueByPointerWithDefault_String_NoAllocator) {
    Document d;
    d.Parse(kJson);

    const Value v("qux");
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, "/foo/0", v));
    EXPECT_TRUE(Value("bar") == GetValueByPointerWithDefault(d, "/foo/0", v));
    EXPECT_TRUE(Value("baz") == GetValueByPointerWithDefault(d, "/foo/1", v));
    EXPECT_TRUE(Value("qux") == GetValueByPointerWithDefault(d, "/foo/2", v));
    EXPECT_TRUE(Value("last") == GetValueByPointerWithDefault(d, "/foo/-", Value("last").Move()));
    EXPECT_STREQ("last", d["foo"][3].GetString());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/null", Value().Move()).IsNull());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/null", "x").IsNull());

    // Generic version
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, "/foo/int", -1).GetInt());
    EXPECT_EQ(-1, GetValueByPointerWithDefault(d, "/foo/int", -2).GetInt());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, "/foo/uint", 0x87654321).GetUint());
    EXPECT_EQ(0x87654321, GetValueByPointerWithDefault(d, "/foo/uint", 0x12345678).GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, "/foo/int64", i64).GetInt64());
    EXPECT_EQ(i64, GetValueByPointerWithDefault(d, "/foo/int64", i64 + 1).GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, "/foo/uint64", u64).GetUint64());
    EXPECT_EQ(u64, GetValueByPointerWithDefault(d, "/foo/uint64", u64 - 1).GetUint64());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/true", true).IsTrue());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/true", false).IsTrue());

    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/false", false).IsFalse());
    EXPECT_TRUE(GetValueByPointerWithDefault(d, "/foo/false", true).IsFalse());

    // StringRef version
    EXPECT_STREQ("Hello", GetValueByPointerWithDefault(d, "/foo/hello", "Hello").GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        EXPECT_STREQ("World", GetValueByPointerWithDefault(d, "/foo/world", buffer).GetString());
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("C++", GetValueByPointerWithDefault(d, Pointer("/foo/C++"), std::string("C++")).GetString());
#endif
}

TEST(Pointer, SetValueByPointer_Pointer) {
    Document d;
    d.Parse(kJson);
    Document::AllocatorType& a = d.GetAllocator();

    // Value version
    SetValueByPointer(d, Pointer("/foo/0"), Value(123).Move(), a);
    EXPECT_EQ(123, d["foo"][0].GetInt());

    SetValueByPointer(d, Pointer("/foo/null"), Value().Move(), a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/null")->IsNull());

    // Const Value version
    const Value foo(d["foo"], d.GetAllocator());
    SetValueByPointer(d, Pointer("/clone"), foo, a);
    EXPECT_EQ(foo, *GetValueByPointer(d, "/clone"));

    // Generic version
    SetValueByPointer(d, Pointer("/foo/int"), -1, a);
    EXPECT_EQ(-1, GetValueByPointer(d, "/foo/int")->GetInt());

    SetValueByPointer(d, Pointer("/foo/uint"), 0x87654321, a);
    EXPECT_EQ(0x87654321, GetValueByPointer(d, "/foo/uint")->GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    SetValueByPointer(d, Pointer("/foo/int64"), i64, a);
    EXPECT_EQ(i64, GetValueByPointer(d, "/foo/int64")->GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    SetValueByPointer(d, Pointer("/foo/uint64"), u64, a);
    EXPECT_EQ(u64, GetValueByPointer(d, "/foo/uint64")->GetUint64());

    SetValueByPointer(d, Pointer("/foo/true"), true, a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/true")->IsTrue());

    SetValueByPointer(d, Pointer("/foo/false"), false, a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/false")->IsFalse());

    // StringRef version
    SetValueByPointer(d, Pointer("/foo/hello"), "Hello", a);
    EXPECT_STREQ("Hello", GetValueByPointer(d, "/foo/hello")->GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        SetValueByPointer(d, Pointer("/foo/world"), buffer, a);
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    SetValueByPointer(d, Pointer("/foo/c++"), std::string("C++"), a);
    EXPECT_STREQ("C++", GetValueByPointer(d, "/foo/c++")->GetString());
#endif
}

TEST(Pointer, SetValueByPointer_String) {
    Document d;
    d.Parse(kJson);
    Document::AllocatorType& a = d.GetAllocator();

    // Value version
    SetValueByPointer(d, "/foo/0", Value(123).Move(), a);
    EXPECT_EQ(123, d["foo"][0].GetInt());

    SetValueByPointer(d, "/foo/null", Value().Move(), a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/null")->IsNull());

    // Const Value version
    const Value foo(d["foo"], d.GetAllocator());
    SetValueByPointer(d, "/clone", foo, a);
    EXPECT_EQ(foo, *GetValueByPointer(d, "/clone"));

    // Generic version
    SetValueByPointer(d, "/foo/int", -1, a);
    EXPECT_EQ(-1, GetValueByPointer(d, "/foo/int")->GetInt());

    SetValueByPointer(d, "/foo/uint", 0x87654321, a);
    EXPECT_EQ(0x87654321, GetValueByPointer(d, "/foo/uint")->GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    SetValueByPointer(d, "/foo/int64", i64, a);
    EXPECT_EQ(i64, GetValueByPointer(d, "/foo/int64")->GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    SetValueByPointer(d, "/foo/uint64", u64, a);
    EXPECT_EQ(u64, GetValueByPointer(d, "/foo/uint64")->GetUint64());

    SetValueByPointer(d, "/foo/true", true, a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/true")->IsTrue());

    SetValueByPointer(d, "/foo/false", false, a);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/false")->IsFalse());

    // StringRef version
    SetValueByPointer(d, "/foo/hello", "Hello", a);
    EXPECT_STREQ("Hello", GetValueByPointer(d, "/foo/hello")->GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        SetValueByPointer(d, "/foo/world", buffer, a);
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    SetValueByPointer(d, "/foo/c++", std::string("C++"), a);
    EXPECT_STREQ("C++", GetValueByPointer(d, "/foo/c++")->GetString());
#endif
}

TEST(Pointer, SetValueByPointer_Pointer_NoAllocator) {
    Document d;
    d.Parse(kJson);

    // Value version
    SetValueByPointer(d, Pointer("/foo/0"), Value(123).Move());
    EXPECT_EQ(123, d["foo"][0].GetInt());

    SetValueByPointer(d, Pointer("/foo/null"), Value().Move());
    EXPECT_TRUE(GetValueByPointer(d, "/foo/null")->IsNull());

    // Const Value version
    const Value foo(d["foo"], d.GetAllocator());
    SetValueByPointer(d, Pointer("/clone"), foo);
    EXPECT_EQ(foo, *GetValueByPointer(d, "/clone"));

    // Generic version
    SetValueByPointer(d, Pointer("/foo/int"), -1);
    EXPECT_EQ(-1, GetValueByPointer(d, "/foo/int")->GetInt());

    SetValueByPointer(d, Pointer("/foo/uint"), 0x87654321);
    EXPECT_EQ(0x87654321, GetValueByPointer(d, "/foo/uint")->GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    SetValueByPointer(d, Pointer("/foo/int64"), i64);
    EXPECT_EQ(i64, GetValueByPointer(d, "/foo/int64")->GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    SetValueByPointer(d, Pointer("/foo/uint64"), u64);
    EXPECT_EQ(u64, GetValueByPointer(d, "/foo/uint64")->GetUint64());

    SetValueByPointer(d, Pointer("/foo/true"), true);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/true")->IsTrue());

    SetValueByPointer(d, Pointer("/foo/false"), false);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/false")->IsFalse());

    // StringRef version
    SetValueByPointer(d, Pointer("/foo/hello"), "Hello");
    EXPECT_STREQ("Hello", GetValueByPointer(d, "/foo/hello")->GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        SetValueByPointer(d, Pointer("/foo/world"), buffer);
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    SetValueByPointer(d, Pointer("/foo/c++"), std::string("C++"));
    EXPECT_STREQ("C++", GetValueByPointer(d, "/foo/c++")->GetString());
#endif
}

TEST(Pointer, SetValueByPointer_String_NoAllocator) {
    Document d;
    d.Parse(kJson);

    // Value version
    SetValueByPointer(d, "/foo/0", Value(123).Move());
    EXPECT_EQ(123, d["foo"][0].GetInt());

    SetValueByPointer(d, "/foo/null", Value().Move());
    EXPECT_TRUE(GetValueByPointer(d, "/foo/null")->IsNull());

    // Const Value version
    const Value foo(d["foo"], d.GetAllocator());
    SetValueByPointer(d, "/clone", foo);
    EXPECT_EQ(foo, *GetValueByPointer(d, "/clone"));

    // Generic version
    SetValueByPointer(d, "/foo/int", -1);
    EXPECT_EQ(-1, GetValueByPointer(d, "/foo/int")->GetInt());

    SetValueByPointer(d, "/foo/uint", 0x87654321);
    EXPECT_EQ(0x87654321, GetValueByPointer(d, "/foo/uint")->GetUint());

    const int64_t i64 = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 0));
    SetValueByPointer(d, "/foo/int64", i64);
    EXPECT_EQ(i64, GetValueByPointer(d, "/foo/int64")->GetInt64());

    const uint64_t u64 = RAPIDJSON_UINT64_C2(0xFFFFFFFFF, 0xFFFFFFFFF);
    SetValueByPointer(d, "/foo/uint64", u64);
    EXPECT_EQ(u64, GetValueByPointer(d, "/foo/uint64")->GetUint64());

    SetValueByPointer(d, "/foo/true", true);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/true")->IsTrue());

    SetValueByPointer(d, "/foo/false", false);
    EXPECT_TRUE(GetValueByPointer(d, "/foo/false")->IsFalse());

    // StringRef version
    SetValueByPointer(d, "/foo/hello", "Hello");
    EXPECT_STREQ("Hello", GetValueByPointer(d, "/foo/hello")->GetString());

    // Copy string version
    {
        char buffer[256];
        strcpy(buffer, "World");
        SetValueByPointer(d, "/foo/world", buffer);
        memset(buffer, 0, sizeof(buffer));
    }
    EXPECT_STREQ("World", GetValueByPointer(d, "/foo/world")->GetString());

#if RAPIDJSON_HAS_STDSTRING
    SetValueByPointer(d, "/foo/c++", std::string("C++"));
    EXPECT_STREQ("C++", GetValueByPointer(d, "/foo/c++")->GetString());
#endif
}

TEST(Pointer, SwapValueByPointer) {
    Document d;
    d.Parse(kJson);
    Document::AllocatorType& a = d.GetAllocator();
    SwapValueByPointer(d, Pointer("/foo/0"), *GetValueByPointer(d, "/foo/1"), a);
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_STREQ("bar", d["foo"][1].GetString());

    SwapValueByPointer(d, "/foo/0", *GetValueByPointer(d, "/foo/1"), a);
    EXPECT_STREQ("bar", d["foo"][0].GetString());
    EXPECT_STREQ("baz", d["foo"][1].GetString());
}

TEST(Pointer, SwapValueByPointer_NoAllocator) {
    Document d;
    d.Parse(kJson);
    SwapValueByPointer(d, Pointer("/foo/0"), *GetValueByPointer(d, "/foo/1"));
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_STREQ("bar", d["foo"][1].GetString());

    SwapValueByPointer(d, "/foo/0", *GetValueByPointer(d, "/foo/1"));
    EXPECT_STREQ("bar", d["foo"][0].GetString());
    EXPECT_STREQ("baz", d["foo"][1].GetString());
}

TEST(Pointer, EraseValueByPointer_Pointer) {
    Document d;
    d.Parse(kJson);

    EXPECT_FALSE(EraseValueByPointer(d, Pointer("")));
    EXPECT_FALSE(Pointer("/foo/nonexist").Erase(d));
    EXPECT_TRUE(EraseValueByPointer(d, Pointer("/foo/0")));
    EXPECT_EQ(1u, d["foo"].Size());
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_TRUE(EraseValueByPointer(d, Pointer("/foo/0")));
    EXPECT_TRUE(d["foo"].Empty());
    EXPECT_TRUE(EraseValueByPointer(d, Pointer("/foo")));
    EXPECT_TRUE(Pointer("/foo").Get(d) == 0);
}

TEST(Pointer, EraseValueByPointer_String) {
    Document d;
    d.Parse(kJson);

    EXPECT_FALSE(EraseValueByPointer(d, ""));
    EXPECT_FALSE(Pointer("/foo/nonexist").Erase(d));
    EXPECT_TRUE(EraseValueByPointer(d, "/foo/0"));
    EXPECT_EQ(1u, d["foo"].Size());
    EXPECT_STREQ("baz", d["foo"][0].GetString());
    EXPECT_TRUE(EraseValueByPointer(d, "/foo/0"));
    EXPECT_TRUE(d["foo"].Empty());
    EXPECT_TRUE(EraseValueByPointer(d, "/foo"));
    EXPECT_TRUE(Pointer("/foo").Get(d) == 0);
}

TEST(Pointer, Ambiguity) {
    {
        Document d;
        d.Parse("{\"0\" : [123]}");
        EXPECT_EQ(123, Pointer("/0/0").Get(d)->GetInt());
        Pointer("/0/a").Set(d, 456);    // Change array [123] to object {456}
        EXPECT_EQ(456, Pointer("/0/a").Get(d)->GetInt());
    }

    {
        Document d;
        EXPECT_FALSE(d.Parse("[{\"0\": 123}]").HasParseError());
        EXPECT_EQ(123, Pointer("/0/0").Get(d)->GetInt());
        Pointer("/0/1").Set(d, 456); // 1 is treated as "1" to index object
        EXPECT_EQ(123, Pointer("/0/0").Get(d)->GetInt());
        EXPECT_EQ(456, Pointer("/0/1").Get(d)->GetInt());
    }
}

TEST(Pointer, ResolveOnObject) {
    Document d;
    EXPECT_FALSE(d.Parse("{\"a\": 123}").HasParseError());

    {
        Value::ConstObject o = static_cast<const Document&>(d).GetObject();
        EXPECT_EQ(123, Pointer("/a").Get(o)->GetInt());
    }

    {
        Value::Object o = d.GetObject();
        Pointer("/a").Set(o, 456, d.GetAllocator());
        EXPECT_EQ(456, Pointer("/a").Get(o)->GetInt());
    }
}

TEST(Pointer, ResolveOnArray) {
    Document d;
    EXPECT_FALSE(d.Parse("[1, 2, 3]").HasParseError());

    {
        Value::ConstArray a = static_cast<const Document&>(d).GetArray();
        EXPECT_EQ(2, Pointer("/1").Get(a)->GetInt());
    }

    {
        Value::Array a = d.GetArray();
        Pointer("/1").Set(a, 123, d.GetAllocator());
        EXPECT_EQ(123, Pointer("/1").Get(a)->GetInt());
    }
}

TEST(Pointer, LessThan) {
    static const struct {
        const char *str;
        bool valid;
    } pointers[] = {
        { "/a/b",       true },
        { "/a",         true },
        { "/d/1",       true },
        { "/d/2/z",     true },
        { "/d/2/3",     true },
        { "/d/2",       true },
        { "/a/c",       true },
        { "/e/f~g",     false },
        { "/d/2/zz",    true },
        { "/d/1",       true },
        { "/d/2/z",     true },
        { "/e/f~~g",    false },
        { "/e/f~0g",    true },
        { "/e/f~1g",    true },
        { "/e/f.g",     true },
        { "",           true }
    };
    static const char *ordered_pointers[] = {
        "",
        "/a",
        "/a/b",
        "/a/c",
        "/d/1",
        "/d/1",
        "/d/2",
        "/e/f.g",
        "/e/f~1g",
        "/e/f~0g",
        "/d/2/3",
        "/d/2/z",
        "/d/2/z",
        "/d/2/zz",
        NULL,       // was invalid "/e/f~g"
        NULL        // was invalid "/e/f~~g"
    };
    typedef MemoryPoolAllocator<> AllocatorType;
    typedef GenericPointer<Value, AllocatorType> PointerType;
    typedef std::multimap<PointerType, size_t> PointerMap;
    PointerMap map;
    PointerMap::iterator it;
    AllocatorType allocator;
    size_t i;

    EXPECT_EQ(sizeof(pointers) / sizeof(pointers[0]),
              sizeof(ordered_pointers) / sizeof(ordered_pointers[0]));

    for (i = 0; i < sizeof(pointers) / sizeof(pointers[0]); ++i) {
        it = map.insert(PointerMap::value_type(PointerType(pointers[i].str, &allocator), i));
        if (!it->first.IsValid()) {
            EXPECT_EQ(++it, map.end());
        }
    }

    for (i = 0, it = map.begin(); it != map.end(); ++it, ++i) {
        EXPECT_TRUE(it->second < sizeof(pointers) / sizeof(pointers[0]));
        EXPECT_EQ(it->first.IsValid(), pointers[it->second].valid);
        EXPECT_TRUE(i < sizeof(ordered_pointers) / sizeof(ordered_pointers[0]));
        EXPECT_EQ(it->first.IsValid(), !!ordered_pointers[i]);
        if (it->first.IsValid()) {
            std::stringstream ss;
            OStreamWrapper os(ss);
            EXPECT_TRUE(it->first.Stringify(os));
            EXPECT_EQ(ss.str(), pointers[it->second].str);
            EXPECT_EQ(ss.str(), ordered_pointers[i]);
        }
    }
}

// https://github.com/Tencent/rapidjson/issues/483
namespace myjson {

class MyAllocator
{
public:
    static const bool kNeedFree = true;
    void * Malloc(size_t _size) { return malloc(_size); }
    void * Realloc(void *_org_p, size_t _org_size, size_t _new_size) { (void)_org_size; return realloc(_org_p, _new_size); }
    static void Free(void *_p) { return free(_p); }
};

typedef rapidjson::GenericDocument<
            rapidjson::UTF8<>,
            rapidjson::MemoryPoolAllocator< MyAllocator >,
            MyAllocator
        > Document;

typedef rapidjson::GenericPointer<
            ::myjson::Document::ValueType,
            MyAllocator
        > Pointer;

typedef ::myjson::Document::ValueType Value;

}

TEST(Pointer, Issue483) {
    std::string mystr, path;
    myjson::Document document;
    myjson::Value value(rapidjson::kStringType);
    value.SetString(mystr.c_str(), static_cast<SizeType>(mystr.length()), document.GetAllocator());
    myjson::Pointer(path.c_str()).Set(document, value, document.GetAllocator());
}

TEST(Pointer, Issue1899) {
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    PointerType p;
    PointerType q = p.Append("foo");
    EXPECT_TRUE(PointerType("/foo") == q);
    q = q.Append(1234);
    EXPECT_TRUE(PointerType("/foo/1234") == q);
    q = q.Append("");
    EXPECT_TRUE(PointerType("/foo/1234/") == q);
}