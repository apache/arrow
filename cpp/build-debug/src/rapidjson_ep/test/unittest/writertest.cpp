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
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/memorybuffer.h"

#ifdef __clang__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(c++98-compat)
#endif

using namespace rapidjson;

TEST(Writer, Compact) {
    StringStream s("{ \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3] } ");
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    buffer.ShrinkToFit();
    Reader reader;
    reader.Parse<0>(s, writer);
    EXPECT_STREQ("{\"hello\":\"world\",\"t\":true,\"f\":false,\"n\":null,\"i\":123,\"pi\":3.1416,\"a\":[1,2,3]}", buffer.GetString());
    EXPECT_EQ(77u, buffer.GetSize());
    EXPECT_TRUE(writer.IsComplete());
}

// json -> parse -> writer -> json
#define TEST_ROUNDTRIP(json) \
    { \
        StringStream s(json); \
        StringBuffer buffer; \
        Writer<StringBuffer> writer(buffer); \
        Reader reader; \
        reader.Parse<kParseFullPrecisionFlag>(s, writer); \
        EXPECT_STREQ(json, buffer.GetString()); \
        EXPECT_TRUE(writer.IsComplete()); \
    }

TEST(Writer, Root) {
    TEST_ROUNDTRIP("null");
    TEST_ROUNDTRIP("true");
    TEST_ROUNDTRIP("false");
    TEST_ROUNDTRIP("0");
    TEST_ROUNDTRIP("\"foo\"");
    TEST_ROUNDTRIP("[]");
    TEST_ROUNDTRIP("{}");
}

TEST(Writer, Int) {
    TEST_ROUNDTRIP("[-1]");
    TEST_ROUNDTRIP("[-123]");
    TEST_ROUNDTRIP("[-2147483648]");
}

TEST(Writer, UInt) {
    TEST_ROUNDTRIP("[0]");
    TEST_ROUNDTRIP("[1]");
    TEST_ROUNDTRIP("[123]");
    TEST_ROUNDTRIP("[2147483647]");
    TEST_ROUNDTRIP("[4294967295]");
}

TEST(Writer, Int64) {
    TEST_ROUNDTRIP("[-1234567890123456789]");
    TEST_ROUNDTRIP("[-9223372036854775808]");
}

TEST(Writer, Uint64) {
    TEST_ROUNDTRIP("[1234567890123456789]");
    TEST_ROUNDTRIP("[9223372036854775807]");
}

TEST(Writer, String) {
    TEST_ROUNDTRIP("[\"Hello\"]");
    TEST_ROUNDTRIP("[\"Hello\\u0000World\"]");
    TEST_ROUNDTRIP("[\"\\\"\\\\/\\b\\f\\n\\r\\t\"]");

#if RAPIDJSON_HAS_STDSTRING
    {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        writer.String(std::string("Hello\n"));
        EXPECT_STREQ("\"Hello\\n\"", buffer.GetString());
    }
#endif
}

TEST(Writer, Issue_889) {
    char buf[100] = "Hello";
    
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartArray();
    writer.String(buf);
    writer.EndArray();
    
    EXPECT_STREQ("[\"Hello\"]", buffer.GetString());
    EXPECT_TRUE(writer.IsComplete()); \
}

TEST(Writer, ScanWriteUnescapedString) {
    const char json[] = "[\" \\\"0123456789ABCDEF\"]";
    //                       ^ scanning stops here.
    char buffer2[sizeof(json) + 32];

    // Use different offset to test different alignments
    for (int i = 0; i < 32; i++) {
        char* p = buffer2 + i;
        memcpy(p, json, sizeof(json));
        TEST_ROUNDTRIP(p);
    }
}

TEST(Writer, Double) {
    TEST_ROUNDTRIP("[1.2345,1.2345678,0.123456789012,1234567.8]");
    TEST_ROUNDTRIP("0.0");
    TEST_ROUNDTRIP("-0.0"); // Issue #289
    TEST_ROUNDTRIP("1e30");
    TEST_ROUNDTRIP("1.0");
    TEST_ROUNDTRIP("5e-324"); // Min subnormal positive double
    TEST_ROUNDTRIP("2.225073858507201e-308"); // Max subnormal positive double
    TEST_ROUNDTRIP("2.2250738585072014e-308"); // Min normal positive double
    TEST_ROUNDTRIP("1.7976931348623157e308"); // Max double

}

// UTF8 -> TargetEncoding -> UTF8
template <typename TargetEncoding>
void TestTranscode(const char* json) {
    StringStream s(json);
    GenericStringBuffer<TargetEncoding> buffer;
    Writer<GenericStringBuffer<TargetEncoding>, UTF8<>, TargetEncoding> writer(buffer);
    Reader reader;
    reader.Parse(s, writer);

    StringBuffer buffer2;
    Writer<StringBuffer> writer2(buffer2);
    GenericReader<TargetEncoding, UTF8<> > reader2;
    GenericStringStream<TargetEncoding> s2(buffer.GetString());
    reader2.Parse(s2, writer2);

    EXPECT_STREQ(json, buffer2.GetString());
}

TEST(Writer, Transcode) {
    const char json[] = "{\"hello\":\"world\",\"t\":true,\"f\":false,\"n\":null,\"i\":123,\"pi\":3.1416,\"a\":[1,2,3],\"dollar\":\"\x24\",\"cents\":\"\xC2\xA2\",\"euro\":\"\xE2\x82\xAC\",\"gclef\":\"\xF0\x9D\x84\x9E\"}";

    // UTF8 -> UTF16 -> UTF8
    TestTranscode<UTF8<> >(json);

    // UTF8 -> ASCII -> UTF8
    TestTranscode<ASCII<> >(json);

    // UTF8 -> UTF16 -> UTF8
    TestTranscode<UTF16<> >(json);

    // UTF8 -> UTF32 -> UTF8
    TestTranscode<UTF32<> >(json);

    // UTF8 -> AutoUTF -> UTF8
    UTFType types[] = { kUTF8, kUTF16LE , kUTF16BE, kUTF32LE , kUTF32BE };
    for (size_t i = 0; i < 5; i++) {
        StringStream s(json);
        MemoryBuffer buffer;
        AutoUTFOutputStream<unsigned, MemoryBuffer> os(buffer, types[i], true);
        Writer<AutoUTFOutputStream<unsigned, MemoryBuffer>, UTF8<>, AutoUTF<unsigned> > writer(os);
        Reader reader;
        reader.Parse(s, writer);

        StringBuffer buffer2;
        Writer<StringBuffer> writer2(buffer2);
        GenericReader<AutoUTF<unsigned>, UTF8<> > reader2;
        MemoryStream s2(buffer.GetBuffer(), buffer.GetSize());
        AutoUTFInputStream<unsigned, MemoryStream> is(s2);
        reader2.Parse(is, writer2);

        EXPECT_STREQ(json, buffer2.GetString());
    }

}

#include <sstream>

class OStreamWrapper {
public:
    typedef char Ch;

    OStreamWrapper(std::ostream& os) : os_(os) {}

    Ch Peek() const { assert(false); return '\0'; }
    Ch Take() { assert(false); return '\0'; }
    size_t Tell() const { return 0; }

    Ch* PutBegin() { assert(false); return 0; }
    void Put(Ch c) { os_.put(c); }
    void Flush() { os_.flush(); }
    size_t PutEnd(Ch*) { assert(false); return 0; }

private:
    OStreamWrapper(const OStreamWrapper&);
    OStreamWrapper& operator=(const OStreamWrapper&);

    std::ostream& os_;
};

TEST(Writer, OStreamWrapper) {
    StringStream s("{ \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3], \"u64\": 1234567890123456789, \"i64\":-1234567890123456789 } ");
    
    std::stringstream ss;
    OStreamWrapper os(ss);
    
    Writer<OStreamWrapper> writer(os);

    Reader reader;
    reader.Parse<0>(s, writer);
    
    std::string actual = ss.str();
    EXPECT_STREQ("{\"hello\":\"world\",\"t\":true,\"f\":false,\"n\":null,\"i\":123,\"pi\":3.1416,\"a\":[1,2,3],\"u64\":1234567890123456789,\"i64\":-1234567890123456789}", actual.c_str());
}

TEST(Writer, AssertRootMayBeAnyValue) {
#define T(x)\
    {\
        StringBuffer buffer;\
        Writer<StringBuffer> writer(buffer);\
        EXPECT_TRUE(x);\
    }
    T(writer.Bool(false));
    T(writer.Bool(true));
    T(writer.Null());
    T(writer.Int(0));
    T(writer.Uint(0));
    T(writer.Int64(0));
    T(writer.Uint64(0));
    T(writer.Double(0));
    T(writer.String("foo"));
#undef T
}

TEST(Writer, AssertIncorrectObjectLevel) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    writer.EndObject();
    ASSERT_THROW(writer.EndObject(), AssertException);
}

TEST(Writer, AssertIncorrectArrayLevel) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartArray();
    writer.EndArray();
    ASSERT_THROW(writer.EndArray(), AssertException);
}

TEST(Writer, AssertIncorrectEndObject) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    ASSERT_THROW(writer.EndArray(), AssertException);
}

TEST(Writer, AssertIncorrectEndArray) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    ASSERT_THROW(writer.EndArray(), AssertException);
}

TEST(Writer, AssertObjectKeyNotString) {
#define T(x)\
    {\
        StringBuffer buffer;\
        Writer<StringBuffer> writer(buffer);\
        writer.StartObject();\
        ASSERT_THROW(x, AssertException); \
    }
    T(writer.Bool(false));
    T(writer.Bool(true));
    T(writer.Null());
    T(writer.Int(0));
    T(writer.Uint(0));
    T(writer.Int64(0));
    T(writer.Uint64(0));
    T(writer.Double(0));
    T(writer.StartObject());
    T(writer.StartArray());
#undef T
}

TEST(Writer, AssertMultipleRoot) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);

    writer.StartObject();
    writer.EndObject();
    ASSERT_THROW(writer.StartObject(), AssertException);

    writer.Reset(buffer);
    writer.Null();
    ASSERT_THROW(writer.Int(0), AssertException);

    writer.Reset(buffer);
    writer.String("foo");
    ASSERT_THROW(writer.StartArray(), AssertException);

    writer.Reset(buffer);
    writer.StartArray();
    writer.EndArray();
    //ASSERT_THROW(writer.Double(3.14), AssertException);
}

TEST(Writer, RootObjectIsComplete) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    EXPECT_FALSE(writer.IsComplete());
    writer.StartObject();
    EXPECT_FALSE(writer.IsComplete());
    writer.String("foo");
    EXPECT_FALSE(writer.IsComplete());
    writer.Int(1);
    EXPECT_FALSE(writer.IsComplete());
    writer.EndObject();
    EXPECT_TRUE(writer.IsComplete());
}

TEST(Writer, RootArrayIsComplete) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    EXPECT_FALSE(writer.IsComplete());
    writer.StartArray();
    EXPECT_FALSE(writer.IsComplete());
    writer.String("foo");
    EXPECT_FALSE(writer.IsComplete());
    writer.Int(1);
    EXPECT_FALSE(writer.IsComplete());
    writer.EndArray();
    EXPECT_TRUE(writer.IsComplete());
}

TEST(Writer, RootValueIsComplete) {
#define T(x)\
    {\
        StringBuffer buffer;\
        Writer<StringBuffer> writer(buffer);\
        EXPECT_FALSE(writer.IsComplete()); \
        x; \
        EXPECT_TRUE(writer.IsComplete()); \
    }
    T(writer.Null());
    T(writer.Bool(true));
    T(writer.Bool(false));
    T(writer.Int(0));
    T(writer.Uint(0));
    T(writer.Int64(0));
    T(writer.Uint64(0));
    T(writer.Double(0));
    T(writer.String(""));
#undef T
}

TEST(Writer, InvalidEncoding) {
    // Fail in decoding invalid UTF-8 sequence http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt
    {
        GenericStringBuffer<UTF16<> > buffer;
        Writer<GenericStringBuffer<UTF16<> >, UTF8<>, UTF16<> > writer(buffer);
        writer.StartArray();
        EXPECT_FALSE(writer.String("\xfe"));
        EXPECT_FALSE(writer.String("\xff"));
        EXPECT_FALSE(writer.String("\xfe\xfe\xff\xff"));
        writer.EndArray();
    }

    // Fail in encoding
    {
        StringBuffer buffer;
        Writer<StringBuffer, UTF32<> > writer(buffer);
        static const UTF32<>::Ch s[] = { 0x110000, 0 }; // Out of U+0000 to U+10FFFF
        EXPECT_FALSE(writer.String(s));
    }

    // Fail in unicode escaping in ASCII output
    {
        StringBuffer buffer;
        Writer<StringBuffer, UTF32<>, ASCII<> > writer(buffer);
        static const UTF32<>::Ch s[] = { 0x110000, 0 }; // Out of U+0000 to U+10FFFF
        EXPECT_FALSE(writer.String(s));
    }
}

TEST(Writer, ValidateEncoding) {
    {
        StringBuffer buffer;
        Writer<StringBuffer, UTF8<>, UTF8<>, CrtAllocator, kWriteValidateEncodingFlag> writer(buffer);
        writer.StartArray();
        EXPECT_TRUE(writer.String("\x24"));             // Dollar sign U+0024
        EXPECT_TRUE(writer.String("\xC2\xA2"));         // Cents sign U+00A2
        EXPECT_TRUE(writer.String("\xE2\x82\xAC"));     // Euro sign U+20AC
        EXPECT_TRUE(writer.String("\xF0\x9D\x84\x9E")); // G clef sign U+1D11E
        EXPECT_TRUE(writer.String("\x01"));             // SOH control U+0001
        EXPECT_TRUE(writer.String("\x1B"));             // Escape control U+001B
        writer.EndArray();
        EXPECT_STREQ("[\"\x24\",\"\xC2\xA2\",\"\xE2\x82\xAC\",\"\xF0\x9D\x84\x9E\",\"\\u0001\",\"\\u001B\"]", buffer.GetString());
    }

    // Fail in decoding invalid UTF-8 sequence http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt
    {
        StringBuffer buffer;
        Writer<StringBuffer, UTF8<>, UTF8<>, CrtAllocator, kWriteValidateEncodingFlag> writer(buffer);
        writer.StartArray();
        EXPECT_FALSE(writer.String("\xfe"));
        EXPECT_FALSE(writer.String("\xff"));
        EXPECT_FALSE(writer.String("\xfe\xfe\xff\xff"));
        writer.EndArray();
    }
}

TEST(Writer, InvalidEventSequence) {
    // {]
    {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        writer.StartObject();
        EXPECT_THROW(writer.EndArray(), AssertException);
        EXPECT_FALSE(writer.IsComplete());
    }

    // [}
    {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        writer.StartArray();
        EXPECT_THROW(writer.EndObject(), AssertException);
        EXPECT_FALSE(writer.IsComplete());
    }

    // { 1: 
    {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        writer.StartObject();
        EXPECT_THROW(writer.Int(1), AssertException);
        EXPECT_FALSE(writer.IsComplete());
    }

    // { 'a' }
    {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        writer.StartObject();
        writer.Key("a");
        EXPECT_THROW(writer.EndObject(), AssertException);
        EXPECT_FALSE(writer.IsComplete());
    }

    // { 'a':'b','c' }
    {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        writer.StartObject();
        writer.Key("a");
        writer.String("b");
        writer.Key("c");
        EXPECT_THROW(writer.EndObject(), AssertException);
        EXPECT_FALSE(writer.IsComplete());
    }
}

TEST(Writer, NaN) {
    double nan = std::numeric_limits<double>::quiet_NaN();

    EXPECT_TRUE(internal::Double(nan).IsNan());
    StringBuffer buffer;
    {
        Writer<StringBuffer> writer(buffer);
        EXPECT_FALSE(writer.Double(nan));
    }
    {
        Writer<StringBuffer, UTF8<>, UTF8<>, CrtAllocator, kWriteNanAndInfFlag> writer(buffer);
        EXPECT_TRUE(writer.Double(nan));
        EXPECT_STREQ("NaN", buffer.GetString());
    }
    GenericStringBuffer<UTF16<> > buffer2;
    Writer<GenericStringBuffer<UTF16<> > > writer2(buffer2);
    EXPECT_FALSE(writer2.Double(nan));
}

TEST(Writer, Inf) {
    double inf = std::numeric_limits<double>::infinity();

    EXPECT_TRUE(internal::Double(inf).IsInf());
    StringBuffer buffer;
    {
        Writer<StringBuffer> writer(buffer);
        EXPECT_FALSE(writer.Double(inf));
    }
    {
        Writer<StringBuffer> writer(buffer);
        EXPECT_FALSE(writer.Double(-inf));
    }
    {
        Writer<StringBuffer, UTF8<>, UTF8<>, CrtAllocator, kWriteNanAndInfFlag> writer(buffer);
        EXPECT_TRUE(writer.Double(inf));
    }
    {
        Writer<StringBuffer, UTF8<>, UTF8<>, CrtAllocator, kWriteNanAndInfFlag> writer(buffer);
        EXPECT_TRUE(writer.Double(-inf));
    }
    EXPECT_STREQ("Infinity-Infinity", buffer.GetString());
}

TEST(Writer, RawValue) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("a");
    writer.Int(1);
    writer.Key("raw");
    const char json[] = "[\"Hello\\nWorld\", 123.456]";
    writer.RawValue(json, strlen(json), kArrayType);
    writer.EndObject();
    EXPECT_TRUE(writer.IsComplete());
    EXPECT_STREQ("{\"a\":1,\"raw\":[\"Hello\\nWorld\", 123.456]}", buffer.GetString());
}

TEST(Write, RawValue_Issue1152) {
    {
        GenericStringBuffer<UTF32<> > sb;
        Writer<GenericStringBuffer<UTF32<> >, UTF8<>, UTF32<> > writer(sb);
        writer.RawValue("null", 4, kNullType);
        EXPECT_TRUE(writer.IsComplete());
        const unsigned *out = sb.GetString();
        EXPECT_EQ(static_cast<unsigned>('n'), out[0]);
        EXPECT_EQ(static_cast<unsigned>('u'), out[1]);
        EXPECT_EQ(static_cast<unsigned>('l'), out[2]);
        EXPECT_EQ(static_cast<unsigned>('l'), out[3]);
        EXPECT_EQ(static_cast<unsigned>(0  ), out[4]);
    }

    {
        GenericStringBuffer<UTF8<> > sb;
        Writer<GenericStringBuffer<UTF8<> >, UTF16<>, UTF8<> > writer(sb);
        writer.RawValue(L"null", 4, kNullType);
        EXPECT_TRUE(writer.IsComplete());
        EXPECT_STREQ("null", sb.GetString());
    }

    {
        // Fail in transcoding
        GenericStringBuffer<UTF16<> > buffer;
        Writer<GenericStringBuffer<UTF16<> >, UTF8<>, UTF16<> > writer(buffer);
        EXPECT_FALSE(writer.RawValue("\"\xfe\"", 3, kStringType));
    }

    {
        // Fail in encoding validation
        StringBuffer buffer;
        Writer<StringBuffer, UTF8<>, UTF8<>, CrtAllocator, kWriteValidateEncodingFlag> writer(buffer);
        EXPECT_FALSE(writer.RawValue("\"\xfe\"", 3, kStringType));
    }
}

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS
static Writer<StringBuffer> WriterGen(StringBuffer &target) {
    Writer<StringBuffer> writer(target);
    writer.StartObject();
    writer.Key("a");
    writer.Int(1);
    return writer;
}

TEST(Writer, MoveCtor) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(WriterGen(buffer));
    writer.EndObject();
    EXPECT_TRUE(writer.IsComplete());
    EXPECT_STREQ("{\"a\":1}", buffer.GetString());
}
#endif

#ifdef __clang__
RAPIDJSON_DIAG_POP
#endif
