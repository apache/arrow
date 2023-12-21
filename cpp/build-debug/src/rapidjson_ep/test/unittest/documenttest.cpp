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
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/encodedstream.h"
#include "rapidjson/stringbuffer.h"
#include <sstream>
#include <algorithm>

#ifdef __clang__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(c++98-compat)
RAPIDJSON_DIAG_OFF(missing-variable-declarations)
#endif

using namespace rapidjson;

template <typename DocumentType>
void ParseCheck(DocumentType& doc) {
    typedef typename DocumentType::ValueType ValueType;

    EXPECT_FALSE(doc.HasParseError());
    if (doc.HasParseError())
        printf("Error: %d at %zu\n", static_cast<int>(doc.GetParseError()), doc.GetErrorOffset());
    EXPECT_TRUE(static_cast<ParseResult>(doc));

    EXPECT_TRUE(doc.IsObject());

    EXPECT_TRUE(doc.HasMember("hello"));
    const ValueType& hello = doc["hello"];
    EXPECT_TRUE(hello.IsString());
    EXPECT_STREQ("world", hello.GetString());

    EXPECT_TRUE(doc.HasMember("t"));
    const ValueType& t = doc["t"];
    EXPECT_TRUE(t.IsTrue());

    EXPECT_TRUE(doc.HasMember("f"));
    const ValueType& f = doc["f"];
    EXPECT_TRUE(f.IsFalse());

    EXPECT_TRUE(doc.HasMember("n"));
    const ValueType& n = doc["n"];
    EXPECT_TRUE(n.IsNull());

    EXPECT_TRUE(doc.HasMember("i"));
    const ValueType& i = doc["i"];
    EXPECT_TRUE(i.IsNumber());
    EXPECT_EQ(123, i.GetInt());

    EXPECT_TRUE(doc.HasMember("pi"));
    const ValueType& pi = doc["pi"];
    EXPECT_TRUE(pi.IsNumber());
    EXPECT_DOUBLE_EQ(3.1416, pi.GetDouble());

    EXPECT_TRUE(doc.HasMember("a"));
    const ValueType& a = doc["a"];
    EXPECT_TRUE(a.IsArray());
    EXPECT_EQ(4u, a.Size());
    for (SizeType j = 0; j < 4; j++)
        EXPECT_EQ(j + 1, a[j].GetUint());
}

template <typename Allocator, typename StackAllocator>
void ParseTest() {
    typedef GenericDocument<UTF8<>, Allocator, StackAllocator> DocumentType;
    DocumentType doc;

    const char* json = " { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, 4] } ";

    doc.Parse(json);
    ParseCheck(doc);

    doc.SetNull();
    StringStream s(json);
    doc.template ParseStream<0>(s);
    ParseCheck(doc);

    doc.SetNull();
    char *buffer = strdup(json);
    doc.ParseInsitu(buffer);
    ParseCheck(doc);
    free(buffer);

    // Parse(const Ch*, size_t)
    size_t length = strlen(json);
    buffer = reinterpret_cast<char*>(malloc(length * 2));
    memcpy(buffer, json, length);
    memset(buffer + length, 'X', length);
#if RAPIDJSON_HAS_STDSTRING
    std::string s2(buffer, length); // backup buffer
#endif
    doc.SetNull();
    doc.Parse(buffer, length);
    free(buffer);
    ParseCheck(doc);

#if RAPIDJSON_HAS_STDSTRING
    // Parse(std::string)
    doc.SetNull();
    doc.Parse(s2);
    ParseCheck(doc);
#endif
}

TEST(Document, Parse) {
    ParseTest<MemoryPoolAllocator<>, CrtAllocator>();
    ParseTest<MemoryPoolAllocator<>, MemoryPoolAllocator<> >();
    ParseTest<CrtAllocator, MemoryPoolAllocator<> >();
    ParseTest<CrtAllocator, CrtAllocator>();
}

TEST(Document, UnchangedOnParseError) {
    Document doc;
    doc.SetArray().PushBack(0, doc.GetAllocator());

    ParseResult noError;
    EXPECT_TRUE(noError);

    ParseResult err = doc.Parse("{]");
    EXPECT_TRUE(doc.HasParseError());
    EXPECT_NE(err, noError);
    EXPECT_NE(err.Code(), noError);
    EXPECT_NE(noError, doc.GetParseError());
    EXPECT_EQ(err.Code(), doc.GetParseError());
    EXPECT_EQ(err.Offset(), doc.GetErrorOffset());
    EXPECT_TRUE(doc.IsArray());
    EXPECT_EQ(doc.Size(), 1u);

    err = doc.Parse("{}");
    EXPECT_FALSE(doc.HasParseError());
    EXPECT_FALSE(err.IsError());
    EXPECT_TRUE(err);
    EXPECT_EQ(err, noError);
    EXPECT_EQ(err.Code(), noError);
    EXPECT_EQ(err.Code(), doc.GetParseError());
    EXPECT_EQ(err.Offset(), doc.GetErrorOffset());
    EXPECT_TRUE(doc.IsObject());
    EXPECT_EQ(doc.MemberCount(), 0u);
}

static FILE* OpenEncodedFile(const char* filename) {
    const char *paths[] = {
        "encodings",
        "bin/encodings",
        "../bin/encodings",
        "../../bin/encodings",
        "../../../bin/encodings"
    };
    char buffer[1024];
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); i++) {
        sprintf(buffer, "%s/%s", paths[i], filename);
        FILE *fp = fopen(buffer, "rb");
        if (fp)
            return fp;
    }
    return 0;
}

TEST(Document, Parse_Encoding) {
    const char* json = " { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, 4] } ";

    typedef GenericDocument<UTF16<> > DocumentType;
    DocumentType doc;
    
    // Parse<unsigned, SourceEncoding>(const SourceEncoding::Ch*)
    // doc.Parse<kParseDefaultFlags, UTF8<> >(json);
    // EXPECT_FALSE(doc.HasParseError());
    // EXPECT_EQ(0, StrCmp(doc[L"hello"].GetString(), L"world"));

    // Parse<unsigned, SourceEncoding>(const SourceEncoding::Ch*, size_t)
    size_t length = strlen(json);
    char* buffer = reinterpret_cast<char*>(malloc(length * 2));
    memcpy(buffer, json, length);
    memset(buffer + length, 'X', length);
#if RAPIDJSON_HAS_STDSTRING
    std::string s2(buffer, length); // backup buffer
#endif
    doc.SetNull();
    doc.Parse<kParseDefaultFlags, UTF8<> >(buffer, length);
    free(buffer);
    EXPECT_FALSE(doc.HasParseError());
    if (doc.HasParseError())
        printf("Error: %d at %zu\n", static_cast<int>(doc.GetParseError()), doc.GetErrorOffset());
    EXPECT_EQ(0, StrCmp(doc[L"hello"].GetString(), L"world"));

#if RAPIDJSON_HAS_STDSTRING
    // Parse<unsigned, SourceEncoding>(std::string)
    doc.SetNull();

#if defined(_MSC_VER) && _MSC_VER < 1800
    doc.Parse<kParseDefaultFlags, UTF8<> >(s2.c_str()); // VS2010 or below cannot handle templated function overloading. Use const char* instead.
#else
    doc.Parse<kParseDefaultFlags, UTF8<> >(s2);
#endif
    EXPECT_FALSE(doc.HasParseError());
    EXPECT_EQ(0, StrCmp(doc[L"hello"].GetString(), L"world"));
#endif
}

TEST(Document, ParseStream_EncodedInputStream) {
    // UTF8 -> UTF16
    FILE* fp = OpenEncodedFile("utf8.json");
    char buffer[256];
    FileReadStream bis(fp, buffer, sizeof(buffer));
    EncodedInputStream<UTF8<>, FileReadStream> eis(bis);

    GenericDocument<UTF16<> > d;
    d.ParseStream<0, UTF8<> >(eis);
    EXPECT_FALSE(d.HasParseError());

    fclose(fp);

    wchar_t expected[] = L"I can eat glass and it doesn't hurt me.";
    GenericValue<UTF16<> >& v = d[L"en"];
    EXPECT_TRUE(v.IsString());
    EXPECT_EQ(sizeof(expected) / sizeof(wchar_t) - 1, v.GetStringLength());
    EXPECT_EQ(0, StrCmp(expected, v.GetString()));

    // UTF16 -> UTF8 in memory
    StringBuffer bos;
    typedef EncodedOutputStream<UTF8<>, StringBuffer> OutputStream;
    OutputStream eos(bos, false);   // Not writing BOM
    {
        Writer<OutputStream, UTF16<>, UTF8<> > writer(eos);
        d.Accept(writer);
    }

    // Condense the original file and compare.
    fp = OpenEncodedFile("utf8.json");
    FileReadStream is(fp, buffer, sizeof(buffer));
    Reader reader;
    StringBuffer bos2;
    Writer<StringBuffer> writer2(bos2);
    reader.Parse(is, writer2);
    fclose(fp);

    EXPECT_EQ(bos.GetSize(), bos2.GetSize());
    EXPECT_EQ(0, memcmp(bos.GetString(), bos2.GetString(), bos2.GetSize()));
}

TEST(Document, ParseStream_AutoUTFInputStream) {
    // Any -> UTF8
    FILE* fp = OpenEncodedFile("utf32be.json");
    char buffer[256];
    FileReadStream bis(fp, buffer, sizeof(buffer));
    AutoUTFInputStream<unsigned, FileReadStream> eis(bis);

    Document d;
    d.ParseStream<0, AutoUTF<unsigned> >(eis);
    EXPECT_FALSE(d.HasParseError());

    fclose(fp);

    char expected[] = "I can eat glass and it doesn't hurt me.";
    Value& v = d["en"];
    EXPECT_TRUE(v.IsString());
    EXPECT_EQ(sizeof(expected) - 1, v.GetStringLength());
    EXPECT_EQ(0, StrCmp(expected, v.GetString()));

    // UTF8 -> UTF8 in memory
    StringBuffer bos;
    Writer<StringBuffer> writer(bos);
    d.Accept(writer);

    // Condense the original file and compare.
    fp = OpenEncodedFile("utf8.json");
    FileReadStream is(fp, buffer, sizeof(buffer));
    Reader reader;
    StringBuffer bos2;
    Writer<StringBuffer> writer2(bos2);
    reader.Parse(is, writer2);
    fclose(fp);

    EXPECT_EQ(bos.GetSize(), bos2.GetSize());
    EXPECT_EQ(0, memcmp(bos.GetString(), bos2.GetString(), bos2.GetSize()));
}

TEST(Document, Swap) {
    Document d1;
    Document::AllocatorType& a = d1.GetAllocator();

    d1.SetArray().PushBack(1, a).PushBack(2, a);

    Value o;
    o.SetObject().AddMember("a", 1, a);

    // Swap between Document and Value
    d1.Swap(o);
    EXPECT_TRUE(d1.IsObject());
    EXPECT_TRUE(o.IsArray());

    d1.Swap(o);
    EXPECT_TRUE(d1.IsArray());
    EXPECT_TRUE(o.IsObject());

    o.Swap(d1);
    EXPECT_TRUE(d1.IsObject());
    EXPECT_TRUE(o.IsArray());

    // Swap between Document and Document
    Document d2;
    d2.SetArray().PushBack(3, a);
    d1.Swap(d2);
    EXPECT_TRUE(d1.IsArray());
    EXPECT_TRUE(d2.IsObject());
    EXPECT_EQ(&d2.GetAllocator(), &a);

    // reset value
    Value().Swap(d1);
    EXPECT_TRUE(d1.IsNull());

    // reset document, including allocator
    // so clear o before so that it doesnt contain dangling elements
    o.Clear();
    Document().Swap(d2);
    EXPECT_TRUE(d2.IsNull());
    EXPECT_NE(&d2.GetAllocator(), &a);

    // testing std::swap compatibility
    d1.SetBool(true);
    using std::swap;
    swap(d1, d2);
    EXPECT_TRUE(d1.IsNull());
    EXPECT_TRUE(d2.IsTrue());

    swap(o, d2);
    EXPECT_TRUE(o.IsTrue());
    EXPECT_TRUE(d2.IsArray());
}


// This should be slow due to assignment in inner-loop.
struct OutputStringStream : public std::ostringstream {
    typedef char Ch;

    virtual ~OutputStringStream();

    void Put(char c) {
        put(c);
    }
    void Flush() {}
};

OutputStringStream::~OutputStringStream() {}

TEST(Document, AcceptWriter) {
    Document doc;
    doc.Parse(" { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, 4] } ");

    OutputStringStream os;
    Writer<OutputStringStream> writer(os);
    doc.Accept(writer);

    EXPECT_EQ("{\"hello\":\"world\",\"t\":true,\"f\":false,\"n\":null,\"i\":123,\"pi\":3.1416,\"a\":[1,2,3,4]}", os.str());
}

TEST(Document, UserBuffer) {
    typedef GenericDocument<UTF8<>, MemoryPoolAllocator<>, MemoryPoolAllocator<> > DocumentType;
    char valueBuffer[4096];
    char parseBuffer[1024];
    MemoryPoolAllocator<> valueAllocator(valueBuffer, sizeof(valueBuffer));
    MemoryPoolAllocator<> parseAllocator(parseBuffer, sizeof(parseBuffer));
    DocumentType doc(&valueAllocator, sizeof(parseBuffer) / 2, &parseAllocator);
    doc.Parse(" { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, 4] } ");
    EXPECT_FALSE(doc.HasParseError());
    EXPECT_LE(valueAllocator.Size(), sizeof(valueBuffer));
    EXPECT_LE(parseAllocator.Size(), sizeof(parseBuffer));

    // Cover MemoryPoolAllocator::Capacity()
    EXPECT_LE(valueAllocator.Size(), valueAllocator.Capacity());
    EXPECT_LE(parseAllocator.Size(), parseAllocator.Capacity());
}

// Issue 226: Value of string type should not point to NULL
TEST(Document, AssertAcceptInvalidNameType) {
    Document doc;
    doc.SetObject();
    doc.AddMember("a", 0, doc.GetAllocator());
    doc.FindMember("a")->name.SetNull(); // Change name to non-string type.

    OutputStringStream os;
    Writer<OutputStringStream> writer(os);
    ASSERT_THROW(doc.Accept(writer), AssertException);
}

// Issue 44:    SetStringRaw doesn't work with wchar_t
TEST(Document, UTF16_Document) {
    GenericDocument< UTF16<> > json;
    json.Parse<kParseValidateEncodingFlag>(L"[{\"created_at\":\"Wed Oct 30 17:13:20 +0000 2012\"}]");

    ASSERT_TRUE(json.IsArray());
    GenericValue< UTF16<> >& v = json[0];
    ASSERT_TRUE(v.IsObject());

    GenericValue< UTF16<> >& s = v[L"created_at"];
    ASSERT_TRUE(s.IsString());

    EXPECT_EQ(0, memcmp(L"Wed Oct 30 17:13:20 +0000 2012", s.GetString(), (s.GetStringLength() + 1) * sizeof(wchar_t)));
}

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS

#if 0 // Many old compiler does not support these. Turn it off temporaily.

#include <type_traits>

TEST(Document, Traits) {
    static_assert(std::is_constructible<Document>::value, "");
    static_assert(std::is_default_constructible<Document>::value, "");
#ifndef _MSC_VER
    static_assert(!std::is_copy_constructible<Document>::value, "");
#endif
    static_assert(std::is_move_constructible<Document>::value, "");

    static_assert(!std::is_nothrow_constructible<Document>::value, "");
    static_assert(!std::is_nothrow_default_constructible<Document>::value, "");
#ifndef _MSC_VER
    static_assert(!std::is_nothrow_copy_constructible<Document>::value, "");
    static_assert(std::is_nothrow_move_constructible<Document>::value, "");
#endif

    static_assert(std::is_assignable<Document,Document>::value, "");
#ifndef _MSC_VER
  static_assert(!std::is_copy_assignable<Document>::value, "");
#endif
    static_assert(std::is_move_assignable<Document>::value, "");

#ifndef _MSC_VER
    static_assert(std::is_nothrow_assignable<Document, Document>::value, "");
#endif
    static_assert(!std::is_nothrow_copy_assignable<Document>::value, "");
#ifndef _MSC_VER
    static_assert(std::is_nothrow_move_assignable<Document>::value, "");
#endif

    static_assert( std::is_destructible<Document>::value, "");
#ifndef _MSC_VER
    static_assert(std::is_nothrow_destructible<Document>::value, "");
#endif
}

#endif

template <typename Allocator>
struct DocumentMove: public ::testing::Test {
};

typedef ::testing::Types< CrtAllocator, MemoryPoolAllocator<> > MoveAllocatorTypes;
TYPED_TEST_CASE(DocumentMove, MoveAllocatorTypes);

TYPED_TEST(DocumentMove, MoveConstructor) {
    typedef TypeParam Allocator;
    typedef GenericDocument<UTF8<>, Allocator> D;
    Allocator allocator;

    D a(&allocator);
    a.Parse("[\"one\", \"two\", \"three\"]");
    EXPECT_FALSE(a.HasParseError());
    EXPECT_TRUE(a.IsArray());
    EXPECT_EQ(3u, a.Size());
    EXPECT_EQ(&a.GetAllocator(), &allocator);

    // Document b(a); // does not compile (!is_copy_constructible)
    D b(std::move(a));
    EXPECT_TRUE(a.IsNull());
    EXPECT_TRUE(b.IsArray());
    EXPECT_EQ(3u, b.Size());
    EXPECT_THROW(a.GetAllocator(), AssertException);
    EXPECT_EQ(&b.GetAllocator(), &allocator);

    b.Parse("{\"Foo\": \"Bar\", \"Baz\": 42}");
    EXPECT_FALSE(b.HasParseError());
    EXPECT_TRUE(b.IsObject());
    EXPECT_EQ(2u, b.MemberCount());

    // Document c = a; // does not compile (!is_copy_constructible)
    D c = std::move(b);
    EXPECT_TRUE(b.IsNull());
    EXPECT_TRUE(c.IsObject());
    EXPECT_EQ(2u, c.MemberCount());
    EXPECT_THROW(b.GetAllocator(), AssertException);
    EXPECT_EQ(&c.GetAllocator(), &allocator);
}

TYPED_TEST(DocumentMove, MoveConstructorParseError) {
    typedef TypeParam Allocator;
    typedef GenericDocument<UTF8<>, Allocator> D;

    ParseResult noError;
    D a;
    a.Parse("{ 4 = 4]");
    ParseResult error(a.GetParseError(), a.GetErrorOffset());
    EXPECT_TRUE(a.HasParseError());
    EXPECT_NE(error, noError);
    EXPECT_NE(error.Code(), noError);
    EXPECT_NE(error.Code(), noError.Code());
    EXPECT_NE(error.Offset(), noError.Offset());

    D b(std::move(a));
    EXPECT_FALSE(a.HasParseError());
    EXPECT_TRUE(b.HasParseError());
    EXPECT_EQ(a.GetParseError(), noError);
    EXPECT_EQ(a.GetParseError(), noError.Code());
    EXPECT_EQ(a.GetErrorOffset(), noError.Offset());
    EXPECT_EQ(b.GetParseError(), error);
    EXPECT_EQ(b.GetParseError(), error.Code());
    EXPECT_EQ(b.GetErrorOffset(), error.Offset());

    D c(std::move(b));
    EXPECT_FALSE(b.HasParseError());
    EXPECT_TRUE(c.HasParseError());
    EXPECT_EQ(b.GetParseError(), noError.Code());
    EXPECT_EQ(c.GetParseError(), error.Code());
    EXPECT_EQ(b.GetErrorOffset(), noError.Offset());
    EXPECT_EQ(c.GetErrorOffset(), error.Offset());
}

// This test does not properly use parsing, just for testing.
// It must call ClearStack() explicitly to prevent memory leak.
// But here we cannot as ClearStack() is private.
#if 0
TYPED_TEST(DocumentMove, MoveConstructorStack) {
    typedef TypeParam Allocator;
    typedef UTF8<> Encoding;
    typedef GenericDocument<Encoding, Allocator> Document;

    Document a;
    size_t defaultCapacity = a.GetStackCapacity();

    // Trick Document into getting GetStackCapacity() to return non-zero
    typedef GenericReader<Encoding, Encoding, Allocator> Reader;
    Reader reader(&a.GetAllocator());
    GenericStringStream<Encoding> is("[\"one\", \"two\", \"three\"]");
    reader.template Parse<kParseDefaultFlags>(is, a);
    size_t capacity = a.GetStackCapacity();
    EXPECT_GT(capacity, 0u);

    Document b(std::move(a));
    EXPECT_EQ(a.GetStackCapacity(), defaultCapacity);
    EXPECT_EQ(b.GetStackCapacity(), capacity);

    Document c = std::move(b);
    EXPECT_EQ(b.GetStackCapacity(), defaultCapacity);
    EXPECT_EQ(c.GetStackCapacity(), capacity);
}
#endif

TYPED_TEST(DocumentMove, MoveAssignment) {
    typedef TypeParam Allocator;
    typedef GenericDocument<UTF8<>, Allocator> D;
    Allocator allocator;

    D a(&allocator);
    a.Parse("[\"one\", \"two\", \"three\"]");
    EXPECT_FALSE(a.HasParseError());
    EXPECT_TRUE(a.IsArray());
    EXPECT_EQ(3u, a.Size());
    EXPECT_EQ(&a.GetAllocator(), &allocator);

    // Document b; b = a; // does not compile (!is_copy_assignable)
    D b;
    b = std::move(a);
    EXPECT_TRUE(a.IsNull());
    EXPECT_TRUE(b.IsArray());
    EXPECT_EQ(3u, b.Size());
    EXPECT_THROW(a.GetAllocator(), AssertException);
    EXPECT_EQ(&b.GetAllocator(), &allocator);

    b.Parse("{\"Foo\": \"Bar\", \"Baz\": 42}");
    EXPECT_FALSE(b.HasParseError());
    EXPECT_TRUE(b.IsObject());
    EXPECT_EQ(2u, b.MemberCount());

    // Document c; c = a; // does not compile (see static_assert)
    D c;
    c = std::move(b);
    EXPECT_TRUE(b.IsNull());
    EXPECT_TRUE(c.IsObject());
    EXPECT_EQ(2u, c.MemberCount());
    EXPECT_THROW(b.GetAllocator(), AssertException);
    EXPECT_EQ(&c.GetAllocator(), &allocator);
}

TYPED_TEST(DocumentMove, MoveAssignmentParseError) {
    typedef TypeParam Allocator;
    typedef GenericDocument<UTF8<>, Allocator> D;

    ParseResult noError;
    D a;
    a.Parse("{ 4 = 4]");
    ParseResult error(a.GetParseError(), a.GetErrorOffset());
    EXPECT_TRUE(a.HasParseError());
    EXPECT_NE(error.Code(), noError.Code());
    EXPECT_NE(error.Offset(), noError.Offset());

    D b;
    b = std::move(a);
    EXPECT_FALSE(a.HasParseError());
    EXPECT_TRUE(b.HasParseError());
    EXPECT_EQ(a.GetParseError(), noError.Code());
    EXPECT_EQ(b.GetParseError(), error.Code());
    EXPECT_EQ(a.GetErrorOffset(), noError.Offset());
    EXPECT_EQ(b.GetErrorOffset(), error.Offset());

    D c;
    c = std::move(b);
    EXPECT_FALSE(b.HasParseError());
    EXPECT_TRUE(c.HasParseError());
    EXPECT_EQ(b.GetParseError(), noError.Code());
    EXPECT_EQ(c.GetParseError(), error.Code());
    EXPECT_EQ(b.GetErrorOffset(), noError.Offset());
    EXPECT_EQ(c.GetErrorOffset(), error.Offset());
}

// This test does not properly use parsing, just for testing.
// It must call ClearStack() explicitly to prevent memory leak.
// But here we cannot as ClearStack() is private.
#if 0
TYPED_TEST(DocumentMove, MoveAssignmentStack) {
    typedef TypeParam Allocator;
    typedef UTF8<> Encoding;
    typedef GenericDocument<Encoding, Allocator> D;

    D a;
    size_t defaultCapacity = a.GetStackCapacity();

    // Trick Document into getting GetStackCapacity() to return non-zero
    typedef GenericReader<Encoding, Encoding, Allocator> Reader;
    Reader reader(&a.GetAllocator());
    GenericStringStream<Encoding> is("[\"one\", \"two\", \"three\"]");
    reader.template Parse<kParseDefaultFlags>(is, a);
    size_t capacity = a.GetStackCapacity();
    EXPECT_GT(capacity, 0u);

    D b;
    b = std::move(a);
    EXPECT_EQ(a.GetStackCapacity(), defaultCapacity);
    EXPECT_EQ(b.GetStackCapacity(), capacity);

    D c;
    c = std::move(b);
    EXPECT_EQ(b.GetStackCapacity(), defaultCapacity);
    EXPECT_EQ(c.GetStackCapacity(), capacity);
}
#endif

#endif // RAPIDJSON_HAS_CXX11_RVALUE_REFS

// Issue 22: Memory corruption via operator=
// Fixed by making unimplemented assignment operator private.
//TEST(Document, Assignment) {
//  Document d1;
//  Document d2;
//  d1 = d2;
//}

#ifdef __clang__
RAPIDJSON_DIAG_POP
#endif
