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

#include "perftest.h"

#if TEST_RAPIDJSON

#include "rapidjson/rapidjson.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/encodedstream.h"
#include "rapidjson/memorystream.h"

#include <fstream>
#include <vector>

#ifdef RAPIDJSON_SSE2
#define SIMD_SUFFIX(name) name##_SSE2
#elif defined(RAPIDJSON_SSE42)
#define SIMD_SUFFIX(name) name##_SSE42
#elif defined(RAPIDJSON_NEON)
#define SIMD_SUFFIX(name) name##_NEON
#else
#define SIMD_SUFFIX(name) name
#endif

using namespace rapidjson;

class RapidJson : public PerfTest {
public:
    RapidJson() : temp_(), doc_() {}

    virtual void SetUp() {
        PerfTest::SetUp();

        // temp buffer for insitu parsing.
        temp_ = (char *)malloc(length_ + 1);

        // Parse as a document
        EXPECT_FALSE(doc_.Parse(json_).HasParseError());

        for (size_t i = 0; i < 8; i++)
            EXPECT_FALSE(typesDoc_[i].Parse(types_[i]).HasParseError());
    }

    virtual void TearDown() {
        PerfTest::TearDown();
        free(temp_);
    }

private:
    RapidJson(const RapidJson&);
    RapidJson& operator=(const RapidJson&);

protected:
    char *temp_;
    Document doc_;
    Document typesDoc_[8];
};

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseInsitu_DummyHandler)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        InsituStringStream s(temp_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse<kParseInsituFlag>(s, h));
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseInsitu_DummyHandler_ValidateEncoding)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        InsituStringStream s(temp_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse<kParseInsituFlag | kParseValidateEncodingFlag>(s, h));
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringStream s(json_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse(s, h));
    }
}

#define TEST_TYPED(index, Name)\
TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_##Name)) {\
    for (size_t i = 0; i < kTrialCount * 10; i++) {\
        StringStream s(types_[index]);\
        BaseReaderHandler<> h;\
        Reader reader;\
        EXPECT_TRUE(reader.Parse(s, h));\
    }\
}\
TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseInsitu_DummyHandler_##Name)) {\
    for (size_t i = 0; i < kTrialCount * 10; i++) {\
        memcpy(temp_, types_[index], typesLength_[index] + 1);\
        InsituStringStream s(temp_);\
        BaseReaderHandler<> h;\
        Reader reader;\
        EXPECT_TRUE(reader.Parse<kParseInsituFlag>(s, h));\
    }\
}

TEST_TYPED(0, Booleans)
TEST_TYPED(1, Floats)
TEST_TYPED(2, Guids)
TEST_TYPED(3, Integers)
TEST_TYPED(4, Mixed)
TEST_TYPED(5, Nulls)
TEST_TYPED(6, Paragraphs)

#undef TEST_TYPED

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_FullPrecision)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringStream s(json_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse<kParseFullPrecisionFlag>(s, h));
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseIterative_DummyHandler)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringStream s(json_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse<kParseIterativeFlag>(s, h));
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseIterativeInsitu_DummyHandler)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        InsituStringStream s(temp_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse<kParseIterativeFlag|kParseInsituFlag>(s, h));
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseIterativePull_DummyHandler)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringStream s(json_);
        BaseReaderHandler<> h;
        Reader reader;
        reader.IterativeParseInit();
        while (!reader.IterativeParseComplete()) {
            if (!reader.IterativeParseNext<kParseDefaultFlags>(s, h))
                break;
        }
        EXPECT_FALSE(reader.HasParseError());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParseIterativePullInsitu_DummyHandler)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        InsituStringStream s(temp_);
        BaseReaderHandler<> h;
        Reader reader;
        reader.IterativeParseInit();
        while (!reader.IterativeParseComplete()) {
            if (!reader.IterativeParseNext<kParseDefaultFlags|kParseInsituFlag>(s, h))
                break;
        }
        EXPECT_FALSE(reader.HasParseError());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_ValidateEncoding)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringStream s(json_);
        BaseReaderHandler<> h;
        Reader reader;
        EXPECT_TRUE(reader.Parse<kParseValidateEncodingFlag>(s, h));
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseInsitu_MemoryPoolAllocator)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        Document doc;
        doc.ParseInsitu(temp_);
        ASSERT_TRUE(doc.IsObject());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseIterativeInsitu_MemoryPoolAllocator)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        Document doc;
        doc.ParseInsitu<kParseIterativeFlag>(temp_);
        ASSERT_TRUE(doc.IsObject());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParse_MemoryPoolAllocator)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        Document doc;
        doc.Parse(json_);
        ASSERT_TRUE(doc.IsObject());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseLength_MemoryPoolAllocator)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        Document doc;
        doc.Parse(json_, length_);
        ASSERT_TRUE(doc.IsObject());
    }
}

#if RAPIDJSON_HAS_STDSTRING
TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseStdString_MemoryPoolAllocator)) {
    const std::string s(json_, length_);
    for (size_t i = 0; i < kTrialCount; i++) {
        Document doc;
        doc.Parse(s);
        ASSERT_TRUE(doc.IsObject());
    }
}
#endif

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseIterative_MemoryPoolAllocator)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        Document doc;
        doc.Parse<kParseIterativeFlag>(json_);
        ASSERT_TRUE(doc.IsObject());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParse_CrtAllocator)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        memcpy(temp_, json_, length_ + 1);
        GenericDocument<UTF8<>, CrtAllocator> doc;
        doc.Parse(temp_);
        ASSERT_TRUE(doc.IsObject());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseEncodedInputStream_MemoryStream)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        MemoryStream ms(json_, length_);
        EncodedInputStream<UTF8<>, MemoryStream> is(ms);
        Document doc;
        doc.ParseStream<0, UTF8<> >(is);
        ASSERT_TRUE(doc.IsObject());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(DocumentParseAutoUTFInputStream_MemoryStream)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        MemoryStream ms(json_, length_);
        AutoUTFInputStream<unsigned, MemoryStream> is(ms);
        Document doc;
        doc.ParseStream<0, AutoUTF<unsigned> >(is);
        ASSERT_TRUE(doc.IsObject());
    }
}

template<typename T>
size_t Traverse(const T& value) {
    size_t count = 1;
    switch(value.GetType()) {
        case kObjectType:
            for (typename T::ConstMemberIterator itr = value.MemberBegin(); itr != value.MemberEnd(); ++itr) {
                count++;    // name
                count += Traverse(itr->value);
            }
            break;

        case kArrayType:
            for (typename T::ConstValueIterator itr = value.Begin(); itr != value.End(); ++itr)
                count += Traverse(*itr);
            break;

        default:
            // Do nothing.
            break;
    }
    return count;
}

TEST_F(RapidJson, DocumentTraverse) {
    for (size_t i = 0; i < kTrialCount; i++) {
        size_t count = Traverse(doc_);
        EXPECT_EQ(4339u, count);
        //if (i == 0)
        //  std::cout << count << std::endl;
    }
}

#ifdef __GNUC__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(effc++)
#endif

struct ValueCounter : public BaseReaderHandler<> {
    ValueCounter() : count_(1) {}   // root

    bool EndObject(SizeType memberCount) { count_ += memberCount * 2; return true; }
    bool EndArray(SizeType elementCount) { count_ += elementCount; return true; }

    SizeType count_;
};

#ifdef __GNUC__
RAPIDJSON_DIAG_POP
#endif

TEST_F(RapidJson, DocumentAccept) {
    for (size_t i = 0; i < kTrialCount; i++) {
        ValueCounter counter;
        doc_.Accept(counter);
        EXPECT_EQ(4339u, counter.count_);
    }
}

TEST_F(RapidJson, DocumentFind) {
    typedef Document::ValueType ValueType;
    typedef ValueType::ConstMemberIterator ConstMemberIterator;
    const Document &doc = typesDoc_[7]; // alotofkeys.json
    if (doc.IsObject()) {
        std::vector<const ValueType*> keys;
        for (ConstMemberIterator it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
            keys.push_back(&it->name);
        }
        for (size_t i = 0; i < kTrialCount; i++) {
            for (size_t j = 0; j < keys.size(); j++) {
                EXPECT_TRUE(doc.FindMember(*keys[j]) != doc.MemberEnd());
            }
        }
    }
}

struct NullStream {
    typedef char Ch;

    NullStream() /*: length_(0)*/ {}
    void Put(Ch) { /*++length_;*/ }
    void Flush() {}
    //size_t length_;
};

TEST_F(RapidJson, Writer_NullStream) {
    for (size_t i = 0; i < kTrialCount; i++) {
        NullStream s;
        Writer<NullStream> writer(s);
        doc_.Accept(writer);
        //if (i == 0)
        //  std::cout << s.length_ << std::endl;
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(Writer_StringBuffer)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringBuffer s(0, 1024 * 1024);
        Writer<StringBuffer> writer(s);
        doc_.Accept(writer);
        const char* str = s.GetString();
        (void)str;
        //if (i == 0)
        //  std::cout << strlen(str) << std::endl;
    }
}

#define TEST_TYPED(index, Name)\
TEST_F(RapidJson, SIMD_SUFFIX(Writer_StringBuffer_##Name)) {\
    for (size_t i = 0; i < kTrialCount * 10; i++) {\
        StringBuffer s(0, 1024 * 1024);\
        Writer<StringBuffer> writer(s);\
        typesDoc_[index].Accept(writer);\
        const char* str = s.GetString();\
        (void)str;\
    }\
}

TEST_TYPED(0, Booleans)
TEST_TYPED(1, Floats)
TEST_TYPED(2, Guids)
TEST_TYPED(3, Integers)
TEST_TYPED(4, Mixed)
TEST_TYPED(5, Nulls)
TEST_TYPED(6, Paragraphs)

#undef TEST_TYPED

TEST_F(RapidJson, SIMD_SUFFIX(PrettyWriter_StringBuffer)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        StringBuffer s(0, 2048 * 1024);
        PrettyWriter<StringBuffer> writer(s);
        writer.SetIndent(' ', 1);
        doc_.Accept(writer);
        const char* str = s.GetString();
        (void)str;
        //if (i == 0)
        //  std::cout << strlen(str) << std::endl;
    }
}

TEST_F(RapidJson, internal_Pow10) {
    double sum = 0;
    for (size_t i = 0; i < kTrialCount * kTrialCount; i++)
        sum += internal::Pow10(int(i & 255));
    EXPECT_GT(sum, 0.0);
}

TEST_F(RapidJson, SkipWhitespace_Basic) {
    for (size_t i = 0; i < kTrialCount; i++) {
        rapidjson::StringStream s(whitespace_);
        while (s.Peek() == ' ' || s.Peek() == '\n' || s.Peek() == '\r' || s.Peek() == '\t')
            s.Take();
        ASSERT_EQ('[', s.Peek());
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(SkipWhitespace)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        rapidjson::StringStream s(whitespace_);
        rapidjson::SkipWhitespace(s);
        ASSERT_EQ('[', s.Peek());
    }
}

TEST_F(RapidJson, SkipWhitespace_strspn) {
    for (size_t i = 0; i < kTrialCount; i++) {
        const char* s = whitespace_ + std::strspn(whitespace_, " \t\r\n");
        ASSERT_EQ('[', *s);
    }
}

TEST_F(RapidJson, UTF8_Validate) {
    NullStream os;

    for (size_t i = 0; i < kTrialCount; i++) {
        StringStream is(json_);
        bool result = true;
        while (is.Peek() != '\0')
            result &= UTF8<>::Validate(is, os);
        EXPECT_TRUE(result);
    }
}

TEST_F(RapidJson, FileReadStream) {
    for (size_t i = 0; i < kTrialCount; i++) {
        FILE *fp = fopen(filename_, "rb");
        char buffer[65536];
        FileReadStream s(fp, buffer, sizeof(buffer));
        while (s.Take() != '\0')
            ;
        fclose(fp);
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_FileReadStream)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        FILE *fp = fopen(filename_, "rb");
        char buffer[65536];
        FileReadStream s(fp, buffer, sizeof(buffer));
        BaseReaderHandler<> h;
        Reader reader;
        reader.Parse(s, h);
        fclose(fp);
    }
}

TEST_F(RapidJson, IStreamWrapper) {
    for (size_t i = 0; i < kTrialCount; i++) {
        std::ifstream is(filename_, std::ios::in | std::ios::binary);
        char buffer[65536];
        IStreamWrapper isw(is, buffer, sizeof(buffer));
        while (isw.Take() != '\0')
            ;
        is.close();
    }
}

TEST_F(RapidJson, IStreamWrapper_Unbuffered) {
    for (size_t i = 0; i < kTrialCount; i++) {
        std::ifstream is(filename_, std::ios::in | std::ios::binary);
        IStreamWrapper isw(is);
        while (isw.Take() != '\0')
            ;
        is.close();
    }
}

TEST_F(RapidJson, IStreamWrapper_Setbuffered) {
    for (size_t i = 0; i < kTrialCount; i++) {
        std::ifstream is;
        char buffer[65536];
        is.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
        is.open(filename_, std::ios::in | std::ios::binary);
        IStreamWrapper isw(is);
        while (isw.Take() != '\0')
            ;
        is.close();
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_IStreamWrapper)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        std::ifstream is(filename_, std::ios::in | std::ios::binary);
        char buffer[65536];
        IStreamWrapper isw(is, buffer, sizeof(buffer));
        BaseReaderHandler<> h;
        Reader reader;
        reader.Parse(isw, h);
        is.close();
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_IStreamWrapper_Unbuffered)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        std::ifstream is(filename_, std::ios::in | std::ios::binary);
        IStreamWrapper isw(is);
        BaseReaderHandler<> h;
        Reader reader;
        reader.Parse(isw, h);
        is.close();
    }
}

TEST_F(RapidJson, SIMD_SUFFIX(ReaderParse_DummyHandler_IStreamWrapper_Setbuffered)) {
    for (size_t i = 0; i < kTrialCount; i++) {
        std::ifstream is;
        char buffer[65536];
        is.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
        is.open(filename_, std::ios::in | std::ios::binary);
        IStreamWrapper isw(is);
        BaseReaderHandler<> h;
        Reader reader;
        reader.Parse(isw, h);
        is.close();
    }
}

TEST_F(RapidJson, StringBuffer) {
    StringBuffer sb;
    for (int i = 0; i < 32 * 1024 * 1024; i++)
        sb.Put(i & 0x7f);
}

#endif // TEST_RAPIDJSON
