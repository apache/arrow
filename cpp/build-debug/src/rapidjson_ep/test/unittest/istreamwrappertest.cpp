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

#include "rapidjson/istreamwrapper.h"
#include "rapidjson/encodedstream.h"
#include "rapidjson/document.h"
#include <sstream>
#include <fstream>

#if defined(_MSC_VER) && !defined(__clang__)
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(4702) // unreachable code
#endif

using namespace rapidjson;
using namespace std;

template <typename StringStreamType>
static void TestStringStream() {
    typedef typename StringStreamType::char_type Ch;

    {
        StringStreamType iss;
        BasicIStreamWrapper<StringStreamType> is(iss);
        EXPECT_EQ(0u, is.Tell());
        if (sizeof(Ch) == 1) {
            EXPECT_EQ(0, is.Peek4());
            EXPECT_EQ(0u, is.Tell());
        }
        EXPECT_EQ(0, is.Peek());
        EXPECT_EQ(0, is.Take());
        EXPECT_EQ(0u, is.Tell());
    }

    {
        Ch s[] = { 'A', 'B', 'C', '\0' };
        StringStreamType iss(s);
        BasicIStreamWrapper<StringStreamType> is(iss);
        EXPECT_EQ(0u, is.Tell());
        if (sizeof(Ch) == 1) {
            EXPECT_EQ(0, is.Peek4()); // less than 4 bytes
        }
        for (int i = 0; i < 3; i++) {
            EXPECT_EQ(static_cast<size_t>(i), is.Tell());
            EXPECT_EQ('A' + i, is.Peek());
            EXPECT_EQ('A' + i, is.Peek());
            EXPECT_EQ('A' + i, is.Take());
        }
        EXPECT_EQ(3u, is.Tell());
        EXPECT_EQ(0, is.Peek());
        EXPECT_EQ(0, is.Take());
    }

    {
        Ch s[] = { 'A', 'B', 'C', 'D', 'E', '\0' };
        StringStreamType iss(s);
        BasicIStreamWrapper<StringStreamType> is(iss);
        if (sizeof(Ch) == 1) {
            const Ch* c = is.Peek4();
            for (int i = 0; i < 4; i++)
                EXPECT_EQ('A' + i, c[i]);
            EXPECT_EQ(0u, is.Tell());
        }
        for (int i = 0; i < 5; i++) {
            EXPECT_EQ(static_cast<size_t>(i), is.Tell());
            EXPECT_EQ('A' + i, is.Peek());
            EXPECT_EQ('A' + i, is.Peek());
            EXPECT_EQ('A' + i, is.Take());
        }
        EXPECT_EQ(5u, is.Tell());
        EXPECT_EQ(0, is.Peek());
        EXPECT_EQ(0, is.Take());
    }
}

TEST(IStreamWrapper, istringstream) {
    TestStringStream<istringstream>();
}

TEST(IStreamWrapper, stringstream) {
    TestStringStream<stringstream>();
}

TEST(IStreamWrapper, wistringstream) {
    TestStringStream<wistringstream>();
}

TEST(IStreamWrapper, wstringstream) {
    TestStringStream<wstringstream>();
}

template <typename FileStreamType>
static bool Open(FileStreamType& fs, const char* filename) {
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
        fs.open(buffer, ios_base::in | ios_base::binary);
        if (fs.is_open())
            return true;
    }
    return false;
}

TEST(IStreamWrapper, ifstream) {
    ifstream ifs;
    ASSERT_TRUE(Open(ifs, "utf8bom.json"));
    IStreamWrapper isw(ifs);
    EncodedInputStream<UTF8<>, IStreamWrapper> eis(isw);
    Document d;
    EXPECT_TRUE(!d.ParseStream(eis).HasParseError());
    EXPECT_TRUE(d.IsObject());
    EXPECT_EQ(5u, d.MemberCount());
}

TEST(IStreamWrapper, fstream) {
    fstream fs;
    ASSERT_TRUE(Open(fs, "utf8bom.json"));
    IStreamWrapper isw(fs);
    EncodedInputStream<UTF8<>, IStreamWrapper> eis(isw);
    Document d;
    EXPECT_TRUE(!d.ParseStream(eis).HasParseError());
    EXPECT_TRUE(d.IsObject());
    EXPECT_EQ(5u, d.MemberCount());
}

// wifstream/wfstream only works on C++11 with codecvt_utf16
// But many C++11 library still not have it.
#if 0
#include <codecvt>

TEST(IStreamWrapper, wifstream) {
    wifstream ifs;
    ASSERT_TRUE(Open(ifs, "utf16bebom.json"));
    ifs.imbue(std::locale(ifs.getloc(),
       new std::codecvt_utf16<wchar_t, 0x10ffff, std::consume_header>));
    WIStreamWrapper isw(ifs);
    GenericDocument<UTF16<> > d;
    d.ParseStream<kParseDefaultFlags, UTF16<>, WIStreamWrapper>(isw);
    EXPECT_TRUE(!d.HasParseError());
    EXPECT_TRUE(d.IsObject());
    EXPECT_EQ(5, d.MemberCount());
}

TEST(IStreamWrapper, wfstream) {
    wfstream fs;
    ASSERT_TRUE(Open(fs, "utf16bebom.json"));
    fs.imbue(std::locale(fs.getloc(),
       new std::codecvt_utf16<wchar_t, 0x10ffff, std::consume_header>));
    WIStreamWrapper isw(fs);
    GenericDocument<UTF16<> > d;
    d.ParseStream<kParseDefaultFlags, UTF16<>, WIStreamWrapper>(isw);
    EXPECT_TRUE(!d.HasParseError());
    EXPECT_TRUE(d.IsObject());
    EXPECT_EQ(5, d.MemberCount());
}

#endif

#if defined(_MSC_VER) && !defined(__clang__)
RAPIDJSON_DIAG_POP
#endif
