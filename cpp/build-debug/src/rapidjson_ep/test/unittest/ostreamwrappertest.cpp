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

#include "rapidjson/ostreamwrapper.h"
#include "rapidjson/encodedstream.h"
#include "rapidjson/document.h"
#include <sstream>
#include <fstream>

using namespace rapidjson;
using namespace std;

template <typename StringStreamType>
static void TestStringStream() {
    typedef typename StringStreamType::char_type Ch;

    Ch s[] = { 'A', 'B', 'C', '\0' };
    StringStreamType oss(s);
    BasicOStreamWrapper<StringStreamType> os(oss);
    for (size_t i = 0; i < 3; i++)
        os.Put(s[i]);
    os.Flush();
    for (size_t i = 0; i < 3; i++)
        EXPECT_EQ(s[i], oss.str()[i]);
}

TEST(OStreamWrapper, ostringstream) {
    TestStringStream<ostringstream>();
}

TEST(OStreamWrapper, stringstream) {
    TestStringStream<stringstream>();
}

TEST(OStreamWrapper, wostringstream) {
    TestStringStream<wostringstream>();
}

TEST(OStreamWrapper, wstringstream) {
    TestStringStream<wstringstream>();
}

TEST(OStreamWrapper, cout) {
    OStreamWrapper os(cout);
    const char* s = "Hello World!\n";
    while (*s)
        os.Put(*s++);
    os.Flush();
}

template <typename FileStreamType>
static void TestFileStream() {
    char filename[L_tmpnam];
    FILE* fp = TempFile(filename);
    fclose(fp);

    const char* s = "Hello World!\n";
    {
        FileStreamType ofs(filename, ios::out | ios::binary);
        BasicOStreamWrapper<FileStreamType> osw(ofs);
        for (const char* p = s; *p; p++)
            osw.Put(*p);
        osw.Flush();
    }

    fp = fopen(filename, "r");
	ASSERT_TRUE( fp != NULL );
    for (const char* p = s; *p; p++)
        EXPECT_EQ(*p, static_cast<char>(fgetc(fp)));
    fclose(fp);
}

TEST(OStreamWrapper, ofstream) {
    TestFileStream<ofstream>();
}

TEST(OStreamWrapper, fstream) {
    TestFileStream<fstream>();
}
