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

#ifndef UNITTEST_H_
#define UNITTEST_H_

// gtest indirectly included inttypes.h, without __STDC_CONSTANT_MACROS.
#ifndef __STDC_CONSTANT_MACROS
#ifdef __clang__
#pragma GCC diagnostic push
#if __has_warning("-Wreserved-id-macro")
#pragma GCC diagnostic ignored "-Wreserved-id-macro"
#endif
#endif

#  define __STDC_CONSTANT_MACROS 1 // required by C++ standard

#ifdef __clang__
#pragma GCC diagnostic pop
#endif
#endif

#ifdef _MSC_VER
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#pragma warning(disable : 4996) // 'function': was declared deprecated
#endif

#if defined(__clang__) || defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#if defined(__clang__) || (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6))
#pragma GCC diagnostic push
#endif
#pragma GCC diagnostic ignored "-Weffc++"
#endif

#include "gtest/gtest.h"
#include <stdexcept>

#if defined(__clang__) || defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6))
#pragma GCC diagnostic pop
#endif

#ifdef __clang__
// All TEST() macro generated this warning, disable globally
#pragma GCC diagnostic ignored "-Wglobal-constructors"
#endif

template <typename Ch>
inline unsigned StrLen(const Ch* s) {
    const Ch* p = s;
    while (*p) p++;
    return unsigned(p - s);
}

template<typename Ch>
inline int StrCmp(const Ch* s1, const Ch* s2) {
    while(*s1 && (*s1 == *s2)) { s1++; s2++; }
    return static_cast<unsigned>(*s1) < static_cast<unsigned>(*s2) ? -1 : static_cast<unsigned>(*s1) > static_cast<unsigned>(*s2);
}

template <typename Ch>
inline Ch* StrDup(const Ch* str) {
    size_t bufferSize = sizeof(Ch) * (StrLen(str) + 1);
    Ch* buffer = static_cast<Ch*>(malloc(bufferSize));
    memcpy(buffer, str, bufferSize);
    return buffer;
}

inline FILE* TempFile(char *filename) {
#if defined(__WIN32__) || defined(_MSC_VER)
    filename = tmpnam(filename);

    // For Visual Studio, tmpnam() adds a backslash in front. Remove it.
    if (filename[0] == '\\')
        for (int i = 0; filename[i] != '\0'; i++)
            filename[i] = filename[i + 1];
        
    return fopen(filename, "wb");
#else
    strcpy(filename, "/tmp/fileXXXXXX");
    int fd = mkstemp(filename);
    return fdopen(fd, "w");
#endif
}

// Use exception for catching assert
#ifdef _MSC_VER
#pragma warning(disable : 4127)
#endif

#ifdef __clang__
#pragma GCC diagnostic push
#if __has_warning("-Wdeprecated")
#pragma GCC diagnostic ignored "-Wdeprecated"
#endif
#endif

class AssertException : public std::logic_error {
public:
    AssertException(const char* w) : std::logic_error(w) {}
    AssertException(const AssertException& rhs) : std::logic_error(rhs) {}
    virtual ~AssertException() throw();
};

#ifdef __clang__
#pragma GCC diagnostic pop
#endif

// Not using noexcept for testing RAPIDJSON_ASSERT()
#define RAPIDJSON_HAS_CXX11_NOEXCEPT 0

#ifndef RAPIDJSON_ASSERT
#define RAPIDJSON_ASSERT(x) (!(x) ? throw AssertException(RAPIDJSON_STRINGIFY(x)) : (void)0u)
#ifndef RAPIDJSON_ASSERT_THROWS
#define RAPIDJSON_ASSERT_THROWS
#endif
#endif

class Random {
public:
    Random(unsigned seed = 0) : mSeed(seed) {}

    unsigned operator()() {
        mSeed = 214013 * mSeed + 2531011;
        return mSeed;
    }

private:
    unsigned mSeed;
};

#endif // UNITTEST_H_
