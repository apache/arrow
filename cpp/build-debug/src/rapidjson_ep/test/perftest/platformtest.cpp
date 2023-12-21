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

// This file is for giving the performance characteristics of the platform (compiler/OS/CPU).

#if TEST_PLATFORM

#include <cmath>
#include <fcntl.h>

// Windows
#ifdef _WIN32
#include <windows.h>
#endif

// UNIX
#if defined(unix) || defined(__unix__) || defined(__unix)
#include <unistd.h>
#ifdef _POSIX_MAPPED_FILES
#include <sys/mman.h>
#endif
#endif

class Platform : public PerfTest {
public:
    virtual void SetUp() {
        PerfTest::SetUp();

        // temp buffer for testing
        temp_ = (char *)malloc(length_ + 1);
        memcpy(temp_, json_, length_);
        checkSum_ = CheckSum();
    }

    char CheckSum() {
        char c = 0;
        for (size_t i = 0; i < length_; ++i)
            c += temp_[i];
        return c;
    }

    virtual void TearDown() {
        PerfTest::TearDown();
        free(temp_);
    }

protected:
    char *temp_;
    char checkSum_;
};

TEST_F(Platform, CheckSum) {
    for (int i = 0; i < kTrialCount; i++)
        EXPECT_EQ(checkSum_, CheckSum());
}

TEST_F(Platform, strlen) {
    for (int i = 0; i < kTrialCount; i++) {
        size_t l = strlen(json_);
        EXPECT_EQ(length_, l);
    }
}

TEST_F(Platform, memcmp) {
    for (int i = 0; i < kTrialCount; i++) {
        EXPECT_EQ(0u, memcmp(temp_, json_, length_));
    }
}

TEST_F(Platform, pow) {
    double sum = 0;
    for (int i = 0; i < kTrialCount * kTrialCount; i++)
        sum += pow(10.0, i & 255);
    EXPECT_GT(sum, 0.0);
}

TEST_F(Platform, Whitespace_strlen) {
    for (int i = 0; i < kTrialCount; i++) {
        size_t l = strlen(whitespace_);
        EXPECT_GT(l, whitespace_length_);
    }       
}

TEST_F(Platform, Whitespace_strspn) {
    for (int i = 0; i < kTrialCount; i++) {
        size_t l = strspn(whitespace_, " \n\r\t");
        EXPECT_EQ(whitespace_length_, l);
    }       
}

TEST_F(Platform, fread) {
    for (int i = 0; i < kTrialCount; i++) {
        FILE *fp = fopen(filename_, "rb");
        ASSERT_EQ(length_, fread(temp_, 1, length_, fp));
        EXPECT_EQ(checkSum_, CheckSum());
        fclose(fp);
    }
}

#ifdef _MSC_VER
TEST_F(Platform, read) {
    for (int i = 0; i < kTrialCount; i++) {
        int fd = _open(filename_, _O_BINARY | _O_RDONLY);
        ASSERT_NE(-1, fd);
        ASSERT_EQ(length_, _read(fd, temp_, length_));
        EXPECT_EQ(checkSum_, CheckSum());
        _close(fd);
    }
}
#else
TEST_F(Platform, read) {
    for (int i = 0; i < kTrialCount; i++) {
        int fd = open(filename_, O_RDONLY);
        ASSERT_NE(-1, fd);
        ASSERT_EQ(length_, read(fd, temp_, length_));
        EXPECT_EQ(checkSum_, CheckSum());
        close(fd);
    }
}
#endif

#ifdef _WIN32
TEST_F(Platform, MapViewOfFile) {
    for (int i = 0; i < kTrialCount; i++) {
        HANDLE file = CreateFile(filename_, GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        ASSERT_NE(INVALID_HANDLE_VALUE, file);
        HANDLE mapObject = CreateFileMapping(file, NULL, PAGE_READONLY, 0, length_, NULL);
        ASSERT_NE(INVALID_HANDLE_VALUE, mapObject);
        void *p = MapViewOfFile(mapObject, FILE_MAP_READ, 0, 0, length_);
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(checkSum_, CheckSum());
        ASSERT_TRUE(UnmapViewOfFile(p) == TRUE);
        ASSERT_TRUE(CloseHandle(mapObject) == TRUE);
        ASSERT_TRUE(CloseHandle(file) == TRUE);
    }
}
#endif

#ifdef _POSIX_MAPPED_FILES
TEST_F(Platform, mmap) {
    for (int i = 0; i < kTrialCount; i++) {
        int fd = open(filename_, O_RDONLY);
        ASSERT_NE(-1, fd);
        void *p = mmap(NULL, length_, PROT_READ, MAP_PRIVATE, fd, 0);
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(checkSum_, CheckSum());
        munmap(p, length_);
        close(fd);
    }
}
#endif

#endif // TEST_PLATFORM
