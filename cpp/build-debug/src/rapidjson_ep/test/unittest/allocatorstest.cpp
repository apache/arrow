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

#include "rapidjson/allocators.h"

#include <map>
#include <string>
#include <utility>
#include <functional>

using namespace rapidjson;

template <typename Allocator>
void TestAllocator(Allocator& a) {
    EXPECT_TRUE(a.Malloc(0) == 0);

    uint8_t* p = static_cast<uint8_t*>(a.Malloc(100));
    EXPECT_TRUE(p != 0);
    for (size_t i = 0; i < 100; i++)
        p[i] = static_cast<uint8_t>(i);

    // Expand
    uint8_t* q = static_cast<uint8_t*>(a.Realloc(p, 100, 200));
    EXPECT_TRUE(q != 0);
    for (size_t i = 0; i < 100; i++)
        EXPECT_EQ(i, q[i]);
    for (size_t i = 100; i < 200; i++)
        q[i] = static_cast<uint8_t>(i);

    // Shrink
    uint8_t *r = static_cast<uint8_t*>(a.Realloc(q, 200, 150));
    EXPECT_TRUE(r != 0);
    for (size_t i = 0; i < 150; i++)
        EXPECT_EQ(i, r[i]);

    Allocator::Free(r);

    // Realloc to zero size
    EXPECT_TRUE(a.Realloc(a.Malloc(1), 1, 0) == 0);
}

struct TestStdAllocatorData {
    TestStdAllocatorData(int &constructions, int &destructions) :
        constructions_(&constructions),
        destructions_(&destructions)
    {
        ++*constructions_;
    }
    TestStdAllocatorData(const TestStdAllocatorData& rhs) :
        constructions_(rhs.constructions_),
        destructions_(rhs.destructions_)
    {
        ++*constructions_;
    }
    TestStdAllocatorData& operator=(const TestStdAllocatorData& rhs)
    {
        this->~TestStdAllocatorData();
        constructions_ = rhs.constructions_;
        destructions_ = rhs.destructions_;
        ++*constructions_;
        return *this;
    }
    ~TestStdAllocatorData()
    {
        ++*destructions_;
    }
private:
    TestStdAllocatorData();
    int *constructions_,
        *destructions_;
};

template <typename Allocator>
void TestStdAllocator(const Allocator& a) {
#if RAPIDJSON_HAS_CXX17
    typedef StdAllocator<bool, Allocator> BoolAllocator;
#else
    typedef StdAllocator<void, Allocator> VoidAllocator;
    typedef typename VoidAllocator::template rebind<bool>::other BoolAllocator;
#endif
    BoolAllocator ba(a), ba2(a);
    EXPECT_TRUE(ba == ba2);
    EXPECT_FALSE(ba!= ba2);
    ba.deallocate(ba.allocate());
    EXPECT_TRUE(ba == ba2);
    EXPECT_FALSE(ba != ba2);

    unsigned long long ll = 0, *llp = &ll;
    const unsigned long long cll = 0, *cllp = &cll;
    StdAllocator<unsigned long long, Allocator> lla(a);
    EXPECT_EQ(lla.address(ll), llp);
    EXPECT_EQ(lla.address(cll), cllp);
    EXPECT_TRUE(lla.max_size() > 0 && lla.max_size() <= SIZE_MAX / sizeof(unsigned long long));

    int *arr;
    StdAllocator<int, Allocator> ia(a);
    arr = ia.allocate(10 * sizeof(int));
    EXPECT_TRUE(arr != 0);
    for (int i = 0; i < 10; ++i) {
        arr[i] = 0x0f0f0f0f;
    }
    ia.deallocate(arr, 10);
    arr = Malloc<int>(ia, 10);
    EXPECT_TRUE(arr != 0);
    for (int i = 0; i < 10; ++i) {
        arr[i] = 0x0f0f0f0f;
    }
    arr = Realloc<int>(ia, arr, 10, 20);
    EXPECT_TRUE(arr != 0);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(arr[i], 0x0f0f0f0f);
    }
    for (int i = 10; i < 20; i++) {
        arr[i] = 0x0f0f0f0f;
    }
    Free<int>(ia, arr, 20);

    int cons = 0, dest = 0;
    StdAllocator<TestStdAllocatorData, Allocator> da(a);
    for (int i = 1; i < 10; i++) {
        TestStdAllocatorData *d = da.allocate();
        EXPECT_TRUE(d != 0);

        da.destroy(new(d) TestStdAllocatorData(cons, dest));
        EXPECT_EQ(cons, i);
        EXPECT_EQ(dest, i);

        da.deallocate(d);
    }

    typedef StdAllocator<char, Allocator> CharAllocator;
    typedef std::basic_string<char, std::char_traits<char>, CharAllocator> String;
#if RAPIDJSON_HAS_CXX11
    String s(CharAllocator{a});
#else
    CharAllocator ca(a);
    String s(ca);
#endif
    for (int i = 0; i < 26; i++) {
        s.push_back(static_cast<char>('A' + i));
    }
    EXPECT_TRUE(s == "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

    typedef StdAllocator<std::pair<const int, bool>, Allocator> MapAllocator;
    typedef std::map<int, bool, std::less<int>, MapAllocator> Map;
#if RAPIDJSON_HAS_CXX11
    Map map(std::less<int>(), MapAllocator{a});
#else
    MapAllocator ma(a);
    Map map(std::less<int>(), ma);
#endif
    for (int i = 0; i < 10; i++) {
        map.insert(std::make_pair(i, (i % 2) == 0));
    }
    EXPECT_TRUE(map.size() == 10);
    for (int i = 0; i < 10; i++) {
        typename Map::iterator it = map.find(i);
        EXPECT_TRUE(it != map.end());
        EXPECT_TRUE(it->second == ((i % 2) == 0));
    }
}

TEST(Allocator, CrtAllocator) {
    CrtAllocator a;

    TestAllocator(a);
    TestStdAllocator(a);

    CrtAllocator a2;
    EXPECT_TRUE(a == a2);
    EXPECT_FALSE(a != a2);
    a2.Free(a2.Malloc(1));
    EXPECT_TRUE(a == a2);
    EXPECT_FALSE(a != a2);
}

TEST(Allocator, MemoryPoolAllocator) {
    const size_t capacity = RAPIDJSON_ALLOCATOR_DEFAULT_CHUNK_CAPACITY;
    MemoryPoolAllocator<> a(capacity);

    a.Clear(); // noop
    EXPECT_EQ(a.Size(), 0u);
    EXPECT_EQ(a.Capacity(), 0u);
    EXPECT_EQ(a.Shared(), false);
    {
        MemoryPoolAllocator<> a2(a);
        EXPECT_EQ(a2.Shared(), true);
        EXPECT_EQ(a.Shared(), true);
        EXPECT_TRUE(a == a2);
        EXPECT_FALSE(a != a2);
        a2.Free(a2.Malloc(1));
        EXPECT_TRUE(a == a2);
        EXPECT_FALSE(a != a2);
    }
    EXPECT_EQ(a.Shared(), false);
    EXPECT_EQ(a.Capacity(), capacity);
    EXPECT_EQ(a.Size(), 8u); // aligned
    a.Clear();
    EXPECT_EQ(a.Capacity(), 0u);
    EXPECT_EQ(a.Size(), 0u);

    TestAllocator(a);
    TestStdAllocator(a);

    for (size_t i = 1; i < 1000; i++) {
        EXPECT_TRUE(a.Malloc(i) != 0);
        EXPECT_LE(a.Size(), a.Capacity());
    }

    CrtAllocator baseAllocator;
    a = MemoryPoolAllocator<>(capacity, &baseAllocator);
    EXPECT_EQ(a.Capacity(), 0u);
    EXPECT_EQ(a.Size(), 0u);
    a.Free(a.Malloc(1));
    EXPECT_EQ(a.Capacity(), capacity);
    EXPECT_EQ(a.Size(), 8u); // aligned

    {
        a.Clear();
        const size_t bufSize = 1024;
        char *buffer = (char *)a.Malloc(bufSize);
        MemoryPoolAllocator<> aligned_a(buffer, bufSize);
        EXPECT_TRUE(aligned_a.Capacity() > 0 && aligned_a.Capacity() <= bufSize);
        EXPECT_EQ(aligned_a.Size(), 0u);
        aligned_a.Free(aligned_a.Malloc(1));
        EXPECT_TRUE(aligned_a.Capacity() > 0 && aligned_a.Capacity() <= bufSize);
        EXPECT_EQ(aligned_a.Size(), 8u); // aligned
    }

    {
        a.Clear();
        const size_t bufSize = 1024;
        char *buffer = (char *)a.Malloc(bufSize);
        RAPIDJSON_ASSERT(bufSize % sizeof(void*) == 0);
        MemoryPoolAllocator<> unaligned_a(buffer + 1, bufSize - 1);
        EXPECT_TRUE(unaligned_a.Capacity() > 0 && unaligned_a.Capacity() <= bufSize - sizeof(void*));
        EXPECT_EQ(unaligned_a.Size(), 0u);
        unaligned_a.Free(unaligned_a.Malloc(1));
        EXPECT_TRUE(unaligned_a.Capacity() > 0 && unaligned_a.Capacity() <= bufSize - sizeof(void*));
        EXPECT_EQ(unaligned_a.Size(), 8u); // aligned
    }
}

TEST(Allocator, Alignment) {
    if (sizeof(size_t) >= 8) {
        EXPECT_EQ(RAPIDJSON_UINT64_C2(0x00000000, 0x00000000), RAPIDJSON_ALIGN(0));
        for (uint64_t i = 1; i < 8; i++) {
            EXPECT_EQ(RAPIDJSON_UINT64_C2(0x00000000, 0x00000008), RAPIDJSON_ALIGN(i));
            EXPECT_EQ(RAPIDJSON_UINT64_C2(0x00000000, 0x00000010), RAPIDJSON_ALIGN(RAPIDJSON_UINT64_C2(0x00000000, 0x00000008) + i));
            EXPECT_EQ(RAPIDJSON_UINT64_C2(0x00000001, 0x00000000), RAPIDJSON_ALIGN(RAPIDJSON_UINT64_C2(0x00000000, 0xFFFFFFF8) + i));
            EXPECT_EQ(RAPIDJSON_UINT64_C2(0xFFFFFFFF, 0xFFFFFFF8), RAPIDJSON_ALIGN(RAPIDJSON_UINT64_C2(0xFFFFFFFF, 0xFFFFFFF0) + i));
        }
    }

    EXPECT_EQ(0u, RAPIDJSON_ALIGN(0u));
    for (uint32_t i = 1; i < 8; i++) {
        EXPECT_EQ(8u, RAPIDJSON_ALIGN(i));
        EXPECT_EQ(0xFFFFFFF8u, RAPIDJSON_ALIGN(0xFFFFFFF0u + i));
    }
}

TEST(Allocator, Issue399) {
    MemoryPoolAllocator<> a;
    void* p = a.Malloc(100);
    void* q = a.Realloc(p, 100, 200);
    EXPECT_EQ(p, q);

    // exhuasive testing
    for (size_t j = 1; j < 32; j++) {
        a.Clear();
        a.Malloc(j); // some unaligned size
        p = a.Malloc(1);
        for (size_t i = 1; i < 1024; i++) {
            q = a.Realloc(p, i, i + 1);
            EXPECT_EQ(p, q);
            p = q;
        }
    }
}
