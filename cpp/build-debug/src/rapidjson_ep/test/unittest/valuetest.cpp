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
#include <algorithm>

#ifdef __clang__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(c++98-compat)
#endif

using namespace rapidjson;

TEST(Value, Size) {
    if (sizeof(SizeType) == 4) {
#if RAPIDJSON_48BITPOINTER_OPTIMIZATION
        EXPECT_EQ(16u, sizeof(Value));
#elif RAPIDJSON_64BIT
        EXPECT_EQ(24u, sizeof(Value));
#else
        EXPECT_EQ(16u, sizeof(Value));
#endif
    }
}

TEST(Value, DefaultConstructor) {
    Value x;
    EXPECT_EQ(kNullType, x.GetType());
    EXPECT_TRUE(x.IsNull());

    //std::cout << "sizeof(Value): " << sizeof(x) << std::endl;
}

// Should not pass compilation
//TEST(Value, copy_constructor) {
//  Value x(1234);
//  Value y = x;
//}

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS

#if 0 // Many old compiler does not support these. Turn it off temporaily.

#include <type_traits>

TEST(Value, Traits) {
    typedef GenericValue<UTF8<>, CrtAllocator> Value;
    static_assert(std::is_constructible<Value>::value, "");
    static_assert(std::is_default_constructible<Value>::value, "");
#ifndef _MSC_VER
    static_assert(!std::is_copy_constructible<Value>::value, "");
#endif
    static_assert(std::is_move_constructible<Value>::value, "");

#ifndef _MSC_VER
    static_assert(std::is_nothrow_constructible<Value>::value, "");
    static_assert(std::is_nothrow_default_constructible<Value>::value, "");
    static_assert(!std::is_nothrow_copy_constructible<Value>::value, "");
    static_assert(std::is_nothrow_move_constructible<Value>::value, "");
#endif

    static_assert(std::is_assignable<Value,Value>::value, "");
#ifndef _MSC_VER
    static_assert(!std::is_copy_assignable<Value>::value, "");
#endif
    static_assert(std::is_move_assignable<Value>::value, "");

#ifndef _MSC_VER
    static_assert(std::is_nothrow_assignable<Value, Value>::value, "");
#endif
    static_assert(!std::is_nothrow_copy_assignable<Value>::value, "");
#ifndef _MSC_VER
    static_assert(std::is_nothrow_move_assignable<Value>::value, "");
#endif

    static_assert(std::is_destructible<Value>::value, "");
#ifndef _MSC_VER
    static_assert(std::is_nothrow_destructible<Value>::value, "");
#endif
}

#endif

TEST(Value, MoveConstructor) {
    typedef GenericValue<UTF8<>, CrtAllocator> V;
    V::AllocatorType allocator;

    V x((V(kArrayType)));
    x.Reserve(4u, allocator);
    x.PushBack(1, allocator).PushBack(2, allocator).PushBack(3, allocator).PushBack(4, allocator);
    EXPECT_TRUE(x.IsArray());
    EXPECT_EQ(4u, x.Size());

    // Value y(x); // does not compile (!is_copy_constructible)
    V y(std::move(x));
    EXPECT_TRUE(x.IsNull());
    EXPECT_TRUE(y.IsArray());
    EXPECT_EQ(4u, y.Size());

    // Value z = y; // does not compile (!is_copy_assignable)
    V z = std::move(y);
    EXPECT_TRUE(y.IsNull());
    EXPECT_TRUE(z.IsArray());
    EXPECT_EQ(4u, z.Size());
}

#endif // RAPIDJSON_HAS_CXX11_RVALUE_REFS

TEST(Value, AssignmentOperator) {
    Value x(1234);
    Value y;
    y = x;
    EXPECT_TRUE(x.IsNull());    // move semantic
    EXPECT_EQ(1234, y.GetInt());

    y = 5678;
    EXPECT_TRUE(y.IsInt());
    EXPECT_EQ(5678, y.GetInt());

    x = "Hello";
    EXPECT_TRUE(x.IsString());
    EXPECT_STREQ(x.GetString(),"Hello");

    y = StringRef(x.GetString(),x.GetStringLength());
    EXPECT_TRUE(y.IsString());
    EXPECT_EQ(y.GetString(),x.GetString());
    EXPECT_EQ(y.GetStringLength(),x.GetStringLength());

    static char mstr[] = "mutable";
    // y = mstr; // should not compile
    y = StringRef(mstr);
    EXPECT_TRUE(y.IsString());
    EXPECT_EQ(y.GetString(),mstr);

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS
    // C++11 move assignment
    x = Value("World");
    EXPECT_TRUE(x.IsString());
    EXPECT_STREQ("World", x.GetString());

    x = std::move(y);
    EXPECT_TRUE(y.IsNull());
    EXPECT_TRUE(x.IsString());
    EXPECT_EQ(x.GetString(), mstr);

    y = std::move(Value().SetInt(1234));
    EXPECT_TRUE(y.IsInt());
    EXPECT_EQ(1234, y);
#endif // RAPIDJSON_HAS_CXX11_RVALUE_REFS
}

template <typename A, typename B> 
void TestEqual(const A& a, const B& b) {
    EXPECT_TRUE (a == b);
    EXPECT_FALSE(a != b);
    EXPECT_TRUE (b == a);
    EXPECT_FALSE(b != a);
}

template <typename A, typename B> 
void TestUnequal(const A& a, const B& b) {
    EXPECT_FALSE(a == b);
    EXPECT_TRUE (a != b);
    EXPECT_FALSE(b == a);
    EXPECT_TRUE (b != a);
}

TEST(Value, EqualtoOperator) {
    Value::AllocatorType allocator;
    Value x(kObjectType);
    x.AddMember("hello", "world", allocator)
        .AddMember("t", Value(true).Move(), allocator)
        .AddMember("f", Value(false).Move(), allocator)
        .AddMember("n", Value(kNullType).Move(), allocator)
        .AddMember("i", 123, allocator)
        .AddMember("pi", 3.14, allocator)
        .AddMember("a", Value(kArrayType).Move().PushBack(1, allocator).PushBack(2, allocator).PushBack(3, allocator), allocator);

    // Test templated operator==() and operator!=()
    TestEqual(x["hello"], "world");
    const char* cc = "world";
    TestEqual(x["hello"], cc);
    char* c = strdup("world");
    TestEqual(x["hello"], c);
    free(c);

    TestEqual(x["t"], true);
    TestEqual(x["f"], false);
    TestEqual(x["i"], 123);
    TestEqual(x["pi"], 3.14);

    // Test operator==() (including different allocators)
    CrtAllocator crtAllocator;
    GenericValue<UTF8<>, CrtAllocator> y;
    GenericDocument<UTF8<>, CrtAllocator> z(&crtAllocator);
    y.CopyFrom(x, crtAllocator);
    z.CopyFrom(y, z.GetAllocator());
    TestEqual(x, y);
    TestEqual(y, z);
    TestEqual(z, x);

    // Swapping member order should be fine.
    EXPECT_TRUE(y.RemoveMember("t"));
    TestUnequal(x, y);
    TestUnequal(z, y);
    EXPECT_TRUE(z.RemoveMember("t"));
    TestUnequal(x, z);
    TestEqual(y, z);
    y.AddMember("t", false, crtAllocator);
    z.AddMember("t", false, z.GetAllocator());
    TestUnequal(x, y);
    TestUnequal(z, x);
    y["t"] = true;
    z["t"] = true;
    TestEqual(x, y);
    TestEqual(y, z);
    TestEqual(z, x);

    // Swapping element order is not OK
    x["a"][0].Swap(x["a"][1]);
    TestUnequal(x, y);
    x["a"][0].Swap(x["a"][1]);
    TestEqual(x, y);

    // Array of different size
    x["a"].PushBack(4, allocator);
    TestUnequal(x, y);
    x["a"].PopBack();
    TestEqual(x, y);

    // Issue #129: compare Uint64
    x.SetUint64(RAPIDJSON_UINT64_C2(0xFFFFFFFF, 0xFFFFFFF0));
    y.SetUint64(RAPIDJSON_UINT64_C2(0xFFFFFFFF, 0xFFFFFFFF));
    TestUnequal(x, y);
}

template <typename Value>
void TestCopyFrom() {
    typename Value::AllocatorType a;
    Value v1(1234);
    Value v2(v1, a); // deep copy constructor
    EXPECT_TRUE(v1.GetType() == v2.GetType());
    EXPECT_EQ(v1.GetInt(), v2.GetInt());

    v1.SetString("foo");
    v2.CopyFrom(v1, a);
    EXPECT_TRUE(v1.GetType() == v2.GetType());
    EXPECT_STREQ(v1.GetString(), v2.GetString());
    EXPECT_EQ(v1.GetString(), v2.GetString()); // string NOT copied

    v1.SetString("bar", a); // copy string
    v2.CopyFrom(v1, a);
    EXPECT_TRUE(v1.GetType() == v2.GetType());
    EXPECT_STREQ(v1.GetString(), v2.GetString());
    EXPECT_NE(v1.GetString(), v2.GetString()); // string copied


    v1.SetArray().PushBack(1234, a);
    v2.CopyFrom(v1, a);
    EXPECT_TRUE(v2.IsArray());
    EXPECT_EQ(v1.Size(), v2.Size());

    v1.PushBack(Value().SetString("foo", a), a); // push string copy
    EXPECT_TRUE(v1.Size() != v2.Size());
    v2.CopyFrom(v1, a);
    EXPECT_TRUE(v1.Size() == v2.Size());
    EXPECT_STREQ(v1[1].GetString(), v2[1].GetString());
    EXPECT_NE(v1[1].GetString(), v2[1].GetString()); // string got copied
}

TEST(Value, CopyFrom) {
    TestCopyFrom<Value>();
    TestCopyFrom<GenericValue<UTF8<>, CrtAllocator> >();
}

TEST(Value, Swap) {
    Value v1(1234);
    Value v2(kObjectType);

    EXPECT_EQ(&v1, &v1.Swap(v2));
    EXPECT_TRUE(v1.IsObject());
    EXPECT_TRUE(v2.IsInt());
    EXPECT_EQ(1234, v2.GetInt());

    // testing std::swap compatibility
    using std::swap;
    swap(v1, v2);
    EXPECT_TRUE(v1.IsInt());
    EXPECT_TRUE(v2.IsObject());
}

TEST(Value, Null) {
    // Default constructor
    Value x;
    EXPECT_EQ(kNullType, x.GetType());
    EXPECT_TRUE(x.IsNull());

    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsNumber());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // Constructor with type
    Value y(kNullType);
    EXPECT_TRUE(y.IsNull());

    // SetNull();
    Value z(true);
    z.SetNull();
    EXPECT_TRUE(z.IsNull());
}

TEST(Value, True) {
    // Constructor with bool
    Value x(true);
    EXPECT_EQ(kTrueType, x.GetType());
    EXPECT_TRUE(x.GetBool());
    EXPECT_TRUE(x.IsBool());
    EXPECT_TRUE(x.IsTrue());

    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsNumber());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // Constructor with type
    Value y(kTrueType);
    EXPECT_TRUE(y.IsTrue());

    // SetBool()
    Value z;
    z.SetBool(true);
    EXPECT_TRUE(z.IsTrue());

    // Templated functions
    EXPECT_TRUE(z.Is<bool>());
    EXPECT_TRUE(z.Get<bool>());
    EXPECT_FALSE(z.Set<bool>(false).Get<bool>());
    EXPECT_TRUE(z.Set(true).Get<bool>());
}

TEST(Value, False) {
    // Constructor with bool
    Value x(false);
    EXPECT_EQ(kFalseType, x.GetType());
    EXPECT_TRUE(x.IsBool());
    EXPECT_TRUE(x.IsFalse());

    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.GetBool());
    //EXPECT_FALSE((bool)x);
    EXPECT_FALSE(x.IsNumber());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // Constructor with type
    Value y(kFalseType);
    EXPECT_TRUE(y.IsFalse());

    // SetBool()
    Value z;
    z.SetBool(false);
    EXPECT_TRUE(z.IsFalse());
}

TEST(Value, Int) {
    // Constructor with int
    Value x(1234);
    EXPECT_EQ(kNumberType, x.GetType());
    EXPECT_EQ(1234, x.GetInt());
    EXPECT_EQ(1234u, x.GetUint());
    EXPECT_EQ(1234, x.GetInt64());
    EXPECT_EQ(1234u, x.GetUint64());
    EXPECT_NEAR(1234.0, x.GetDouble(), 0.0);
    //EXPECT_EQ(1234, (int)x);
    //EXPECT_EQ(1234, (unsigned)x);
    //EXPECT_EQ(1234, (int64_t)x);
    //EXPECT_EQ(1234, (uint64_t)x);
    //EXPECT_EQ(1234, (double)x);
    EXPECT_TRUE(x.IsNumber());
    EXPECT_TRUE(x.IsInt());
    EXPECT_TRUE(x.IsUint());
    EXPECT_TRUE(x.IsInt64());
    EXPECT_TRUE(x.IsUint64());

    EXPECT_FALSE(x.IsDouble());
    EXPECT_FALSE(x.IsFloat());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    Value nx(-1234);
    EXPECT_EQ(-1234, nx.GetInt());
    EXPECT_EQ(-1234, nx.GetInt64());
    EXPECT_TRUE(nx.IsInt());
    EXPECT_TRUE(nx.IsInt64());
    EXPECT_FALSE(nx.IsUint());
    EXPECT_FALSE(nx.IsUint64());

    // Constructor with type
    Value y(kNumberType);
    EXPECT_TRUE(y.IsNumber());
    EXPECT_TRUE(y.IsInt());
    EXPECT_EQ(0, y.GetInt());

    // SetInt()
    Value z;
    z.SetInt(1234);
    EXPECT_EQ(1234, z.GetInt());

    // operator=(int)
    z = 5678;
    EXPECT_EQ(5678, z.GetInt());

    // Templated functions
    EXPECT_TRUE(z.Is<int>());
    EXPECT_EQ(5678, z.Get<int>());
    EXPECT_EQ(5679, z.Set(5679).Get<int>());
    EXPECT_EQ(5680, z.Set<int>(5680).Get<int>());

#ifdef _MSC_VER
    // long as int on MSC platforms
    RAPIDJSON_STATIC_ASSERT(sizeof(long) == sizeof(int));
    z.SetInt(2222);
    EXPECT_TRUE(z.Is<long>());
    EXPECT_EQ(2222l, z.Get<long>());
    EXPECT_EQ(3333l, z.Set(3333l).Get<long>());
    EXPECT_EQ(4444l, z.Set<long>(4444l).Get<long>());
    EXPECT_TRUE(z.IsInt());
#endif
}

TEST(Value, Uint) {
    // Constructor with int
    Value x(1234u);
    EXPECT_EQ(kNumberType, x.GetType());
    EXPECT_EQ(1234, x.GetInt());
    EXPECT_EQ(1234u, x.GetUint());
    EXPECT_EQ(1234, x.GetInt64());
    EXPECT_EQ(1234u, x.GetUint64());
    EXPECT_TRUE(x.IsNumber());
    EXPECT_TRUE(x.IsInt());
    EXPECT_TRUE(x.IsUint());
    EXPECT_TRUE(x.IsInt64());
    EXPECT_TRUE(x.IsUint64());
    EXPECT_NEAR(1234.0, x.GetDouble(), 0.0);   // Number can always be cast as double but !IsDouble().

    EXPECT_FALSE(x.IsDouble());
    EXPECT_FALSE(x.IsFloat());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // SetUint()
    Value z;
    z.SetUint(1234);
    EXPECT_EQ(1234u, z.GetUint());

    // operator=(unsigned)
    z = 5678u;
    EXPECT_EQ(5678u, z.GetUint());

    z = 2147483648u;    // 2^31, cannot cast as int
    EXPECT_EQ(2147483648u, z.GetUint());
    EXPECT_FALSE(z.IsInt());
    EXPECT_TRUE(z.IsInt64());   // Issue 41: Incorrect parsing of unsigned int number types

    // Templated functions
    EXPECT_TRUE(z.Is<unsigned>());
    EXPECT_EQ(2147483648u, z.Get<unsigned>());
    EXPECT_EQ(2147483649u, z.Set(2147483649u).Get<unsigned>());
    EXPECT_EQ(2147483650u, z.Set<unsigned>(2147483650u).Get<unsigned>());

#ifdef _MSC_VER
    // unsigned long as unsigned on MSC platforms
    RAPIDJSON_STATIC_ASSERT(sizeof(unsigned long) == sizeof(unsigned));
    z.SetUint(2222);
    EXPECT_TRUE(z.Is<unsigned long>());
    EXPECT_EQ(2222ul, z.Get<unsigned long>());
    EXPECT_EQ(3333ul, z.Set(3333ul).Get<unsigned long>());
    EXPECT_EQ(4444ul, z.Set<unsigned long>(4444ul).Get<unsigned long>());
    EXPECT_TRUE(x.IsUint());
#endif
}

TEST(Value, Int64) {
    // Constructor with int
    Value x(int64_t(1234));
    EXPECT_EQ(kNumberType, x.GetType());
    EXPECT_EQ(1234, x.GetInt());
    EXPECT_EQ(1234u, x.GetUint());
    EXPECT_EQ(1234, x.GetInt64());
    EXPECT_EQ(1234u, x.GetUint64());
    EXPECT_TRUE(x.IsNumber());
    EXPECT_TRUE(x.IsInt());
    EXPECT_TRUE(x.IsUint());
    EXPECT_TRUE(x.IsInt64());
    EXPECT_TRUE(x.IsUint64());

    EXPECT_FALSE(x.IsDouble());
    EXPECT_FALSE(x.IsFloat());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    Value nx(int64_t(-1234));
    EXPECT_EQ(-1234, nx.GetInt());
    EXPECT_EQ(-1234, nx.GetInt64());
    EXPECT_TRUE(nx.IsInt());
    EXPECT_TRUE(nx.IsInt64());
    EXPECT_FALSE(nx.IsUint());
    EXPECT_FALSE(nx.IsUint64());

    // SetInt64()
    Value z;
    z.SetInt64(1234);
    EXPECT_EQ(1234, z.GetInt64());

    z.SetInt64(2147483648u);   // 2^31, cannot cast as int
    EXPECT_FALSE(z.IsInt());
    EXPECT_TRUE(z.IsUint());
    EXPECT_NEAR(2147483648.0, z.GetDouble(), 0.0);

    z.SetInt64(int64_t(4294967295u) + 1);   // 2^32, cannot cast as uint
    EXPECT_FALSE(z.IsInt());
    EXPECT_FALSE(z.IsUint());
    EXPECT_NEAR(4294967296.0, z.GetDouble(), 0.0);

    z.SetInt64(-int64_t(2147483648u) - 1);   // -2^31-1, cannot cast as int
    EXPECT_FALSE(z.IsInt());
    EXPECT_NEAR(-2147483649.0, z.GetDouble(), 0.0);

    int64_t i = static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x80000000, 00000000));
    z.SetInt64(i);
    EXPECT_DOUBLE_EQ(-9223372036854775808.0, z.GetDouble());

    // Templated functions
    EXPECT_TRUE(z.Is<int64_t>());
    EXPECT_EQ(i, z.Get<int64_t>());
#if 0 // signed integer underflow is undefined behaviour
    EXPECT_EQ(i - 1, z.Set(i - 1).Get<int64_t>());
    EXPECT_EQ(i - 2, z.Set<int64_t>(i - 2).Get<int64_t>());
#endif
}

TEST(Value, Uint64) {
    // Constructor with int
    Value x(uint64_t(1234));
    EXPECT_EQ(kNumberType, x.GetType());
    EXPECT_EQ(1234, x.GetInt());
    EXPECT_EQ(1234u, x.GetUint());
    EXPECT_EQ(1234, x.GetInt64());
    EXPECT_EQ(1234u, x.GetUint64());
    EXPECT_TRUE(x.IsNumber());
    EXPECT_TRUE(x.IsInt());
    EXPECT_TRUE(x.IsUint());
    EXPECT_TRUE(x.IsInt64());
    EXPECT_TRUE(x.IsUint64());

    EXPECT_FALSE(x.IsDouble());
    EXPECT_FALSE(x.IsFloat());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // SetUint64()
    Value z;
    z.SetUint64(1234);
    EXPECT_EQ(1234u, z.GetUint64());

    z.SetUint64(uint64_t(2147483648u));  // 2^31, cannot cast as int
    EXPECT_FALSE(z.IsInt());
    EXPECT_TRUE(z.IsUint());
    EXPECT_TRUE(z.IsInt64());

    z.SetUint64(uint64_t(4294967295u) + 1);  // 2^32, cannot cast as uint
    EXPECT_FALSE(z.IsInt());
    EXPECT_FALSE(z.IsUint());
    EXPECT_TRUE(z.IsInt64());

    uint64_t u = RAPIDJSON_UINT64_C2(0x80000000, 0x00000000);
    z.SetUint64(u);    // 2^63 cannot cast as int64
    EXPECT_FALSE(z.IsInt64());
    EXPECT_EQ(u, z.GetUint64()); // Issue 48
    EXPECT_DOUBLE_EQ(9223372036854775808.0, z.GetDouble());

    // Templated functions
    EXPECT_TRUE(z.Is<uint64_t>());
    EXPECT_EQ(u, z.Get<uint64_t>());
    EXPECT_EQ(u + 1, z.Set(u + 1).Get<uint64_t>());
    EXPECT_EQ(u + 2, z.Set<uint64_t>(u + 2).Get<uint64_t>());
}

TEST(Value, Double) {
    // Constructor with double
    Value x(12.34);
    EXPECT_EQ(kNumberType, x.GetType());
    EXPECT_NEAR(12.34, x.GetDouble(), 0.0);
    EXPECT_TRUE(x.IsNumber());
    EXPECT_TRUE(x.IsDouble());

    EXPECT_FALSE(x.IsInt());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // SetDouble()
    Value z;
    z.SetDouble(12.34);
    EXPECT_NEAR(12.34, z.GetDouble(), 0.0);

    z = 56.78;
    EXPECT_NEAR(56.78, z.GetDouble(), 0.0);

    // Templated functions
    EXPECT_TRUE(z.Is<double>());
    EXPECT_EQ(56.78, z.Get<double>());
    EXPECT_EQ(57.78, z.Set(57.78).Get<double>());
    EXPECT_EQ(58.78, z.Set<double>(58.78).Get<double>());
}

TEST(Value, Float) {
    // Constructor with double
    Value x(12.34f);
    EXPECT_EQ(kNumberType, x.GetType());
    EXPECT_NEAR(12.34f, x.GetFloat(), 0.0);
    EXPECT_TRUE(x.IsNumber());
    EXPECT_TRUE(x.IsDouble());
    EXPECT_TRUE(x.IsFloat());

    EXPECT_FALSE(x.IsInt());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    // SetFloat()
    Value z;
    z.SetFloat(12.34f);
    EXPECT_NEAR(12.34f, z.GetFloat(), 0.0f);

    // Issue 573
    z.SetInt(0);
    EXPECT_EQ(0.0f, z.GetFloat());

    z = 56.78f;
    EXPECT_NEAR(56.78f, z.GetFloat(), 0.0f);

    // Templated functions
    EXPECT_TRUE(z.Is<float>());
    EXPECT_EQ(56.78f, z.Get<float>());
    EXPECT_EQ(57.78f, z.Set(57.78f).Get<float>());
    EXPECT_EQ(58.78f, z.Set<float>(58.78f).Get<float>());
}

TEST(Value, IsLosslessDouble) {
    EXPECT_TRUE(Value(0.0).IsLosslessDouble());
    EXPECT_TRUE(Value(12.34).IsLosslessDouble());
    EXPECT_TRUE(Value(-123).IsLosslessDouble());
    EXPECT_TRUE(Value(2147483648u).IsLosslessDouble());
    EXPECT_TRUE(Value(-static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x40000000, 0x00000000))).IsLosslessDouble());
#if !(defined(_MSC_VER) && _MSC_VER < 1800) // VC2010 has problem
    EXPECT_TRUE(Value(RAPIDJSON_UINT64_C2(0xA0000000, 0x00000000)).IsLosslessDouble());
#endif

    EXPECT_FALSE(Value(static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x7FFFFFFF, 0xFFFFFFFF))).IsLosslessDouble()); // INT64_MAX
    EXPECT_FALSE(Value(-static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x7FFFFFFF, 0xFFFFFFFF))).IsLosslessDouble()); // -INT64_MAX
    EXPECT_TRUE(Value(-static_cast<int64_t>(RAPIDJSON_UINT64_C2(0x7FFFFFFF, 0xFFFFFFFF)) - 1).IsLosslessDouble()); // INT64_MIN
    EXPECT_FALSE(Value(RAPIDJSON_UINT64_C2(0xFFFFFFFF, 0xFFFFFFFF)).IsLosslessDouble()); // UINT64_MAX

    EXPECT_TRUE(Value(3.4028234e38f).IsLosslessDouble()); // FLT_MAX
    EXPECT_TRUE(Value(-3.4028234e38f).IsLosslessDouble()); // -FLT_MAX
    EXPECT_TRUE(Value(1.17549435e-38f).IsLosslessDouble()); // FLT_MIN
    EXPECT_TRUE(Value(-1.17549435e-38f).IsLosslessDouble()); // -FLT_MIN
    EXPECT_TRUE(Value(1.7976931348623157e+308).IsLosslessDouble()); // DBL_MAX
    EXPECT_TRUE(Value(-1.7976931348623157e+308).IsLosslessDouble()); // -DBL_MAX
    EXPECT_TRUE(Value(2.2250738585072014e-308).IsLosslessDouble()); // DBL_MIN
    EXPECT_TRUE(Value(-2.2250738585072014e-308).IsLosslessDouble()); // -DBL_MIN
}

TEST(Value, IsLosslessFloat) {
    EXPECT_TRUE(Value(12.25).IsLosslessFloat());
    EXPECT_TRUE(Value(-123).IsLosslessFloat());
    EXPECT_TRUE(Value(2147483648u).IsLosslessFloat());
    EXPECT_TRUE(Value(3.4028234e38f).IsLosslessFloat());
    EXPECT_TRUE(Value(-3.4028234e38f).IsLosslessFloat());
    EXPECT_FALSE(Value(3.4028235e38).IsLosslessFloat());
    EXPECT_FALSE(Value(0.3).IsLosslessFloat());
}

TEST(Value, String) {
    // Construction with const string
    Value x("Hello", 5); // literal
    EXPECT_EQ(kStringType, x.GetType());
    EXPECT_TRUE(x.IsString());
    EXPECT_STREQ("Hello", x.GetString());
    EXPECT_EQ(5u, x.GetStringLength());

    EXPECT_FALSE(x.IsNumber());
    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsObject());
    EXPECT_FALSE(x.IsArray());

    static const char cstr[] = "World"; // const array
    Value(cstr).Swap(x);
    EXPECT_TRUE(x.IsString());
    EXPECT_EQ(x.GetString(), cstr);
    EXPECT_EQ(x.GetStringLength(), sizeof(cstr)-1);

    static char mstr[] = "Howdy"; // non-const array
    // Value(mstr).Swap(x); // should not compile
    Value(StringRef(mstr)).Swap(x);
    EXPECT_TRUE(x.IsString());
    EXPECT_EQ(x.GetString(), mstr);
    EXPECT_EQ(x.GetStringLength(), sizeof(mstr)-1);
    strncpy(mstr,"Hello", sizeof(mstr));
    EXPECT_STREQ(x.GetString(), "Hello");

    const char* pstr = cstr;
    //Value(pstr).Swap(x); // should not compile
    Value(StringRef(pstr)).Swap(x);
    EXPECT_TRUE(x.IsString());
    EXPECT_EQ(x.GetString(), cstr);
    EXPECT_EQ(x.GetStringLength(), sizeof(cstr)-1);

    char* mpstr = mstr;
    Value(StringRef(mpstr,sizeof(mstr)-1)).Swap(x);
    EXPECT_TRUE(x.IsString());
    EXPECT_EQ(x.GetString(), mstr);
    EXPECT_EQ(x.GetStringLength(), 5u);
    EXPECT_STREQ(x.GetString(), "Hello");

    // Constructor with copy string
    MemoryPoolAllocator<> allocator;
    Value c(x.GetString(), x.GetStringLength(), allocator);
    EXPECT_NE(x.GetString(), c.GetString());
    EXPECT_EQ(x.GetStringLength(), c.GetStringLength());
    EXPECT_STREQ(x.GetString(), c.GetString());
    //x.SetString("World");
    x.SetString("World", 5);
    EXPECT_STREQ("Hello", c.GetString());
    EXPECT_EQ(5u, c.GetStringLength());

    // Constructor with type
    Value y(kStringType);
    EXPECT_TRUE(y.IsString());
    EXPECT_STREQ("", y.GetString());    // Empty string should be "" instead of 0 (issue 226)
    EXPECT_EQ(0u, y.GetStringLength());

    // SetConsttring()
    Value z;
    z.SetString("Hello");
    EXPECT_TRUE(x.IsString());
    z.SetString("Hello", 5);
    EXPECT_STREQ("Hello", z.GetString());
    EXPECT_STREQ("Hello", z.GetString());
    EXPECT_EQ(5u, z.GetStringLength());

    z.SetString("Hello");
    EXPECT_TRUE(z.IsString());
    EXPECT_STREQ("Hello", z.GetString());

    //z.SetString(mstr); // should not compile
    //z.SetString(pstr); // should not compile
    z.SetString(StringRef(mstr));
    EXPECT_TRUE(z.IsString());
    EXPECT_STREQ(z.GetString(), mstr);

    z.SetString(cstr);
    EXPECT_TRUE(z.IsString());
    EXPECT_EQ(cstr, z.GetString());

    z = cstr;
    EXPECT_TRUE(z.IsString());
    EXPECT_EQ(cstr, z.GetString());

    // SetString()
    char s[] = "World";
    Value w;
    w.SetString(s, static_cast<SizeType>(strlen(s)), allocator);
    s[0] = '\0';
    EXPECT_STREQ("World", w.GetString());
    EXPECT_EQ(5u, w.GetStringLength());

    // templated functions
    EXPECT_TRUE(z.Is<const char*>());
    EXPECT_STREQ(cstr, z.Get<const char*>());
    EXPECT_STREQ("Apple", z.Set<const char*>("Apple").Get<const char*>());

#if RAPIDJSON_HAS_STDSTRING
    {
        std::string str = "Hello World";
        str[5] = '\0';
        EXPECT_STREQ(str.data(),"Hello"); // embedded '\0'
        EXPECT_EQ(str.size(), 11u);

        // no copy
        Value vs0(StringRef(str));
        EXPECT_TRUE(vs0.IsString());
        EXPECT_EQ(vs0.GetString(), str.data());
        EXPECT_EQ(vs0.GetStringLength(), str.size());
        TestEqual(vs0, str);

        // do copy
        Value vs1(str, allocator);
        EXPECT_TRUE(vs1.IsString());
        EXPECT_NE(vs1.GetString(), str.data());
        EXPECT_NE(vs1.GetString(), str); // not equal due to embedded '\0'
        EXPECT_EQ(vs1.GetStringLength(), str.size());
        TestEqual(vs1, str);

        // SetString
        str = "World";
        vs0.SetNull().SetString(str, allocator);
        EXPECT_TRUE(vs0.IsString());
        EXPECT_STREQ(vs0.GetString(), str.c_str());
        EXPECT_EQ(vs0.GetStringLength(), str.size());
        TestEqual(str, vs0);
        TestUnequal(str, vs1);

        // vs1 = str; // should not compile
        vs1 = StringRef(str);
        TestEqual(str, vs1);
        TestEqual(vs0, vs1);

        // Templated function.
        EXPECT_TRUE(vs0.Is<std::string>());
        EXPECT_EQ(str, vs0.Get<std::string>());
        vs0.Set<std::string>(std::string("Apple"), allocator);
        EXPECT_EQ(std::string("Apple"), vs0.Get<std::string>());
        vs0.Set(std::string("Orange"), allocator);
        EXPECT_EQ(std::string("Orange"), vs0.Get<std::string>());
    }
#endif // RAPIDJSON_HAS_STDSTRING
}

// Issue 226: Value of string type should not point to NULL
TEST(Value, SetStringNull) {

    MemoryPoolAllocator<> allocator;
    const char* nullPtr = 0;
    {
        // Construction with string type creates empty string
        Value v(kStringType);
        EXPECT_NE(v.GetString(), nullPtr); // non-null string returned
        EXPECT_EQ(v.GetStringLength(), 0u);

        // Construction from/setting to null without length not allowed
        EXPECT_THROW(Value(StringRef(nullPtr)), AssertException);
        EXPECT_THROW(Value(StringRef(nullPtr), allocator), AssertException);
        EXPECT_THROW(v.SetString(nullPtr, allocator), AssertException);

        // Non-empty length with null string is not allowed
        EXPECT_THROW(v.SetString(nullPtr, 17u), AssertException);
        EXPECT_THROW(v.SetString(nullPtr, 42u, allocator), AssertException);

        // Setting to null string with empty length is allowed
        v.SetString(nullPtr, 0u);
        EXPECT_NE(v.GetString(), nullPtr); // non-null string returned
        EXPECT_EQ(v.GetStringLength(), 0u);

        v.SetNull();
        v.SetString(nullPtr, 0u, allocator);
        EXPECT_NE(v.GetString(), nullPtr); // non-null string returned
        EXPECT_EQ(v.GetStringLength(), 0u);
    }
    // Construction with null string and empty length is allowed
    {
        Value v(nullPtr,0u);
        EXPECT_NE(v.GetString(), nullPtr); // non-null string returned
        EXPECT_EQ(v.GetStringLength(), 0u);
    }
    {
        Value v(nullPtr, 0u, allocator);
        EXPECT_NE(v.GetString(), nullPtr); // non-null string returned
        EXPECT_EQ(v.GetStringLength(), 0u);
    }
}

template <typename T, typename Allocator>
static void TestArray(T& x, Allocator& allocator) {
    const T& y = x;

    // PushBack()
    Value v;
    x.PushBack(v, allocator);
    v.SetBool(true);
    x.PushBack(v, allocator);
    v.SetBool(false);
    x.PushBack(v, allocator);
    v.SetInt(123);
    x.PushBack(v, allocator);
    //x.PushBack((const char*)"foo", allocator); // should not compile
    x.PushBack("foo", allocator);

    EXPECT_FALSE(x.Empty());
    EXPECT_EQ(5u, x.Size());
    EXPECT_FALSE(y.Empty());
    EXPECT_EQ(5u, y.Size());
    EXPECT_TRUE(x[SizeType(0)].IsNull());
    EXPECT_TRUE(x[1].IsTrue());
    EXPECT_TRUE(x[2].IsFalse());
    EXPECT_TRUE(x[3].IsInt());
    EXPECT_EQ(123, x[3].GetInt());
    EXPECT_TRUE(y[SizeType(0)].IsNull());
    EXPECT_TRUE(y[1].IsTrue());
    EXPECT_TRUE(y[2].IsFalse());
    EXPECT_TRUE(y[3].IsInt());
    EXPECT_EQ(123, y[3].GetInt());
    EXPECT_TRUE(y[4].IsString());
    EXPECT_STREQ("foo", y[4].GetString());

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS
    // PushBack(GenericValue&&, Allocator&);
    {
        Value y2(kArrayType);
        y2.PushBack(Value(true), allocator);
        y2.PushBack(std::move(Value(kArrayType).PushBack(Value(1), allocator).PushBack("foo", allocator)), allocator);
        EXPECT_EQ(2u, y2.Size());
        EXPECT_TRUE(y2[0].IsTrue());
        EXPECT_TRUE(y2[1].IsArray());
        EXPECT_EQ(2u, y2[1].Size());
        EXPECT_TRUE(y2[1][0].IsInt());
        EXPECT_TRUE(y2[1][1].IsString());
    }
#endif

    // iterator
    typename T::ValueIterator itr = x.Begin();
    EXPECT_TRUE(itr != x.End());
    EXPECT_TRUE(itr->IsNull());
    ++itr;
    EXPECT_TRUE(itr != x.End());
    EXPECT_TRUE(itr->IsTrue());
    ++itr;
    EXPECT_TRUE(itr != x.End());
    EXPECT_TRUE(itr->IsFalse());
    ++itr;
    EXPECT_TRUE(itr != x.End());
    EXPECT_TRUE(itr->IsInt());
    EXPECT_EQ(123, itr->GetInt());
    ++itr;
    EXPECT_TRUE(itr != x.End());
    EXPECT_TRUE(itr->IsString());
    EXPECT_STREQ("foo", itr->GetString());

    // const iterator
    typename T::ConstValueIterator citr = y.Begin();
    EXPECT_TRUE(citr != y.End());
    EXPECT_TRUE(citr->IsNull());
    ++citr;
    EXPECT_TRUE(citr != y.End());
    EXPECT_TRUE(citr->IsTrue());
    ++citr;
    EXPECT_TRUE(citr != y.End());
    EXPECT_TRUE(citr->IsFalse());
    ++citr;
    EXPECT_TRUE(citr != y.End());
    EXPECT_TRUE(citr->IsInt());
    EXPECT_EQ(123, citr->GetInt());
    ++citr;
    EXPECT_TRUE(citr != y.End());
    EXPECT_TRUE(citr->IsString());
    EXPECT_STREQ("foo", citr->GetString());

    // PopBack()
    x.PopBack();
    EXPECT_EQ(4u, x.Size());
    EXPECT_TRUE(y[SizeType(0)].IsNull());
    EXPECT_TRUE(y[1].IsTrue());
    EXPECT_TRUE(y[2].IsFalse());
    EXPECT_TRUE(y[3].IsInt());

    // Clear()
    x.Clear();
    EXPECT_TRUE(x.Empty());
    EXPECT_EQ(0u, x.Size());
    EXPECT_TRUE(y.Empty());
    EXPECT_EQ(0u, y.Size());

    // Erase(ValueIterator)

    // Use array of array to ensure removed elements' destructor is called.
    // [[0],[1],[2],...]
    for (int i = 0; i < 10; i++)
        x.PushBack(Value(kArrayType).PushBack(i, allocator).Move(), allocator);

    // Erase the first
    itr = x.Erase(x.Begin());
    EXPECT_EQ(x.Begin(), itr);
    EXPECT_EQ(9u, x.Size());
    for (int i = 0; i < 9; i++)
        EXPECT_EQ(i + 1, x[static_cast<SizeType>(i)][0].GetInt());

    // Ease the last
    itr = x.Erase(x.End() - 1);
    EXPECT_EQ(x.End(), itr);
    EXPECT_EQ(8u, x.Size());
    for (int i = 0; i < 8; i++)
        EXPECT_EQ(i + 1, x[static_cast<SizeType>(i)][0].GetInt());

    // Erase the middle
    itr = x.Erase(x.Begin() + 4);
    EXPECT_EQ(x.Begin() + 4, itr);
    EXPECT_EQ(7u, x.Size());
    for (int i = 0; i < 4; i++)
        EXPECT_EQ(i + 1, x[static_cast<SizeType>(i)][0].GetInt());
    for (int i = 4; i < 7; i++)
        EXPECT_EQ(i + 2, x[static_cast<SizeType>(i)][0].GetInt());

    // Erase(ValueIterator, ValueIterator)
    // Exhaustive test with all 0 <= first < n, first <= last <= n cases
    const unsigned n = 10;
    for (unsigned first = 0; first < n; first++) {
        for (unsigned last = first; last <= n; last++) {
            x.Clear();
            for (unsigned i = 0; i < n; i++)
                x.PushBack(Value(kArrayType).PushBack(i, allocator).Move(), allocator);
            
            itr = x.Erase(x.Begin() + first, x.Begin() + last);
            if (last == n)
                EXPECT_EQ(x.End(), itr);
            else
                EXPECT_EQ(x.Begin() + first, itr);

            size_t removeCount = last - first;
            EXPECT_EQ(n - removeCount, x.Size());
            for (unsigned i = 0; i < first; i++)
                EXPECT_EQ(i, x[i][0].GetUint());
            for (unsigned i = first; i < n - removeCount; i++)
                EXPECT_EQ(i + removeCount, x[static_cast<SizeType>(i)][0].GetUint());
        }
    }
}

TEST(Value, Array) {
    Value::AllocatorType allocator;
    Value x(kArrayType);
    const Value& y = x;

    EXPECT_EQ(kArrayType, x.GetType());
    EXPECT_TRUE(x.IsArray());
    EXPECT_TRUE(x.Empty());
    EXPECT_EQ(0u, x.Size());
    EXPECT_TRUE(y.IsArray());
    EXPECT_TRUE(y.Empty());
    EXPECT_EQ(0u, y.Size());

    EXPECT_FALSE(x.IsNull());
    EXPECT_FALSE(x.IsBool());
    EXPECT_FALSE(x.IsFalse());
    EXPECT_FALSE(x.IsTrue());
    EXPECT_FALSE(x.IsString());
    EXPECT_FALSE(x.IsObject());

    TestArray(x, allocator);

    // Working in gcc without C++11, but VS2013 cannot compile. To be diagnosed.
    // http://en.wikipedia.org/wiki/Erase-remove_idiom
    x.Clear();
    for (int i = 0; i < 10; i++)
        if (i % 2 == 0)
            x.PushBack(i, allocator);
        else
            x.PushBack(Value(kNullType).Move(), allocator);

    const Value null(kNullType);
    x.Erase(std::remove(x.Begin(), x.End(), null), x.End());
    EXPECT_EQ(5u, x.Size());
    for (int i = 0; i < 5; i++)
        EXPECT_EQ(i * 2, x[static_cast<SizeType>(i)]);

    // SetArray()
    Value z;
    z.SetArray();
    EXPECT_TRUE(z.IsArray());
    EXPECT_TRUE(z.Empty());

    // PR #1503: assign from inner Value
    {
        CrtAllocator a; // Free() is not a noop
        GenericValue<UTF8<>, CrtAllocator> nullValue;
        GenericValue<UTF8<>, CrtAllocator> arrayValue(kArrayType);
        arrayValue.PushBack(nullValue, a);
        arrayValue = arrayValue[0]; // shouldn't crash (use after free)
        EXPECT_TRUE(arrayValue.IsNull());
    }
}

TEST(Value, ArrayHelper) {
    Value::AllocatorType allocator;
    {
        Value x(kArrayType);
        Value::Array a = x.GetArray();
        TestArray(a, allocator);
    }

    {
        Value x(kArrayType);
        Value::Array a = x.GetArray();
        a.PushBack(1, allocator);

        Value::Array a2(a); // copy constructor
        EXPECT_EQ(1u, a2.Size());

        Value::Array a3 = a;
        EXPECT_EQ(1u, a3.Size());

        Value::ConstArray y = static_cast<const Value&>(x).GetArray();
        (void)y;
        // y.PushBack(1, allocator); // should not compile

        // Templated functions
        x.Clear();
        EXPECT_TRUE(x.Is<Value::Array>());
        EXPECT_TRUE(x.Is<Value::ConstArray>());
        a.PushBack(1, allocator);
        EXPECT_EQ(1, x.Get<Value::Array>()[0].GetInt());
        EXPECT_EQ(1, x.Get<Value::ConstArray>()[0].GetInt());

        Value x2;
        x2.Set<Value::Array>(a);
        EXPECT_TRUE(x.IsArray());   // IsArray() is invariant after moving.
        EXPECT_EQ(1, x2.Get<Value::Array>()[0].GetInt());
    }

    {
        Value y(kArrayType);
        y.PushBack(123, allocator);

        Value x(y.GetArray());      // Construct value form array.
        EXPECT_TRUE(x.IsArray());
        EXPECT_EQ(123, x[0].GetInt());
        EXPECT_TRUE(y.IsArray());   // Invariant
        EXPECT_TRUE(y.Empty());
    }

    {
        Value x(kArrayType);
        Value y(kArrayType);
        y.PushBack(123, allocator);
        x.PushBack(y.GetArray(), allocator);    // Implicit constructor to convert Array to GenericValue

        EXPECT_EQ(1u, x.Size());
        EXPECT_EQ(123, x[0][0].GetInt());
        EXPECT_TRUE(y.IsArray());
        EXPECT_TRUE(y.Empty());
    }
}

#if RAPIDJSON_HAS_CXX11_RANGE_FOR
TEST(Value, ArrayHelperRangeFor) {
    Value::AllocatorType allocator;
    Value x(kArrayType);

    for (int i = 0; i < 10; i++)
        x.PushBack(i, allocator);

    {
        int i = 0;
        for (auto& v : x.GetArray()) {
            EXPECT_EQ(i, v.GetInt());
            i++;
        }
        EXPECT_EQ(i, 10);
    }
    {
        int i = 0;
        for (const auto& v : const_cast<const Value&>(x).GetArray()) {
            EXPECT_EQ(i, v.GetInt());
            i++;
        }
        EXPECT_EQ(i, 10);
    }

    // Array a = x.GetArray();
    // Array ca = const_cast<const Value&>(x).GetArray();
}
#endif

template <typename T, typename Allocator>
static void TestObject(T& x, Allocator& allocator) {
    const T& y = x; // const version

    // AddMember()
    x.AddMember("A", "Apple", allocator);
    EXPECT_FALSE(x.ObjectEmpty());
    EXPECT_EQ(1u, x.MemberCount());

    Value value("Banana", 6);
    x.AddMember("B", "Banana", allocator);
    EXPECT_EQ(2u, x.MemberCount());

    // AddMember<T>(StringRefType, T, Allocator)
    {
        Value o(kObjectType);
        o.AddMember("true", true, allocator);
        o.AddMember("false", false, allocator);
        o.AddMember("int", -1, allocator);
        o.AddMember("uint", 1u, allocator);
        o.AddMember("int64", int64_t(-4294967296), allocator);
        o.AddMember("uint64", uint64_t(4294967296), allocator);
        o.AddMember("double", 3.14, allocator);
        o.AddMember("string", "Jelly", allocator);

        EXPECT_TRUE(o["true"].GetBool());
        EXPECT_FALSE(o["false"].GetBool());
        EXPECT_EQ(-1, o["int"].GetInt());
        EXPECT_EQ(1u, o["uint"].GetUint());
        EXPECT_EQ(int64_t(-4294967296), o["int64"].GetInt64());
        EXPECT_EQ(uint64_t(4294967296), o["uint64"].GetUint64());
        EXPECT_STREQ("Jelly",o["string"].GetString());
        EXPECT_EQ(8u, o.MemberCount());
    }

    // AddMember<T>(Value&, T, Allocator)
    {
        Value o(kObjectType);

        Value n("s");
        o.AddMember(n, "string", allocator);
        EXPECT_EQ(1u, o.MemberCount());

        Value count("#");
        o.AddMember(count, o.MemberCount(), allocator);
        EXPECT_EQ(2u, o.MemberCount());
    }

#if RAPIDJSON_HAS_STDSTRING
    {
        // AddMember(StringRefType, const std::string&, Allocator)
        Value o(kObjectType);
        o.AddMember("b", std::string("Banana"), allocator);
        EXPECT_STREQ("Banana", o["b"].GetString());

        // RemoveMember(const std::string&)
        o.RemoveMember(std::string("b"));
        EXPECT_TRUE(o.ObjectEmpty());
    }
#endif

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS
    // AddMember(GenericValue&&, ...) variants
    {
        Value o(kObjectType);
        o.AddMember(Value("true"), Value(true), allocator);
        o.AddMember(Value("false"), Value(false).Move(), allocator);    // value is lvalue ref
        o.AddMember(Value("int").Move(), Value(-1), allocator);         // name is lvalue ref
        o.AddMember("uint", std::move(Value().SetUint(1u)), allocator); // name is literal, value is rvalue
        EXPECT_TRUE(o["true"].GetBool());
        EXPECT_FALSE(o["false"].GetBool());
        EXPECT_EQ(-1, o["int"].GetInt());
        EXPECT_EQ(1u, o["uint"].GetUint());
        EXPECT_EQ(4u, o.MemberCount());
    }
#endif

    // Tests a member with null character
    Value name;
    const Value C0D("C\0D", 3);
    name.SetString(C0D.GetString(), 3);
    value.SetString("CherryD", 7);
    x.AddMember(name, value, allocator);

    // HasMember()
    EXPECT_TRUE(x.HasMember("A"));
    EXPECT_TRUE(x.HasMember("B"));
    EXPECT_TRUE(y.HasMember("A"));
    EXPECT_TRUE(y.HasMember("B"));

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_TRUE(x.HasMember(std::string("A")));
#endif

    name.SetString("C\0D");
    EXPECT_TRUE(x.HasMember(name));
    EXPECT_TRUE(y.HasMember(name));

    GenericValue<UTF8<>, CrtAllocator> othername("A");
    EXPECT_TRUE(x.HasMember(othername));
    EXPECT_TRUE(y.HasMember(othername));
    othername.SetString("C\0D");
    EXPECT_TRUE(x.HasMember(othername));
    EXPECT_TRUE(y.HasMember(othername));

    // operator[]
    EXPECT_STREQ("Apple", x["A"].GetString());
    EXPECT_STREQ("Banana", x["B"].GetString());
    EXPECT_STREQ("CherryD", x[C0D].GetString());
    EXPECT_STREQ("CherryD", x[othername].GetString());
    EXPECT_THROW(x["nonexist"], AssertException);

    // const operator[]
    EXPECT_STREQ("Apple", y["A"].GetString());
    EXPECT_STREQ("Banana", y["B"].GetString());
    EXPECT_STREQ("CherryD", y[C0D].GetString());

#if RAPIDJSON_HAS_STDSTRING
    EXPECT_STREQ("Apple", x["A"].GetString());
    EXPECT_STREQ("Apple", y[std::string("A")].GetString());
#endif

    // member iterator
    Value::MemberIterator itr = x.MemberBegin(); 
    EXPECT_TRUE(itr != x.MemberEnd());
    EXPECT_STREQ("A", itr->name.GetString());
    EXPECT_STREQ("Apple", itr->value.GetString());
    ++itr;
    EXPECT_TRUE(itr != x.MemberEnd());
    EXPECT_STREQ("B", itr->name.GetString());
    EXPECT_STREQ("Banana", itr->value.GetString());
    ++itr;
    EXPECT_TRUE(itr != x.MemberEnd());
    EXPECT_TRUE(memcmp(itr->name.GetString(), "C\0D", 4) == 0);
    EXPECT_STREQ("CherryD", itr->value.GetString());
    ++itr;
    EXPECT_FALSE(itr != x.MemberEnd());

    // const member iterator
    Value::ConstMemberIterator citr = y.MemberBegin(); 
    EXPECT_TRUE(citr != y.MemberEnd());
    EXPECT_STREQ("A", citr->name.GetString());
    EXPECT_STREQ("Apple", citr->value.GetString());
    ++citr;
    EXPECT_TRUE(citr != y.MemberEnd());
    EXPECT_STREQ("B", citr->name.GetString());
    EXPECT_STREQ("Banana", citr->value.GetString());
    ++citr;
    EXPECT_TRUE(citr != y.MemberEnd());
    EXPECT_TRUE(memcmp(citr->name.GetString(), "C\0D", 4) == 0);
    EXPECT_STREQ("CherryD", citr->value.GetString());
    ++citr;
    EXPECT_FALSE(citr != y.MemberEnd());

    // member iterator conversions/relations
    itr  = x.MemberBegin();
    citr = x.MemberBegin(); // const conversion
    TestEqual(itr, citr);
    EXPECT_TRUE(itr < x.MemberEnd());
    EXPECT_FALSE(itr > y.MemberEnd());
    EXPECT_TRUE(citr < x.MemberEnd());
    EXPECT_FALSE(citr > y.MemberEnd());
    ++citr;
    TestUnequal(itr, citr);
    EXPECT_FALSE(itr < itr);
    EXPECT_TRUE(itr < citr);
    EXPECT_FALSE(itr > itr);
    EXPECT_TRUE(citr > itr);
    EXPECT_EQ(1, citr - x.MemberBegin());
    EXPECT_EQ(0, itr - y.MemberBegin());
    itr += citr - x.MemberBegin();
    EXPECT_EQ(1, itr - y.MemberBegin());
    TestEqual(citr, itr);
    EXPECT_TRUE(itr <= citr);
    EXPECT_TRUE(citr <= itr);
    itr++;
    EXPECT_TRUE(itr >= citr);
    EXPECT_FALSE(citr >= itr);

    // RemoveMember()
    EXPECT_TRUE(x.RemoveMember("A"));
    EXPECT_FALSE(x.HasMember("A"));

    EXPECT_TRUE(x.RemoveMember("B"));
    EXPECT_FALSE(x.HasMember("B"));

    EXPECT_FALSE(x.RemoveMember("nonexist"));

    EXPECT_TRUE(x.RemoveMember(othername));
    EXPECT_FALSE(x.HasMember(name));

    EXPECT_TRUE(x.MemberBegin() == x.MemberEnd());

    // EraseMember(ConstMemberIterator)

    // Use array members to ensure removed elements' destructor is called.
    // { "a": [0], "b": [1],[2],...]
    const char keys[][2] = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
    for (int i = 0; i < 10; i++)
        x.AddMember(keys[i], Value(kArrayType).PushBack(i, allocator), allocator);

    // MemberCount, iterator difference
    EXPECT_EQ(x.MemberCount(), SizeType(x.MemberEnd() - x.MemberBegin()));

    // Erase the first
    itr = x.EraseMember(x.MemberBegin());
    EXPECT_FALSE(x.HasMember(keys[0]));
    EXPECT_EQ(x.MemberBegin(), itr);
    EXPECT_EQ(9u, x.MemberCount());
    for (; itr != x.MemberEnd(); ++itr) {
        size_t i = static_cast<size_t>((itr - x.MemberBegin())) + 1;
        EXPECT_STREQ(itr->name.GetString(), keys[i]);
        EXPECT_EQ(static_cast<int>(i), itr->value[0].GetInt());
    }

    // Erase the last
    itr = x.EraseMember(x.MemberEnd() - 1);
    EXPECT_FALSE(x.HasMember(keys[9]));
    EXPECT_EQ(x.MemberEnd(), itr);
    EXPECT_EQ(8u, x.MemberCount());
    for (; itr != x.MemberEnd(); ++itr) {
        size_t i = static_cast<size_t>(itr - x.MemberBegin()) + 1;
        EXPECT_STREQ(itr->name.GetString(), keys[i]);
        EXPECT_EQ(static_cast<int>(i), itr->value[0].GetInt());
    }

    // Erase the middle
    itr = x.EraseMember(x.MemberBegin() + 4);
    EXPECT_FALSE(x.HasMember(keys[5]));
    EXPECT_EQ(x.MemberBegin() + 4, itr);
    EXPECT_EQ(7u, x.MemberCount());
    for (; itr != x.MemberEnd(); ++itr) {
        size_t i = static_cast<size_t>(itr - x.MemberBegin());
        i += (i < 4) ? 1 : 2;
        EXPECT_STREQ(itr->name.GetString(), keys[i]);
        EXPECT_EQ(static_cast<int>(i), itr->value[0].GetInt());
    }

    // EraseMember(ConstMemberIterator, ConstMemberIterator)
    // Exhaustive test with all 0 <= first < n, first <= last <= n cases
    const unsigned n = 10;
    for (unsigned first = 0; first < n; first++) {
        for (unsigned last = first; last <= n; last++) {
            x.RemoveAllMembers();
            for (unsigned i = 0; i < n; i++)
                x.AddMember(keys[i], Value(kArrayType).PushBack(i, allocator), allocator);

            itr = x.EraseMember(x.MemberBegin() + static_cast<int>(first), x.MemberBegin() + static_cast<int>(last));
            if (last == n)
                EXPECT_EQ(x.MemberEnd(), itr);
            else
                EXPECT_EQ(x.MemberBegin() + static_cast<int>(first), itr);

            size_t removeCount = last - first;
            EXPECT_EQ(n - removeCount, x.MemberCount());
            for (unsigned i = 0; i < first; i++)
                EXPECT_EQ(i, x[keys[i]][0].GetUint());
            for (unsigned i = first; i < n - removeCount; i++)
                EXPECT_EQ(i + removeCount, x[keys[i+removeCount]][0].GetUint());
        }
    }

    // RemoveAllMembers()
    x.RemoveAllMembers();
    EXPECT_TRUE(x.ObjectEmpty());
    EXPECT_EQ(0u, x.MemberCount());
}

TEST(Value, Object) {
    Value::AllocatorType allocator;
    Value x(kObjectType);
    const Value& y = x; // const version

    EXPECT_EQ(kObjectType, x.GetType());
    EXPECT_TRUE(x.IsObject());
    EXPECT_TRUE(x.ObjectEmpty());
    EXPECT_EQ(0u, x.MemberCount());
    EXPECT_EQ(kObjectType, y.GetType());
    EXPECT_TRUE(y.IsObject());
    EXPECT_TRUE(y.ObjectEmpty());
    EXPECT_EQ(0u, y.MemberCount());

    TestObject(x, allocator);

    // SetObject()
    Value z;
    z.SetObject();
    EXPECT_TRUE(z.IsObject());
}

TEST(Value, ObjectHelper) {
    Value::AllocatorType allocator;
    {
        Value x(kObjectType);
        Value::Object o = x.GetObject();
        TestObject(o, allocator);
    }

    {
        Value x(kObjectType);
        Value::Object o = x.GetObject();
        o.AddMember("1", 1, allocator);

        Value::Object o2(o); // copy constructor
        EXPECT_EQ(1u, o2.MemberCount());

        Value::Object o3 = o;
        EXPECT_EQ(1u, o3.MemberCount());

        Value::ConstObject y = static_cast<const Value&>(x).GetObject();
        (void)y;
        // y.AddMember("1", 1, allocator); // should not compile

        // Templated functions
        x.RemoveAllMembers();
        EXPECT_TRUE(x.Is<Value::Object>());
        EXPECT_TRUE(x.Is<Value::ConstObject>());
        o.AddMember("1", 1, allocator);
        EXPECT_EQ(1, x.Get<Value::Object>()["1"].GetInt());
        EXPECT_EQ(1, x.Get<Value::ConstObject>()["1"].GetInt());

        Value x2;
        x2.Set<Value::Object>(o);
        EXPECT_TRUE(x.IsObject());   // IsObject() is invariant after moving
        EXPECT_EQ(1, x2.Get<Value::Object>()["1"].GetInt());
    }

    {
        Value x(kObjectType);
        x.AddMember("a", "apple", allocator);
        Value y(x.GetObject());
        EXPECT_STREQ("apple", y["a"].GetString());
        EXPECT_TRUE(x.IsObject());  // Invariant
    }
    
    {
        Value x(kObjectType);
        x.AddMember("a", "apple", allocator);
        Value y(kObjectType);
        y.AddMember("fruits", x.GetObject(), allocator);
        EXPECT_STREQ("apple", y["fruits"]["a"].GetString());
        EXPECT_TRUE(x.IsObject());  // Invariant
    }
}

#if RAPIDJSON_HAS_CXX11_RANGE_FOR
TEST(Value, ObjectHelperRangeFor) {
    Value::AllocatorType allocator;
    Value x(kObjectType);

    for (int i = 0; i < 10; i++) {
        char name[10];
        Value n(name, static_cast<SizeType>(sprintf(name, "%d", i)), allocator);
        x.AddMember(n, i, allocator);
    }

    {
        int i = 0;
        for (auto& m : x.GetObject()) {
            char name[11];
            sprintf(name, "%d", i);
            EXPECT_STREQ(name, m.name.GetString());
            EXPECT_EQ(i, m.value.GetInt());
            i++;
        }
        EXPECT_EQ(i, 10);
    }
    {
        int i = 0;
        for (const auto& m : const_cast<const Value&>(x).GetObject()) {
            char name[11];
            sprintf(name, "%d", i);
            EXPECT_STREQ(name, m.name.GetString());
            EXPECT_EQ(i, m.value.GetInt());
            i++;
        }
        EXPECT_EQ(i, 10);
    }

    // Object a = x.GetObject();
    // Object ca = const_cast<const Value&>(x).GetObject();
}
#endif

TEST(Value, EraseMember_String) {
    Value::AllocatorType allocator;
    Value x(kObjectType);
    x.AddMember("A", "Apple", allocator);
    x.AddMember("B", "Banana", allocator);

    EXPECT_TRUE(x.EraseMember("B"));
    EXPECT_FALSE(x.HasMember("B"));

    EXPECT_FALSE(x.EraseMember("nonexist"));

    GenericValue<UTF8<>, CrtAllocator> othername("A");
    EXPECT_TRUE(x.EraseMember(othername));
    EXPECT_FALSE(x.HasMember("A"));

    EXPECT_TRUE(x.MemberBegin() == x.MemberEnd());
}

TEST(Value, BigNestedArray) {
    MemoryPoolAllocator<> allocator;
    Value x(kArrayType);
    static const SizeType  n = 200;

    for (SizeType i = 0; i < n; i++) {
        Value y(kArrayType);
        for (SizeType  j = 0; j < n; j++) {
            Value number(static_cast<int>(i * n + j));
            y.PushBack(number, allocator);
        }
        x.PushBack(y, allocator);
    }

    for (SizeType i = 0; i < n; i++)
        for (SizeType j = 0; j < n; j++) {
            EXPECT_TRUE(x[i][j].IsInt());
            EXPECT_EQ(static_cast<int>(i * n + j), x[i][j].GetInt());
        }
}

TEST(Value, BigNestedObject) {
    MemoryPoolAllocator<> allocator;
    Value x(kObjectType);
    static const SizeType n = 200;

    for (SizeType i = 0; i < n; i++) {
        char name1[10];
        sprintf(name1, "%d", i);

        // Value name(name1); // should not compile
        Value name(name1, static_cast<SizeType>(strlen(name1)), allocator);
        Value object(kObjectType);

        for (SizeType j = 0; j < n; j++) {
            char name2[10];
            sprintf(name2, "%d", j);

            Value name3(name2, static_cast<SizeType>(strlen(name2)), allocator);
            Value number(static_cast<int>(i * n + j));
            object.AddMember(name3, number, allocator);
        }

        // x.AddMember(name1, object, allocator); // should not compile
        x.AddMember(name, object, allocator);
    }

    for (SizeType i = 0; i < n; i++) {
        char name1[10];
        sprintf(name1, "%d", i);
        
        for (SizeType j = 0; j < n; j++) {
            char name2[10];
            sprintf(name2, "%d", j);
            x[name1];
            EXPECT_EQ(static_cast<int>(i * n + j), x[name1][name2].GetInt());
        }
    }
}

// Issue 18: Error removing last element of object
// http://code.google.com/p/rapidjson/issues/detail?id=18
TEST(Value, RemoveLastElement) {
    rapidjson::Document doc;
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
    rapidjson::Value objVal(rapidjson::kObjectType);        
    objVal.AddMember("var1", 123, allocator);       
    objVal.AddMember("var2", "444", allocator);
    objVal.AddMember("var3", 555, allocator);
    EXPECT_TRUE(objVal.HasMember("var3"));
    objVal.RemoveMember("var3");    // Assertion here in r61
    EXPECT_FALSE(objVal.HasMember("var3"));
}

// Issue 38:    Segmentation fault with CrtAllocator
TEST(Document, CrtAllocator) {
    typedef GenericValue<UTF8<>, CrtAllocator> V;

    V::AllocatorType allocator;
    V o(kObjectType);
    o.AddMember("x", 1, allocator); // Should not call destructor on uninitialized name/value of newly allocated members.

    V a(kArrayType);
    a.PushBack(1, allocator);   // Should not call destructor on uninitialized Value of newly allocated elements.
}

static void TestShortStringOptimization(const char* str) {
    const rapidjson::SizeType len = static_cast<rapidjson::SizeType>(strlen(str));
	
    rapidjson::Document doc;
    rapidjson::Value val;
    val.SetString(str, len, doc.GetAllocator());
	
	EXPECT_EQ(val.GetStringLength(), len);
	EXPECT_STREQ(val.GetString(), str);
}

TEST(Value, AllocateShortString) {
	TestShortStringOptimization("");                 // edge case: empty string
	TestShortStringOptimization("12345678");         // regular case for short strings: 8 chars
	TestShortStringOptimization("12345678901");      // edge case: 11 chars in 32-bit mode (=> short string)
	TestShortStringOptimization("123456789012");     // edge case: 12 chars in 32-bit mode (=> regular string)
	TestShortStringOptimization("123456789012345");  // edge case: 15 chars in 64-bit mode (=> short string)
	TestShortStringOptimization("1234567890123456"); // edge case: 16 chars in 64-bit mode (=> regular string)
}

template <int e>
struct TerminateHandler {
    bool Null() { return e != 0; }
    bool Bool(bool) { return e != 1; }
    bool Int(int) { return e != 2; }
    bool Uint(unsigned) { return e != 3; }
    bool Int64(int64_t) { return e != 4; }
    bool Uint64(uint64_t) { return e != 5; }
    bool Double(double) { return e != 6; }
    bool RawNumber(const char*, SizeType, bool) { return e != 7; }
    bool String(const char*, SizeType, bool) { return e != 8; }
    bool StartObject() { return e != 9; }
    bool Key(const char*, SizeType, bool)  { return e != 10; }
    bool EndObject(SizeType) { return e != 11; }
    bool StartArray() { return e != 12; }
    bool EndArray(SizeType) { return e != 13; }
};

#define TEST_TERMINATION(e, json)\
{\
    Document d; \
    EXPECT_FALSE(d.Parse(json).HasParseError()); \
    Reader reader; \
    TerminateHandler<e> h;\
    EXPECT_FALSE(d.Accept(h));\
}

TEST(Value, AcceptTerminationByHandler) {
    TEST_TERMINATION(0, "[null]");
    TEST_TERMINATION(1, "[true]");
    TEST_TERMINATION(1, "[false]");
    TEST_TERMINATION(2, "[-1]");
    TEST_TERMINATION(3, "[2147483648]");
    TEST_TERMINATION(4, "[-1234567890123456789]");
    TEST_TERMINATION(5, "[9223372036854775808]");
    TEST_TERMINATION(6, "[0.5]");
    // RawNumber() is never called
    TEST_TERMINATION(8, "[\"a\"]");
    TEST_TERMINATION(9, "[{}]");
    TEST_TERMINATION(10, "[{\"a\":1}]");
    TEST_TERMINATION(11, "[{}]");
    TEST_TERMINATION(12, "{\"a\":[]}");
    TEST_TERMINATION(13, "{\"a\":[]}");
}

struct ValueIntComparer {
    bool operator()(const Value& lhs, const Value& rhs) const {
        return lhs.GetInt() < rhs.GetInt();
    }
};

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS
TEST(Value, Sorting) {
    Value::AllocatorType allocator;
    Value a(kArrayType);
    a.PushBack(5, allocator);
    a.PushBack(1, allocator);
    a.PushBack(3, allocator);
    std::sort(a.Begin(), a.End(), ValueIntComparer());
    EXPECT_EQ(1, a[0].GetInt());
    EXPECT_EQ(3, a[1].GetInt());
    EXPECT_EQ(5, a[2].GetInt());
}
#endif

// http://stackoverflow.com/questions/35222230/

static void MergeDuplicateKey(Value& v, Value::AllocatorType& a) {
    if (v.IsObject()) {
        // Convert all key:value into key:[value]
        for (Value::MemberIterator itr = v.MemberBegin(); itr != v.MemberEnd(); ++itr)
            itr->value = Value(kArrayType).Move().PushBack(itr->value, a);
        
        // Merge arrays if key is duplicated
        for (Value::MemberIterator itr = v.MemberBegin(); itr != v.MemberEnd();) {
            Value::MemberIterator itr2 = v.FindMember(itr->name);
            if (itr != itr2) {
                itr2->value.PushBack(itr->value[0], a);
                itr = v.EraseMember(itr);
            }
            else
                ++itr;
        }

        // Convert key:[values] back to key:value if there is only one value
        for (Value::MemberIterator itr = v.MemberBegin(); itr != v.MemberEnd(); ++itr) {
            if (itr->value.Size() == 1)
                itr->value = itr->value[0];
            MergeDuplicateKey(itr->value, a); // Recursion on the value
        }
    }
    else if (v.IsArray())
        for (Value::ValueIterator itr = v.Begin(); itr != v.End(); ++itr)
            MergeDuplicateKey(*itr, a);
}

TEST(Value, MergeDuplicateKey) {
    Document d;
    d.Parse(
        "{"
        "    \"key1\": {"
        "        \"a\": \"asdf\","
        "        \"b\": \"foo\","
        "        \"b\": \"bar\","
        "        \"c\": \"fdas\""
        "    }"
        "}");

    Document d2;
    d2.Parse(
        "{"
        "    \"key1\": {"
        "        \"a\": \"asdf\","
        "        \"b\": ["
        "            \"foo\","
        "            \"bar\""
        "        ],"
        "        \"c\": \"fdas\""
        "    }"
        "}");

    EXPECT_NE(d2, d);
    MergeDuplicateKey(d, d.GetAllocator());
    EXPECT_EQ(d2, d);
}

#ifdef __clang__
RAPIDJSON_DIAG_POP
#endif
