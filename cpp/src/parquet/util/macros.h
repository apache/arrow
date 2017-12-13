// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PARQUET_UTIL_MACROS_H
#define PARQUET_UTIL_MACROS_H

// Useful macros from elsewhere

// From Google gutil
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;      \
  void operator=(const TypeName&) = delete
#endif

#if defined(__GNUC__)
#define PARQUET_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PARQUET_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define PARQUET_NORETURN __attribute__((noreturn))
#define PARQUET_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define PARQUET_NORETURN __declspec(noreturn)
#define PARQUET_PREDICT_FALSE(x) x
#define PARQUET_PREDICT_TRUE(x) x
#define PARQUET_PREFETCH(addr)
#else
#define PARQUET_NORETURN
#define PARQUET_PREDICT_FALSE(x) x
#define PARQUET_PREDICT_TRUE(x) x
#define PARQUET_PREFETCH(addr)
#endif

// ----------------------------------------------------------------------
// From googletest

// When you need to test the private or protected members of a class,
// use the FRIEND_TEST macro to declare your tests as friends of the
// class.  For example:
//
// class MyClass {
//  private:
//   void MyMethod();
//   FRIEND_TEST(MyClassTest, MyMethod);
// };
//
// class MyClassTest : public testing::Test {
//   // ...
// };
//
// TEST_F(MyClassTest, MyMethod) {
//   // Can call MyClass::MyMethod() here.
// }

#define FRIEND_TEST(test_case_name, test_name) \
  friend class test_case_name##_##test_name##_Test

#endif  // PARQUET_UTIL_MACROS_H
