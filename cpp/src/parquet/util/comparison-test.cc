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

#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>
#include <vector>

#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/comparison.h"

namespace parquet {

namespace test {

using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

static ByteArray ByteArrayFromString(const std::string& s) {
  auto ptr = reinterpret_cast<const uint8_t*>(s.data());
  return ByteArray(static_cast<uint32_t>(s.size()), ptr);
}

static FLBA FLBAFromString(const std::string& s) {
  auto ptr = reinterpret_cast<const uint8_t*>(s.data());
  return FLBA(ptr);
}

TEST(Comparison, signedByteArray) {
  NodePtr node =
      PrimitiveNode::Make("SignedByteArray", Repetition::REQUIRED, Type::BYTE_ARRAY);
  ColumnDescriptor descr(node, 0, 0);

  CompareDefaultByteArray less;

  std::string s1 = "12345";
  std::string s2 = "12345678";
  ByteArray s1ba = ByteArrayFromString(s1);
  ByteArray s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(less(s1ba, s2ba));

  // This is case where signed comparision UTF-8 (PARQUET-686) is incorrect
  // This example is to only check signed comparison and not UTF-8.
  s1 = u8"bügeln";
  s2 = u8"braten";
  s1ba = ByteArrayFromString(s1);
  s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(less(s1ba, s2ba));
}

TEST(Comparison, UnsignedByteArray) {
  NodePtr node = PrimitiveNode::Make("UnsignedByteArray", Repetition::REQUIRED,
                                     Type::BYTE_ARRAY, LogicalType::UTF8);
  ColumnDescriptor descr(node, 0, 0);

  // Check if UTF-8 is compared using unsigned correctly
  CompareUnsignedByteArray uless;

  std::string s1 = "arrange";
  std::string s2 = "arrangement";
  ByteArray s1ba = ByteArrayFromString(s1);
  ByteArray s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(uless(s1ba, s2ba));

  // Multi-byte UTF-8 characters
  s1 = u8"braten";
  s2 = u8"bügeln";
  s1ba = ByteArrayFromString(s1);
  s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(uless(s1ba, s2ba));

  s1 = u8"ünk123456";  // ü = 252
  s2 = u8"ănk123456";  // ă = 259
  s1ba = ByteArrayFromString(s1);
  s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(uless(s1ba, s2ba));
}

TEST(Comparison, SignedFLBA) {
  int size = 10;
  NodePtr node = PrimitiveNode::Make("SignedFLBA", Repetition::REQUIRED,
                                     Type::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE, size);
  ColumnDescriptor descr(node, 0, 0);

  CompareDefaultFLBA less(descr.type_length());

  std::string s1 = "Anti123456";
  std::string s2 = "Bunkd123456";
  FLBA s1flba = FLBAFromString(s1);
  FLBA s2flba = FLBAFromString(s2);
  ASSERT_TRUE(less(s1flba, s2flba));

  s1 = "Bünk123456";
  s2 = "Bunk123456";
  s1flba = FLBAFromString(s1);
  s2flba = FLBAFromString(s2);
  ASSERT_TRUE(less(s1flba, s2flba));
}

TEST(Comparison, UnsignedFLBA) {
  int size = 10;
  NodePtr node = PrimitiveNode::Make("UnsignedFLBA", Repetition::REQUIRED,
                                     Type::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE, size);
  ColumnDescriptor descr(node, 0, 0);

  CompareUnsignedFLBA uless(descr.type_length());

  std::string s1 = "Anti123456";
  std::string s2 = "Bunkd123456";
  FLBA s1flba = FLBAFromString(s1);
  FLBA s2flba = FLBAFromString(s2);
  ASSERT_TRUE(uless(s1flba, s2flba));

  s1 = "Bunk123456";
  s2 = "Bünk123456";
  s1flba = FLBAFromString(s1);
  s2flba = FLBAFromString(s2);
  ASSERT_TRUE(uless(s1flba, s2flba));
}

TEST(Comparison, SignedInt96) {
  parquet::Int96 a{{1, 41, 14}}, b{{1, 41, 42}};
  parquet::Int96 aa{{1, 41, 14}}, bb{{1, 41, 14}};
  parquet::Int96 aaa{{1, 41, static_cast<uint32_t>(-14)}}, bbb{{1, 41, 42}};

  NodePtr node = PrimitiveNode::Make("SignedInt96", Repetition::REQUIRED, Type::INT96);
  ColumnDescriptor descr(node, 0, 0);

  CompareDefaultInt96 less;

  ASSERT_TRUE(less(a, b));
  ASSERT_TRUE(!less(aa, bb) && !less(bb, aa));
  ASSERT_TRUE(less(aaa, bbb));
}

TEST(Comparison, UnsignedInt96) {
  parquet::Int96 a{{1, 41, 14}}, b{{1, static_cast<uint32_t>(-41), 42}};
  parquet::Int96 aa{{1, 41, 14}}, bb{{1, 41, static_cast<uint32_t>(-14)}};
  parquet::Int96 aaa, bbb;

  NodePtr node = PrimitiveNode::Make("UnsignedInt96", Repetition::REQUIRED, Type::INT96);
  ColumnDescriptor descr(node, 0, 0);

  CompareUnsignedInt96 uless;

  ASSERT_TRUE(uless(a, b));
  ASSERT_TRUE(uless(aa, bb));

  // INT96 Timestamp
  aaa.value[2] = 2451545;  // 2000-01-01
  bbb.value[2] = 2451546;  // 2000-01-02
  // 12 hours + 34 minutes + 56 seconds.
  Int96SetNanoSeconds(aaa, 45296000000000);
  // 12 hours + 34 minutes + 50 seconds.
  Int96SetNanoSeconds(bbb, 45290000000000);
  ASSERT_TRUE(uless(aaa, bbb));

  aaa.value[2] = 2451545;  // 2000-01-01
  bbb.value[2] = 2451545;  // 2000-01-01
  // 11 hours + 34 minutes + 56 seconds.
  Int96SetNanoSeconds(aaa, 41696000000000);
  // 12 hours + 34 minutes + 50 seconds.
  Int96SetNanoSeconds(bbb, 45290000000000);
  ASSERT_TRUE(uless(aaa, bbb));

  aaa.value[2] = 2451545;  // 2000-01-01
  bbb.value[2] = 2451545;  // 2000-01-01
  // 12 hours + 34 minutes + 55 seconds.
  Int96SetNanoSeconds(aaa, 45295000000000);
  // 12 hours + 34 minutes + 56 seconds.
  Int96SetNanoSeconds(bbb, 45296000000000);
  ASSERT_TRUE(uless(aaa, bbb));
}

TEST(Comparison, SignedInt64) {
  int64_t a = 1, b = 4;
  int64_t aa = 1, bb = 1;
  int64_t aaa = -1, bbb = 1;

  NodePtr node = PrimitiveNode::Make("SignedInt64", Repetition::REQUIRED, Type::INT64);
  ColumnDescriptor descr(node, 0, 0);

  CompareDefaultInt64 less;

  ASSERT_TRUE(less(a, b));
  ASSERT_TRUE(!less(aa, bb) && !less(bb, aa));
  ASSERT_TRUE(less(aaa, bbb));
}

TEST(Comparison, UnsignedInt64) {
  uint64_t a = 1, b = 4;
  uint64_t aa = 1, bb = 1;
  uint64_t aaa = 1, bbb = -1;

  NodePtr node = PrimitiveNode::Make("UnsignedInt64", Repetition::REQUIRED, Type::INT64);
  ColumnDescriptor descr(node, 0, 0);

  CompareUnsignedInt64 less;

  ASSERT_TRUE(less(a, b));
  ASSERT_TRUE(!less(aa, bb) && !less(bb, aa));
  ASSERT_TRUE(less(aaa, bbb));
}

TEST(Comparison, UnsignedInt32) {
  uint32_t a = 1, b = 4;
  uint32_t aa = 1, bb = 1;
  uint32_t aaa = 1, bbb = -1;

  NodePtr node = PrimitiveNode::Make("UnsignedInt32", Repetition::REQUIRED, Type::INT32);
  ColumnDescriptor descr(node, 0, 0);

  CompareUnsignedInt32 less;

  ASSERT_TRUE(less(a, b));
  ASSERT_TRUE(!less(aa, bb) && !less(bb, aa));
  ASSERT_TRUE(less(aaa, bbb));
}

TEST(Comparison, UnknownSortOrder) {
  NodePtr node =
      PrimitiveNode::Make("Unknown", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          LogicalType::INTERVAL, 12);
  ColumnDescriptor descr(node, 0, 0);

  ASSERT_THROW(Comparator::Make(&descr), ParquetException);
}

}  // namespace test

}  // namespace parquet
