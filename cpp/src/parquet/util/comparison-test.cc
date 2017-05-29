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

TEST(Comparison, ByteArray) {
  NodePtr node = PrimitiveNode::Make("bytearray", Repetition::REQUIRED, Type::BYTE_ARRAY);
  ColumnDescriptor descr(node, 0, 0);
  Compare<parquet::ByteArray> less(&descr);

  std::string a = "arrange";
  std::string b = "arrangement";
  auto arr1 = ByteArrayFromString(a);
  auto arr2 = ByteArrayFromString(b);
  ASSERT_TRUE(less(arr1, arr2));

  a = u8"braten";
  b = u8"bügeln";
  auto arr3 = ByteArrayFromString(a);
  auto arr4 = ByteArrayFromString(b);
  // see PARQUET-686 discussion about binary comparison
  ASSERT_TRUE(!less(arr3, arr4));
}

TEST(Comparison, FLBA) {
  std::string a = "Antidisestablishmentarianism";
  std::string b = "Bundesgesundheitsministerium";
  auto arr1 = FLBAFromString(a);
  auto arr2 = FLBAFromString(b);

  NodePtr node = PrimitiveNode::Make("FLBA", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE, static_cast<int>(a.size()));
  ColumnDescriptor descr(node, 0, 0);
  Compare<parquet::FixedLenByteArray> less(&descr);
  ASSERT_TRUE(less(arr1, arr2));
}

TEST(Comparison, Int96) {
  parquet::Int96 a{{1, 41, 14}}, b{{1, 41, 42}};

  NodePtr node = PrimitiveNode::Make("int96", Repetition::REQUIRED, Type::INT96);
  ColumnDescriptor descr(node, 0, 0);
  Compare<parquet::Int96> less(&descr);
  ASSERT_TRUE(less(a, b));
  b.value[2] = 14;
  ASSERT_TRUE(!less(a, b) && !less(b, a));
}

}  // namespace test

}  // namespace parquet
