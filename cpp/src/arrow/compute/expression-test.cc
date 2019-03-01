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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/compute/operation.h"

namespace arrow {
namespace compute {

// A placeholder operator implementation to use for testing various Expr
// behavior
class DummyOp : public Operation {
 public:
  Status ToExpr(std::shared_ptr<Expr>* out) const override {
    return Status::NotImplemented("NYI");
  }
};

TEST(TestLogicalType, NonNestedToString) {
  std::vector<std::pair<std::shared_ptr<LogicalType>, std::string>> cases = {
      {type::any(), "Any"},
      {type::null(), "Null"},
      {type::boolean(), "Bool"},
      {type::number(), "Number"},
      {type::floating(), "Floating"},
      {type::integer(), "Integer"},
      {type::signed_integer(), "SignedInteger"},
      {type::unsigned_integer(), "UnsignedInteger"},
      {type::int8(), "Int8"},
      {type::int16(), "Int16"},
      {type::int32(), "Int32"},
      {type::int64(), "Int64"},
      {type::uint8(), "UInt8"},
      {type::uint16(), "UInt16"},
      {type::uint32(), "UInt32"},
      {type::uint64(), "UInt64"},
      {type::float_(), "Float"},
      {type::double_(), "Double"},
      {type::binary(), "Binary"},
      {type::utf8(), "Utf8"}};

  for (auto& case_ : cases) {
    ASSERT_EQ(case_.second, case_.first->ToString());
  }
}

TEST(TestLogicalType, Any) {
  auto op = std::make_shared<DummyOp>();
  auto t = type::any();
  ASSERT_TRUE(t->IsInstance(*std::make_shared<scalar::Int32>(op)));
  ASSERT_TRUE(t->IsInstance(*std::make_shared<array::Binary>(op)));
}

TEST(TestLogicalType, Number) {
  auto op = std::make_shared<DummyOp>();
  auto t = type::number();

  ASSERT_TRUE(t->IsInstance(*std::make_shared<scalar::Int32>(op)));
  ASSERT_TRUE(t->IsInstance(*std::make_shared<scalar::Double>(op)));
  ASSERT_FALSE(t->IsInstance(*std::make_shared<scalar::Bool>(op)));
  ASSERT_FALSE(t->IsInstance(*std::make_shared<scalar::Null>(op)));
  ASSERT_FALSE(t->IsInstance(*std::make_shared<scalar::Binary>(op)));
}

TEST(TestLogicalType, IntegerBaseTypes) {
  auto op = std::make_shared<DummyOp>();
  auto all_ty = type::integer();
  auto signed_ty = type::signed_integer();
  auto unsigned_ty = type::unsigned_integer();

  ASSERT_TRUE(all_ty->IsInstance(*std::make_shared<scalar::Int32>(op)));
  ASSERT_TRUE(all_ty->IsInstance(*std::make_shared<scalar::UInt32>(op)));
  ASSERT_FALSE(all_ty->IsInstance(*std::make_shared<array::Double>(op)));
  ASSERT_FALSE(all_ty->IsInstance(*std::make_shared<array::Binary>(op)));

  ASSERT_TRUE(signed_ty->IsInstance(*std::make_shared<array::Int32>(op)));
  ASSERT_FALSE(signed_ty->IsInstance(*std::make_shared<scalar::UInt32>(op)));

  ASSERT_TRUE(unsigned_ty->IsInstance(*std::make_shared<scalar::UInt32>(op)));
  ASSERT_TRUE(unsigned_ty->IsInstance(*std::make_shared<array::UInt32>(op)));
  ASSERT_FALSE(unsigned_ty->IsInstance(*std::make_shared<array::Int8>(op)));
}

TEST(TestLogicalType, ConcreteTypes) { auto op = std::make_shared<DummyOp>(); }

}  // namespace compute
}  // namespace arrow
