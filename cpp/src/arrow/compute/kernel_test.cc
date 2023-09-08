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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array/util.h"
#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// TypeMatcher

TEST(TypeMatcher, SameTypeId) {
  std::shared_ptr<TypeMatcher> matcher = match::SameTypeId(Type::DECIMAL);
  ASSERT_TRUE(matcher->Matches(*decimal(12, 2)));
  ASSERT_FALSE(matcher->Matches(*int8()));

  ASSERT_EQ("Type::DECIMAL128", matcher->ToString());

  ASSERT_TRUE(matcher->Equals(*matcher));
  ASSERT_TRUE(matcher->Equals(*match::SameTypeId(Type::DECIMAL)));
  ASSERT_FALSE(matcher->Equals(*match::SameTypeId(Type::TIMESTAMP)));
}

TEST(TypeMatcher, TimestampTypeUnit) {
  auto matcher = match::TimestampTypeUnit(TimeUnit::MILLI);
  auto matcher2 = match::Time32TypeUnit(TimeUnit::MILLI);

  ASSERT_TRUE(matcher->Matches(*timestamp(TimeUnit::MILLI)));
  ASSERT_TRUE(matcher->Matches(*timestamp(TimeUnit::MILLI, "utc")));
  ASSERT_FALSE(matcher->Matches(*timestamp(TimeUnit::SECOND)));
  ASSERT_FALSE(matcher->Matches(*time32(TimeUnit::MILLI)));
  ASSERT_TRUE(matcher2->Matches(*time32(TimeUnit::MILLI)));

  // Check ToString representation
  ASSERT_EQ("timestamp(s)", match::TimestampTypeUnit(TimeUnit::SECOND)->ToString());
  ASSERT_EQ("timestamp(ms)", match::TimestampTypeUnit(TimeUnit::MILLI)->ToString());
  ASSERT_EQ("timestamp(us)", match::TimestampTypeUnit(TimeUnit::MICRO)->ToString());
  ASSERT_EQ("timestamp(ns)", match::TimestampTypeUnit(TimeUnit::NANO)->ToString());

  // Equals implementation
  ASSERT_TRUE(matcher->Equals(*matcher));
  ASSERT_TRUE(matcher->Equals(*match::TimestampTypeUnit(TimeUnit::MILLI)));
  ASSERT_FALSE(matcher->Equals(*match::TimestampTypeUnit(TimeUnit::MICRO)));
  ASSERT_FALSE(matcher->Equals(*match::Time32TypeUnit(TimeUnit::MILLI)));
}

TEST(TypeMatcher, RunEndInteger) {
  auto matcher = match::RunEndInteger();
  ASSERT_FALSE(matcher->Matches(*int8()));
  ASSERT_FALSE(matcher->Matches(*uint8()));
  ASSERT_FALSE(matcher->Matches(*uint16()));
  ASSERT_FALSE(matcher->Matches(*uint32()));
  ASSERT_FALSE(matcher->Matches(*uint64()));

  ASSERT_TRUE(matcher->Matches(*int16()));
  ASSERT_TRUE(matcher->Matches(*int32()));
  ASSERT_TRUE(matcher->Matches(*int64()));
}

TEST(TypeMatcher, RunEndEncoded) {
  auto string_ree_matcher = match::RunEndEncoded(match::SameTypeId(Type::STRING));
  auto int32_ree_matcher = match::RunEndEncoded(match::SameTypeId(Type::INT32));
  auto empty_ree_matcher =
      match::RunEndEncoded(match::SameTypeId(Type::INT8), match::SameTypeId(Type::INT32));
  for (auto run_end_type : {int16(), int32(), int64()}) {
    ASSERT_TRUE(string_ree_matcher->Matches(*run_end_encoded(run_end_type, utf8())));
    ASSERT_FALSE(string_ree_matcher->Matches(*run_end_encoded(run_end_type, int32())));

    ASSERT_FALSE(int32_ree_matcher->Matches(*run_end_encoded(run_end_type, utf8())));
    ASSERT_TRUE(int32_ree_matcher->Matches(*run_end_encoded(run_end_type, int32())));

    ASSERT_FALSE(empty_ree_matcher->Matches(*run_end_encoded(run_end_type, int32())));
  }
}

// ----------------------------------------------------------------------
// InputType

TEST(InputType, AnyTypeConstructor) {
  // Check the ANY_TYPE ctors
  InputType ty;
  ASSERT_EQ(InputType::ANY_TYPE, ty.kind());
}

TEST(InputType, Constructors) {
  // Exact type constructor
  InputType ty1(int8());
  ASSERT_EQ(InputType::EXACT_TYPE, ty1.kind());
  AssertTypeEqual(*int8(), *ty1.type());

  InputType ty1_implicit = int8();
  ASSERT_TRUE(ty1.Equals(ty1_implicit));

  // Same type id constructor
  InputType ty2(Type::DECIMAL);
  ASSERT_EQ(InputType::USE_TYPE_MATCHER, ty2.kind());
  ASSERT_EQ("Type::DECIMAL128", ty2.ToString());
  ASSERT_TRUE(ty2.type_matcher().Matches(*decimal(12, 2)));
  ASSERT_FALSE(ty2.type_matcher().Matches(*int16()));

  // Implicit construction in a vector
  std::vector<InputType> types = {int8(), InputType(Type::DECIMAL)};
  ASSERT_TRUE(types[0].Equals(ty1));
  ASSERT_TRUE(types[1].Equals(ty2));

  // Copy constructor
  InputType ty3 = ty1;
  InputType ty4 = ty2;
  ASSERT_TRUE(ty3.Equals(ty1));
  ASSERT_TRUE(ty4.Equals(ty2));

  // Move constructor
  InputType ty5 = std::move(ty3);
  InputType ty6 = std::move(ty4);
  ASSERT_TRUE(ty5.Equals(ty1));
  ASSERT_TRUE(ty6.Equals(ty2));

  // ToString
  ASSERT_EQ("int8", ty1.ToString());
  ASSERT_EQ("Type::DECIMAL128", ty2.ToString());

  InputType ty7(match::TimestampTypeUnit(TimeUnit::MICRO));
  ASSERT_EQ("timestamp(us)", ty7.ToString());

  InputType ty8;
  ASSERT_EQ("any", ty8.ToString());
}

TEST(InputType, Equals) {
  InputType t1 = int8();
  InputType t2 = int8();
  InputType t3 = int32();

  InputType t5(Type::DECIMAL);
  InputType t6(Type::DECIMAL);

  ASSERT_TRUE(t1.Equals(t2));
  ASSERT_EQ(t1, t2);
  ASSERT_NE(t1, t3);

  ASSERT_FALSE(t1.Equals(t5));
  ASSERT_NE(t1, t5);

  ASSERT_EQ(t5, t5);
  ASSERT_EQ(t5, t6);

  // NOTE: For the time being, we treat int32() and Type::INT32 as being
  // different. This could obviously be fixed later to make these equivalent
  ASSERT_NE(InputType(int8()), InputType(Type::INT32));

  // Check that field metadata excluded from equality checks
  InputType t9 = list(
      field("item", utf8(), /*nullable=*/true, key_value_metadata({"foo"}, {"bar"})));
  InputType t10 = list(field("item", utf8()));
  ASSERT_TRUE(t9.Equals(t10));
}

TEST(InputType, Hash) {
  InputType t0;
  InputType t1 = int8();
  InputType t2(Type::DECIMAL);

  // These checks try to determine first of all whether Hash always returns the
  // same value, and whether the elements of the type are all incorporated into
  // the Hash
  ASSERT_EQ(t0.Hash(), t0.Hash());
  ASSERT_EQ(t1.Hash(), t1.Hash());
  ASSERT_EQ(t2.Hash(), t2.Hash());
  ASSERT_NE(t0.Hash(), t1.Hash());
  ASSERT_NE(t0.Hash(), t2.Hash());
  ASSERT_NE(t1.Hash(), t2.Hash());
}

TEST(InputType, Matches) {
  InputType input1 = int8();

  ASSERT_TRUE(input1.Matches(*int8()));
  ASSERT_TRUE(input1.Matches(*int8()));
  ASSERT_FALSE(input1.Matches(*int16()));

  InputType input2(Type::DECIMAL);
  ASSERT_TRUE(input2.Matches(*decimal(12, 2)));

  auto ty2 = decimal(12, 2);
  auto ty3 = float64();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> arr2, MakeArrayOfNull(ty2, 1));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> arr3, MakeArrayOfNull(ty3, 1));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> scalar2, arr2->GetScalar(0));
  ASSERT_TRUE(input2.Matches(Datum(arr2)));
  ASSERT_TRUE(input2.Matches(Datum(scalar2)));
  ASSERT_FALSE(input2.Matches(*ty3));
  ASSERT_FALSE(input2.Matches(arr3));
}

// ----------------------------------------------------------------------
// OutputType

TEST(OutputType, Constructors) {
  OutputType ty1 = int8();
  ASSERT_EQ(OutputType::FIXED, ty1.kind());
  AssertTypeEqual(*int8(), *ty1.type());

  auto DummyResolver = [](KernelContext*,
                          const std::vector<TypeHolder>& args) -> Result<TypeHolder> {
    return int32();
  };
  OutputType ty2(DummyResolver);
  ASSERT_EQ(OutputType::COMPUTED, ty2.kind());

  ASSERT_OK_AND_ASSIGN(TypeHolder out_type2, ty2.Resolve(nullptr, {}));
  ASSERT_EQ(out_type2, int32());

  // Copy constructor
  OutputType ty3 = ty1;
  ASSERT_EQ(OutputType::FIXED, ty3.kind());
  AssertTypeEqual(*ty1.type(), *ty3.type());

  OutputType ty4 = ty2;
  ASSERT_EQ(OutputType::COMPUTED, ty4.kind());
  ASSERT_OK_AND_ASSIGN(TypeHolder out_type4, ty4.Resolve(nullptr, {}));
  ASSERT_EQ(out_type4, int32());

  // Move constructor
  OutputType ty5 = std::move(ty1);
  ASSERT_EQ(OutputType::FIXED, ty5.kind());
  AssertTypeEqual(*int8(), *ty5.type());

  OutputType ty6 = std::move(ty4);
  ASSERT_EQ(OutputType::COMPUTED, ty6.kind());
  ASSERT_OK_AND_ASSIGN(TypeHolder out_type6, ty6.Resolve(nullptr, {}));
  ASSERT_EQ(out_type6, int32());

  // ToString

  // ty1 was copied to ty3
  ASSERT_EQ("int8", ty3.ToString());
  ASSERT_EQ("computed", ty2.ToString());
}

TEST(OutputType, Resolve) {
  OutputType ty1(int32());

  ASSERT_OK_AND_ASSIGN(TypeHolder result, ty1.Resolve(nullptr, {}));
  ASSERT_EQ(result, int32());

  ASSERT_OK_AND_ASSIGN(result, ty1.Resolve(nullptr, {int8()}));
  ASSERT_EQ(result, int32());

  ASSERT_OK_AND_ASSIGN(result, ty1.Resolve(nullptr, {int8(), int8()}));
  ASSERT_EQ(result, int32());

  auto resolver = [](KernelContext*,
                     const std::vector<TypeHolder>& args) -> Result<TypeHolder> {
    return args[0];
  };
  OutputType ty2(resolver);

  ASSERT_OK_AND_ASSIGN(result, ty2.Resolve(nullptr, {utf8()}));
  ASSERT_EQ(result, utf8());

  // Type resolver that returns an error
  OutputType ty3(
      [](KernelContext* ctx, const std::vector<TypeHolder>& types) -> Result<TypeHolder> {
        // NB: checking the value types versus the function arity should be
        // validated elsewhere, so this is just for illustration purposes
        if (types.size() == 0) {
          return Status::Invalid("Need at least one argument");
        }
        return types[0];
      });
  ASSERT_RAISES(Invalid, ty3.Resolve(nullptr, {}));

  // Type resolver that returns a fixed value
  OutputType ty4(
      [](KernelContext* ctx, const std::vector<TypeHolder>& types) -> Result<TypeHolder> {
        return int32();
      });

  ASSERT_OK_AND_ASSIGN(result, ty4.Resolve(nullptr, {int8()}));
  ASSERT_EQ(result, int32());
  ASSERT_OK_AND_ASSIGN(result, ty4.Resolve(nullptr, {int8()}));
  ASSERT_EQ(result, int32());
}

// ----------------------------------------------------------------------
// KernelSignature

TEST(KernelSignature, Basics) {
  // (int8, decimal) -> utf8
  std::vector<InputType> in_types({int8(), InputType(Type::DECIMAL)});
  OutputType out_type(utf8());

  KernelSignature sig(in_types, out_type);
  ASSERT_EQ(2, sig.in_types().size());
  ASSERT_TRUE(sig.in_types()[0].type()->Equals(*int8()));
  ASSERT_TRUE(sig.in_types()[0].Matches(*int8()));
  ASSERT_TRUE(sig.in_types()[1].Matches(*decimal(12, 2)));
}

TEST(KernelSignature, Equals) {
  KernelSignature sig1({}, utf8());
  KernelSignature sig1_copy({}, utf8());
  KernelSignature sig2({int8()}, utf8());

  // Output type doesn't matter (for now)
  KernelSignature sig3({int8()}, int32());

  KernelSignature sig4({int8(), int16()}, utf8());
  KernelSignature sig4_copy({int8(), int16()}, utf8());
  KernelSignature sig5({int8(), int16(), int32()}, utf8());

  ASSERT_EQ(sig1, sig1);

  ASSERT_EQ(sig2, sig3);
  ASSERT_NE(sig3, sig4);

  // Different sig objects, but same sig
  ASSERT_EQ(sig1, sig1_copy);
  ASSERT_EQ(sig4, sig4_copy);

  // Match first 2 args, but not third
  ASSERT_NE(sig4, sig5);
}

TEST(KernelSignature, VarArgsEquals) {
  KernelSignature sig1({int8()}, utf8(), /*is_varargs=*/true);
  KernelSignature sig2({int8()}, utf8(), /*is_varargs=*/true);
  KernelSignature sig3({int8()}, utf8());

  ASSERT_EQ(sig1, sig2);
  ASSERT_NE(sig2, sig3);
}

TEST(KernelSignature, Hash) {
  // Some basic tests to ensure that the hashes are deterministic and that all
  // input arguments are incorporated
  KernelSignature sig1({}, utf8());
  KernelSignature sig2({int8()}, utf8());
  KernelSignature sig3({int8(), int32()}, utf8());

  ASSERT_EQ(sig1.Hash(), sig1.Hash());
  ASSERT_EQ(sig2.Hash(), sig2.Hash());
  ASSERT_NE(sig1.Hash(), sig2.Hash());
  ASSERT_NE(sig2.Hash(), sig3.Hash());
}

TEST(KernelSignature, MatchesInputs) {
  // () -> boolean
  KernelSignature sig1({}, boolean());

  ASSERT_TRUE(sig1.MatchesInputs({}));
  ASSERT_FALSE(sig1.MatchesInputs({int8()}));

  // (int8, decimal) -> boolean
  KernelSignature sig2({int8(), InputType(Type::DECIMAL)}, boolean());

  ASSERT_FALSE(sig2.MatchesInputs({}));
  ASSERT_FALSE(sig2.MatchesInputs({int8()}));
  ASSERT_TRUE(sig2.MatchesInputs({int8(), decimal(12, 2)}));

  // (int8, int32) -> boolean
  KernelSignature sig3({int8(), int32()}, boolean());

  ASSERT_FALSE(sig3.MatchesInputs({}));

  // Unqualified, these are ANY type and do not match because the kernel
  // requires a scalar and an array
  ASSERT_TRUE(sig3.MatchesInputs({int8(), int32()}));
  ASSERT_FALSE(sig3.MatchesInputs({int8(), int16()}));
}

TEST(KernelSignature, VarArgsMatchesInputs) {
  {
    KernelSignature sig({int8()}, utf8(), /*is_varargs=*/true);

    std::vector<TypeHolder> args = {int8()};
    ASSERT_TRUE(sig.MatchesInputs(args));
    args.push_back(int8());
    args.push_back(int8());
    ASSERT_TRUE(sig.MatchesInputs(args));
    args.push_back(int32());
    ASSERT_FALSE(sig.MatchesInputs(args));
  }
  {
    KernelSignature sig({int8(), utf8()}, utf8(), /*is_varargs=*/true);

    std::vector<TypeHolder> args = {int8()};
    ASSERT_TRUE(sig.MatchesInputs(args));
    args.push_back(utf8());
    args.push_back(utf8());
    ASSERT_TRUE(sig.MatchesInputs(args));
    args.push_back(int32());
    ASSERT_FALSE(sig.MatchesInputs(args));
  }
}

TEST(KernelSignature, ToString) {
  std::vector<InputType> in_types = {InputType(int8()), InputType(Type::DECIMAL),
                                     InputType(utf8())};
  KernelSignature sig(in_types, utf8());
  ASSERT_EQ("(int8, Type::DECIMAL128, string) -> string", sig.ToString());

  OutputType out_type(
      [](KernelContext*, const std::vector<TypeHolder>& args) -> Result<TypeHolder> {
        return Status::Invalid("NYI");
      });
  KernelSignature sig2({int8(), Type::DECIMAL}, out_type);
  ASSERT_EQ("(int8, Type::DECIMAL128) -> computed", sig2.ToString());
}

TEST(KernelSignature, VarArgsToString) {
  KernelSignature sig({int8()}, utf8(), /*is_varargs=*/true);
  ASSERT_EQ("varargs[int8*] -> string", sig.ToString());

  KernelSignature sig2({utf8(), int8()}, utf8(), /*is_varargs=*/true);
  ASSERT_EQ("varargs[string, int8*] -> string", sig2.ToString());
}

}  // namespace compute
}  // namespace arrow
