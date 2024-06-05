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

#include "arrow/compute/expression.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/expression_internal.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

using testing::Eq;
using testing::HasSubstr;
using testing::UnorderedElementsAreArray;

using namespace std::chrono_literals;  // NOLINT build/namespaces

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

const std::shared_ptr<Schema> kBoringSchema = schema({
    field("bool", boolean()),
    field("i8", int8()),
    field("i32", int32()),
    field("i32_req", int32(), /*nullable=*/false),
    field("u32", uint32()),
    field("i64", int64()),
    field("f32", float32()),
    field("f32_req", float32(), /*nullable=*/false),
    field("f64", float64()),
    field("date64", date64()),
    field("str", utf8()),
    field("dict_str", dictionary(int32(), utf8())),
    field("dict_i32", dictionary(int32(), int32())),
    field("ts_ns", timestamp(TimeUnit::NANO)),
    field("ts_s", timestamp(TimeUnit::SECOND)),
    field("binary", binary()),
    field("ts_s_utc", timestamp(TimeUnit::SECOND, "UTC")),
});

#define EXPECT_OK ARROW_EXPECT_OK

Expression cast(Expression argument, std::shared_ptr<DataType> to_type) {
  return call("cast", {std::move(argument)},
              compute::CastOptions::Safe(std::move(to_type)));
}

Expression true_unless_null(Expression argument) {
  return call("true_unless_null", {std::move(argument)});
}

Expression add(Expression l, Expression r) {
  return call("add", {std::move(l), std::move(r)});
}

const auto no_change = std::nullopt;

TEST(ExpressionUtils, Comparison) {
  auto cmp_name = [](Datum l, Datum r) {
    return Comparison::Execute(l, r).Map(Comparison::GetName);
  };

  Datum zero(0), one(1), two(2), null(std::make_shared<Int32Scalar>());
  Datum str("hello"), bin(std::make_shared<BinaryScalar>(Buffer::FromString("hello")));
  Datum dict_str(DictionaryScalar::Make(std::make_shared<Int32Scalar>(0),
                                        ArrayFromJSON(utf8(), R"(["a", "b", "c"])")));

  auto RaisesNotImpl =
      Raises(StatusCode::NotImplemented, HasSubstr("no kernel matching input types"));

  EXPECT_THAT(cmp_name(one, one), ResultWith(Eq("equal")));
  EXPECT_THAT(cmp_name(one, two), ResultWith(Eq("less")));
  EXPECT_THAT(cmp_name(one, zero), ResultWith(Eq("greater")));

  EXPECT_THAT(cmp_name(one, null), ResultWith(Eq("na")));
  EXPECT_THAT(cmp_name(null, one), ResultWith(Eq("na")));

  // strings and ints are not comparable without explicit casts
  EXPECT_THAT(cmp_name(str, one), RaisesNotImpl);
  EXPECT_THAT(cmp_name(one, str), RaisesNotImpl);
  EXPECT_THAT(cmp_name(str, null), RaisesNotImpl);  // not even null ints

  // string -> binary implicit cast allowed
  EXPECT_THAT(cmp_name(str, bin), ResultWith(Eq("equal")));
  EXPECT_THAT(cmp_name(bin, str), ResultWith(Eq("equal")));

  // dict_str -> string, implicit casts allowed
  EXPECT_THAT(cmp_name(dict_str, str), ResultWith(Eq("less")));
  EXPECT_THAT(cmp_name(dict_str, bin), ResultWith(Eq("less")));
}

TEST(ExpressionUtils, StripOrderPreservingCasts) {
  auto Expect = [](Expression expr, std::optional<Expression> expected_stripped) {
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));
    if (!expected_stripped) {
      expected_stripped = expr;
    } else {
      ASSERT_OK_AND_ASSIGN(expected_stripped, expected_stripped->Bind(*kBoringSchema));
    }
    EXPECT_EQ(Comparison::StripOrderPreservingCasts(expr), *expected_stripped);
  };

  // Casting int to float preserves ordering.
  // For example, let
  //   a = 3, b = 2, assert(a > b)
  // After injecting a cast to float, this ordering still holds
  //   float(a) == 3.0, float(b) == 2.0, assert(float(a) > float(b))
  Expect(cast(field_ref("i32"), float32()), field_ref("i32"));

  // Casting an integral type to a wider integral type preserves ordering.
  Expect(cast(field_ref("i32"), int64()), field_ref("i32"));
  Expect(cast(field_ref("i32"), int32()), field_ref("i32"));
  Expect(cast(field_ref("i32"), int16()), no_change);
  Expect(cast(field_ref("i32"), int8()), no_change);

  Expect(cast(field_ref("u32"), uint64()), field_ref("u32"));
  Expect(cast(field_ref("u32"), uint32()), field_ref("u32"));
  Expect(cast(field_ref("u32"), uint16()), no_change);
  Expect(cast(field_ref("u32"), uint8()), no_change);

  Expect(cast(field_ref("u32"), int64()), field_ref("u32"));
  Expect(cast(field_ref("u32"), int32()), field_ref("u32"));
  Expect(cast(field_ref("u32"), int16()), no_change);
  Expect(cast(field_ref("u32"), int8()), no_change);

  // Casting float to int can affect ordering.
  // For example, let
  //   a = 3.5, b = 3.0, assert(a > b)
  // After injecting a cast to integer, this ordering may no longer hold
  //   int(a) == 3, int(b) == 3, assert(!(int(a) > int(b)))
  Expect(cast(field_ref("f32"), int32()), no_change);

  // casting any float type to another preserves ordering
  Expect(cast(field_ref("f32"), float64()), field_ref("f32"));
  Expect(cast(field_ref("f64"), float32()), field_ref("f64"));

  // casting signed integer to unsigned can alter ordering
  Expect(cast(field_ref("i32"), uint32()), no_change);
  Expect(cast(field_ref("i32"), uint64()), no_change);
}

TEST(ExpressionUtils, MakeExecBatch) {
  auto Expect = [](std::shared_ptr<RecordBatch> partial_batch) {
    SCOPED_TRACE(partial_batch->ToString());
    ASSERT_OK_AND_ASSIGN(auto batch, MakeExecBatch(*kBoringSchema, partial_batch));

    ASSERT_EQ(batch.num_values(), kBoringSchema->num_fields());
    for (int i = 0; i < kBoringSchema->num_fields(); ++i) {
      const auto& field = *kBoringSchema->field(i);

      SCOPED_TRACE("Field#" + std::to_string(i) + " " + field.ToString());

      EXPECT_TRUE(batch[i].type()->Equals(field.type()))
          << "Incorrect type " << batch[i].type()->ToString();

      ASSERT_OK_AND_ASSIGN(auto col, FieldRef(field.name()).GetOneOrNone(*partial_batch));

      if (batch[i].is_scalar()) {
        EXPECT_FALSE(batch[i].scalar()->is_valid)
            << "Non-null placeholder scalar was injected";

        EXPECT_EQ(col, nullptr)
            << "Placeholder scalar overwrote column " << col->ToString();
      } else {
        AssertDatumsEqual(col, batch[i]);
      }
    }
  };

  auto GetField = [](std::string name) { return kBoringSchema->GetFieldByName(name); };

  constexpr int64_t kNumRows = 3;
  auto i32 = ArrayFromJSON(int32(), "[1, 2, 3]");
  auto f32 = ArrayFromJSON(float32(), "[1.5, 2.25, 3.125]");

  // empty
  Expect(RecordBatchFromJSON(kBoringSchema, "[]"));

  // subset
  Expect(RecordBatch::Make(schema({GetField("i32"), GetField("f32")}), kNumRows,
                           {i32, f32}));

  // flipped subset
  Expect(RecordBatch::Make(schema({GetField("f32"), GetField("i32")}), kNumRows,
                           {f32, i32}));

  auto duplicated_names =
      RecordBatch::Make(schema({GetField("i32"), GetField("i32")}), kNumRows, {i32, i32});
  ASSERT_RAISES(Invalid, MakeExecBatch(*kBoringSchema, duplicated_names));
}

class WidgetifyOptions : public compute::FunctionOptions {
 public:
  explicit WidgetifyOptions(bool really = true);
  bool really;
};
class WidgetifyOptionsType : public FunctionOptionsType {
 public:
  static const FunctionOptionsType* GetInstance() {
    static std::unique_ptr<FunctionOptionsType> instance(new WidgetifyOptionsType());
    return instance.get();
  }
  const char* type_name() const override { return "widgetify"; }
  std::string Stringify(const FunctionOptions& options) const override {
    return type_name();
  }
  bool Compare(const FunctionOptions& options,
               const FunctionOptions& other) const override {
    return true;
  }
  std::unique_ptr<FunctionOptions> Copy(const FunctionOptions& options) const override {
    const auto& opts = static_cast<const WidgetifyOptions&>(options);
    return std::make_unique<WidgetifyOptions>(opts.really);
  }
};
WidgetifyOptions::WidgetifyOptions(bool really)
    : FunctionOptions(WidgetifyOptionsType::GetInstance()), really(really) {}

TEST(Expression, ToString) {
  EXPECT_EQ(field_ref("alpha").ToString(), "alpha");

  EXPECT_EQ(literal(3).ToString(), "3");
  EXPECT_EQ(literal("a").ToString(), "\"a\"");
  EXPECT_EQ(literal("a\nb").ToString(), "\"a\\nb\"");
  EXPECT_EQ(literal(std::make_shared<BooleanScalar>()).ToString(), "null[bool]");
  EXPECT_EQ(literal(std::make_shared<Int64Scalar>()).ToString(), "null[int64]");
  EXPECT_EQ(literal(std::make_shared<BinaryScalar>(Buffer::FromString("az"))).ToString(),
            "\"617A\"");

  auto ts = *TimestampScalar::FromISO8601("1990-10-23 10:23:33", TimeUnit::NANO);
  EXPECT_EQ(literal(ts).ToString(), "1990-10-23 10:23:33.000000000");

  EXPECT_EQ(add(literal(3), field_ref("beta")).ToString(), "add(3, beta)");

  auto in_12 = call("index_in", {field_ref("beta")},
                    compute::SetLookupOptions{ArrayFromJSON(int32(), "[1,2]")});

  EXPECT_EQ(
      in_12.ToString(),
      "index_in(beta, {value_set=int32:[\n  1,\n  2\n], null_matching_behavior=MATCH})");

  EXPECT_EQ(and_(field_ref("a"), field_ref("b")).ToString(), "(a and b)");
  EXPECT_EQ(or_(field_ref("a"), field_ref("b")).ToString(), "(a or b)");
  EXPECT_EQ(not_(field_ref("a")).ToString(), "invert(a)");

  EXPECT_EQ(
      cast(field_ref("a"), int32()).ToString(),
      "cast(a, {to_type=int32, allow_int_overflow=false, allow_time_truncate=false, "
      "allow_time_overflow=false, allow_decimal_truncate=false, "
      "allow_float_truncate=false, allow_invalid_utf8=false})");
  EXPECT_EQ(
      cast(field_ref("a"), nullptr).ToString(),
      "cast(a, {to_type=<NULLPTR>, allow_int_overflow=false, allow_time_truncate=false, "
      "allow_time_overflow=false, allow_decimal_truncate=false, "
      "allow_float_truncate=false, allow_invalid_utf8=false})");

  EXPECT_EQ(call("widgetify", {}).ToString(), "widgetify()");
  EXPECT_EQ(
      call("widgetify", {literal(1)}, std::make_shared<WidgetifyOptions>()).ToString(),
      "widgetify(1, widgetify)");

  EXPECT_EQ(equal(field_ref("a"), literal(1)).ToString(), "(a == 1)");
  EXPECT_EQ(less(field_ref("a"), literal(2)).ToString(), "(a < 2)");
  EXPECT_EQ(greater(field_ref("a"), literal(3)).ToString(), "(a > 3)");
  EXPECT_EQ(not_equal(field_ref("a"), literal("a")).ToString(), "(a != \"a\")");
  EXPECT_EQ(less_equal(field_ref("a"), literal("b")).ToString(), "(a <= \"b\")");
  EXPECT_EQ(greater_equal(field_ref("a"), literal("c")).ToString(), "(a >= \"c\")");

  EXPECT_EQ(project(
                {
                    field_ref("a"),
                    field_ref("a"),
                    literal(3),
                    in_12,
                },
                {
                    "a",
                    "renamed_a",
                    "three",
                    "b",
                })
                .ToString(),
            "{a=a, renamed_a=a, three=3, b=" + in_12.ToString() + "}");

  EXPECT_EQ(call("round", {literal(3.14)}, compute::RoundOptions()).ToString(),
            "round(3.14, {ndigits=0, round_mode=HALF_TO_EVEN})");
  EXPECT_EQ(call("random", {}, compute::RandomOptions()).ToString(),
            "random({initializer=SystemRandom, seed=0})");
}

TEST(Expression, Equality) {
  EXPECT_EQ(literal(1), literal(1));
  EXPECT_NE(literal(1), literal(2));

  // NaN literals (of the same type) should be equal.  This allows, for example,
  // the expression x == NaN to equal itself.
  auto double_nan_literal = literal(std::numeric_limits<double>::quiet_NaN());
  auto float_nan_literal = literal(std::numeric_limits<float>::quiet_NaN());
  EXPECT_EQ(double_nan_literal, double_nan_literal);
  EXPECT_NE(double_nan_literal, float_nan_literal);
  // The literals may be equal but the values should not be
  Expression nans_eq = equal(double_nan_literal, double_nan_literal);
  ASSERT_OK_AND_ASSIGN(nans_eq, nans_eq.Bind(*kBoringSchema));
  ASSERT_OK_AND_ASSIGN(Datum nans_eq_rsp, ExecuteScalarExpression(nans_eq, ExecBatch()));
  EXPECT_FALSE(nans_eq_rsp.scalar_as<BooleanScalar>().value);
  if (std::numeric_limits<double>::has_signaling_NaN) {
    // We intentionally do not care about signaling and may even discard it on conversion.
    EXPECT_EQ(literal(std::numeric_limits<double>::quiet_NaN()),
              literal(std::numeric_limits<double>::signaling_NaN()));
  }

  EXPECT_EQ(field_ref("a"), field_ref("a"));
  EXPECT_NE(field_ref("a"), field_ref("b"));
  EXPECT_NE(field_ref("a"), literal(2));

  EXPECT_EQ(add(literal(3), field_ref("a")), add(literal(3), field_ref("a")));
  EXPECT_NE(add(literal(3), field_ref("a")), add(literal(2), field_ref("a")));
  EXPECT_NE(add(field_ref("a"), literal(3)), add(literal(3), field_ref("a")));

  auto in_123 = compute::SetLookupOptions{ArrayFromJSON(int32(), "[1,2,3]")};
  EXPECT_EQ(add(literal(3), call("index_in", {field_ref("beta")}, in_123)),
            add(literal(3), call("index_in", {field_ref("beta")}, in_123)));

  auto in_12 = compute::SetLookupOptions{ArrayFromJSON(int32(), "[1,2]")};
  EXPECT_NE(add(literal(3), call("index_in", {field_ref("beta")}, in_12)),
            add(literal(3), call("index_in", {field_ref("beta")}, in_123)));

  EXPECT_EQ(cast(field_ref("a"), int32()), cast(field_ref("a"), int32()));
  EXPECT_NE(cast(field_ref("a"), int32()), cast(field_ref("a"), int64()));
  EXPECT_NE(cast(field_ref("a"), int32()),
            call("cast", {field_ref("a")}, compute::CastOptions::Unsafe(int32())));
}

Expression null_literal(const std::shared_ptr<DataType>& type) {
  return Expression(MakeNullScalar(type));
}

TEST(Expression, Hash) {
  std::unordered_set<Expression, Expression::Hash> set;

  EXPECT_TRUE(set.emplace(field_ref("alpha")).second);
  EXPECT_TRUE(set.emplace(field_ref("beta")).second);
  EXPECT_FALSE(set.emplace(field_ref("beta")).second) << "already inserted";
  EXPECT_TRUE(set.emplace(literal(1)).second);
  EXPECT_FALSE(set.emplace(literal(1)).second) << "already inserted";
  EXPECT_TRUE(set.emplace(literal(3)).second);

  EXPECT_TRUE(set.emplace(null_literal(int32())).second);
  EXPECT_FALSE(set.emplace(null_literal(int32())).second) << "already inserted";
  EXPECT_TRUE(set.emplace(null_literal(float32())).second);
  // NB: no validation on construction; we couldn't execute
  //     add with zero arguments
  EXPECT_TRUE(set.emplace(call("add", {})).second);
  EXPECT_FALSE(set.emplace(call("add", {})).second) << "already inserted";

  // NB: unbound expressions don't check for availability in any registry
  EXPECT_TRUE(set.emplace(call("widgetify", {})).second);

  EXPECT_EQ(set.size(), 8);
}

TEST(Expression, IsScalarExpression) {
  EXPECT_TRUE(literal(true).IsScalarExpression());

  auto arr = ArrayFromJSON(int8(), "[]");
  EXPECT_FALSE(literal(arr).IsScalarExpression());

  EXPECT_TRUE(field_ref("a").IsScalarExpression());

  EXPECT_TRUE(equal(field_ref("a"), literal(1)).IsScalarExpression());

  EXPECT_FALSE(equal(field_ref("a"), literal(arr)).IsScalarExpression());

  EXPECT_TRUE(call("is_in", {field_ref("a")}, compute::SetLookupOptions{arr, true})
                  .IsScalarExpression());

  // non scalar function
  EXPECT_FALSE(call("take", {field_ref("a"), literal(arr)}).IsScalarExpression());
}

TEST(Expression, IsSatisfiable) {
  auto Bind = [](Expression expr) { return expr.Bind(*kBoringSchema).ValueOrDie(); };

  EXPECT_TRUE(literal(true).IsSatisfiable());
  EXPECT_FALSE(literal(false).IsSatisfiable());

  auto null = std::make_shared<BooleanScalar>();
  EXPECT_FALSE(literal(null).IsSatisfiable());

  // NB: no implicit conversion to bool
  EXPECT_TRUE(literal(0).IsSatisfiable());

  EXPECT_TRUE(field_ref("i32").IsSatisfiable());
  EXPECT_TRUE(Bind(field_ref("i32")).IsSatisfiable());

  EXPECT_TRUE(equal(field_ref("i32"), literal(1)).IsSatisfiable());
  EXPECT_TRUE(Bind(equal(field_ref("i32"), literal(1))).IsSatisfiable());

  // NB: no constant folding here
  EXPECT_TRUE(Bind(equal(literal(0), literal(1))).IsSatisfiable());

  // Special case invert(true_unless_null(x)): arises in simplification against a
  // guarantee with a nullable caveat.
  EXPECT_FALSE(Bind(not_(true_unless_null(field_ref("i32")))).IsSatisfiable());
  // NB: no effort to examine unbound expressions
  EXPECT_TRUE(not_(true_unless_null(field_ref("i32"))).IsSatisfiable());

  // When a top level conjunction contains an Expression which is not satisfiable
  // (guaranteed to evaluate to null or false), it can only evaluate to null or false.
  // This special case arises when (for example) an absent column has made
  // one member of the conjunction always-null.
  for (const auto& never_true : {
           // N.B. this is "and_kleene"
           and_(literal(false), field_ref("bool")),
           and_(literal(null), field_ref("bool")),
           call("and", {literal(false), field_ref("bool")}),
           call("and", {literal(null), field_ref("bool")}),
       }) {
    ARROW_SCOPED_TRACE(never_true.ToString());
    EXPECT_FALSE(Bind(never_true).IsSatisfiable());
    // ... but it may appear in satisfiable filters if coalesced (for example, wrapped in
    // fill_na)
    EXPECT_TRUE(Bind(call("is_null", {never_true})).IsSatisfiable());
  }

  for (const auto& might_true : {
           // N.B. this is "or_kleene"
           or_(literal(false), field_ref("bool")),
           or_(literal(null), field_ref("bool")),
           call("or", {literal(false), field_ref("bool")}),
           call("or", {literal(null), field_ref("bool")}),
       }) {
    ARROW_SCOPED_TRACE(might_true.ToString());
    EXPECT_TRUE(Bind(might_true).IsSatisfiable());
  }

  for (const auto& never_true : {
           // N.B. this is "or_kleene"
           or_(literal(false), literal(null)),
           call("or", {literal(false), literal(null)}),
       }) {
    ARROW_SCOPED_TRACE(never_true.ToString());
    EXPECT_FALSE(Bind(never_true).IsSatisfiable());
    // ... but it may appear in satisfiable filters if coalesced (for example, wrapped in
    // fill_na)
    EXPECT_TRUE(Bind(call("is_null", {never_true})).IsSatisfiable());
  }
}

TEST(Expression, FieldsInExpression) {
  auto ExpectFieldsAre = [](Expression expr, std::vector<FieldRef> expected) {
    EXPECT_THAT(FieldsInExpression(expr), testing::ContainerEq(expected));
  };

  ExpectFieldsAre(literal(true), {});

  ExpectFieldsAre(field_ref("a"), {"a"});

  ExpectFieldsAre(equal(field_ref("a"), literal(1)), {"a"});

  ExpectFieldsAre(equal(field_ref("a"), field_ref("b")), {"a", "b"});

  ExpectFieldsAre(
      or_(equal(field_ref("a"), literal(1)), equal(field_ref("a"), literal(2))),
      {"a", "a"});

  ExpectFieldsAre(
      or_(equal(field_ref("a"), literal(1)), equal(field_ref("b"), literal(2))),
      {"a", "b"});

  ExpectFieldsAre(or_(and_(not_(equal(field_ref("a"), literal(1))),
                           equal(field_ref("b"), literal(2))),
                      not_(less(field_ref("c"), literal(3)))),
                  {"a", "b", "c"});
}

TEST(Expression, ExpressionHasFieldRefs) {
  EXPECT_FALSE(ExpressionHasFieldRefs(literal(true)));

  EXPECT_FALSE(ExpressionHasFieldRefs(add(literal(1), literal(3))));

  EXPECT_TRUE(ExpressionHasFieldRefs(field_ref("a")));

  EXPECT_TRUE(ExpressionHasFieldRefs(equal(field_ref("a"), literal(1))));

  EXPECT_TRUE(ExpressionHasFieldRefs(equal(field_ref("a"), field_ref("b"))));

  EXPECT_TRUE(ExpressionHasFieldRefs(
      or_(equal(field_ref("a"), literal(1)), equal(field_ref("a"), literal(2)))));

  EXPECT_TRUE(ExpressionHasFieldRefs(
      or_(equal(field_ref("a"), literal(1)), equal(field_ref("b"), literal(2)))));

  EXPECT_TRUE(ExpressionHasFieldRefs(or_(
      and_(not_(equal(field_ref("a"), literal(1))), equal(field_ref("b"), literal(2))),
      not_(less(field_ref("c"), literal(3))))));
}

TEST(Expression, BindLiteral) {
  for (Datum dat : {
           Datum(3),
           Datum(3.5),
           Datum(ArrayFromJSON(int32(), "[1,2,3]")),
       }) {
    // literals are always considered bound
    Expression expr = literal(dat);
    EXPECT_TRUE(dat.type()->Equals(*expr.type()));
    EXPECT_TRUE(expr.IsBound());
  }
}

void ExpectBindsTo(Expression expr, std::optional<Expression> expected,
                   Expression* bound_out = nullptr,
                   const Schema& schema = *kBoringSchema) {
  if (!expected) {
    expected = expr;
  }

  ASSERT_OK_AND_ASSIGN(auto bound, expr.Bind(schema));
  EXPECT_TRUE(bound.IsBound());

  ASSERT_OK_AND_ASSIGN(expected, expected->Bind(schema));
  EXPECT_EQ(bound, *expected) << " unbound: " << expr.ToString();

  if (bound_out) {
    *bound_out = bound;
  }
}

TEST(Expression, BindFieldRef) {
  // an unbound field_ref does not have the output type set
  auto expr = field_ref("alpha");
  EXPECT_EQ(expr.type(), nullptr);
  EXPECT_FALSE(expr.IsBound());

  ExpectBindsTo(field_ref("i32"), no_change, &expr);
  EXPECT_TRUE(expr.type()->Equals(*int32()));

  // if the field is not found, an error will be raised
  ASSERT_RAISES(Invalid, field_ref("no such field").Bind(*kBoringSchema));

  // referencing a field by name is not supported if that name is not unique
  // in the input schema
  ASSERT_RAISES(Invalid, field_ref("alpha").Bind(Schema(
                             {field("alpha", int32()), field("alpha", float32())})));
}

TEST(Expression, BindNestedFieldRef) {
  Expression expr;
  auto schema = Schema({field("a", struct_({field("b", int32())}))});

  ExpectBindsTo(field_ref(FieldRef("a", "b")), no_change, &expr, schema);
  EXPECT_TRUE(expr.IsBound());
  EXPECT_TRUE(expr.type()->Equals(*int32()));

  ExpectBindsTo(field_ref(FieldRef(FieldPath({0, 0}))), no_change, &expr, schema);
  EXPECT_TRUE(expr.IsBound());
  EXPECT_TRUE(expr.type()->Equals(*int32()));

  ASSERT_RAISES(Invalid, field_ref(FieldPath({0, 1})).Bind(schema));
  ASSERT_RAISES(Invalid, field_ref(FieldRef("a", "b"))
                             .Bind(Schema({field("a", struct_({field("b", int32()),
                                                               field("b", int64())}))})));
}

TEST(Expression, BindCall) {
  auto expr = add(field_ref("i32"), field_ref("i32_req"));
  EXPECT_FALSE(expr.IsBound());

  ExpectBindsTo(expr, no_change, &expr);
  EXPECT_TRUE(expr.type()->Equals(*int32()));

  ExpectBindsTo(add(field_ref("f32"), literal(3)), add(field_ref("f32"), literal(3.0F)));

  ExpectBindsTo(add(field_ref("i32"), literal(3.5F)),
                add(cast(field_ref("i32"), float32()), literal(3.5F)));
}

TEST(Expression, BindWithAliasCasts) {
  auto fm = GetFunctionRegistry();
  EXPECT_OK(fm->AddAlias("alias_cast", "cast"));

  auto expr = call("alias_cast", {field_ref("f1")}, CastOptions::Unsafe(arrow::int32()));
  EXPECT_FALSE(expr.IsBound());

  auto schema = arrow::schema({field("f1", decimal128(30, 3))});
  ExpectBindsTo(expr, no_change, &expr, *schema);
}

TEST(Expression, BindWithDecimalArithmeticOps) {
  for (std::string arith_op : {"add", "subtract", "multiply", "divide"}) {
    auto expr = call(arith_op, {field_ref("d1"), field_ref("d2")});
    EXPECT_FALSE(expr.IsBound());

    static const std::vector<std::pair<int, int>> scales = {{3, 9}, {6, 6}, {9, 3}};
    for (auto s : scales) {
      auto schema = arrow::schema(
          {field("d1", decimal256(30, s.first)), field("d2", decimal256(20, s.second))});
      ExpectBindsTo(expr, no_change, &expr, *schema);
    }
  }
}

TEST(Expression, BindWithImplicitCasts) {
  for (auto cmp : {equal, not_equal, less, less_equal, greater, greater_equal}) {
    // cast arguments to common numeric type
    ExpectBindsTo(cmp(field_ref("i64"), field_ref("i32")),
                  cmp(field_ref("i64"), cast(field_ref("i32"), int64())));

    ExpectBindsTo(cmp(field_ref("i64"), field_ref("f32")),
                  cmp(cast(field_ref("i64"), float32()), field_ref("f32")));

    ExpectBindsTo(cmp(field_ref("i32"), field_ref("i64")),
                  cmp(cast(field_ref("i32"), int64()), field_ref("i64")));

    ExpectBindsTo(cmp(field_ref("i8"), field_ref("u32")),
                  cmp(cast(field_ref("i8"), int64()), cast(field_ref("u32"), int64())));

    // cast dictionary to value type
    ExpectBindsTo(cmp(field_ref("dict_str"), field_ref("str")),
                  cmp(cast(field_ref("dict_str"), utf8()), field_ref("str")));

    // Should prefer the literal
    ExpectBindsTo(cmp(field_ref("dict_i32"), literal(int64_t(4))),
                  cmp(field_ref("dict_i32"), literal(int32_t(4))));
    ExpectBindsTo(cmp(field_ref("dict_i32"), literal(int64_t(4))),
                  cmp(field_ref("dict_i32"), literal(int32_t(4))));
    ExpectBindsTo(cmp(field_ref("ts_s"),
                      literal(std::make_shared<TimestampScalar>(0, TimeUnit::NANO))),
                  cmp(field_ref("ts_s"),
                      literal(std::make_shared<TimestampScalar>(0, TimeUnit::SECOND))));
    // GH-37110
    ExpectBindsTo(
        cmp(field_ref("ts_s_utc"),
            literal(std::make_shared<TimestampScalar>(0, TimeUnit::NANO, "UTC"))),
        cmp(field_ref("ts_s_utc"),
            literal(std::make_shared<TimestampScalar>(0, TimeUnit::SECOND, "UTC"))));
    ExpectBindsTo(
        cmp(field_ref("ts_s_utc"),
            literal(std::make_shared<TimestampScalar>(123000, TimeUnit::NANO, "UTC"))),
        cmp(field_ref("ts_s_utc"),
            literal(std::make_shared<TimestampScalar>(123, TimeUnit::MICRO, "UTC"))));

    ExpectBindsTo(
        cmp(field_ref("binary"), literal(std::make_shared<LargeBinaryScalar>("foo"))),
        cmp(field_ref("binary"), literal(std::make_shared<BinaryScalar>("foo"))));

    // We will not implicitly cast a literal from signed to unsigned or vice versa
    ExpectBindsTo(cmp(field_ref("i8"), literal(uint8_t(4))),
                  cmp(cast(field_ref("i8"), int16()), literal(int16_t(4))));
    ExpectBindsTo(cmp(field_ref("u32"), literal(int64_t(4))),
                  cmp(cast(field_ref("u32"), int64()), literal(int64_t(4))));

    // NaN / Inf can be float or double as needed
    ExpectBindsTo(
        cmp(field_ref("f32"), literal(std::numeric_limits<double>::quiet_NaN())),
        cmp(field_ref("f32"), literal(std::numeric_limits<float>::quiet_NaN())));
    ExpectBindsTo(cmp(field_ref("f32"), literal(std::numeric_limits<double>::infinity())),
                  cmp(field_ref("f32"), literal(std::numeric_limits<float>::infinity())));

    // Bit of an odd case, both fields are cast
    ExpectBindsTo(cmp(field_ref("i32"), literal(std::make_shared<DoubleScalar>(10.0))),
                  cmp(cast(field_ref("i32"), float32()),
                      literal(std::make_shared<FloatScalar>(10.0f))));
  }

  compute::SetLookupOptions in_a{ArrayFromJSON(utf8(), R"(["a"])")};

  // cast dictionary to value type
  ExpectBindsTo(call("is_in", {field_ref("dict_str")}, in_a),
                call("is_in", {cast(field_ref("dict_str"), utf8())}, in_a));
}

TEST(Expression, BindNestedCall) {
  auto expr = add(field_ref("a"),
                  call("subtract", {call("multiply", {field_ref("b"), field_ref("c")}),
                                    field_ref("d")}));
  EXPECT_FALSE(expr.IsBound());

  ASSERT_OK_AND_ASSIGN(expr,
                       expr.Bind(Schema({field("a", int32()), field("b", int32()),
                                         field("c", int32()), field("d", int32())})));
  EXPECT_TRUE(expr.type()->Equals(*int32()));
  EXPECT_TRUE(expr.IsBound());
}

TEST(Expression, ExecuteFieldRef) {
  auto ExpectRefIs = [](FieldRef ref, Datum in, Datum expected) {
    auto expr = field_ref(ref);

    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(in.type()));
    ASSERT_OK_AND_ASSIGN(Datum actual,
                         ExecuteScalarExpression(expr, Schema(in.type()->fields()), in));

    AssertDatumsEqual(actual, expected, /*verbose=*/true);
  };

  ExpectRefIs("a", ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, 0.0, -1])"));

  ExpectRefIs("a",
              ArrayFromJSON(struct_({
                                field("a", float64()),
                                field("b", float64()),
                            }),
                            R"([
    {"a": 6.125, "b": 7.5},
    {"a": 0.0,   "b": 2.125},
    {"a": -1,    "b": 4.0}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, 0.0, -1])"));

  ExpectRefIs("b",
              ArrayFromJSON(struct_({
                                field("a", float64()),
                                field("b", float64()),
                            }),
                            R"([
    {"a": 6.125, "b": 7.5},
    {"a": 0.0,   "b": 2.125},
    {"a": -1,    "b": 4.0}
  ])"),
              ArrayFromJSON(float64(), R"([7.5, 2.125, 4.0])"));

  ExpectRefIs(FieldRef(FieldPath({0, 0})),
              ArrayFromJSON(struct_({field("a", struct_({field("b", float64())}))}), R"([
    {"a": {"b": 6.125}},
    {"a": {"b": 0.0}},
    {"a": {"b": -1}}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, 0.0, -1])"));

  ExpectRefIs(FieldRef("a", "b"),
              ArrayFromJSON(struct_({field("a", struct_({field("b", float64())}))}), R"([
    {"a": {"b": 6.125}},
    {"a": {"b": 0.0}},
    {"a": {"b": -1}}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, 0.0, -1])"));

  ExpectRefIs(FieldRef("a", "b"),
              ArrayFromJSON(struct_({field("a", struct_({field("b", float64())}))}), R"([
    {"a": {"b": 6.125}},
    {"a": null},
    {"a": {"b": null}}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, null, null])"));

  ExpectRefIs(
      FieldRef("a", "b"),
      ScalarFromJSON(struct_({field("a", struct_({field("b", float64())}))}), "[[64.0]]"),
      ScalarFromJSON(float64(), "64.0"));

  ExpectRefIs(
      FieldRef("a", "b"),
      ScalarFromJSON(struct_({field("a", struct_({field("b", float64())}))}), "[[null]]"),
      ScalarFromJSON(float64(), "null"));

  ExpectRefIs(
      FieldRef("a", "b"),
      ScalarFromJSON(struct_({field("a", struct_({field("b", float64())}))}), "[null]"),
      ScalarFromJSON(float64(), "null"));
}

Result<Datum> NaiveExecuteScalarExpression(const Expression& expr, const Datum& input) {
  if (auto lit = expr.literal()) {
    return *lit;
  }

  if (auto ref = expr.field_ref()) {
    if (input.type()) {
      return ref->GetOneOrNone(*input.make_array());
    }
    return ref->GetOneOrNone(*input.record_batch());
  }

  auto call = CallNotNull(expr);

  std::vector<Datum> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(arguments[i],
                          NaiveExecuteScalarExpression(call->arguments[i], input));
  }

  compute::ExecContext exec_context;
  ARROW_ASSIGN_OR_RAISE(auto function, GetFunction(*call, &exec_context));

  std::vector<TypeHolder> types = GetTypes(call->arguments);
  ARROW_ASSIGN_OR_RAISE(auto expected_kernel, function->DispatchExact(types));

  EXPECT_EQ(call->kernel, expected_kernel);
  return function->Execute(arguments, call->options.get(), &exec_context);
}

void ExpectExecute(Expression expr, Datum in, Datum* actual_out = NULLPTR) {
  std::shared_ptr<Schema> schm;
  if (in.is_value()) {
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(in.type()));
    schm = schema(in.type()->fields());
  } else {
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*in.schema()));
    schm = in.schema();
  }

  ASSERT_OK_AND_ASSIGN(Datum actual, ExecuteScalarExpression(expr, *schm, in));

  ASSERT_OK_AND_ASSIGN(Datum expected, NaiveExecuteScalarExpression(expr, in));

  AssertDatumsEqual(actual, expected, /*verbose=*/true);

  if (actual_out) {
    *actual_out = actual;
  }
}

TEST(Expression, ExecuteCall) {
  ExpectExecute(add(field_ref("a"), literal(3.5)),
                ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"));

  ExpectExecute(
      add(field_ref("a"), call("subtract", {literal(3.5), field_ref("b")})),
      ArrayFromJSON(struct_({field("a", float64()), field("b", float64())}), R"([
    {"a": 6.125, "b": 3.375},
    {"a": 0.0,   "b": 1},
    {"a": -1,    "b": 4.75}
  ])"));

  ExpectExecute(call("strptime", {field_ref("a")},
                     compute::StrptimeOptions("%m/%d/%Y", TimeUnit::MICRO, true)),
                ArrayFromJSON(struct_({field("a", utf8())}), R"([
    {"a": "5/1/2020"},
    {"a": null},
    {"a": "12/11/1900"}
  ])"));

  ExpectExecute(project({add(field_ref("a"), literal(3.5))}, {"a + 3.5"}),
                ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"));

  ExpectExecute(add(field_ref(FieldRef("a", "a")), field_ref(FieldRef("a", "b"))),
                ArrayFromJSON(struct_({field("a", struct_({
                                                      field("a", float64()),
                                                      field("b", float64()),
                                                  }))}),
                              R"([
    {"a": {"a": 6.125, "b": 3.375}},
    {"a": {"a": 0.0,   "b": 1}},
    {"a": {"a": -1,    "b": 4.75}}
  ])"));
}

TEST(Expression, ExecuteCallWithNoArguments) {
  const int kCount = 10;
  auto random_options = RandomOptions::FromSeed(/*seed=*/0);
  ExecBatch input({}, kCount);

  Expression random_expr = call("random", {}, random_options);
  ASSERT_OK_AND_ASSIGN(random_expr, random_expr.Bind(float64()));
  ASSERT_OK_AND_ASSIGN(auto simplify_expr,
                       SimplifyWithGuarantee(random_expr, input.guarantee));

  ASSERT_OK_AND_ASSIGN(Datum actual, ExecuteScalarExpression(simplify_expr, input));
  compute::ExecContext* exec_context = default_exec_context();
  ASSERT_OK_AND_ASSIGN(auto function,
                       exec_context->func_registry()->GetFunction("random"));
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       function->Execute(input, &random_options, exec_context));
  AssertDatumsEqual(actual, expected, /*verbose=*/true);

  EXPECT_EQ(actual.length(), kCount);
}

TEST(Expression, ExecuteDictionaryTransparent) {
  ExpectExecute(
      equal(field_ref("a"), field_ref("b")),
      ArrayFromJSON(
          struct_({field("a", dictionary(int32(), utf8())), field("b", utf8())}), R"([
    {"a": "hi", "b": "hi"},
    {"a": "",   "b": ""},
    {"a": "hi", "b": "hello"}
  ])"));

  ASSERT_OK_AND_ASSIGN(
      auto expr, project({field_ref("i32"), field_ref("dict_str")}, {"i32", "dict_str"})
                     .Bind(*kBoringSchema));

  ASSERT_OK_AND_ASSIGN(
      expr, SimplifyWithGuarantee(expr, equal(field_ref("dict_str"), literal("eh"))));

  ASSERT_OK_AND_ASSIGN(auto res, ExecuteScalarExpression(
                                     expr, *kBoringSchema,
                                     ArrayFromJSON(struct_({field("i32", int32())}), R"([
    {"i32": 0},
    {"i32": 1},
    {"i32": 2}
  ])")));

  AssertDatumsEqual(
      res, ArrayFromJSON(struct_({field("i32", int32()),
                                  field("dict_str", dictionary(int32(), utf8()))}),
                         R"([
    {"i32": 0, "dict_str": "eh"},
    {"i32": 1, "dict_str": "eh"},
    {"i32": 2, "dict_str": "eh"}
  ])"));
}

void ExpectIdenticalIfUnchanged(Expression modified, Expression original) {
  if (modified == original) {
    // no change -> must be identical
    EXPECT_TRUE(Identical(modified, original)) << "  " << original.ToString();
  }
}

struct {
  void operator()(Expression expr, Expression expected) {
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(expected, expected.Bind(*kBoringSchema));

    ASSERT_OK_AND_ASSIGN(auto folded, FoldConstants(expr));

    EXPECT_EQ(folded, expected);
    ExpectIdenticalIfUnchanged(folded, expr);
  }
} ExpectFoldsTo;

TEST(Expression, FoldConstants) {
  // literals are unchanged
  ExpectFoldsTo(literal(3), literal(3));

  // field_refs are unchanged
  ExpectFoldsTo(field_ref("i32"), field_ref("i32"));

  // call against literals (3 + 2 == 5)
  ExpectFoldsTo(add(literal(3), literal(2)), literal(5));

  ExpectFoldsTo(equal(literal(3), literal(3)), literal(true));

  // addition of durations folds as expected
  ExpectFoldsTo(add(literal(5min), literal(5min)), literal(10min));

  // addition of duration, timestamp folds as expected
  auto ts = *TimestampScalar::FromISO8601("1990-10-23 10:23:33", TimeUnit::SECOND);
  auto ts_two_hours_later =
      *TimestampScalar::FromISO8601("1990-10-23 12:23:33", TimeUnit::SECOND);
  ExpectFoldsTo(add(literal(2h), literal(ts)), literal(ts_two_hours_later));
  ExpectFoldsTo(add(literal(ts), literal(2h)), literal(ts_two_hours_later));

  // call against literal and field_ref
  ExpectFoldsTo(add(literal(3), field_ref("i32")), add(literal(3), field_ref("i32")));

  // nested call against literals ((8 - (2 * 3)) + 2 == 4)
  ExpectFoldsTo(add(call("subtract",
                         {
                             literal(8),
                             call("multiply", {literal(2), literal(3)}),
                         }),
                    literal(2)),
                literal(4));

  // INTERSECTION null handling and null input -> null output
  ExpectFoldsTo(call("equal", {field_ref("i32"), null_literal(int32())}),
                null_literal(boolean()));

  // nested call against literals with one field_ref
  // (i32 - (2 * 3)) + 2 == (i32 - 6) + 2
  // NB this could be improved further by using associativity of addition; another pass
  ExpectFoldsTo(add(call("subtract",
                         {
                             field_ref("i32"),
                             call("multiply", {literal(2), literal(3)}),
                         }),
                    literal(2)),
                add(call("subtract",
                         {
                             field_ref("i32"),
                             literal(6),
                         }),
                    literal(2)));

  compute::SetLookupOptions in_123(ArrayFromJSON(int32(), "[1,2,3]"));

  ExpectFoldsTo(call("is_in", {literal(2)}, in_123), literal(true));

  ExpectFoldsTo(
      call("is_in", {add(field_ref("i32"), call("multiply", {literal(2), literal(3)}))},
           in_123),
      call("is_in", {add(field_ref("i32"), literal(6))}, in_123));
}

TEST(Expression, FoldConstantsBoolean) {
  // test and_kleene/or_kleene-specific optimizations
  auto one = literal(1);
  auto two = literal(2);
  auto whatever = equal(add(one, field_ref("i32")), two);

  auto true_ = literal(true);
  auto false_ = literal(false);

  ExpectFoldsTo(and_(false_, whatever), false_);
  ExpectFoldsTo(and_(true_, whatever), whatever);
  ExpectFoldsTo(and_(whatever, whatever), whatever);

  ExpectFoldsTo(or_(true_, whatever), true_);
  ExpectFoldsTo(or_(false_, whatever), whatever);
  ExpectFoldsTo(or_(whatever, whatever), whatever);
}

void ExpectRemovesRefsTo(Expression expr, Expression expected,
                         const Schema& schema = *kBoringSchema) {
  ASSERT_OK_AND_ASSIGN(expr, expr.Bind(schema));
  ASSERT_OK_AND_ASSIGN(expected, expected.Bind(schema));

  ASSERT_OK_AND_ASSIGN(auto without_named_refs, RemoveNamedRefs(expr));

  EXPECT_EQ(without_named_refs, expected);
}

TEST(Expression, RemoveNamedRefs) {
  ExpectRemovesRefsTo(field_ref("i32"), field_ref(2));
  ExpectRemovesRefsTo(call("add", {literal(4), field_ref("i32")}),
                      call("add", {literal(4), field_ref(2)}));
  auto nested_schema = Schema({field("a", struct_({field("b", int32())}))});
  ExpectRemovesRefsTo(field_ref({"a", "b"}), field_ref({0, 0}), nested_schema);
}

TEST(Expression, ExtractKnownFieldValues) {
  struct {
    void operator()(Expression guarantee,
                    std::unordered_map<FieldRef, Datum, FieldRef::Hash> expected) {
      ASSERT_OK_AND_ASSIGN(auto actual, ExtractKnownFieldValues(guarantee));
      EXPECT_THAT(actual.map, UnorderedElementsAreArray(expected))
          << "  guarantee: " << guarantee.ToString();
    }
  } ExpectKnown;

  ExpectKnown(equal(field_ref("i32"), literal(3)), {{"i32", Datum(3)}});

  ExpectKnown(greater(field_ref("i32"), literal(3)), {});

  // FIXME known null should be expressed with is_null rather than equality
  auto null_int32 = std::make_shared<Int32Scalar>();
  ExpectKnown(equal(field_ref("i32"), literal(null_int32)), {{"i32", Datum(null_int32)}});

  ExpectKnown(
      and_({equal(field_ref("i32"), literal(3)), equal(field_ref("f32"), literal(1.5F))}),
      {{"i32", Datum(3)}, {"f32", Datum(1.5F)}});

  // NB: guarantees are *not* automatically canonicalized
  ExpectKnown(
      and_({equal(field_ref("i32"), literal(3)), equal(literal(1.5F), field_ref("f32"))}),
      {{"i32", Datum(3)}});

  // NB: guarantees are *not* automatically simplified
  // (the below could be constant folded to a usable guarantee)
  ExpectKnown(or_({equal(field_ref("i32"), literal(3)), literal(false)}), {});

  // NB: guarantees are unbound; applying them may require casts
  ExpectKnown(equal(field_ref("i32"), literal("1234324")), {{"i32", Datum("1234324")}});

  ExpectKnown(
      and_({equal(field_ref("i32"), literal(3)), equal(field_ref("f32"), literal(2.F)),
            equal(field_ref("i32_req"), literal(1))}),
      {{"i32", Datum(3)}, {"f32", Datum(2.F)}, {"i32_req", Datum(1)}});

  ExpectKnown(
      and_(or_(equal(field_ref("i32"), literal(3)), equal(field_ref("i32"), literal(4))),
           equal(field_ref("f32"), literal(2.F))),
      {{"f32", Datum(2.F)}});

  ExpectKnown(and_({equal(field_ref("i32"), literal(3)),
                    equal(field_ref("f32"), field_ref("f32_req")),
                    equal(field_ref("i32_req"), literal(1))}),
              {{"i32", Datum(3)}, {"i32_req", Datum(1)}});
}

TEST(Expression, ReplaceFieldsWithKnownValues) {
  auto ExpectReplacesTo =
      [](Expression expr,
         const std::unordered_map<FieldRef, Datum, FieldRef::Hash>& known_values,
         Expression unbound_expected) {
        ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));
        ASSERT_OK_AND_ASSIGN(auto expected, unbound_expected.Bind(*kBoringSchema));
        ASSERT_OK_AND_ASSIGN(auto replaced, ReplaceFieldsWithKnownValues(
                                                KnownFieldValues{known_values}, expr));

        EXPECT_EQ(replaced, expected);
        ExpectIdenticalIfUnchanged(replaced, expr);
      };

  std::unordered_map<FieldRef, Datum, FieldRef::Hash> i32_is_3{{"i32", Datum(3)}};

  ExpectReplacesTo(literal(1), i32_is_3, literal(1));

  ExpectReplacesTo(field_ref("i32"), i32_is_3, literal(3));

  // NB: known_values will be cast
  ExpectReplacesTo(field_ref("i32"), {{"i32", Datum("3")}}, literal(3));

  ExpectReplacesTo(field_ref("f32"), i32_is_3, field_ref("f32"));

  ExpectReplacesTo(equal(field_ref("i32"), literal(1)), i32_is_3,
                   equal(literal(3), literal(1)));

  Datum dict_str{
      DictionaryScalar::Make(MakeScalar(0), ArrayFromJSON(utf8(), R"(["3"])"))};
  ExpectReplacesTo(field_ref("dict_str"), {{"dict_str", dict_str}}, literal(dict_str));

  ExpectReplacesTo(add(call("subtract",
                            {
                                field_ref("i32"),
                                call("multiply", {literal(2), literal(3)}),
                            }),
                       literal(2)),
                   i32_is_3,
                   add(call("subtract",
                            {
                                literal(3),
                                call("multiply", {literal(2), literal(3)}),
                            }),
                       literal(2)));

  std::unordered_map<FieldRef, Datum, FieldRef::Hash> i32_valid_str_null{
      {"i32", Datum(3)}, {"str", MakeNullScalar(utf8())}};

  ExpectReplacesTo(is_null(field_ref("i32")), i32_valid_str_null, is_null(literal(3)));

  ExpectReplacesTo(is_valid(field_ref("i32")), i32_valid_str_null, is_valid(literal(3)));

  ExpectReplacesTo(is_null(field_ref("str")), i32_valid_str_null,
                   is_null(null_literal(utf8())));

  ExpectReplacesTo(is_valid(field_ref("str")), i32_valid_str_null,
                   is_valid(null_literal(utf8())));

  Datum dict_i32{
      DictionaryScalar::Make(MakeScalar<int32_t>(0), ArrayFromJSON(int32(), R"([3])"))};
  // cast dictionary(int32(), int32()) -> dictionary(int32(), utf8())
  ExpectReplacesTo(field_ref("dict_str"), {{"dict_str", dict_i32}}, literal(dict_str));

  // cast dictionary(int8(), utf8()) -> dictionary(int32(), utf8())
  auto dict_int8_str = Datum{
      DictionaryScalar::Make(MakeScalar<int8_t>(0), ArrayFromJSON(utf8(), R"(["3"])"))};
  ExpectReplacesTo(field_ref("dict_str"), {{"dict_str", dict_int8_str}},
                   literal(dict_str));
}

struct {
  void operator()(Expression expr, Expression unbound_expected) const {
    ASSERT_OK_AND_ASSIGN(auto bound, expr.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto expected, unbound_expected.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto actual, Canonicalize(bound));

    EXPECT_EQ(actual, expected);
    ExpectIdenticalIfUnchanged(actual, bound);
  }
} ExpectCanonicalizesTo;

TEST(Expression, CanonicalizeTrivial) {
  ExpectCanonicalizesTo(literal(1), literal(1));

  ExpectCanonicalizesTo(field_ref("i32"), field_ref("i32"));

  ExpectCanonicalizesTo(equal(field_ref("i32"), field_ref("i32_req")),
                        equal(field_ref("i32"), field_ref("i32_req")));
}

TEST(Expression, CanonicalizeAnd) {
  // some aliases for brevity:
  auto true_ = literal(true);
  auto null_ = literal(std::make_shared<BooleanScalar>());

  auto b = field_ref("bool");
  auto c = equal(literal(1), literal(2));

  // no change possible:
  ExpectCanonicalizesTo(and_(b, c), and_(b, c));

  // literals are placed innermost
  ExpectCanonicalizesTo(and_(b, true_), and_(true_, b));
  ExpectCanonicalizesTo(and_(true_, b), and_(true_, b));

  ExpectCanonicalizesTo(and_(b, and_(true_, c)), and_(and_(true_, b), c));
  ExpectCanonicalizesTo(and_(b, and_(and_(true_, true_), c)),
                        and_(and_(and_(true_, true_), b), c));
  ExpectCanonicalizesTo(and_(b, and_(and_(true_, null_), c)),
                        and_(and_(and_(null_, true_), b), c));
  ExpectCanonicalizesTo(and_(b, and_(and_(true_, null_), and_(c, null_))),
                        and_(and_(and_(and_(null_, null_), true_), b), c));

  // catches and_kleene even when it's a subexpression
  ExpectCanonicalizesTo(is_valid(and_(b, true_)), is_valid(and_(true_, b)));
}

TEST(Expression, CanonicalizeAdd) {
  auto ts = field_ref("ts_s");
  ExpectCanonicalizesTo(add(ts, literal(5min)), add(literal(5min), ts));
  ExpectCanonicalizesTo(add(add(ts, literal(5min)), add(literal(5min), literal(5min))),
                        add(add(add(literal(5min), literal(5min)), literal(5min)), ts));
}

TEST(Expression, CanonicalizeComparison) {
  ExpectCanonicalizesTo(equal(literal(1), field_ref("i32")),
                        equal(field_ref("i32"), literal(1)));

  ExpectCanonicalizesTo(equal(field_ref("i32"), literal(1)),
                        equal(field_ref("i32"), literal(1)));

  ExpectCanonicalizesTo(less(literal(1), field_ref("i32")),
                        greater(field_ref("i32"), literal(1)));

  ExpectCanonicalizesTo(less(field_ref("i32"), literal(1)),
                        less(field_ref("i32"), literal(1)));
}

struct Simplify {
  Expression expr;

  struct Expectable {
    Expression expr, guarantee;

    void Expect(Expression unbound_expected) {
      ASSERT_OK_AND_ASSIGN(auto bound, expr.Bind(*kBoringSchema));

      ASSERT_OK_AND_ASSIGN(auto simplified, SimplifyWithGuarantee(bound, guarantee));

      ASSERT_OK_AND_ASSIGN(auto expected, unbound_expected.Bind(*kBoringSchema));
      EXPECT_EQ(simplified, expected) << "  original:   " << expr.ToString() << "\n"
                                      << "  guarantee:  " << guarantee.ToString() << "\n"
                                      << (simplified == bound ? "  (no change)\n" : "");

      ExpectIdenticalIfUnchanged(simplified, bound);
    }
    void ExpectUnchanged() { Expect(expr); }
    void Expect(bool constant) { Expect(literal(constant)); }
  };

  Expectable WithGuarantee(Expression guarantee) { return {expr, guarantee}; }
};

TEST(Expression, SingleComparisonGuarantees) {
  auto i32 = field_ref("i32");

  // i32 is guaranteed equal to 3, so the projection can just materialize that constant
  // and need not incur IO
  Simplify{project({add(i32, literal(1))}, {"i32 + 1"})}
      .WithGuarantee(equal(i32, literal(3)))
      .Expect(literal(
          std::make_shared<StructScalar>(ScalarVector{std::make_shared<Int32Scalar>(4)},
                                         struct_({field("i32 + 1", int32())}))));

  // i32 is guaranteed equal to 5 everywhere, so filtering i32==5 is redundant and the
  // filter can be simplified to true (== select everything)
  Simplify{
      equal(i32, literal(5)),
  }
      .WithGuarantee(equal(i32, literal(5)))
      .Expect(true);

  Simplify{
      equal(i32, literal(5)),
  }
      .WithGuarantee(equal(i32, literal(5)))
      .Expect(true);

  Simplify{
      less_equal(i32, literal(5)),
  }
      .WithGuarantee(equal(i32, literal(5)))
      .Expect(true);

  Simplify{
      less(i32, literal(5)),
  }
      .WithGuarantee(equal(i32, literal(3)))
      .Expect(true);

  Simplify{
      greater_equal(i32, literal(5)),
  }
      .WithGuarantee(greater(i32, literal(5)))
      .Expect(true);

  // i32 is guaranteed less than 3 everywhere, so filtering i32==5 is redundant and the
  // filter can be simplified to false (== select nothing)
  Simplify{
      equal(i32, literal(5)),
  }
      .WithGuarantee(less(i32, literal(3)))
      .Expect(false);

  Simplify{
      less(i32, literal(5)),
  }
      .WithGuarantee(equal(i32, literal(5)))
      .Expect(false);

  Simplify{
      less_equal(i32, literal(3)),
  }
      .WithGuarantee(equal(i32, literal(5)))
      .Expect(false);

  Simplify{
      equal(i32, literal(0.5)),
  }
      .WithGuarantee(greater_equal(i32, literal(1)))
      .Expect(false);

  // no simplification possible:
  Simplify{
      not_equal(i32, literal(3)),
  }
      .WithGuarantee(less(i32, literal(5)))
      .ExpectUnchanged();

  // exhaustive coverage of all single comparison simplifications
  for (std::string filter_op :
       {"equal", "not_equal", "less", "less_equal", "greater", "greater_equal"}) {
    for (auto filter_rhs : {literal(5), literal(3), literal(7)}) {
      auto filter = call(filter_op, {i32, filter_rhs});
      for (std::string guarantee_op :
           {"equal", "less", "less_equal", "greater", "greater_equal"}) {
        auto guarantee = call(guarantee_op, {i32, literal(5)});

        // generate data which satisfies the guarantee
        static std::unordered_map<std::string, std::string> satisfying_i32{
            {"equal", "[5]"},
            {"less", "[4, 3, 2, 1]"},
            {"less_equal", "[5, 4, 3, 2, 1]"},
            {"greater", "[6, 7, 8, 9]"},
            {"greater_equal", "[5, 6, 7, 8, 9]"},
        };

        ASSERT_OK_AND_ASSIGN(
            Datum input,
            StructArray::Make({ArrayFromJSON(int32(), satisfying_i32[guarantee_op])},
                              {"i32"}));

        ASSERT_OK_AND_ASSIGN(filter, filter.Bind(*kBoringSchema));
        ASSERT_OK_AND_ASSIGN(Datum evaluated,
                             ExecuteScalarExpression(filter, *kBoringSchema, input));

        // ensure that the simplified filter is as simplified as it could be
        // (this is always possible for single comparisons)
        bool all = true, none = true;
        for (int64_t i = 0; i < input.length(); ++i) {
          if (evaluated.array_as<BooleanArray>()->Value(i)) {
            none = false;
          } else {
            all = false;
          }
        }
        Simplify{filter}.WithGuarantee(guarantee).Expect(all    ? literal(true)
                                                         : none ? literal(false)
                                                                : filter);
      }
    }
  }
}

static Status RegisterMyRandom() {
  const std::string name = "my_random";
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), FunctionDoc::Empty(),
                                               nullptr, /*is_pure=*/false);

  auto func_exec = [](KernelContext* /*ctx*/, const ExecSpan& /*batch*/,
                      ExecResult* /*out*/) -> Status { return Status::OK(); };

  ScalarKernel kernel({int32()}, float64(), func_exec);
  ARROW_RETURN_NOT_OK(func->AddKernel(kernel));

  auto registry = GetFunctionRegistry();
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

  return Status::OK();
}

TEST(Expression, SimplifyImpureFunctionCall) {
  // skip simplification for impure function with no arguments
  auto impure_expr = call("random", {});
  Simplify{impure_expr}.WithGuarantee(literal("")).Expect(impure_expr);

  // simplify impure function's arguments
  ASSERT_OK(RegisterMyRandom());
  auto pure_expr = call("add", {field_ref("i32"), literal(3)});
  Simplify{call("my_random", {pure_expr})}
      .WithGuarantee(equal(field_ref("i32"), literal(1)))
      .Expect(call("my_random", {literal(4)}));
}

TEST(Expression, SimplifyWithGuarantee) {
  // drop both members of a conjunctive filter
  Simplify{
      and_(equal(field_ref("i32"), literal(2)), equal(field_ref("f32"), literal(3.5F)))}
      .WithGuarantee(and_(greater_equal(field_ref("i32"), literal(0)),
                          less_equal(field_ref("i32"), literal(1))))
      .Expect(false);

  // drop one member of a conjunctive filter
  Simplify{
      and_(equal(field_ref("i32"), literal(0)), equal(field_ref("f32"), literal(3.5F)))}
      .WithGuarantee(equal(field_ref("i32"), literal(0)))
      .Expect(equal(field_ref("f32"), literal(3.5F)));

  // drop both members of a disjunctive filter
  Simplify{
      or_(equal(field_ref("i32"), literal(0)), equal(field_ref("f32"), literal(3.5F)))}
      .WithGuarantee(equal(field_ref("i32"), literal(0)))
      .Expect(true);

  // drop one member of a disjunctive filter
  Simplify{or_(equal(field_ref("i32"), literal(0)), equal(field_ref("i32"), literal(3)))}
      .WithGuarantee(and_(greater_equal(field_ref("i32"), literal(0)),
                          less_equal(field_ref("i32"), literal(1))))
      .Expect(equal(field_ref("i32"), literal(0)));

  Simplify{or_(equal(field_ref("f32"), literal(0)), equal(field_ref("i32"), literal(3)))}
      .WithGuarantee(greater(field_ref("f32"), literal(0.0)))
      .Expect(equal(field_ref("i32"), literal(3)));

  // simplification can see through implicit casts
  compute::SetLookupOptions in_123{ArrayFromJSON(int32(), "[1,2,3]"), true};
  Simplify{or_({equal(field_ref("f32"), literal(0)),
                call("is_in", {field_ref("i64")}, in_123)})}
      .WithGuarantee(greater(field_ref("f32"), literal(0.F)))
      .Expect(call("is_in", {field_ref("i64")}, in_123));

  Simplify{greater(field_ref("dict_i32"), literal(int64_t(1)))}
      .WithGuarantee(equal(field_ref("dict_i32"), literal(0)))
      .Expect(false);

  Simplify{equal(field_ref("i32"), literal(7))}
      .WithGuarantee(equal(field_ref("i32"), literal(7)))
      .Expect(literal(true));

  Simplify{equal(field_ref("i32"), literal(7))}
      .WithGuarantee(not_(equal(field_ref("i32"), literal(7))))
      .Expect(equal(field_ref("i32"), literal(7)));

  // In the absence of is_null(i32) we assume i32 is valid
  Simplify{
      is_null(field_ref("i32")),
  }
      .WithGuarantee(greater_equal(field_ref("i32"), literal(1)))
      .Expect(false);

  Simplify{
      is_null(field_ref("i32")),
  }
      .WithGuarantee(
          or_(greater_equal(field_ref("i32"), literal(1)), is_null(field_ref("i32"))))
      .Expect(is_null(field_ref("i32")));

  Simplify{
      is_null(field_ref("i32")),
  }
      .WithGuarantee(
          and_(greater_equal(field_ref("i32"), literal(1)), is_valid(field_ref("i32"))))
      .Expect(false);

  Simplify{
      is_valid(field_ref("i32")),
  }
      .WithGuarantee(greater_equal(field_ref("i32"), literal(1)))
      .Expect(true);

  Simplify{
      is_valid(field_ref("i32")),
  }
      .WithGuarantee(
          or_(greater_equal(field_ref("i32"), literal(1)), is_null(field_ref("i32"))))
      .Expect(is_valid(field_ref("i32")));

  Simplify{
      is_valid(field_ref("i32")),
  }
      .WithGuarantee(
          and_(greater_equal(field_ref("i32"), literal(1)), is_valid(field_ref("i32"))))
      .Expect(true);
}

TEST(Expression, SimplifyWithValidityGuarantee) {
  Simplify{is_null(field_ref("i32"))}
      .WithGuarantee(is_null(field_ref("i32")))
      .Expect(literal(true));

  Simplify{is_valid(field_ref("i32"))}
      .WithGuarantee(is_null(field_ref("i32")))
      .Expect(literal(false));

  Simplify{{true_unless_null(field_ref("i32"))}}
      .WithGuarantee(is_null(field_ref("i32")))
      .Expect(null_literal(boolean()));

  Simplify{is_valid(field_ref("i32"))}
      .WithGuarantee(is_valid(field_ref("i32")))
      .Expect(literal(true));

  Simplify{is_valid(field_ref("i32"))}
      .WithGuarantee(is_valid(field_ref("dict_i32")))  // different field
      .Expect(is_valid(field_ref("i32")));

  Simplify{is_null(field_ref("i32"))}
      .WithGuarantee(is_valid(field_ref("i32")))
      .Expect(literal(false));

  Simplify{true_unless_null(field_ref("i32"))}
      .WithGuarantee(is_valid(field_ref("i32")))
      .Expect(literal(true));

  Simplify{{equal(field_ref("i32"), literal(7))}}
      .WithGuarantee(is_null(field_ref("i32")))
      .Expect(null_literal(boolean()));

  auto i32_is_2_or_null =
      or_(equal(field_ref("i32"), literal(2)), is_null(field_ref("i32")));

  Simplify{i32_is_2_or_null}
      .WithGuarantee(is_null(field_ref("i32")))
      .Expect(literal(true));

  Simplify{{greater(field_ref("i32"), literal(7))}}
      .WithGuarantee(is_null(field_ref("i32")))
      .Expect(null_literal(boolean()));
}

TEST(Expression, SimplifyWithComparisonAndNullableCaveat) {
  auto i32_is_2_or_null =
      or_(equal(field_ref("i32"), literal(2)), is_null(field_ref("i32")));

  Simplify{equal(field_ref("i32"), literal(2))}
      .WithGuarantee(i32_is_2_or_null)
      .Expect(true_unless_null(field_ref("i32")));

  // XXX: needs a rule for 'true_unless_null(x) || is_null(x)'
  // Simplify{i32_is_2_or_null}.WithGuarantee(i32_is_2_or_null).Expect(literal(true));

  Simplify{equal(field_ref("i32"), literal(3))}
      .WithGuarantee(i32_is_2_or_null)
      .Expect(not_(
          true_unless_null(field_ref("i32"))));  // not satisfiable, will drop row group
}

TEST(Expression, SimplifyThenExecute) {
  auto filter =
      or_({equal(field_ref("f32"), literal(0)),
           call("is_in", {field_ref("i64")},
                compute::SetLookupOptions{ArrayFromJSON(int32(), "[1,2,3]"), true})});

  ASSERT_OK_AND_ASSIGN(filter, filter.Bind(*kBoringSchema));
  auto guarantee = greater(field_ref("f32"), literal(0.0));

  ASSERT_OK_AND_ASSIGN(auto simplified, SimplifyWithGuarantee(filter, guarantee));

  auto input = RecordBatchFromJSON(kBoringSchema, R"([
      {"i64": 0, "f32": 0.1},
      {"i64": 0, "f32": 0.3},
      {"i64": 1, "f32": 0.5},
      {"i64": 2, "f32": 0.1},
      {"i64": 0, "f32": 0.1},
      {"i64": 0, "f32": 0.4},
      {"i64": 0, "f32": 1.0}
  ])");

  Datum evaluated, simplified_evaluated;
  ExpectExecute(filter, input, &evaluated);
  ExpectExecute(simplified, input, &simplified_evaluated);
  AssertDatumsEqual(evaluated, simplified_evaluated, /*verbose=*/true);
}

TEST(Expression, Filter) {
  auto ExpectFilter = [](Expression filter, std::string batch_json) {
    ASSERT_OK_AND_ASSIGN(auto s, kBoringSchema->AddField(0, field("in", boolean())));
    auto batch = RecordBatchFromJSON(s, batch_json);
    auto expected_mask = batch->column(0);

    ASSERT_OK_AND_ASSIGN(filter, filter.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(Datum mask,
                         ExecuteScalarExpression(filter, *kBoringSchema, batch));

    AssertDatumsEqual(expected_mask, mask);
  };

  ExpectFilter(equal(field_ref("i32"), literal(0)), R"([
      {"i32": 0, "f32": -0.1, "in": 1},
      {"i32": 0, "f32":  0.3, "in": 1},
      {"i32": 1, "f32":  0.2, "in": 0},
      {"i32": 2, "f32": -0.1, "in": 0},
      {"i32": 0, "f32":  0.1, "in": 1},
      {"i32": 0, "f32": null, "in": 1},
      {"i32": 0, "f32":  1.0, "in": 1}
  ])");

  ExpectFilter(
      greater(call("multiply", {field_ref("f32"), field_ref("f64")}), literal(0)), R"([
      {"f64":  0.3, "f32":  0.1, "in": 1},
      {"f64": -0.1, "f32":  0.3, "in": 0},
      {"f64":  0.1, "f32":  0.2, "in": 1},
      {"f64":  0.0, "f32": -0.1, "in": 0},
      {"f64":  1.0, "f32":  0.1, "in": 1},
      {"f64": -2.0, "f32": null, "in": null},
      {"f64":  3.0, "f32":  1.0, "in": 1}
  ])");
}

TEST(Expression, SerializationRoundTrips) {
  auto ExpectRoundTrips = [](const Expression& expr) {
    ASSERT_OK_AND_ASSIGN(auto serialized, Serialize(expr));
    ASSERT_OK_AND_ASSIGN(Expression roundtripped, Deserialize(serialized));
    EXPECT_EQ(expr, roundtripped);
  };

  ExpectRoundTrips(literal(MakeNullScalar(null())));

  ExpectRoundTrips(literal(MakeNullScalar(int32())));

  ExpectRoundTrips(
      literal(MakeNullScalar(struct_({field("i", int32()), field("s", utf8())}))));

  ExpectRoundTrips(literal(true));

  ExpectRoundTrips(literal(false));

  ExpectRoundTrips(literal(1));

  ExpectRoundTrips(literal(1.125));

  ExpectRoundTrips(literal("stringy strings"));

  ExpectRoundTrips(field_ref("field"));

  ExpectRoundTrips(field_ref(FieldRef("foo", "bar", "baz")));

  ExpectRoundTrips(greater(field_ref("a"), literal(0.25)));

  ExpectRoundTrips(
      or_({equal(field_ref("a"), literal(1)), not_equal(field_ref("b"), literal("hello")),
           equal(field_ref("b"), literal("foo bar"))}));

  ExpectRoundTrips(or_({equal(field_ref(FieldRef("a", "b")), literal(1)),
                        not_equal(field_ref("b"), literal("hello")),
                        equal(field_ref(FieldRef("c", "d")), literal("foo bar"))}));

  ExpectRoundTrips(not_(field_ref("alpha")));

  ExpectRoundTrips(call("is_in", {literal(1)},
                        compute::SetLookupOptions{ArrayFromJSON(int32(), "[1, 2, 3]")}));

  ExpectRoundTrips(
      call("is_in",
           {call("cast", {field_ref("version")}, compute::CastOptions::Safe(float64()))},
           compute::SetLookupOptions{ArrayFromJSON(float64(), "[0.5, 1.0, 2.0]"), true}));

  ExpectRoundTrips(call("is_valid", {field_ref("validity")}));

  ExpectRoundTrips(and_({and_(greater_equal(field_ref("x"), literal(-1.5)),
                              less(field_ref("x"), literal(0.0))),
                         and_(greater_equal(field_ref("y"), literal(0.0)),
                              less(field_ref("y"), literal(1.5))),
                         and_(greater(field_ref("z"), literal(1.5)),
                              less_equal(field_ref("z"), literal(3.0)))}));

  ExpectRoundTrips(and_({equal(field_ref("year"), literal(int16_t(1999))),
                         equal(field_ref("month"), literal(int8_t(12))),
                         equal(field_ref("day"), literal(int8_t(31))),
                         equal(field_ref("hour"), literal(int8_t(0))),
                         equal(field_ref("alpha"), literal(int32_t(0))),
                         equal(field_ref("beta"), literal(3.25f))}));
}

TEST(Projection, AugmentWithNull) {
  // NB: input contains *no columns* except i32
  auto input = ArrayFromJSON(struct_({kBoringSchema->GetFieldByName("i32")}),
                             R"([{"i32": 0}, {"i32": 1}, {"i32": 2}])");

  auto ExpectProject = [&](Expression proj, Datum expected) {
    ASSERT_OK_AND_ASSIGN(proj, proj.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto actual,
                         ExecuteScalarExpression(proj, *kBoringSchema, input));
    AssertDatumsEqual(Datum(expected), actual);
  };

  ExpectProject(project({field_ref("f64"), field_ref("i32")},
                        {"projected double", "projected int"}),
                // "projected double" is materialized as a column of nulls
                ArrayFromJSON(struct_({field("projected double", float64()),
                                       field("projected int", int32())}),
                              R"([
                                  [null, 0],
                                  [null, 1],
                                  [null, 2]
                              ])"));

  ExpectProject(
      project({field_ref("f64")}, {"projected double"}),
      // NB: only a scalar was projected, this is *not* automatically broadcast
      // to an array. "projected double" is materialized as a null scalar
      Datum(*StructScalar::Make({MakeNullScalar(float64())}, {"projected double"})));
}

TEST(Projection, AugmentWithKnownValues) {
  auto input = ArrayFromJSON(struct_({kBoringSchema->GetFieldByName("i32")}),
                             R"([{"i32": 0}, {"i32": 1}, {"i32": 2}])");

  auto ExpectSimplifyAndProject = [&](Expression proj, Datum expected,
                                      Expression guarantee) {
    ASSERT_OK_AND_ASSIGN(proj, proj.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(proj, SimplifyWithGuarantee(proj, guarantee));
    ASSERT_OK_AND_ASSIGN(auto actual,
                         ExecuteScalarExpression(proj, *kBoringSchema, input));
    AssertDatumsEqual(Datum(expected), actual);
  };

  ExpectSimplifyAndProject(
      project({field_ref("str"), field_ref("f64"), field_ref("i64"), field_ref("i32")},
              {"str", "f64", "i64", "i32"}),
      ArrayFromJSON(struct_({
                        field("str", utf8()),
                        field("f64", float64()),
                        field("i64", int64()),
                        field("i32", int32()),
                    }),
                    // str is explicitly null
                    // f64 is explicitly 3.5
                    // i64 is not specified in the guarantee and implicitly null
                    // i32 is present in the input and passed through
                    R"([
                        {"str": null, "f64": 3.5, "i64": null, "i32": 0},
                        {"str": null, "f64": 3.5, "i64": null, "i32": 1},
                        {"str": null, "f64": 3.5, "i64": null, "i32": 2}
                    ])"),
      and_({
          equal(field_ref("f64"), literal(3.5)),
          is_null(field_ref("str")),
      }));
}

}  // namespace compute
}  // namespace arrow
