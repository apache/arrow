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

#include "arrow/dataset/expression.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/registry.h"
#include "arrow/dataset/expression_internal.h"
#include "arrow/testing/gtest_util.h"

using testing::UnorderedElementsAreArray;

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace dataset {

const Schema kBoringSchema{{
    field("i32", int32()),
    field("f32", float32()),
}};

#define EXPECT_OK ARROW_EXPECT_OK

TEST(Expression2, ToString) {
  EXPECT_EQ(field_ref("alpha").ToString(), "FieldRef(alpha)");

  EXPECT_EQ(literal(3).ToString(), "3");

  EXPECT_EQ(call("add", {literal(3), field_ref("beta")}).ToString(),
            "add(3,FieldRef(beta))");

  EXPECT_EQ(call("add", {literal(3), call("index_in", {field_ref("beta")})}).ToString(),
            "add(3,index_in(FieldRef(beta)))");
}

TEST(Expression2, Equality) {
  EXPECT_EQ(literal(1), literal(1));

  EXPECT_EQ(field_ref("a"), field_ref("a"));

  EXPECT_EQ(call("add", {literal(3), field_ref("a")}),
            call("add", {literal(3), field_ref("a")}));

  EXPECT_EQ(call("add", {literal(3), call("index_in", {field_ref("beta")})}),
            call("add", {literal(3), call("index_in", {field_ref("beta")})}));
}

TEST(Expression2, Hash) {
  std::unordered_set<Expression2, Expression2::Hash> set;

  EXPECT_TRUE(set.emplace(field_ref("alpha")).second);
  EXPECT_TRUE(set.emplace(field_ref("beta")).second);
  EXPECT_FALSE(set.emplace(field_ref("beta")).second) << "already inserted";
  EXPECT_TRUE(set.emplace(literal(1)).second);
  EXPECT_FALSE(set.emplace(literal(1)).second) << "already inserted";
  EXPECT_TRUE(set.emplace(literal(3)).second);

  // NB: no validation on construction; we couldn't execute
  //     add with zero arguments
  EXPECT_TRUE(set.emplace(call("add", {})).second);
  EXPECT_FALSE(set.emplace(call("add", {})).second) << "already inserted";

  // NB: unbound expressions don't check for availability in any registry
  EXPECT_TRUE(set.emplace(call("widgetify", {})).second);

  EXPECT_EQ(set.size(), 6);
}

TEST(Expression2, BindLiteral) {
  for (Datum dat : {
           Datum(3),
           Datum(3.5),
           Datum(ArrayFromJSON(int32(), "[1,2,3]")),
       }) {
    // literals are always considered bound
    auto expr = literal(dat);
    EXPECT_EQ(expr.descr(), dat.descr());
    EXPECT_TRUE(expr.IsBound());
  }
}

TEST(Expression2, BindFieldRef) {
  // an unbound field_ref does not have the output ValueDescr set
  {
    auto expr = field_ref("alpha");
    EXPECT_EQ(expr.descr(), ValueDescr{});
    EXPECT_FALSE(expr.IsBound());
  }

  {
    auto expr = field_ref("alpha");
    // binding a field_ref looks up that field's type in the input Schema
    ASSERT_OK_AND_ASSIGN(std::tie(expr, std::ignore),
                         expr.Bind(Schema({field("alpha", int32())})));
    EXPECT_EQ(expr.descr(), ValueDescr::Array(int32()));
    EXPECT_TRUE(expr.IsBound());
  }

  {
    // if the field is not found, a null scalar will be emitted
    auto expr = field_ref("alpha");
    ASSERT_OK_AND_ASSIGN(std::tie(expr, std::ignore), expr.Bind(Schema({})));
    EXPECT_EQ(expr.descr(), ValueDescr::Scalar(null()));
    EXPECT_TRUE(expr.IsBound());
  }

  {
    // referencing a field by name is not supported if that name is not unique
    // in the input schema
    auto expr = field_ref("alpha");
    ASSERT_RAISES(
        Invalid, expr.Bind(Schema({field("alpha", int32()), field("alpha", float32())})));
  }

  {
    // referencing nested fields is supported
    auto expr = field_ref("a", "b");
    ASSERT_OK_AND_ASSIGN(std::tie(expr, std::ignore),
                         expr.Bind(Schema({field("a", struct_({field("b", int32())}))})));
    EXPECT_EQ(expr.descr(), ValueDescr::Array(int32()));
    EXPECT_TRUE(expr.IsBound());
  }
}

TEST(Expression2, BindCall) {
  auto expr = call("add", {field_ref("a"), field_ref("b")});
  EXPECT_FALSE(expr.IsBound());

  ASSERT_OK_AND_ASSIGN(std::tie(expr, std::ignore),
                       expr.Bind(Schema({field("a", int32()), field("b", int32())})));
  EXPECT_EQ(expr.descr(), ValueDescr::Array(int32()));
  EXPECT_TRUE(expr.IsBound());

  expr = call("add", {field_ref("a"), literal(3.5)});
  ASSERT_RAISES(NotImplemented,
                expr.Bind(Schema({field("a", int32()), field("b", int32())})));
}

TEST(Expression2, BindNestedCall) {
  auto expr =
      call("add", {field_ref("a"),
                   call("subtract", {call("multiply", {field_ref("b"), field_ref("c")}),
                                     field_ref("d")})});
  EXPECT_FALSE(expr.IsBound());

  ASSERT_OK_AND_ASSIGN(std::tie(expr, std::ignore),
                       expr.Bind(Schema({field("a", int32()), field("b", int32()),
                                         field("c", int32()), field("d", int32())})));
  EXPECT_EQ(expr.descr(), ValueDescr::Array(int32()));
  EXPECT_TRUE(expr.IsBound());
}

TEST(Expression2, ExecuteFieldRef) {
  auto AssertRefIs = [](FieldRef ref, Datum in, Datum expected) {
    auto expr = field_ref(ref);

    ASSERT_OK_AND_ASSIGN(std::tie(expr, std::ignore), expr.Bind(in.descr()));
    ASSERT_OK_AND_ASSIGN(Datum actual,
                         ExecuteScalarExpression(expr, /*state=*/nullptr, in));

    AssertDatumsEqual(actual, expected, /*verbose=*/true);
  };

  AssertRefIs("a", ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, 0.0, -1])"));

  // more nested:
  AssertRefIs(FieldRef{"a", "a"},
              ArrayFromJSON(struct_({field("a", struct_({field("a", float64())}))}), R"([
    {"a": {"a": 6.125}},
    {"a": {"a": 0.0}},
    {"a": {"a": -1}}
  ])"),
              ArrayFromJSON(float64(), R"([6.125, 0.0, -1])"));

  // absent fields are resolved as a null scalar:
  AssertRefIs(FieldRef{"b"}, ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"),
              MakeNullScalar(float64()));
}

Result<Datum> NaiveExecuteScalarExpression(const Expression2& expr,
                                           ExpressionState* state, const Datum& input) {
  auto call = expr.call();
  if (call == nullptr) {
    // already tested execution of field_ref, execution of literal is trivial
    return ExecuteScalarExpression(expr, state, input);
  }

  auto call_state = checked_cast<CallState*>(state);

  std::vector<Datum> arguments(call->arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    auto argument_state = call_state->argument_states[i].get();
    ARROW_ASSIGN_OR_RAISE(arguments[i], NaiveExecuteScalarExpression(
                                            call->arguments[i], argument_state, input));

    EXPECT_EQ(call->arguments[i].descr(), arguments[i].descr());
  }

  ARROW_ASSIGN_OR_RAISE(auto function,
                        compute::GetFunctionRegistry()->GetFunction(call->function));
  ARROW_ASSIGN_OR_RAISE(auto expected_kernel,
                        function->DispatchExact(GetDescriptors(call->arguments)));

  EXPECT_EQ(call->kernel, expected_kernel);

  compute::ExecContext exec_context;
  return function->Execute(arguments, call->options.get(), &exec_context);
}

void AssertExecute(Expression2 expr, Datum in) {
  std::shared_ptr<ExpressionState> state;
  ASSERT_OK_AND_ASSIGN(std::tie(expr, state), expr.Bind(in.descr()));

  ASSERT_OK_AND_ASSIGN(Datum actual, ExecuteScalarExpression(expr, state.get(), in));

  ASSERT_OK_AND_ASSIGN(Datum expected,
                       NaiveExecuteScalarExpression(expr, state.get(), in));

  AssertDatumsEqual(actual, expected, /*verbose=*/true);
}

TEST(Expression2, ExecuteCall) {
  AssertExecute(call("add", {field_ref("a"), literal(3.5)}),
                ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"));

  AssertExecute(
      call("add", {field_ref("a"), call("subtract", {literal(3.5), field_ref("b")})}),
      ArrayFromJSON(struct_({field("a", float64()), field("b", float64())}), R"([
    {"a": 6.125, "b": 3.375},
    {"a": 0.0,   "b": 1},
    {"a": -1,    "b": 4.75}
  ])"));

  AssertExecute(call("strptime", {field_ref("a")},
                     compute::StrptimeOptions("%m/%d/%Y", TimeUnit::MICRO)),
                ArrayFromJSON(struct_({field("a", utf8())}), R"([
    {"a": "5/1/2020"},
    {"a": null},
    {"a": "12/11/1900"}
  ])"));

  AssertExecute(project({"a + 3.5"}, {call("add", {field_ref("a"), literal(3.5)})}),
                ArrayFromJSON(struct_({field("a", float64())}), R"([
    {"a": 6.125},
    {"a": 0.0},
    {"a": -1}
  ])"));
}

struct {
  void operator()(Expression2 expr, Expression2 expected) {
    std::shared_ptr<ExpressionState> state;
    ASSERT_OK_AND_ASSIGN(std::tie(expr, state), expr.Bind(kBoringSchema));
    ASSERT_OK_AND_ASSIGN(std::tie(expected, std::ignore), expected.Bind(kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto actual, FoldConstants(expr, state.get()));
    EXPECT_EQ(actual, expected);
    if (actual == expr) {
      // no change -> must be identical
      EXPECT_TRUE(Identical(actual, expr));
    }
  }
} ExpectFoldsTo;

TEST(Expression2, FoldConstants) {
  // literals are unchanged
  ExpectFoldsTo(literal(3), literal(3));

  // field_refs are unchanged
  ExpectFoldsTo(field_ref("i32"), field_ref("i32"));

  // call against literals (3 + 2 == 5)
  ExpectFoldsTo(call("add", {literal(3), literal(2)}), literal(5));

  // call against literal and field_ref
  ExpectFoldsTo(call("add", {literal(3), field_ref("i32")}),
                call("add", {literal(3), field_ref("i32")}));

  // nested call against literals ((8 - (2 * 3)) + 2 == 4)
  ExpectFoldsTo(call("add",
                     {
                         call("subtract",
                              {
                                  literal(8),
                                  call("multiply", {literal(2), literal(3)}),
                              }),
                         literal(2),
                     }),
                literal(4));

  // nested call against literals with one field_ref
  // (i32 - (2 * 3)) + 2 == (i32 - 6) + 2
  // NB this could be improved further by using associativity of addition; another pass
  ExpectFoldsTo(call("add",
                     {
                         call("subtract",
                              {
                                  field_ref("i32"),
                                  call("multiply", {literal(2), literal(3)}),
                              }),
                         literal(2),
                     }),
                call("add", {
                                call("subtract",
                                     {
                                         field_ref("i32"),
                                         literal(6),
                                     }),
                                literal(2),
                            }));
}

TEST(Expression2, FoldConstantsBoolean) {
  // test and_kleene/or_kleene-specific optimizations
  auto one = literal(1);
  auto two = literal(2);
  auto whatever = call("equal", {call("add", {one, field_ref("i32")}), two});

  auto true_ = literal(true);
  auto false_ = literal(false);

  ExpectFoldsTo(call("and_kleene", {false_, whatever}), false_);
  ExpectFoldsTo(call("and_kleene", {true_, whatever}), whatever);

  ExpectFoldsTo(call("or_kleene", {true_, whatever}), true_);
  ExpectFoldsTo(call("or_kleene", {false_, whatever}), whatever);
}

TEST(Expression2, ExtractKnownFieldValues) {
  struct {
    void operator()(Expression2 guarantee,
                    std::unordered_map<FieldRef, Datum, FieldRef::Hash> expected) {
      ASSERT_OK_AND_ASSIGN(auto actual, ExtractKnownFieldValues(guarantee));
      EXPECT_THAT(actual, UnorderedElementsAreArray(expected));
    }
  } ExpectKnown;

  ExpectKnown(call("equal", {field_ref("a"), literal(3)}), {{"a", Datum(3)}});

  ExpectKnown(call("greater", {field_ref("a"), literal(3)}), {});

  // FIXME known null should be expressed with is_null rather than equality
  auto null_int32 = std::make_shared<Int32Scalar>();
  ExpectKnown(call("equal", {field_ref("a"), literal(null_int32)}),
              {{"a", Datum(null_int32)}});

  ExpectKnown(call("and_kleene", {call("equal", {field_ref("a"), literal(3)}),
                                  call("equal", {literal(1), field_ref("b")})}),
              {{"a", Datum(3)}, {"b", Datum(1)}});

  ExpectKnown(call("and_kleene",
                   {call("equal", {field_ref("a"), literal(3)}),
                    call("and_kleene", {call("equal", {field_ref("b"), literal(2)}),
                                        call("equal", {literal(1), field_ref("c")})})}),
              {{"a", Datum(3)}, {"b", Datum(2)}, {"c", Datum(1)}});

  ExpectKnown(call("and_kleene",
                   {call("or_kleene", {call("equal", {field_ref("a"), literal(3)}),
                                       call("equal", {field_ref("a"), literal(4)})}),
                    call("equal", {literal(1), field_ref("b")})}),
              {{"b", Datum(1)}});

  ExpectKnown(
      call("and_kleene",
           {call("equal", {field_ref("a"), literal(3)}),
            call("and_kleene", {call("and_kleene", {field_ref("b"), field_ref("d")}),
                                call("equal", {literal(1), field_ref("c")})})}),
      {{"a", Datum(3)}, {"c", Datum(1)}});
}

TEST(Expression2, ReplaceFieldsWithKnownValues) {
  auto ExpectSimplifiesTo =
      [](Expression2 expr,
         std::unordered_map<FieldRef, Datum, FieldRef::Hash> known_values,
         Expression2 expected) {
        ASSERT_OK_AND_ASSIGN(auto actual,
                             ReplaceFieldsWithKnownValues(known_values, expr));
        EXPECT_EQ(actual, expected);

        if (actual == expr) {
          // no change -> must be identical
          EXPECT_TRUE(Identical(actual, expr));
        }
      };

  std::unordered_map<FieldRef, Datum, FieldRef::Hash> a_is_3{{"a", Datum(3)}};

  ExpectSimplifiesTo(literal(1), a_is_3, literal(1));

  ExpectSimplifiesTo(field_ref("a"), a_is_3, literal(3));

  ExpectSimplifiesTo(field_ref("b"), a_is_3, field_ref("b"));

  ExpectSimplifiesTo(call("equal", {field_ref("a"), literal(1)}), a_is_3,
                     call("equal", {literal(3), literal(1)}));

  ExpectSimplifiesTo(call("add",
                          {
                              call("subtract",
                                   {
                                       field_ref("a"),
                                       call("multiply", {literal(2), literal(3)}),
                                   }),
                              literal(2),
                          }),
                     a_is_3,
                     call("add", {
                                     call("subtract",
                                          {
                                              literal(3),
                                              call("multiply", {literal(2), literal(3)}),
                                          }),
                                     literal(2),
                                 }));
}

struct {
  void operator()(Expression2 expr, Expression2 expected) const {
    ASSERT_OK_AND_ASSIGN(auto actual, Canonicalize(expr));
    EXPECT_EQ(actual, expected);

    if (expr.IsBound()) {
      EXPECT_TRUE(actual.IsBound());
    }

    if (actual == expr) {
      // no change -> must be identical
      EXPECT_TRUE(Identical(actual, expr));
    }
  }
} ExpectCanonicalizesTo;

TEST(Expression2, CanonicalizeTrivial) {
  ExpectCanonicalizesTo(literal(1), literal(1));

  ExpectCanonicalizesTo(field_ref("b"), field_ref("b"));

  ExpectCanonicalizesTo(call("equal", {field_ref("a"), field_ref("b")}),
                        call("equal", {field_ref("a"), field_ref("b")}));
}

TEST(Expression2, CanonicalizeAnd) {
  // some aliases for brevity:
  auto true_ = literal(true);
  auto null_ = literal(std::make_shared<BooleanScalar>());

  auto a = field_ref("a");
  auto c = call("equal", {literal(1), literal(2)});

  auto and_ = [](Expression2 l, Expression2 r) { return call("and_kleene", {l, r}); };

  // no change possible:
  ExpectCanonicalizesTo(and_(a, c), and_(a, c));

  // literals are placed innermost
  ExpectCanonicalizesTo(and_(a, true_), and_(true_, a));
  ExpectCanonicalizesTo(and_(true_, a), and_(true_, a));

  ExpectCanonicalizesTo(and_(a, and_(true_, c)), and_(and_(true_, a), c));
  ExpectCanonicalizesTo(and_(a, and_(and_(true_, true_), c)),
                        and_(and_(and_(true_, true_), a), c));
  ExpectCanonicalizesTo(and_(a, and_(and_(true_, null_), c)),
                        and_(and_(and_(null_, true_), a), c));
  ExpectCanonicalizesTo(and_(a, and_(and_(true_, null_), and_(c, null_))),
                        and_(and_(and_(and_(null_, null_), true_), a), c));

  // catches and_kleene even when it's a subexpression
  ExpectCanonicalizesTo(call("is_valid", {and_(a, true_)}),
                        call("is_valid", {and_(true_, a)}));
}

TEST(Expression2, CanonicalizeComparison) {
  // some aliases for brevity:
  auto equal = [](Expression2 l, Expression2 r) { return call("equal", {l, r}); };
  auto less = [](Expression2 l, Expression2 r) { return call("less", {l, r}); };
  auto greater = [](Expression2 l, Expression2 r) { return call("greater", {l, r}); };

  ExpectCanonicalizesTo(equal(literal(1), field_ref("a")),
                        equal(field_ref("a"), literal(1)));

  ExpectCanonicalizesTo(equal(field_ref("a"), literal(1)),
                        equal(field_ref("a"), literal(1)));

  ExpectCanonicalizesTo(less(literal(1), field_ref("a")),
                        greater(field_ref("a"), literal(1)));

  ExpectCanonicalizesTo(less(field_ref("a"), literal(1)),
                        less(field_ref("a"), literal(1)));
}

struct Simplify {
  Expression2 filter;

  struct Expectable {
    Expression2 filter, guarantee;

    void Expect(Expression2 expected) {
      std::shared_ptr<ExpressionState> state;
      ASSERT_OK_AND_ASSIGN(std::tie(filter, state), filter.Bind(kBoringSchema));

      ASSERT_OK_AND_ASSIGN(std::tie(expected, std::ignore), expected.Bind(kBoringSchema));
      ASSERT_OK_AND_ASSIGN(auto simplified,
                           SimplifyWithGuarantee(filter, state.get(), guarantee));
      EXPECT_EQ(simplified, expected) << "  original:   " << filter.ToString() << "\n"
                                      << "  guarantee:  " << guarantee.ToString() << "\n"
                                      << (simplified == filter ? "  (no change)\n" : "");

      if (simplified == filter) {
        EXPECT_TRUE(Identical(simplified, filter));
      }
    }
    void ExpectUnchanged() { Expect(filter); }
    void Expect(bool constant) { Expect(literal(constant)); }
  };

  Expectable WithGuarantee(Expression2 guarantee) { return {filter, guarantee}; }
};

TEST(Expression2, SingleComparisonGuarantees) {
  // some aliases for brevity:
  auto equal = [](Expression2 l, Expression2 r) { return call("equal", {l, r}); };
  auto less = [](Expression2 l, Expression2 r) { return call("less", {l, r}); };
  auto greater = [](Expression2 l, Expression2 r) { return call("greater", {l, r}); };
  auto not_equal = [](Expression2 l, Expression2 r) { return call("not_equal", {l, r}); };
  auto less_equal = [](Expression2 l, Expression2 r) {
    return call("less_equal", {l, r});
  };
  auto greater_equal = [](Expression2 l, Expression2 r) {
    return call("greater_equal", {l, r});
  };
  auto i32 = field_ref("i32");

  // i32 is guaranteed equal to 3, so the projection can just materialize that constant
  // and need not incur IO
  Simplify{project({"i32 + 1"}, {call("add", {i32, literal(1)})})}
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

        std::shared_ptr<ExpressionState> state;
        ASSERT_OK_AND_ASSIGN(std::tie(filter, state), filter.Bind(kBoringSchema));
        ASSERT_OK_AND_ASSIGN(Datum evaluated,
                             ExecuteScalarExpression(filter, state.get(), input));

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
        Simplify{filter}.WithGuarantee(guarantee).Expect(
            all ? literal(true) : none ? literal(false) : filter);
      }
    }
  }
}

TEST(Expression2, SimplifyWithGuarantee) {
  // drop both members of a conjunctive filter
  Simplify{call("and_kleene",
                {
                    call("equal", {field_ref("i32"), literal(2)}),
                    call("equal", {field_ref("f32"), literal(3.5F)}),
                })}
      .WithGuarantee(call("and_kleene",
                          {
                              call("greater_equal", {field_ref("i32"), literal(0)}),
                              call("less_equal", {field_ref("i32"), literal(1)}),
                          }))
      .Expect(false);

  // drop one member of a conjunctive filter
  Simplify{call("and_kleene",
                {
                    call("equal", {field_ref("i32"), literal(0)}),
                    call("equal", {field_ref("f32"), literal(3.5F)}),
                })}
      .WithGuarantee(call("equal", {field_ref("i32"), literal(0)}))
      .Expect(call("equal", {field_ref("f32"), literal(3.5F)}));

  // drop both members of a disjunctive filter
  Simplify{call("or_kleene",
                {
                    call("equal", {field_ref("i32"), literal(0)}),
                    call("equal", {field_ref("f32"), literal(3.5F)}),
                })}
      .WithGuarantee(call("equal", {field_ref("i32"), literal(0)}))
      .Expect(true);

  // drop one member of a disjunctive filter
  Simplify{call("or_kleene",
                {
                    call("equal", {field_ref("i32"), literal(0)}),
                    call("equal", {field_ref("i32"), literal(3)}),
                })}
      .WithGuarantee(call("and_kleene",
                          {
                              call("greater_equal", {field_ref("i32"), literal(0)}),
                              call("less_equal", {field_ref("i32"), literal(1)}),
                          }))
      .Expect(call("equal", {field_ref("i32"), literal(0)}));
}

}  // namespace dataset
}  // namespace arrow
