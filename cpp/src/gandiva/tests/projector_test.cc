// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "integ/test_util.h"
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;
using arrow::float32;
using arrow::boolean;

class TestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestProjector, TestIntSumSub) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr = TreeExprBuilder::MakeExpression("subtract", {field0, field1},
                                                  field_sub);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {sum_expr, sub_expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 1, 2, 3, 4 }, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({ 11, 13, 15, 17 }, { true, true, false, true });
  // expected output
  auto exp_sum = MakeArrowArrayInt32({ 12, 15, 0, 0 }, { true, true, false, false });
  auto exp_sub = MakeArrowArrayInt32({ -10, -11, 0, 0 }, { true, true, false, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sub, outputs.at(1));
}

TEST_F(TestProjector, TestIntSumSubCustomConfig) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr = TreeExprBuilder::MakeExpression("subtract", {field0, field1},
                                                  field_sub);

  std::shared_ptr<Projector> projector;
  ConfigurationBuilder config_builder;
  std::shared_ptr<Configuration> config = config_builder.build();

  Status status = Projector::Make(schema,
                                 {sum_expr, sub_expr}, pool_, config, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 1, 2, 3, 4 }, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({ 11, 13, 15, 17 }, { true, true, false, true });
  // expected output
  auto exp_sum = MakeArrowArrayInt32({ 12, 15, 0, 0 }, { true, true, false, false });
  auto exp_sub = MakeArrowArrayInt32({ -10, -11, 0, 0 }, { true, true, false, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sub, outputs.at(1));
}

template<typename TYPE, typename C_TYPE>
static void TestArithmeticOpsForType(arrow::MemoryPool *pool) {
  auto atype =  arrow::TypeTraits<TYPE>::type_singleton();

  // schema for input fields
  auto field0 = field("f0", atype);
  auto field1 = field("f1", atype);
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", atype);
  auto field_sub = field("subtract", atype);
  auto field_mul = field("multiply", atype);
  auto field_div = field("divide", atype);
  auto field_eq = field("equal", arrow::boolean());
  auto field_lt = field("less_than", arrow::boolean());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr = TreeExprBuilder::MakeExpression("subtract", {field0, field1},
                                                  field_sub);
  auto mul_expr = TreeExprBuilder::MakeExpression("multiply", {field0, field1},
                                                  field_mul);
  auto div_expr = TreeExprBuilder::MakeExpression("divide", {field0, field1}, field_div);
  auto eq_expr = TreeExprBuilder::MakeExpression("equal", {field0, field1}, field_eq);
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_lt);

  std::shared_ptr<Projector> projector;
  Status status =
      Projector::Make(schema, {sum_expr, sub_expr, mul_expr, div_expr, eq_expr, lt_expr},
                      pool, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  std::vector<C_TYPE> input0 = {1, 2, 53, 84};
  std::vector<C_TYPE> input1 = {10, 15, 23, 84};
  std::vector<bool> validity = {true, true, true, true};

  auto array0 = MakeArrowArray<TYPE, C_TYPE>(input0, validity);
  auto array1 = MakeArrowArray<TYPE, C_TYPE>(input1, validity);

  // expected output
  std::vector<C_TYPE> sum;
  std::vector<C_TYPE> sub;
  std::vector<C_TYPE> mul;
  std::vector<C_TYPE> div;
  std::vector<bool> eq;
  std::vector<bool> lt;
  for (int i = 0; i < num_records; i++) {
    sum.push_back(input0[i] + input1[i]);
    sub.push_back(input0[i] - input1[i]);
    mul.push_back(input0[i] * input1[i]);
    div.push_back(input0[i] / input1[i]);
    eq.push_back(input0[i] == input1[i]);
    lt.push_back(input0[i] < input1[i]);
  }
  auto exp_sum = MakeArrowArray<TYPE, C_TYPE>(sum, validity);
  auto exp_sub = MakeArrowArray<TYPE, C_TYPE>(sub, validity);
  auto exp_mul = MakeArrowArray<TYPE, C_TYPE>(mul, validity);
  auto exp_div = MakeArrowArray<TYPE, C_TYPE>(div, validity);
  auto exp_eq = MakeArrowArray<arrow::BooleanType, bool>(eq, validity);
  auto exp_lt = MakeArrowArray<arrow::BooleanType, bool>(lt, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sub, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mul, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_div, outputs.at(3));
  EXPECT_ARROW_ARRAY_EQUALS(exp_eq, outputs.at(4));
  EXPECT_ARROW_ARRAY_EQUALS(exp_lt, outputs.at(5));
}

TEST_F(TestProjector, TestAllIntTypes) {
  TestArithmeticOpsForType<arrow::UInt8Type, uint8_t>(pool_);
  TestArithmeticOpsForType<arrow::UInt16Type, uint16_t>(pool_);
  TestArithmeticOpsForType<arrow::UInt32Type, uint32_t>(pool_);
  TestArithmeticOpsForType<arrow::UInt64Type, uint64_t>(pool_);
  TestArithmeticOpsForType<arrow::Int8Type, int8_t>(pool_);
  TestArithmeticOpsForType<arrow::Int16Type, int16_t>(pool_);
  TestArithmeticOpsForType<arrow::Int32Type, int32_t>(pool_);
  TestArithmeticOpsForType<arrow::Int64Type, int64_t>(pool_);
}

TEST_F(TestProjector, TestFloatLessThan) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1},
                                                 field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {lt_expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({ 1.0, 8.9, 3.0 }, { true, true, false });
  auto array1 = MakeArrowArrayFloat32({ 4.0, 3.4, 6.8 }, { true, true, true });
  // expected output
  auto exp = MakeArrowArrayBool({ true, false, false }, { true, true, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestIsNotNull) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto myexpr = TreeExprBuilder::MakeExpression("isnotnull", {field0}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {myexpr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({ 1.0, 8.9, 3.0 }, { true, true, false });
  // expected output
  auto exp = MakeArrowArrayBool({ true, true, false }, { true, true, true });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestNullInternal) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  auto myexpr = TreeExprBuilder::MakeExpression("half_or_null", {field0}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {myexpr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 =
      MakeArrowArrayInt32({ 10, 10, -20, 5, -7 }, { true, false, true, true, true });

  // expected output
  auto exp =
      MakeArrowArrayInt32({ 5, 0, -10, 0, 0 }, { true, false, true, false, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestNestedFunctions) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_res1 = field("res1", int32());
  auto field_res2 = field("res2", boolean());

  // build expression.
  // expr1 : half_or_null(f0) * f1
  // expr2 : isnull(half_or_null(f0) * f1)
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto half = TreeExprBuilder::MakeFunction("half_or_null", {node_f0}, int32());
  auto mult = TreeExprBuilder::MakeFunction("multiply", {half, node_f1}, int32());
  auto expr1 = TreeExprBuilder::MakeExpression(mult, field_res1);

  auto isnull = TreeExprBuilder::MakeFunction("isnull", {mult}, boolean());
  auto expr2 = TreeExprBuilder::MakeExpression(isnull, field_res2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr1, expr2}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 10, 10, -20, 5}, { true, false, true, true });
  auto array1 = MakeArrowArrayInt32({ 11, 13, 15, 17 }, { true, true, false, true });

  // expected output
  auto exp1 = MakeArrowArrayInt32({ 55, 65, -150, 0 }, { true, false, false, false });
  auto exp2 = MakeArrowArrayBool({ false, true, true, true }, { true, true, true, true });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp2, outputs.at(1));
}

TEST_F(TestProjector, TestZeroCopy) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // output fields
  auto res = field("res", float32());

  // Build expression
  auto cast_expr = TreeExprBuilder::MakeExpression("castFLOAT4", {field0}, res);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {cast_expr}, nullptr /*pool*/, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 1, 2, 3, 4 }, { true, true, true, false });
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayFloat32({ 1, 2, 3, 0 }, { true, true, true, false });

  // allocate output buffers
  int64_t bitmap_sz = arrow::BitUtil::BytesForBits(num_records);
  std::unique_ptr<uint8_t[]> bitmap(new uint8_t[bitmap_sz]);
  std::shared_ptr<arrow::MutableBuffer> bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(bitmap.get(), bitmap_sz);

  int64_t data_sz = sizeof (float) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  auto array_data = arrow::ArrayData::Make(
    float32(),
    num_records,
    {bitmap_buf, data_buf});

  // Evaluate expression
  status = projector->Evaluate(*in_batch, {array_data});
  EXPECT_TRUE(status.ok());

  // Validate results
  auto output = arrow::MakeArray(array_data);
  EXPECT_ARROW_ARRAY_EQUALS(exp, output);
}

TEST_F(TestProjector, TestZeroCopyNegative) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // output fields
  auto res = field("res", float32());

  // Build expression
  auto cast_expr = TreeExprBuilder::MakeExpression("castFLOAT4", {field0}, res);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {cast_expr}, nullptr /*pool*/, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 1, 2, 3, 4 }, { true, true, true, false });
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayFloat32({ 1, 2, 3, 0 }, { true, true, true, false });

  // allocate output buffers
  int64_t bitmap_sz = arrow::BitUtil::BytesForBits(num_records);
  std::unique_ptr<uint8_t[]> bitmap(new uint8_t[bitmap_sz]);
  std::shared_ptr<arrow::MutableBuffer> bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(bitmap.get(), bitmap_sz);

  int64_t data_sz = sizeof (float) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  auto array_data = arrow::ArrayData::Make(
    float32(),
    num_records,
    {bitmap_buf, data_buf});

  // the batch can't be empty.
  auto bad_batch = arrow::RecordBatch::Make(schema, 0 /*num_records*/, {array0});
  status = projector->Evaluate(*bad_batch, {array_data});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output array can't be null.
  std::shared_ptr<arrow::ArrayData> null_array_data;
  status = projector->Evaluate(*in_batch, {null_array_data});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output array must have atleast two buffers.
  auto bad_array_data = arrow::ArrayData::Make(float32(), num_records, {bitmap_buf});
  status = projector->Evaluate(*in_batch, {bad_array_data});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output buffers must have sufficiently sized data_buf.
  std::shared_ptr<arrow::MutableBuffer> bad_data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz - 1);
  auto bad_array_data2 = arrow::ArrayData::Make(float32(),
                                                num_records, {bitmap_buf, bad_data_buf});
  status = projector->Evaluate(*in_batch, {bad_array_data2});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output buffers must have sufficiently sized bitmap_buf.
  std::shared_ptr<arrow::MutableBuffer> bad_bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(bitmap.get(), bitmap_sz - 1);
  auto bad_array_data3 = arrow::ArrayData::Make(float32(),
                                                num_records, {bad_bitmap_buf, data_buf});
  status = projector->Evaluate(*in_batch, {bad_array_data3});
  EXPECT_EQ(status.code(), StatusCode::Invalid);
}

} // namespace gandiva
