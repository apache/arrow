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
#include "gandiva/projector.h"
#include "gandiva/status.h"
#include "gandiva/tree_expr_builder.h"
#include "integ/test_util.h"

namespace gandiva {

using arrow::binary;
using arrow::boolean;
using arrow::int32;

class TestBinary : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestBinary, TestSimple) {
  // schema for input fields
  auto field_a = field("a", binary());
  auto field_b = field("b", binary());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res = field("res", int32());

  // build expressions.
  // a > b ? octet_length(a) : octet_length(b)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto octet_len_a = TreeExprBuilder::MakeFunction("octet_length", {node_a}, int32());
  auto octet_len_b = TreeExprBuilder::MakeFunction("octet_length", {node_b}, int32());

  auto is_greater =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, node_b}, boolean());
  auto if_greater =
      TreeExprBuilder::MakeIf(is_greater, octet_len_a, octet_len_b, int32());
  auto expr = TreeExprBuilder::MakeExpression(if_greater, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, pool_, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayBinary({"foo", "hello", "hi", "bye"}, {true, true, true, false});
  auto array_b =
      MakeArrowArrayBinary({"fo", "hellos", "hi", "bye"}, {true, true, true, true});

  // expected output
  auto exp = MakeArrowArrayInt32({3, 6, 2, 3}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

}  // namespace gandiva
