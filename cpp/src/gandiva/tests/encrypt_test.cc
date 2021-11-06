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

#include "arrow/memory_pool.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

class TestEncrypt : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestEncrypt, TestAesEncryptDecrypt) {
  auto field0 = field("f0", arrow::utf8());
  auto field1 = field("f1", arrow::utf8());
  auto schema = arrow::schema({field0, field1});

  auto cypher_res = field("cypher", arrow::utf8());
  auto plain_res = field("plain", arrow::utf8());

  auto encrypt_expr =
      TreeExprBuilder::MakeExpression("aes_encrypt", {field0, field1}, cypher_res);
  auto decrypt_expr =
      TreeExprBuilder::MakeExpression("aes_decrypt", {field0, field1}, plain_res);

  auto configuration = TestConfiguration();

  std::shared_ptr<Projector> projector_en;
  auto status = Projector::Make(schema, {encrypt_expr}, configuration, &projector_en);
  ASSERT_OK(status);

  int num_records = 4;

  const char* key_32_bytes = "12345678abcdefgh12345678abcdefgh";
  const char* key_64_bytes =
      "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh";
  const char* key_128_bytes =
      "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12"
      "345678abcdefgh12345678abcdefgh12345678abcdefgh";
  const char* key_256_bytes =
      "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12"
      "345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh1234"
      "5678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh123456"
      "78abcdefgh";

  auto array_data = MakeArrowArrayUtf8({"abc", "some words", "to be encrypted", "hyah\n"},
                                       {true, true, true, true});
  auto array_key =
      MakeArrowArrayUtf8({key_32_bytes, key_64_bytes, key_128_bytes, key_256_bytes},
                         {true, true, true, true});

  auto array_holder_en = MakeArrowArrayUtf8({"", "", "", ""}, {true, true, true, true});

  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_data, array_key});

  // Evaluate expression
  arrow::ArrayVector outputs_en;
  status = projector_en->Evaluate(*in_batch, pool_, &outputs_en);
  EXPECT_TRUE(status.ok());

  std::shared_ptr<Projector> projector_de;
  status = Projector::Make(schema, {decrypt_expr}, configuration, &projector_de);
  ASSERT_OK(status);

  array_holder_en = outputs_en.at(0);

  auto in_batch_de =
      arrow::RecordBatch::Make(schema, num_records, {array_holder_en, array_key});

  arrow::ArrayVector outputs_de;
  status = projector_de->Evaluate(*in_batch_de, pool_, &outputs_de);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(array_data, outputs_de.at(0));
}
}  // namespace gandiva