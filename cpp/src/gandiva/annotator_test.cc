/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <gtest/gtest.h>
#include <arrow/memory_pool.h>
#include "codegen/annotator.h"
#include "codegen/field_descriptor.h"

namespace gandiva {

class TestAnnotator : public ::testing::Test {
 protected:
  ArrayPtr MakeInt32Array(int length);
};

ArrayPtr TestAnnotator::MakeInt32Array(int length) {
  arrow::Status status;

  std::shared_ptr<arrow::Buffer> validity;
  status = arrow::AllocateBuffer(arrow::default_memory_pool(),
                                 (length + 63) / 8,
                                 &validity);
  DCHECK_EQ(status.ok(), true);

  std::shared_ptr<arrow::Buffer> value;
  status = AllocateBuffer(arrow::default_memory_pool(),
                          length * sizeof (int32_t),
                          &value);
  DCHECK_EQ(status.ok(), true);

  auto array_data = arrow::ArrayData::Make(arrow::int32(), length, {validity, value});
  return arrow::MakeArray(array_data);
}

TEST_F(TestAnnotator, TestAdd) {
  Annotator annotator;

  auto field_a = arrow::field("a", arrow::int32());
  auto field_b = arrow::field("b", arrow::int32());
  auto in_schema = arrow::schema({field_a, field_b});
  auto field_sum = arrow::field("sum", arrow::int32());

  FieldDescriptorPtr desc_a = annotator.CheckAndAddInputFieldDescriptor(field_a);
  EXPECT_EQ(desc_a->field(), field_a);
  EXPECT_EQ(desc_a->data_idx(), 0);
  EXPECT_EQ(desc_a->validity_idx(), 1);

  // duplicate add shouldn't cause a new descriptor.
  FieldDescriptorPtr dup = annotator.CheckAndAddInputFieldDescriptor(field_a);
  EXPECT_EQ(dup, desc_a);
  EXPECT_EQ(dup->validity_idx(), desc_a->validity_idx());

  FieldDescriptorPtr desc_b = annotator.CheckAndAddInputFieldDescriptor(field_b);
  EXPECT_EQ(desc_b->field(), field_b);
  EXPECT_EQ(desc_b->data_idx(), 2);
  EXPECT_EQ(desc_b->validity_idx(), 3);

  FieldDescriptorPtr desc_sum = annotator.AddOutputFieldDescriptor(field_sum);
  EXPECT_EQ(desc_sum->field(), field_sum);
  EXPECT_EQ(desc_sum->data_idx(), 4);
  EXPECT_EQ(desc_sum->validity_idx(), 5);

  /* prepare record batch */
  int num_records = 100;
  auto arrow_v0 = MakeInt32Array(num_records);
  auto arrow_v1 = MakeInt32Array(num_records);

  /* prepare input record batch */
  auto record_batch = arrow::RecordBatch::Make(in_schema,
                                               num_records, {arrow_v0, arrow_v1});

  auto arrow_sum = MakeInt32Array(num_records);
  EvalBatchPtr batch = annotator.PrepareEvalBatch(*record_batch, {arrow_sum->data()});
  EXPECT_EQ(batch->num_buffers(), 6);

  auto buffers = batch->buffers();
  EXPECT_EQ(buffers[desc_a->validity_idx()], arrow_v0->data()->buffers.at(0)->data());
  EXPECT_EQ(buffers[desc_a->data_idx()], arrow_v0->data()->buffers.at(1)->data());
  EXPECT_EQ(buffers[desc_b->validity_idx()], arrow_v1->data()->buffers.at(0)->data());
  EXPECT_EQ(buffers[desc_b->data_idx()], arrow_v1->data()->buffers.at(1)->data());
  EXPECT_EQ(buffers[desc_sum->validity_idx()], arrow_sum->data()->buffers.at(0)->data());
  EXPECT_EQ(buffers[desc_sum->data_idx()], arrow_sum->data()->buffers.at(1)->data());
}

} // namespace gandiva

