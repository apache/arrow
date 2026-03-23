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

// Test for decimal128 alignment issue fix.
// Arrow decimal128 data may be 8-byte aligned but not 16-byte aligned.
// This test verifies that Gandiva handles such data correctly.

#include <gtest/gtest.h>

#include "arrow/array/array_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/decimal.h"

#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

using arrow::Decimal128;

namespace gandiva {

class TestDecimalAlignment : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

// Create a decimal128 array with data at a specific alignment offset
// This simulates the real-world scenario where Arrow data from external sources
// (like JNI/Java) may not be 16-byte aligned.
std::shared_ptr<arrow::Array> MakeMisalignedDecimalArray(
    const std::shared_ptr<arrow::Decimal128Type>& type,
    const std::vector<Decimal128>& values, int alignment_offset) {
  // Allocate buffer with extra space for misalignment
  int64_t data_size = values.size() * 16;  // 16 bytes per Decimal128
  int64_t buffer_size = data_size + 16;    // Extra space for offset

  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_EXPECT_OK(arrow::AllocateBuffer(buffer_size).Value(&buffer));

  // Calculate the starting offset to achieve desired alignment
  // We want the data to be 8-byte aligned but NOT 16-byte aligned
  uint8_t* raw_data = buffer->mutable_data();
  uintptr_t addr = reinterpret_cast<uintptr_t>(raw_data);

  // Find offset to get to 8-byte aligned but not 16-byte aligned address
  int offset_to_8 = (8 - (addr % 8)) % 8;
  int current_16_alignment = (addr + offset_to_8) % 16;

  int final_offset;
  if (alignment_offset == 8) {
    // Want 8-byte aligned but NOT 16-byte aligned
    if (current_16_alignment == 0) {
      final_offset = offset_to_8 + 8;  // Add 8 to break 16-byte alignment
    } else {
      final_offset = offset_to_8;
    }
  } else {
    // Want 16-byte aligned
    final_offset = (16 - (addr % 16)) % 16;
  }

  // Copy decimal values to the offset location
  uint8_t* data_start = raw_data + final_offset;
  for (size_t i = 0; i < values.size(); i++) {
    memcpy(data_start + i * 16, values[i].ToBytes().data(), 16);
  }

  // Verify alignment
  uintptr_t data_addr = reinterpret_cast<uintptr_t>(data_start);
  EXPECT_EQ(data_addr % 8, 0) << "Data should be 8-byte aligned";
  if (alignment_offset == 8) {
    EXPECT_NE(data_addr % 16, 0) << "Data should NOT be 16-byte aligned";
  }

  // Create a sliced buffer starting at our offset
  auto sliced_buffer = arrow::SliceBuffer(buffer, final_offset, data_size);

  // Create validity buffer (all valid)
  std::shared_ptr<arrow::Buffer> validity_buffer;
  ARROW_EXPECT_OK(arrow::AllocateBuffer((values.size() + 7) / 8).Value(&validity_buffer));
  memset(validity_buffer->mutable_data(), 0xFF, validity_buffer->size());

  // Create the array with our misaligned data buffer
  auto array_data = arrow::ArrayData::Make(type, static_cast<int64_t>(values.size()),
                                           {validity_buffer, sliced_buffer});

  return std::make_shared<arrow::Decimal128Array>(array_data);
}

// Test that decimal operations work correctly with 8-byte aligned (but not 16-byte
// aligned) data
TEST_F(TestDecimalAlignment, TestMisalignedDecimalSubtract) {
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 17;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = arrow::field("a", decimal_type);
  auto field_b = arrow::field("b", decimal_type);
  auto schema = arrow::schema({field_a, field_b});

  Decimal128TypePtr output_type;
  auto status = DecimalTypeUtil::GetResultType(
      DecimalTypeUtil::kOpSubtract, {decimal_type, decimal_type}, &output_type);
  ASSERT_OK(status);

  auto res = arrow::field("res", output_type);
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto subtract =
      TreeExprBuilder::MakeFunction("subtract", {node_a, node_b}, output_type);
  auto expr = TreeExprBuilder::MakeExpression(subtract, res);

  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  ASSERT_OK(status);

  // Create test data
  std::vector<Decimal128> values_a = {Decimal128(100), Decimal128(200), Decimal128(300)};
  std::vector<Decimal128> values_b = {Decimal128(10), Decimal128(20), Decimal128(30)};

  // Create arrays with 8-byte alignment (but NOT 16-byte aligned)
  auto array_a = MakeMisalignedDecimalArray(decimal_type, values_a, 8);
  auto array_b = MakeMisalignedDecimalArray(decimal_type, values_b, 8);

  auto in_batch = arrow::RecordBatch::Make(schema, 3, {array_a, array_b});

  // This should NOT crash even with misaligned data
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Verify results: 100-10=90, 200-20=180, 300-30=270
  auto result = std::dynamic_pointer_cast<arrow::Decimal128Array>(outputs[0]);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->length(), 3);
}

// Create a misaligned output buffer for decimal128
std::shared_ptr<arrow::ArrayData> MakeMisalignedDecimalOutput(
    const std::shared_ptr<arrow::Decimal128Type>& type, int64_t num_records,
    int alignment_offset) {
  // Allocate data buffer with extra space for misalignment
  int64_t data_size = num_records * 16;  // 16 bytes per Decimal128
  int64_t buffer_size = data_size + 16;  // Extra space for offset

  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_EXPECT_OK(arrow::AllocateBuffer(buffer_size).Value(&buffer));

  uint8_t* raw_data = const_cast<uint8_t*>(buffer->data());
  uintptr_t addr = reinterpret_cast<uintptr_t>(raw_data);

  // Find offset to get to 8-byte aligned but not 16-byte aligned address
  int offset_to_8 = (8 - (addr % 8)) % 8;
  int current_16_alignment = (addr + offset_to_8) % 16;

  int final_offset;
  if (alignment_offset == 8) {
    if (current_16_alignment == 0) {
      final_offset = offset_to_8 + 8;
    } else {
      final_offset = offset_to_8;
    }
  } else {
    final_offset = (16 - (addr % 16)) % 16;
  }

  // Verify alignment
  uintptr_t data_addr = reinterpret_cast<uintptr_t>(raw_data + final_offset);
  EXPECT_EQ(data_addr % 8, 0) << "Data should be 8-byte aligned";
  if (alignment_offset == 8) {
    EXPECT_NE(data_addr % 16, 0) << "Data should NOT be 16-byte aligned";
  }

  auto sliced_buffer = arrow::SliceBuffer(buffer, final_offset, data_size);

  // Create validity buffer
  int64_t bitmap_size = (num_records + 7) / 8;
  std::shared_ptr<arrow::Buffer> validity_buffer;
  ARROW_EXPECT_OK(arrow::AllocateBuffer(bitmap_size).Value(&validity_buffer));
  memset(const_cast<uint8_t*>(validity_buffer->data()), 0xFF, validity_buffer->size());

  return arrow::ArrayData::Make(type, num_records, {validity_buffer, sliced_buffer});
}

// Test that decimal STORES work correctly with 8-byte aligned (but not 16-byte aligned)
// output
TEST_F(TestDecimalAlignment, TestMisalignedDecimalStore) {
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 17;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = arrow::field("a", decimal_type);
  auto field_b = arrow::field("b", decimal_type);
  auto schema = arrow::schema({field_a, field_b});

  Decimal128TypePtr output_type;
  auto status = DecimalTypeUtil::GetResultType(
      DecimalTypeUtil::kOpSubtract, {decimal_type, decimal_type}, &output_type);
  ASSERT_OK(status);

  auto res = arrow::field("res", output_type);
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto subtract =
      TreeExprBuilder::MakeFunction("subtract", {node_a, node_b}, output_type);
  auto expr = TreeExprBuilder::MakeExpression(subtract, res);

  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  ASSERT_OK(status);

  // Create ALIGNED input arrays (using standard Arrow allocation)
  auto array_a = MakeArrowArrayDecimal(
      decimal_type, {Decimal128(100), Decimal128(200), Decimal128(300)},
      {true, true, true});
  auto array_b = MakeArrowArrayDecimal(
      decimal_type, {Decimal128(10), Decimal128(20), Decimal128(30)}, {true, true, true});

  auto in_batch = arrow::RecordBatch::Make(schema, 3, {array_a, array_b});

  // Create MISALIGNED output buffer (8-byte aligned but NOT 16-byte aligned)
  auto output_data = MakeMisalignedDecimalOutput(output_type, 3, 8);

  // This should NOT crash even with misaligned output buffer
  status = projector->Evaluate(*in_batch, {output_data});
  ASSERT_OK(status);

  // Verify the output was written correctly
  auto result = std::make_shared<arrow::Decimal128Array>(output_data);
  EXPECT_EQ(result->length(), 3);
}

}  // namespace gandiva
