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

#include "gandiva/selection_vector.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {

class TestSelectionVector : public ::testing::Test {
 protected:
  virtual void SetUp() { pool_ = arrow::default_memory_pool(); }

  arrow::MemoryPool* pool_;
};

static inline uint32_t RoundUpNumi64(uint32_t value) { return (value + 63) >> 6; }

TEST_F(TestSelectionVector, TestInt16Make) {
  int max_slots = 10;

  // Test with pool allocation
  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt16(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();
  EXPECT_EQ(selection->GetMaxSlots(), max_slots);
  EXPECT_EQ(selection->GetNumSlots(), 0);

  // Test with pre-alloced buffer
  std::shared_ptr<SelectionVector> selection2;
  std::shared_ptr<arrow::Buffer> buffer;
  auto buffer_len = max_slots * sizeof(int16_t);
  auto astatus = arrow::AllocateBuffer(pool_, buffer_len, &buffer);
  EXPECT_EQ(astatus.ok(), true);

  status = SelectionVector::MakeInt16(max_slots, buffer, &selection2);
  EXPECT_EQ(status.ok(), true) << status.message();
  EXPECT_EQ(selection2->GetMaxSlots(), max_slots);
  EXPECT_EQ(selection2->GetNumSlots(), 0);
}

TEST_F(TestSelectionVector, TestInt16MakeNegative) {
  int max_slots = 10;

  std::shared_ptr<SelectionVector> selection;
  std::shared_ptr<arrow::Buffer> buffer;
  auto buffer_len = max_slots * sizeof(int16_t);

  // alloc a buffer that's insufficient.
  auto astatus = arrow::AllocateBuffer(pool_, buffer_len - 16, &buffer);
  EXPECT_EQ(astatus.ok(), true);

  auto status = SelectionVector::MakeInt16(max_slots, buffer, &selection);
  EXPECT_EQ(status.IsInvalid(), true);
}

TEST_F(TestSelectionVector, TestInt16Set) {
  int max_slots = 10;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt16(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  selection->SetIndex(0, 100);
  EXPECT_EQ(selection->GetIndex(0), 100);

  selection->SetIndex(1, 200);
  EXPECT_EQ(selection->GetIndex(1), 200);

  selection->SetNumSlots(2);
  EXPECT_EQ(selection->GetNumSlots(), 2);

  // TopArray() should return an array with 100,200
  auto array_raw = selection->ToArray();
  const auto& array = dynamic_cast<const arrow::UInt16Array&>(*array_raw);
  EXPECT_EQ(array.length(), 2) << array_raw->ToString();
  EXPECT_EQ(array.Value(0), 100) << array_raw->ToString();
  EXPECT_EQ(array.Value(1), 200) << array_raw->ToString();
}

TEST_F(TestSelectionVector, TestInt16PopulateFromBitMap) {
  int max_slots = 200;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt16(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  int bitmap_size = RoundUpNumi64(max_slots) * 8;
  std::vector<uint8_t> bitmap(bitmap_size);

  arrow::BitUtil::SetBit(&bitmap[0], 0);
  arrow::BitUtil::SetBit(&bitmap[0], 5);
  arrow::BitUtil::SetBit(&bitmap[0], 121);
  arrow::BitUtil::SetBit(&bitmap[0], 220);

  status = selection->PopulateFromBitMap(&bitmap[0], bitmap_size, max_slots - 1);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(selection->GetNumSlots(), 3);
  EXPECT_EQ(selection->GetIndex(0), 0);
  EXPECT_EQ(selection->GetIndex(1), 5);
  EXPECT_EQ(selection->GetIndex(2), 121);
}

TEST_F(TestSelectionVector, TestInt16PopulateFromBitMapNegative) {
  int max_slots = 2;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt16(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  int bitmap_size = 16;
  std::vector<uint8_t> bitmap(bitmap_size);

  arrow::BitUtil::SetBit(&bitmap[0], 0);
  arrow::BitUtil::SetBit(&bitmap[0], 1);
  arrow::BitUtil::SetBit(&bitmap[0], 2);

  // The bitmap has three set bits, whereas the selection vector has capacity for only 2.
  status = selection->PopulateFromBitMap(&bitmap[0], bitmap_size, 2);
  EXPECT_EQ(status.IsInvalid(), true);
}

TEST_F(TestSelectionVector, TestInt32Set) {
  int max_slots = 10;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt32(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  selection->SetIndex(0, 100);
  EXPECT_EQ(selection->GetIndex(0), 100);

  selection->SetIndex(1, 200);
  EXPECT_EQ(selection->GetIndex(1), 200);

  selection->SetIndex(2, 100000);
  EXPECT_EQ(selection->GetIndex(2), 100000);

  selection->SetNumSlots(3);
  EXPECT_EQ(selection->GetNumSlots(), 3);

  // TopArray() should return an array with 100,200,100000
  auto array_raw = selection->ToArray();
  const auto& array = dynamic_cast<const arrow::UInt32Array&>(*array_raw);
  EXPECT_EQ(array.length(), 3) << array_raw->ToString();
  EXPECT_EQ(array.Value(0), 100) << array_raw->ToString();
  EXPECT_EQ(array.Value(1), 200) << array_raw->ToString();
  EXPECT_EQ(array.Value(2), 100000) << array_raw->ToString();
}

TEST_F(TestSelectionVector, TestInt32PopulateFromBitMap) {
  int max_slots = 200;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt32(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  int bitmap_size = RoundUpNumi64(max_slots) * 8;
  std::vector<uint8_t> bitmap(bitmap_size);

  arrow::BitUtil::SetBit(&bitmap[0], 0);
  arrow::BitUtil::SetBit(&bitmap[0], 5);
  arrow::BitUtil::SetBit(&bitmap[0], 121);
  arrow::BitUtil::SetBit(&bitmap[0], 220);

  status = selection->PopulateFromBitMap(&bitmap[0], bitmap_size, max_slots - 1);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(selection->GetNumSlots(), 3);
  EXPECT_EQ(selection->GetIndex(0), 0);
  EXPECT_EQ(selection->GetIndex(1), 5);
  EXPECT_EQ(selection->GetIndex(2), 121);
}

TEST_F(TestSelectionVector, TestInt32MakeNegative) {
  int max_slots = 10;

  std::shared_ptr<SelectionVector> selection;
  std::shared_ptr<arrow::Buffer> buffer;
  auto buffer_len = max_slots * sizeof(int32_t);

  // alloc a buffer that's insufficient.
  auto astatus = arrow::AllocateBuffer(pool_, buffer_len - 1, &buffer);
  EXPECT_EQ(astatus.ok(), true);

  auto status = SelectionVector::MakeInt32(max_slots, buffer, &selection);
  EXPECT_EQ(status.IsInvalid(), true);
}

TEST_F(TestSelectionVector, TestInt64Set) {
  int max_slots = 10;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt64(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  selection->SetIndex(0, 100);
  EXPECT_EQ(selection->GetIndex(0), 100);

  selection->SetIndex(1, 200);
  EXPECT_EQ(selection->GetIndex(1), 200);

  selection->SetIndex(2, 100000);
  EXPECT_EQ(selection->GetIndex(2), 100000);

  selection->SetNumSlots(3);
  EXPECT_EQ(selection->GetNumSlots(), 3);

  // TopArray() should return an array with 100,200,100000
  auto array_raw = selection->ToArray();
  const auto& array = dynamic_cast<const arrow::UInt64Array&>(*array_raw);
  EXPECT_EQ(array.length(), 3) << array_raw->ToString();
  EXPECT_EQ(array.Value(0), 100) << array_raw->ToString();
  EXPECT_EQ(array.Value(1), 200) << array_raw->ToString();
  EXPECT_EQ(array.Value(2), 100000) << array_raw->ToString();
}

TEST_F(TestSelectionVector, TestInt64PopulateFromBitMap) {
  int max_slots = 200;

  std::shared_ptr<SelectionVector> selection;
  auto status = SelectionVector::MakeInt64(max_slots, pool_, &selection);
  EXPECT_EQ(status.ok(), true) << status.message();

  int bitmap_size = RoundUpNumi64(max_slots) * 8;
  std::vector<uint8_t> bitmap(bitmap_size);

  arrow::BitUtil::SetBit(&bitmap[0], 0);
  arrow::BitUtil::SetBit(&bitmap[0], 5);
  arrow::BitUtil::SetBit(&bitmap[0], 121);
  arrow::BitUtil::SetBit(&bitmap[0], 220);

  status = selection->PopulateFromBitMap(&bitmap[0], bitmap_size, max_slots - 1);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(selection->GetNumSlots(), 3);
  EXPECT_EQ(selection->GetIndex(0), 0);
  EXPECT_EQ(selection->GetIndex(1), 5);
  EXPECT_EQ(selection->GetIndex(2), 121);
}

TEST_F(TestSelectionVector, TestInt64MakeNegative) {
  int max_slots = 10;

  std::shared_ptr<SelectionVector> selection;
  std::shared_ptr<arrow::Buffer> buffer;
  auto buffer_len = max_slots * sizeof(int64_t);

  // alloc a buffer that's insufficient.
  auto astatus = arrow::AllocateBuffer(pool_, buffer_len - 1, &buffer);
  EXPECT_EQ(astatus.ok(), true);

  auto status = SelectionVector::MakeInt64(max_slots, buffer, &selection);
  EXPECT_EQ(status.IsInvalid(), true);
}

}  // namespace gandiva
