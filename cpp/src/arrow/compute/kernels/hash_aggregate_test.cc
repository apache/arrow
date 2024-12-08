// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "arrow/compute/kernels/hash_aggregate.h"
#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

class TestHashAggregate : public ::testing::Test {
 protected:
  void SetUp() override { pool_ = default_memory_pool(); }
  MemoryPool* pool_;
};

// ----------------------------------------------------------------------
// GroupedCountImpl Tests

TEST_F(TestHashAggregate, GroupedCountImpl_MergeWithValidInputs) {
    // General test case: Performs the merge correctly
    GroupedCountImpl count_aggregator1;
    count_aggregator1.AddEntry(10);
    count_aggregator1.AddEntry(20);

    GroupedCountImpl count_aggregator2;
    count_aggregator2.AddEntry(5);
    count_aggregator2.AddEntry(15);

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/2, {nullptr, Buffer::FromString("\x00\x01")});

    ASSERT_OK(count_aggregator1.Merge(std::move(count_aggregator2), *group_id_mapping));
    EXPECT_EQ(count_aggregator1.GetValue(0), 15);  // 10 + 5
    EXPECT_EQ(count_aggregator1.GetValue(1), 35);  // 20 + 15
}

TEST_F(TestHashAggregate, GroupedCountImpl_MergeWithInvalidMapping) {
    // Unable to perform the merge: No matching group ID mapping
    GroupedCountImpl count_aggregator1;
    count_aggregator1.AddEntry(10);
    count_aggregator1.AddEntry(20);

    GroupedCountImpl count_aggregator2;
    count_aggregator2.AddEntry(5);

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/3, {nullptr, Buffer::FromString("\x00\x01\x02")});  // Invalid group mapping created
    auto status = count_aggregator1.Merge(std::move(count_aggregator2), *group_id_mapping);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Group ID mapping length"), status);
}

// ----------------------------------------------------------------------
// GroupedMinMaxImpl Tests
TEST_F(TestHashAggregate, GroupedMinMaxImpl_MergeWithValidInputs) {
    // General test case: Performs the merge correctly
    GroupedMinMaxImpl minmax_aggregator1;
    minmax_aggregator1.AddEntry(10, 50);  // Min: 10, Max: 50
    minmax_aggregator1.AddEntry(20, 40);  // Min: 20, Max: 40

    GroupedMinMaxImpl minmax_aggregator2;
    minmax_aggregator2.AddEntry(5, 55);  // Min: 5, Max: 55
    minmax_aggregator2.AddEntry(25, 35); // Min: 25, Max: 35

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/2, {nullptr, Buffer::FromString("\x00\x01")});

    ASSERT_OK(minmax_aggregator1.Merge(std::move(minmax_aggregator2), *group_id_mapping));
    EXPECT_EQ(minmax_aggregator1.GetMin(0), 5);  // Min of (10, 5)
    EXPECT_EQ(minmax_aggregator1.GetMax(0), 55); // Max of (50, 55)
    EXPECT_EQ(minmax_aggregator1.GetMin(1), 20); // Min of (20, 25)
    EXPECT_EQ(minmax_aggregator1.GetMax(1), 40); // Max of (40, 35)
}

TEST_F(TestHashAggregate, GroupedMinMaxImpl_MergeWithOutOfBoundsIndices) {
    // Unable to perform the merge: No matching group index
    GroupedMinMaxImpl minmax_aggregator1;
    minmax_aggregator1.AddEntry(10, 50);
    minmax_aggregator1.AddEntry(20, 40);

    GroupedMinMaxImpl minmax_aggregator2;
    minmax_aggregator2.AddEntry(5, 55);

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/3, {nullptr, Buffer::FromString("\x00\x01\x02")}); 
    auto status = minmax_aggregator1.Merge(std::move(minmax_aggregator2), *group_id_mapping);
    EXPECT_RAISES_WITH_MESSAGE_THAT(IndexError, testing::HasSubstr("Group index out of bounds"), status);
}

// ----------------------------------------------------------------------
// GroupedCountAllImpl Tests
TEST_F(TestHashAggregate, GroupedCountAllImpl_MergeWithValidInputs) {
    // General test case: Performs the merge correctly
    GroupedCountAllImpl countall_aggregator1;
    countall_aggregator1.AddEntry(100);
    countall_aggregator1.AddEntry(200);

    GroupedCountAllImpl countall_aggregator2;
    countall_aggregator2.AddEntry(50);
    countall_aggregator2.AddEntry(150);

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/2, {nullptr, Buffer::FromString("\x00\x01")});
    ASSERT_OK(countall_aggregator1.Merge(std::move(countall_aggregator2), *group_id_mapping));
    EXPECT_EQ(countall_aggregator1.GetValue(0), 150); // 100 + 50
    EXPECT_EQ(countall_aggregator1.GetValue(1), 350); // 200 + 150
}

TEST_F(TestHashAggregate, GroupedCountAllImpl_MergeWithInvalidMapping) {
    // Unable to perform the merge: No matching group ID mapping
    GroupedCountAllImpl countall_aggregator1;
    countall_aggregator1.AddEntry(100);

    GroupedCountAllImpl countall_aggregator2;
    countall_aggregator2.AddEntry(50);

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/3, {nullptr, Buffer::FromString("\x00\x01\x02")}); // Invalid group mapping created

    auto status = countall_aggregator1.Merge(std::move(countall_aggregator2), *group_id_mapping);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Group ID mapping length"), status);
}

TEST_F(TestHashAggregate, GroupedCountAllImpl_MergeWithNullGroupIdMapping) {
    // Unable to perform the merge: Mix of null and non-null values, fails due to invalud groupId
    GroupedCountAllImpl countall_aggregator1;
    countall_aggregator1.AddEntry(100);
    countall_aggregator1.AddEntry(nullptr); 

    GroupedCountAllImpl countall_aggregator2;
    countall_aggregator2.AddEntry(50);
    countall_aggregator2.AddEntry(nullptr); 

    auto group_id_mapping = ArrayData::Make(uint32(), /*length=*/2, {nullptr, Buffer::FromString("\x00\xFF")});  // Invalid mapping includes null-like entry

    auto status = countall_aggregator1.Merge(std::move(countall_aggregator2), *group_id_mapping);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid or null group ID mapping during merge"), status);
}