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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/builder_binary.h"
#include "arrow/compute/api.h"
#include "arrow/testing/gtest_util.h"

#include "geometry_statistics.h"
#include "parquet/test_util.h"

namespace parquet::geometry {

TEST(TestGeospatialStatistics, TestDefaults) {
  GeospatialStatistics stats;
  EXPECT_EQ(stats.GetGeometryTypes().size(), 0);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_FALSE(stats.HasZ());
  EXPECT_FALSE(stats.HasM());
  EXPECT_EQ(stats.GetXMax() - stats.GetXMin(), -kInf);
  EXPECT_EQ(stats.GetYMax() - stats.GetYMin(), -kInf);
  EXPECT_EQ(stats.GetZMax() - stats.GetZMin(), -kInf);
  EXPECT_EQ(stats.GetMMax() - stats.GetMMin(), -kInf);
  EXPECT_TRUE(stats.Equals(GeospatialStatistics()));

  auto encoded = stats.Encode();
  EXPECT_FALSE(encoded.is_set());
  EXPECT_FALSE(encoded.has_z());
  EXPECT_FALSE(encoded.has_m());
  EXPECT_TRUE(GeospatialStatistics(encoded).Equals(stats));

  stats.Merge(GeospatialStatistics());
  EXPECT_TRUE(GeospatialStatistics(encoded).Equals(stats));
}

TEST(TestGeospatialStatistics, TestUpdateByteArray) {
  GeospatialStatistics stats;

  double xyzm0[] = {10, 11, 12, 13};
  std::string xyzm_wkb0 = test::MakeWKBPoint(xyzm0, true, true);
  ByteArray item0{static_cast<uint32_t>(xyzm_wkb0.size()),
                  reinterpret_cast<const uint8_t*>(xyzm_wkb0.data())};

  stats.Update(&item0, 1, 0);
  EXPECT_EQ(stats.GetXMin(), 10);
  EXPECT_EQ(stats.GetXMax(), 10);
  EXPECT_EQ(stats.GetYMin(), 11);
  EXPECT_EQ(stats.GetYMax(), 11);
  EXPECT_EQ(stats.GetZMin(), 12);
  EXPECT_EQ(stats.GetZMax(), 12);
  EXPECT_EQ(stats.GetMMin(), 13);
  EXPECT_EQ(stats.GetMMax(), 13);
  EXPECT_THAT(stats.GetGeometryTypes(), ::testing::ElementsAre(3001));

  double xyzm1[] = {20, 21, 22, 23};
  std::string xyzm_wkb1 = test::MakeWKBPoint(xyzm1, true, true);
  ByteArray item1{static_cast<uint32_t>(xyzm_wkb1.size()),
                  reinterpret_cast<const uint8_t*>(xyzm_wkb1.data())};

  stats.Update(&item1, 1, 0);
  EXPECT_EQ(stats.GetXMin(), 10);
  EXPECT_EQ(stats.GetXMax(), 20);
  EXPECT_EQ(stats.GetYMin(), 11);
  EXPECT_EQ(stats.GetYMax(), 21);
  EXPECT_EQ(stats.GetZMin(), 12);
  EXPECT_EQ(stats.GetZMax(), 22);
  EXPECT_EQ(stats.GetMMin(), 13);
  EXPECT_EQ(stats.GetMMax(), 23);
  EXPECT_THAT(stats.GetGeometryTypes(), ::testing::ElementsAre(3001));

  // Check recreating the statistics with actual values
  auto encoded = stats.Encode();
  EXPECT_TRUE(GeospatialStatistics(encoded).Equals(stats));

  // Check resetting to the original state
  stats.Reset();
  EXPECT_TRUE(stats.Equals(GeospatialStatistics()));

  // Check UpdateSpaced()

  // A null value that should be skipped
  double xyzm2[] = {-30, -31, -32, -33};
  std::string xyzm_wkb2 = test::MakeWKBPoint(xyzm2, true, true);
  ByteArray item2{static_cast<uint32_t>(xyzm_wkb2.size()),
                  reinterpret_cast<const uint8_t*>(xyzm_wkb2.data())};

  // A non-null value that shouldn't be skipped
  double xyzm3[] = {30, 31, 32, 33};
  std::string xyzm_wkb3 = test::MakeWKBPoint(xyzm3, true, true);
  ByteArray item3{static_cast<uint32_t>(xyzm_wkb3.size()),
                  reinterpret_cast<const uint8_t*>(xyzm_wkb3.data())};

  ByteArray items[] = {item0, item1, item2, item3};
  // Validity bitmap with an extra bit on the front to check non-zero bits offset
  uint8_t validity = 0b00010111;
  GeospatialStatistics stats_spaced;
  stats_spaced.UpdateSpaced(items, &validity, 1, 4, 4, 1);

  EXPECT_EQ(stats_spaced.GetXMin(), 10);
  EXPECT_EQ(stats_spaced.GetXMax(), 30);
  EXPECT_EQ(stats_spaced.GetYMin(), 11);
  EXPECT_EQ(stats_spaced.GetYMax(), 31);
  EXPECT_EQ(stats_spaced.GetZMin(), 12);
  EXPECT_EQ(stats_spaced.GetZMax(), 32);
  EXPECT_EQ(stats_spaced.GetMMin(), 13);
  EXPECT_EQ(stats_spaced.GetMMax(), 33);
  EXPECT_THAT(stats_spaced.GetGeometryTypes(), ::testing::ElementsAre(3001));

  // Check merge
  stats.Merge(stats_spaced);
  EXPECT_TRUE(stats.Equals(stats_spaced));

  // Check ingest of invalid WKB
  ByteArray invalid;
  stats.Update(&invalid, 1, 0);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_FALSE(stats.Encode().is_set());

  // This should be true even after ingesting more values
  stats.Update(&item0, 1, 0);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_FALSE(stats.Encode().is_set());

  // And should cause other statistics to become invalid when merged with them
  stats_spaced.Merge(stats);
  EXPECT_FALSE(stats_spaced.is_valid());
  EXPECT_FALSE(stats_spaced.Encode().is_set());
}

TEST(TestGeospatialStatistics, TestUpdateArray) {
  // Build WKB array with a null from POINT (0 1)...POINT (14, 15)
  ::arrow::BinaryBuilder builder;
  std::array<char, test::kWkbPointSize> item;
  for (int k = 0; k < 10; k++) {
    test::GenerateWKBPoint(reinterpret_cast<uint8_t*>(item.data()), k, k + 1);
    ASSERT_OK(builder.AppendValues({std::string(item.data(), item.size())}));
  }

  ASSERT_OK(builder.AppendNull());

  for (int k = 10; k < 15; k++) {
    test::GenerateWKBPoint(reinterpret_cast<uint8_t*>(item.data()), k, k + 1);
    ASSERT_OK(builder.AppendValues({std::string(item.data(), item.size())}));
  }

  // Ensure we have both a binary array and a large binary array to work with
  ASSERT_OK_AND_ASSIGN(const auto binary_array, builder.Finish());
  ASSERT_OK_AND_ASSIGN(const auto large_binary_array,
                       ::arrow::compute::Cast(binary_array, ::arrow::large_binary()));

  GeospatialStatistics stats;
  stats.Update(*binary_array);
  EXPECT_EQ(stats.GetXMin(), 0);
  EXPECT_EQ(stats.GetYMin(), 1);
  EXPECT_EQ(stats.GetXMax(), 14);
  EXPECT_EQ(stats.GetYMax(), 15);

  GeospatialStatistics stats_large;
  stats_large.Update(*large_binary_array.make_array());

  EXPECT_TRUE(stats_large.Equals(stats));
}

TEST(TestGeospatialStatistics, TestUpdateArrayInvalid) {
  // Build WKB array with invalid WKB (here, an empty string)
  ::arrow::BinaryBuilder builder;
  ASSERT_OK(builder.AppendValues({std::string()}));
  ASSERT_OK_AND_ASSIGN(const auto invalid_wkb, builder.Finish());

  GeospatialStatistics stats;
  stats.Update(*invalid_wkb);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_FALSE(stats.Encode().is_set());
}

}  // namespace parquet::geometry
