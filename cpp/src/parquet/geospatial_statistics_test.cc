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

#include "parquet/geospatial_statistics.h"
#include "parquet/test_util.h"

namespace parquet::geometry {

TEST(TestGeoStatistics, TestDefaults) {
  GeoStatistics stats;
  EXPECT_EQ(stats.get_geometry_types().size(), 0);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_TRUE(stats.is_empty());
  EXPECT_FALSE(stats.has_z());
  EXPECT_FALSE(stats.has_m());
  EXPECT_EQ(stats.get_xmax() - stats.get_xmin(), -kInf);
  EXPECT_EQ(stats.get_ymax() - stats.get_ymin(), -kInf);
  EXPECT_EQ(stats.get_zmax() - stats.get_zmin(), -kInf);
  EXPECT_EQ(stats.get_mmax() - stats.get_mmin(), -kInf);
  EXPECT_TRUE(stats.Equals(GeoStatistics()));

  EXPECT_EQ(stats.ToString(),
            "GeoStatistics \n  x: [inf, -inf]\n  y: [inf, -inf]\n  geometry_types:\n");

  auto encoded = stats.Encode();
  EXPECT_FALSE(encoded.is_set());
  EXPECT_FALSE(encoded.has_z());
  EXPECT_FALSE(encoded.has_m());
  EXPECT_TRUE(GeoStatistics(encoded).Equals(stats));

  stats.Merge(GeoStatistics());
  EXPECT_TRUE(GeoStatistics(encoded).Equals(stats));
}

TEST(TestGeoStatistics, TestUpdateByteArray) {
  GeoStatistics stats;

  // Make items with all dimensions to ensure all dimensions are updated
  // and returned properly (POINT XYZM has an integer code of 3001)
  std::string xyzm0 = test::MakeWKBPoint({10, 11, 12, 13}, true, true);
  ByteArray item0{xyzm0};

  stats.Update(&item0, /*num_values=*/1);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats.get_lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.get_upper_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.get_geometry_types(), ::testing::ElementsAre(3001));

  std::string xyzm1 = test::MakeWKBPoint({20, 21, 22, 23}, true, true);
  ByteArray item1{xyzm1};

  stats.Update(&item1, /*num_values=*/1);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats.get_lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.get_upper_bound(), ::testing::ElementsAre(20, 21, 22, 23));
  EXPECT_THAT(stats.get_geometry_types(), ::testing::ElementsAre(3001));

  // Check recreating the statistics with actual values
  auto encoded = stats.Encode();
  EXPECT_TRUE(GeoStatistics(encoded).Equals(stats));
  EXPECT_EQ(stats.ToString(),
            "GeoStatistics \n  x: [10, 20]\n  y: [11, 21]\n  z: [12, 22]\n  m: "
            "[13, 23]\n  geometry_types: 3001\n");

  // Check resetting to the original state
  stats.Reset();
  EXPECT_TRUE(stats.Equals(GeoStatistics()));

  // Check UpdateSpaced()

  // A null value that should be skipped
  std::string xyzm2 = test::MakeWKBPoint({-30, -31, -32, -33}, true, true);
  ByteArray item2{xyzm2};

  // A non-null value that shouldn't be skipped
  std::string xyzm3 = test::MakeWKBPoint({30, 31, 32, 33}, true, true);
  ByteArray item3{xyzm3};

  ByteArray items[] = {item0, item1, item2, item3};
  // Validity bitmap with an extra bit on the front to check non-zero bits offset
  uint8_t validity = 0b00010111;
  GeoStatistics stats_spaced;
  stats_spaced.UpdateSpaced(items, &validity, 1, 4, 4);

  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats_spaced.get_lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats_spaced.get_upper_bound(), ::testing::ElementsAre(30, 31, 32, 33));
  EXPECT_THAT(stats_spaced.get_geometry_types(), ::testing::ElementsAre(3001));

  // Check merge
  stats.Merge(stats_spaced);
  EXPECT_TRUE(stats.Equals(stats_spaced));

  // Check ingest of invalid WKB
  ByteArray invalid;
  stats.Update(&invalid, /*num_values=*/1);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_FALSE(stats.Encode().is_set());

  // This should be true even after ingesting more values
  stats.Update(&item0, /*num_values=*/1);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_FALSE(stats.Encode().is_set());
  EXPECT_EQ(stats.ToString(), "GeoStatistics <invalid>\n");

  // And should cause other statistics to become invalid when merged with them
  stats_spaced.Merge(stats);
  EXPECT_FALSE(stats_spaced.is_valid());
  EXPECT_FALSE(stats_spaced.Encode().is_set());
}

TEST(TestGeoStatistics, TestUpdateArray) {
  // Build WKB array with a null from POINT (0 1)...POINT (14, 15)
  ::arrow::BinaryBuilder builder;
  for (int k = 0; k < 10; k++) {
    std::string item = test::MakeWKBPoint(
        {static_cast<double>(k), static_cast<double>(k + 1)}, false, false);
    ASSERT_OK(builder.Append(item));
  }

  ASSERT_OK(builder.AppendNull());

  for (int k = 10; k < 15; k++) {
    std::string item = test::MakeWKBPoint(
        {static_cast<double>(k), static_cast<double>(k + 1)}, false, false);
    ASSERT_OK(builder.Append(item));
  }

  // Ensure we have both a binary array and a large binary array to work with
  ASSERT_OK_AND_ASSIGN(const auto binary_array, builder.Finish());
  ASSERT_OK_AND_ASSIGN(const auto large_binary_array,
                       ::arrow::compute::Cast(binary_array, ::arrow::large_binary()));

  GeoStatistics stats;
  stats.Update(*binary_array);
  EXPECT_EQ(stats.get_xmin(), 0);
  EXPECT_EQ(stats.get_ymin(), 1);
  EXPECT_EQ(stats.get_xmax(), 14);
  EXPECT_EQ(stats.get_ymax(), 15);

  GeoStatistics stats_large;
  stats_large.Update(*large_binary_array.make_array());

  EXPECT_TRUE(stats_large.Equals(stats));
}

TEST(TestGeoStatistics, TestUpdateArrayInvalid) {
  // Build WKB array with invalid WKB (here, an empty string)
  ::arrow::BinaryBuilder builder;
  ASSERT_OK(builder.Append(std::string()));
  ASSERT_OK_AND_ASSIGN(const auto invalid_wkb, builder.Finish());

  // This should result in statistics that are "unset"
  GeoStatistics invalid;
  invalid.Update(*invalid_wkb);
  EXPECT_FALSE(invalid.is_valid());
  EXPECT_FALSE(invalid.Encode().is_set());

  // Make some valid statistics
  EncodedGeoStatistics encoded_valid;
  encoded_valid.xmin = 0;
  encoded_valid.xmax = 10;
  encoded_valid.ymin = 20;
  encoded_valid.ymax = 30;
  GeoStatistics valid(encoded_valid);

  // Make some statistics with unsupported wraparound
  EncodedGeoStatistics encoded_unsupported;
  encoded_unsupported.xmin = 10;
  encoded_unsupported.xmax = 0;
  encoded_unsupported.ymin = 20;
  encoded_unsupported.ymax = 30;
  GeoStatistics unsupported(encoded_unsupported);

  EXPECT_THROW(valid.Merge(unsupported), ParquetException);
  EXPECT_THROW(unsupported.Merge(valid), ParquetException);
}

}  // namespace parquet::geometry
