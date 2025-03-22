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

static constexpr double kInf = std::numeric_limits<double>::infinity();

TEST(TestGeoStatistics, TestDefaults) {
  GeoStatistics stats;
  EXPECT_EQ(stats.geometry_types().size(), 0);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_TRUE(stats.is_empty());
  EXPECT_FALSE(stats.has_x());
  EXPECT_FALSE(stats.has_y());
  EXPECT_FALSE(stats.has_z());
  EXPECT_FALSE(stats.has_m());
  EXPECT_EQ(stats.xmax() - stats.xmin(), -kInf);
  EXPECT_EQ(stats.ymax() - stats.ymin(), -kInf);
  EXPECT_EQ(stats.zmax() - stats.zmin(), -kInf);
  EXPECT_EQ(stats.mmax() - stats.mmin(), -kInf);
  EXPECT_TRUE(stats.Equals(GeoStatistics()));

  EXPECT_EQ(stats.ToString(),
            "GeoStatistics \n  x: [inf, -inf]\n  y: [inf, -inf]\n  geometry_types:\n");

  auto encoded = stats.Encode();
  EXPECT_FALSE(encoded.is_set());
  EXPECT_FALSE(encoded.has_x());
  EXPECT_FALSE(encoded.has_y());
  EXPECT_FALSE(encoded.has_z());
  EXPECT_FALSE(encoded.has_m());
  EXPECT_TRUE(GeoStatistics(encoded).Equals(stats));

  stats.Merge(GeoStatistics());
  EXPECT_TRUE(GeoStatistics(encoded).Equals(stats));
}

TEST(TestGeoStatistics, TestImportEncodedWithNaN) {
  double nan_dbl = std::numeric_limits<double>::quiet_NaN();

  EncodedGeoStatistics encoded_with_nan;
  encoded_with_nan.xmin = nan_dbl;
  encoded_with_nan.xmax = nan_dbl;
  encoded_with_nan.ymin = nan_dbl;
  encoded_with_nan.ymax = nan_dbl;
  encoded_with_nan.zmin = nan_dbl;
  encoded_with_nan.zmax = nan_dbl;
  encoded_with_nan.mmin = nan_dbl;
  encoded_with_nan.mmax = nan_dbl;

  GeoStatistics stats(encoded_with_nan);
  EXPECT_TRUE(stats.has_x());
  EXPECT_TRUE(stats.has_y());
  EXPECT_TRUE(stats.has_z());
  EXPECT_TRUE(stats.has_m());

  EXPECT_TRUE(std::isnan(stats.xmin()));
  EXPECT_TRUE(std::isnan(stats.xmax()));
  EXPECT_TRUE(std::isnan(stats.ymin()));
  EXPECT_TRUE(std::isnan(stats.ymax()));
  EXPECT_TRUE(std::isnan(stats.zmin()));
  EXPECT_TRUE(std::isnan(stats.zmax()));
  EXPECT_TRUE(std::isnan(stats.mmin()));
  EXPECT_TRUE(std::isnan(stats.mmax()));

  // Ensure that if we merge finite values into nans we get nans
  GeoStatistics stats_finite;
  std::string xyzm0 = test::MakeWKBPoint({10, 11, 12, 13}, true, true);
  ByteArray item0{xyzm0};
  stats_finite.Update(&item0, /*num_values=*/1);

  stats.Merge(stats_finite);
  EXPECT_TRUE(std::isnan(stats.xmin()));
  EXPECT_TRUE(std::isnan(stats.xmax()));
  EXPECT_TRUE(std::isnan(stats.ymin()));
  EXPECT_TRUE(std::isnan(stats.ymax()));
  EXPECT_TRUE(std::isnan(stats.zmin()));
  EXPECT_TRUE(std::isnan(stats.zmax()));
  EXPECT_TRUE(std::isnan(stats.mmin()));
  EXPECT_TRUE(std::isnan(stats.mmax()));

  // Ensure that if we merge nans into finite values we also get nans
  stats_finite.Merge(stats);
  EXPECT_TRUE(std::isnan(stats_finite.xmin()));
  EXPECT_TRUE(std::isnan(stats_finite.xmax()));
  EXPECT_TRUE(std::isnan(stats_finite.ymin()));
  EXPECT_TRUE(std::isnan(stats_finite.ymax()));
  EXPECT_TRUE(std::isnan(stats_finite.zmin()));
  EXPECT_TRUE(std::isnan(stats_finite.zmax()));
  EXPECT_TRUE(std::isnan(stats_finite.mmin()));
  EXPECT_TRUE(std::isnan(stats_finite.mmax()));

  // Ensure that if we decode nans, we also encode nans
  EncodedGeoStatistics stats_encoded = stats.Encode();
  EXPECT_TRUE(std::isnan(stats_encoded.xmin));
  EXPECT_TRUE(std::isnan(stats_encoded.xmax));
  EXPECT_TRUE(std::isnan(stats_encoded.ymin));
  EXPECT_TRUE(std::isnan(stats_encoded.ymax));
  EXPECT_TRUE(std::isnan(stats_encoded.zmin));
  EXPECT_TRUE(std::isnan(stats_encoded.zmax));
  EXPECT_TRUE(std::isnan(stats_encoded.mmin));
  EXPECT_TRUE(std::isnan(stats_encoded.mmax));
}

TEST(TestGeoStatistics, TestUpdateByteArray) {
  GeoStatistics stats;

  // Make items with all dimensions to ensure all dimensions are updated
  // and returned properly (POINT XYZM has an integer code of 3001)
  std::string xyzm0 = test::MakeWKBPoint({10, 11, 12, 13}, true, true);
  ByteArray item0{xyzm0};

  stats.Update(&item0, /*num_values=*/1);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.geometry_types(), ::testing::ElementsAre(3001));
  EXPECT_TRUE(stats.has_x());
  EXPECT_TRUE(stats.has_y());
  EXPECT_TRUE(stats.has_z());
  EXPECT_TRUE(stats.has_m());

  std::string xyzm1 = test::MakeWKBPoint({20, 21, 22, 23}, true, true);
  ByteArray item1{xyzm1};

  stats.Update(&item1, /*num_values=*/1);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(20, 21, 22, 23));
  EXPECT_THAT(stats.geometry_types(), ::testing::ElementsAre(3001));

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
  EXPECT_THAT(stats_spaced.lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats_spaced.upper_bound(), ::testing::ElementsAre(30, 31, 32, 33));
  EXPECT_THAT(stats_spaced.geometry_types(), ::testing::ElementsAre(3001));

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

TEST(TestGeoStatistics, TestUpdateXYZM) {
  GeoStatistics stats;

  // Test existence of x, y, z, and m by ingesting an XY, an XYZ, then an XYM
  // and check that the has_(xyzm)() methods are working as expected and that
  // geometry_types() accumulate (1 is the geometry type code for POINT,
  // 1001 is the code for POINT Z, and 2001 is the code for POINT M).
  std::string xy = test::MakeWKBPoint({10, 11, 0, 0}, false, false);
  std::string xyz = test::MakeWKBPoint({10, 11, 12, 0}, true, false);
  std::string xym = test::MakeWKBPoint({10, 11, 0, 13}, false, true);

  ByteArray item_xy{xy};
  stats.Update(&item_xy, /*num_values=*/1);
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, kInf, kInf));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(10, 11, -kInf, -kInf));
  EXPECT_THAT(stats.geometry_types(), ::testing::ElementsAre(1));
  EXPECT_TRUE(stats.has_x());
  EXPECT_TRUE(stats.has_y());
  EXPECT_FALSE(stats.has_z());
  EXPECT_FALSE(stats.has_m());

  ByteArray item_xyz{xyz};
  stats.Update(&item_xyz, /*num_values=*/1);
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, kInf));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(10, 11, 12, -kInf));
  EXPECT_THAT(stats.geometry_types(), ::testing::ElementsAre(1, 1001));
  EXPECT_TRUE(stats.has_x());
  EXPECT_TRUE(stats.has_y());
  EXPECT_TRUE(stats.has_z());
  EXPECT_FALSE(stats.has_m());

  ByteArray item_xym{xym};
  stats.Update(&item_xym, /*num_values=*/1);
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.geometry_types(), ::testing::ElementsAre(1, 1001, 2001));
  EXPECT_TRUE(stats.has_x());
  EXPECT_TRUE(stats.has_y());
  EXPECT_TRUE(stats.has_z());
  EXPECT_TRUE(stats.has_m());
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
  EXPECT_EQ(stats.xmin(), 0);
  EXPECT_EQ(stats.ymin(), 1);
  EXPECT_EQ(stats.xmax(), 14);
  EXPECT_EQ(stats.ymax(), 15);

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
