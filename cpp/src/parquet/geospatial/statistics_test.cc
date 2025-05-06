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

#include "parquet/geospatial/statistics.h"
#include "parquet/test_util.h"

static constexpr double kInf = std::numeric_limits<double>::infinity();

namespace parquet::geospatial {

TEST(TestGeoStatistics, TestDefaults) {
  GeoStatistics stats;
  EXPECT_TRUE(stats.geometry_types().has_value());
  EXPECT_EQ(stats.geometry_types().value().size(), 0);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats.dimension_empty(), ::testing::ElementsAre(true, true, true, true));
  EXPECT_THAT(stats.dimension_valid(), ::testing::ElementsAre(true, true, true, true));
  for (int i = 0; i < kMaxDimensions; i++) {
    EXPECT_EQ(stats.lower_bound()[i], kInf);
    EXPECT_EQ(stats.upper_bound()[i], -kInf);
  }

  EXPECT_TRUE(stats.Equals(GeoStatistics()));
  EXPECT_EQ(stats.ToString(),
            "<GeoStatistics> x: empty y: empty z: empty m: empty "
            "geometry_types: []");

  // Merging empty with empty should equal empty
  stats.Merge(GeoStatistics());
  EXPECT_TRUE(stats.Equals(GeoStatistics()));

  // There's no way to encode empty ranges in Thrift, so we pretend that we
  // didn't calculate them.
  auto encoded = stats.Encode();
  EXPECT_FALSE(encoded->xy_bounds_present);
  EXPECT_FALSE(encoded->z_bounds_present);
  EXPECT_FALSE(encoded->m_bounds_present);
  EXPECT_FALSE(encoded->geospatial_types_present());

  // When imported, the statistics marked with everything uncalculated should
  // be "valid" but should have individual components marked as invalid.
  GeoStatistics valid_but_uncalculated;
  valid_but_uncalculated.Decode(*encoded);

  EXPECT_TRUE(valid_but_uncalculated.is_valid());
  EXPECT_THAT(valid_but_uncalculated.dimension_valid(),
              ::testing::ElementsAre(false, false, false, false));
  EXPECT_EQ(valid_but_uncalculated.geometry_types(), std::nullopt);
}

TEST(TestGeoStatistics, TestImportEncodedWithNaN) {
  double nan_dbl = std::numeric_limits<double>::quiet_NaN();

  EncodedGeoStatistics encoded_with_nan;
  encoded_with_nan.xy_bounds_present = true;
  encoded_with_nan.xmin = nan_dbl;
  encoded_with_nan.xmax = nan_dbl;
  encoded_with_nan.ymin = nan_dbl;
  encoded_with_nan.ymax = nan_dbl;
  encoded_with_nan.z_bounds_present = true;
  encoded_with_nan.zmin = nan_dbl;
  encoded_with_nan.zmax = nan_dbl;
  encoded_with_nan.m_bounds_present = true;
  encoded_with_nan.mmin = nan_dbl;
  encoded_with_nan.mmax = nan_dbl;

  GeoStatistics stats(encoded_with_nan);
  EXPECT_THAT(stats.dimension_valid(),
              ::testing::ElementsAre(false, false, false, false));
  for (int i = 0; i < kMaxDimensions; i++) {
    EXPECT_TRUE(std::isnan(stats.lower_bound()[i]));
    EXPECT_TRUE(std::isnan(stats.upper_bound()[i]));
  }

  // Ensure that if we merge finite values into nans we get dimensions marked as invalid
  GeoStatistics stats_finite;
  std::string xyzm0 = test::MakeWKBPoint({10, 11, 12, 13}, true, true);
  ByteArray item0{xyzm0};
  stats_finite.Update(&item0, /*num_values=*/1);

  stats.Merge(stats_finite);
  EXPECT_THAT(stats.dimension_valid(),
              ::testing::ElementsAre(false, false, false, false));

  // Ensure that if we merge nans into finite values we also get dimensions marked as
  // invalid
  stats_finite.Merge(stats);
  EXPECT_THAT(stats.dimension_valid(),
              ::testing::ElementsAre(false, false, false, false));

  // Ensure that if we decode nans, we mark the ranges as uncalculated
  auto encoded = stats.Encode();
  EXPECT_FALSE(encoded->xy_bounds_present);
  EXPECT_FALSE(encoded->z_bounds_present);
  EXPECT_FALSE(encoded->m_bounds_present);
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
  EXPECT_THAT(*stats.geometry_types(), ::testing::ElementsAre(3001));
  EXPECT_THAT(stats.dimension_empty(),
              ::testing::ElementsAre(false, false, false, false));

  std::string xyzm1 = test::MakeWKBPoint({20, 21, 22, 23}, true, true);
  ByteArray item1{xyzm1};

  stats.Update(&item1, /*num_values=*/1);
  EXPECT_TRUE(stats.is_valid());
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(20, 21, 22, 23));
  EXPECT_THAT(*stats.geometry_types(), ::testing::ElementsAre(3001));

  // Check recreating the statistics with actual values
  auto encoded = stats.Encode();
  EXPECT_TRUE(GeoStatistics(*encoded).Equals(stats));
  EXPECT_EQ(stats.ToString(),
            "<GeoStatistics> x: [10, 20] y: [11, 21] z: [12, 22] m: "
            "[13, 23] geometry_types: [3001]");

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
  EXPECT_THAT(*stats_spaced.geometry_types(), ::testing::ElementsAre(3001));

  // Check merge
  stats.Merge(stats_spaced);
  EXPECT_TRUE(stats.Equals(stats_spaced));

  // Check ingest of invalid WKB
  ByteArray invalid;
  stats.Update(&invalid, /*num_values=*/1);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_EQ(stats.Encode(), std::nullopt);

  // This should be true even after ingesting more values
  stats.Update(&item0, /*num_values=*/1);
  EXPECT_FALSE(stats.is_valid());
  EXPECT_EQ(stats.Encode(), std::nullopt);
  EXPECT_EQ(stats.ToString(), "<GeoStatistics> invalid");

  // And should cause other statistics to become invalid when merged with them
  stats_spaced.Merge(stats);
  EXPECT_FALSE(stats_spaced.is_valid());
  EXPECT_EQ(stats_spaced.Encode(), std::nullopt);
}

TEST(TestGeoStatistics, TestUpdateXYZM) {
  GeoStatistics stats;
  GeoStatistics stats_not_equal;
  EncodedGeoStatistics encoded;
  GeoStatistics from_encoded;

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
  EXPECT_THAT(*stats.geometry_types(), ::testing::ElementsAre(1));
  EXPECT_THAT(stats.dimension_empty(), ::testing::ElementsAre(false, false, true, true));
  EXPECT_FALSE(stats.Equals(stats_not_equal));
  stats_not_equal.Merge(stats);

  // When we encode + decode the statistcs, we should ensure that the non-empty
  // dimensions are kept and that the previously empty dimensions are now invalid
  encoded = *stats.Encode();
  EXPECT_TRUE(encoded.xy_bounds_present);
  EXPECT_FALSE(encoded.z_bounds_present);
  EXPECT_FALSE(encoded.m_bounds_present);
  from_encoded.Decode(encoded);
  EXPECT_THAT(from_encoded.dimension_empty(),
              ::testing::ElementsAre(false, false, false, false));
  EXPECT_THAT(from_encoded.dimension_valid(),
              ::testing::ElementsAre(true, true, false, false));

  ByteArray item_xyz{xyz};
  stats.Update(&item_xyz, /*num_values=*/1);
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, kInf));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(10, 11, 12, -kInf));
  EXPECT_THAT(*stats.geometry_types(), ::testing::ElementsAre(1, 1001));
  EXPECT_THAT(stats.dimension_empty(), ::testing::ElementsAre(false, false, false, true));
  EXPECT_FALSE(stats.Equals(stats_not_equal));
  stats_not_equal.Merge(stats);

  encoded = *stats.Encode();
  EXPECT_TRUE(encoded.xy_bounds_present);
  EXPECT_TRUE(encoded.z_bounds_present);
  EXPECT_FALSE(encoded.m_bounds_present);
  from_encoded.Decode(encoded);
  EXPECT_THAT(from_encoded.dimension_empty(),
              ::testing::ElementsAre(false, false, false, false));
  EXPECT_THAT(from_encoded.dimension_valid(),
              ::testing::ElementsAre(true, true, true, false));

  ByteArray item_xym{xym};
  stats.Update(&item_xym, /*num_values=*/1);
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(10, 11, 12, 13));
  EXPECT_THAT(*stats.geometry_types(), ::testing::ElementsAre(1, 1001, 2001));
  EXPECT_THAT(stats.dimension_empty(),
              ::testing::ElementsAre(false, false, false, false));
  EXPECT_FALSE(stats.Equals(stats_not_equal));

  encoded = *stats.Encode();
  EXPECT_TRUE(encoded.xy_bounds_present);
  EXPECT_TRUE(encoded.z_bounds_present);
  EXPECT_TRUE(encoded.m_bounds_present);
  from_encoded.Decode(encoded);
  EXPECT_THAT(from_encoded.dimension_empty(),
              ::testing::ElementsAre(false, false, false, false));
  EXPECT_THAT(from_encoded.dimension_valid(),
              ::testing::ElementsAre(true, true, true, true));
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
  EXPECT_THAT(stats.lower_bound(), ::testing::ElementsAre(0, 1, kInf, kInf));
  EXPECT_THAT(stats.upper_bound(), ::testing::ElementsAre(14, 15, -kInf, -kInf));

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
  EXPECT_EQ(invalid.Encode(), std::nullopt);
  EXPECT_FALSE(invalid.Equals(GeoStatistics()));

  // Make some valid statistics
  EncodedGeoStatistics encoded_valid;
  encoded_valid.xy_bounds_present = true;
  encoded_valid.xmin = 0;
  encoded_valid.xmax = 10;
  encoded_valid.ymin = 20;
  encoded_valid.ymax = 30;
  GeoStatistics valid(encoded_valid);

  // Make some statistics with unsupported wraparound
  EncodedGeoStatistics encoded_unsupported;
  encoded_unsupported.xy_bounds_present = true;
  encoded_unsupported.xmin = 10;
  encoded_unsupported.xmax = 0;
  encoded_unsupported.ymin = 20;
  encoded_unsupported.ymax = 30;
  GeoStatistics unsupported(encoded_unsupported);

  // Check that X values are marked as invalid if merging wraparound
  // and a non-wraparound statistics
  GeoStatistics valid_merge_unsupported(encoded_valid);
  valid_merge_unsupported.Merge(unsupported);
  EXPECT_THAT(valid_merge_unsupported.dimension_valid(),
              ::testing::ElementsAre(false, true, false, false));

  GeoStatistics unsupported_merge_valid(encoded_unsupported);
  unsupported_merge_valid.Merge(valid);
  EXPECT_THAT(valid_merge_unsupported.dimension_valid(),
              ::testing::ElementsAre(false, true, false, false));
}

TEST(TestGeoStatistics, TestEquals) {
  GeoStatistics stats_a;
  GeoStatistics stats_b;

  std::string wkb_empty = test::MakeWKBPoint({kNaN, kNaN, kNaN, kNaN}, true, true);
  ByteArray pt_empty{wkb_empty};
  std::string wkb_pt = test::MakeWKBPoint({10, 11, 12, 13}, true, true);
  ByteArray pt{wkb_pt};
  std::string wkb_pt2 = test::MakeWKBPoint({14, 15, 16, 17}, true, true);
  ByteArray pt2{wkb_pt2};
  ByteArray invalid;

  // Both empty
  EXPECT_TRUE(stats_a.Equals(stats_b));

  // Both empty but one has different geometry types
  stats_b.Update(&pt_empty, 1);
  EXPECT_EQ(stats_a.is_valid(), stats_b.is_valid());
  EXPECT_EQ(stats_a.upper_bound(), stats_b.upper_bound());
  EXPECT_EQ(stats_a.lower_bound(), stats_b.lower_bound());
  EXPECT_NE(stats_a.geometry_types(), stats_b.geometry_types());
  EXPECT_FALSE(stats_a.Equals(stats_b));

  // Both empty
  stats_b.Reset();
  EXPECT_TRUE(stats_a.Equals(stats_b));

  // Geometry types equal but bounds not equal
  stats_a.Update(&pt, 1);
  stats_b.Update(&pt2, 1);
  EXPECT_EQ(stats_a.is_valid(), stats_b.is_valid());
  EXPECT_NE(stats_a.upper_bound(), stats_b.upper_bound());
  EXPECT_NE(stats_a.lower_bound(), stats_b.lower_bound());
  EXPECT_EQ(stats_a.geometry_types(), stats_b.geometry_types());
  EXPECT_FALSE(stats_a.Equals(stats_b));

  // Both empty
  stats_a.Reset();
  stats_b.Reset();
  EXPECT_TRUE(stats_a.Equals(stats_b));

  // Only validity is different
  stats_b.Update(&invalid, 1);
  EXPECT_NE(stats_a.is_valid(), stats_b.is_valid());
  EXPECT_EQ(stats_a.upper_bound(), stats_b.upper_bound());
  EXPECT_EQ(stats_a.lower_bound(), stats_b.lower_bound());
  EXPECT_EQ(stats_a.geometry_types(), stats_b.geometry_types());
  EXPECT_FALSE(stats_a.Equals(stats_b));

  // Everything equal for a non-empty case
  stats_a.Reset();
  stats_b.Reset();
  stats_a.Update(&pt, 1);
  stats_b.Update(&pt, 1);
  EXPECT_TRUE(stats_a.Equals(stats_b));
}

}  // namespace parquet::geospatial
