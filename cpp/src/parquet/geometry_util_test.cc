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

#include "arrow/testing/gtest_compat.h"

#include "parquet/geometry_util.h"

namespace parquet::geometry {

TEST(TestGeometryUtil, TestDimensions) {
  EXPECT_EQ(Dimensions::size<Dimensions::XY>(), 2);
  EXPECT_EQ(Dimensions::size<Dimensions::XYZ>(), 3);
  EXPECT_EQ(Dimensions::size<Dimensions::XYM>(), 3);
  EXPECT_EQ(Dimensions::size<Dimensions::XYZM>(), 4);

  EXPECT_EQ(Dimensions::ToString(Dimensions::XY), "XY");
  EXPECT_EQ(Dimensions::ToString(Dimensions::XYZ), "XYZ");
  EXPECT_EQ(Dimensions::ToString(Dimensions::XYM), "XYM");
  EXPECT_EQ(Dimensions::ToString(Dimensions::XYZM), "XYZM");

  EXPECT_EQ(Dimensions::FromWKB(1), Dimensions::XY);
  EXPECT_EQ(Dimensions::FromWKB(1001), Dimensions::XYZ);
  EXPECT_EQ(Dimensions::FromWKB(2001), Dimensions::XYM);
  EXPECT_EQ(Dimensions::FromWKB(3001), Dimensions::XYZM);
  EXPECT_THROW(Dimensions::FromWKB(4001), ParquetException);
}

TEST(TestGeometryUtil, TestGeometryType) {
  EXPECT_EQ(GeometryType::ToString(GeometryType::POINT), "POINT");
  EXPECT_EQ(GeometryType::ToString(GeometryType::LINESTRING), "LINESTRING");
  EXPECT_EQ(GeometryType::ToString(GeometryType::POLYGON), "POLYGON");
  EXPECT_EQ(GeometryType::ToString(GeometryType::MULTIPOINT), "MULTIPOINT");
  EXPECT_EQ(GeometryType::ToString(GeometryType::MULTILINESTRING), "MULTILINESTRING");
  EXPECT_EQ(GeometryType::ToString(GeometryType::MULTIPOLYGON), "MULTIPOLYGON");
  EXPECT_EQ(GeometryType::ToString(GeometryType::GEOMETRYCOLLECTION),
            "GEOMETRYCOLLECTION");

  EXPECT_EQ(GeometryType::FromWKB(1), GeometryType::POINT);
  EXPECT_EQ(GeometryType::FromWKB(1001), GeometryType::POINT);
  EXPECT_EQ(GeometryType::FromWKB(1002), GeometryType::LINESTRING);
  EXPECT_EQ(GeometryType::FromWKB(1003), GeometryType::POLYGON);
  EXPECT_EQ(GeometryType::FromWKB(1004), GeometryType::MULTIPOINT);
  EXPECT_EQ(GeometryType::FromWKB(1005), GeometryType::MULTILINESTRING);
  EXPECT_EQ(GeometryType::FromWKB(1006), GeometryType::MULTIPOLYGON);
  EXPECT_EQ(GeometryType::FromWKB(1007), GeometryType::GEOMETRYCOLLECTION);
  EXPECT_THROW(GeometryType::FromWKB(1100), ParquetException);
}

TEST(TestGeometryUtil, TestBoundingBox) {
  BoundingBox box;
  EXPECT_EQ(box, BoundingBox(Dimensions::XYZM, {kInf, kInf, kInf, kInf},
                             {-kInf, -kInf, -kInf, -kInf}));
  EXPECT_EQ(box.ToString(),
            "BoundingBox XYZM [inf => -inf, inf => -inf, inf => -inf, inf => -inf]");

  BoundingBox box_xyzm(Dimensions::XYZM, {-1, -2, -3, -4}, {1, 2, 3, 4});

  BoundingBox box_xy(Dimensions::XY, {-10, -20, kInf, kInf}, {10, 20, -kInf, -kInf});
  BoundingBox box_xyz(Dimensions::XYZ, {kInf, kInf, -30, kInf},
                      {-kInf, -kInf, 30, -kInf});
  BoundingBox box_xym(Dimensions::XYM, {kInf, kInf, -40, kInf},
                      {-kInf, -kInf, 40, -kInf});

  box_xyzm.Merge(box_xy);
  EXPECT_EQ(box_xyzm, BoundingBox(Dimensions::XYZM, {-10, -20, -3, -4}, {10, 20, 3, 4}));

  box_xyzm.Merge(box_xyz);
  EXPECT_EQ(box_xyzm,
            BoundingBox(Dimensions::XYZM, {-10, -20, -30, -4}, {10, 20, 30, 4}));

  box_xyzm.Merge(box_xym);
  EXPECT_EQ(box_xyzm,
            BoundingBox(Dimensions::XYZM, {-10, -20, -30, -40}, {10, 20, 30, 40}));

  box_xyzm.Reset();
  EXPECT_EQ(box_xyzm, BoundingBox());
}

}  // namespace parquet::geometry
