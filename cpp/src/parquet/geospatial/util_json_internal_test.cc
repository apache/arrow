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

#include "parquet/geospatial/util_json_internal.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/simdjson_internal.h"

#include "parquet/test_util.h"

namespace parquet {

struct LogicalTypeToGeoArrowCase {
  std::shared_ptr<const LogicalType> logical_type;
  std::string expected_metadata;
};

class LogicalTypeToGeoArrowTest
    : public ::testing::TestWithParam<LogicalTypeToGeoArrowCase> {};

TEST_P(LogicalTypeToGeoArrowTest, ConvertsMetadata) {
  ::arrow::ExtensionTypeGuard guard(test::geoarrow_wkb());
  const auto& test_case = GetParam();
  ARROW_SCOPED_TRACE("logical type = ", test_case.logical_type->ToString());

  ASSERT_OK_AND_ASSIGN(
      auto type,
      GeoArrowTypeFromLogicalType(*test_case.logical_type, nullptr, ::arrow::binary()));
  auto extension = std::dynamic_pointer_cast<::arrow::ExtensionType>(type);
  ASSERT_NE(extension, nullptr);
  ASSERT_OK_AND_ASSIGN(auto actual,
                       ::arrow::internal::MinifyJson(extension->Serialize()));
  EXPECT_EQ(actual, test_case.expected_metadata);
}

INSTANTIATE_TEST_SUITE_P(
    GeoArrowConversion, LogicalTypeToGeoArrowTest,
    ::testing::Values(
        LogicalTypeToGeoArrowCase{LogicalType::Geometry(), R"({"crs":"OGC:CRS84"})"},
        LogicalTypeToGeoArrowCase{LogicalType::Geometry("srid:0"), R"({})"},
        LogicalTypeToGeoArrowCase{LogicalType::Geometry("EPSG:4267"),
                                  R"({"crs":"EPSG:4267"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geometry(R"({"id":{"authority":"EPSG","code":4326}})"),
            R"({"crs":{"id":{"authority":"EPSG","code":4326}}})"},
        LogicalTypeToGeoArrowCase{LogicalType::Geography(),
                                  R"({"crs":"OGC:CRS84","edges":"spherical"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geography("",
                                   LogicalType::EdgeInterpolationAlgorithm::SPHERICAL),
            R"({"crs":"OGC:CRS84","edges":"spherical"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::KARNEY),
            R"({"crs":"OGC:CRS84","edges":"karney"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::VINCENTY),
            R"({"crs":"OGC:CRS84","edges":"vincenty"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::ANDOYER),
            R"({"crs":"OGC:CRS84","edges":"andoyer"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::THOMAS),
            R"({"crs":"OGC:CRS84","edges":"thomas"})"},
        LogicalTypeToGeoArrowCase{LogicalType::Geography("srid:0"),
                                  R"({"edges":"spherical"})"},
        LogicalTypeToGeoArrowCase{LogicalType::Geography("EPSG:4267"),
                                  R"({"crs":"EPSG:4267","edges":"spherical"})"},
        LogicalTypeToGeoArrowCase{
            LogicalType::Geography(R"({"id":{"authority":"EPSG","code":4326}})"),
            R"({"crs":{"id":{"authority":"EPSG","code":4326}},"edges":"spherical"})"}));

struct GeoArrowToLogicalTypeCase {
  std::string metadata;
  std::shared_ptr<const LogicalType> expected_logical_type;
};

class GeoArrowToLogicalTypeTest
    : public ::testing::TestWithParam<GeoArrowToLogicalTypeCase> {};

TEST_P(GeoArrowToLogicalTypeTest, ConvertsMetadata) {
  const auto& test_case = GetParam();
  ARROW_SCOPED_TRACE("metadata = ", test_case.metadata);
  ASSERT_OK_AND_ASSIGN(auto actual, LogicalTypeFromGeoArrowMetadata(test_case.metadata));
  EXPECT_TRUE(actual->Equals(*test_case.expected_logical_type))
      << "Actual: " << actual->ToString()
      << ", expected: " << test_case.expected_logical_type->ToString();
}

INSTANTIATE_TEST_SUITE_P(
    GeoArrowConversion, GeoArrowToLogicalTypeTest,
    ::testing::Values(
        GeoArrowToLogicalTypeCase{R"({})", LogicalType::Geometry("srid:0")},
        GeoArrowToLogicalTypeCase{R"({"crs":"EPSG:4267"})",
                                  LogicalType::Geometry("EPSG:4267")},
        GeoArrowToLogicalTypeCase{
            R"({"crs":{"id":{"authority":"EPSG","code":3857}}})",
            LogicalType::Geometry(R"({"id":{"authority":"EPSG","code":3857}})")},
        GeoArrowToLogicalTypeCase{R"({"crs":"OGC:CRS84"})", LogicalType::Geometry()},
        GeoArrowToLogicalTypeCase{R"({"crs":"EPSG:4326"})", LogicalType::Geometry()},
        GeoArrowToLogicalTypeCase{R"({"crs":{"id":{"authority":"EPSG","code":4326}}})",
                                  LogicalType::Geometry()},
        GeoArrowToLogicalTypeCase{R"({"crs":{"id":{"authority":"EPSG","code":"4326"}}})",
                                  LogicalType::Geometry()},
        GeoArrowToLogicalTypeCase{R"({"crs":{"id":{"authority":"OGC","code":"CRS84"}}})",
                                  LogicalType::Geometry()},
        GeoArrowToLogicalTypeCase{R"({"edges":"spherical"})",
                                  LogicalType::Geography("srid:0")},
        GeoArrowToLogicalTypeCase{R"({"crs":"OGC:CRS84","edges":"spherical"})",
                                  LogicalType::Geography()},
        GeoArrowToLogicalTypeCase{
            R"({"crs":"OGC:CRS84","edges":"karney"})",
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::KARNEY)},
        GeoArrowToLogicalTypeCase{
            R"({"crs":"OGC:CRS84","edges":"vincenty"})",
            LogicalType::Geography("",
                                   LogicalType::EdgeInterpolationAlgorithm::VINCENTY)},
        GeoArrowToLogicalTypeCase{
            R"({"crs":"OGC:CRS84","edges":"andoyer"})",
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::ANDOYER)},
        GeoArrowToLogicalTypeCase{
            R"({"crs":"OGC:CRS84","edges":"thomas"})",
            LogicalType::Geography("", LogicalType::EdgeInterpolationAlgorithm::THOMAS)},
        GeoArrowToLogicalTypeCase{
            R"({"crs":"EPSG:4267","edges":"karney"})",
            LogicalType::Geography("EPSG:4267",
                                   LogicalType::EdgeInterpolationAlgorithm::KARNEY)},
        GeoArrowToLogicalTypeCase{
            R"({"crs":{"id":{"authority":"EPSG","code":4267}},"edges":"spherical"})",
            LogicalType::Geography(R"({"id":{"authority":"EPSG","code":4267}})")}));

TEST(UtilJsonInternal, InvalidProjJsonIsEscaped) {
  ::arrow::ExtensionTypeGuard guard(test::geoarrow_wkb());

  auto metadata = ::arrow::key_value_metadata(
      {"proj"}, {R"({"a":[1,2,]})"});  // Invalid JSON (trailing comma)

  auto logical_type = LogicalType::Geometry("projjson:proj");

  ASSERT_OK_AND_ASSIGN(
      auto type, GeoArrowTypeFromLogicalType(*logical_type, metadata, ::arrow::binary()));

  auto extension = std::dynamic_pointer_cast<::arrow::ExtensionType>(type);
  ASSERT_NE(extension, nullptr);

  ASSERT_OK_AND_ASSIGN(auto actual,
                       ::arrow::internal::MinifyJson(extension->Serialize()));

  EXPECT_EQ(actual, "{\"crs\":\"{\\\"a\\\":[1,2,]}\",\"crs_type\":\"projjson\"}");
}

TEST(UtilJsonInternal, EscapedCrsKeyIsRecognized) {
  std::string metadata = R"({"cr\u0073":"EPSG:3857","crs_type":"authority_code"})";

  ASSERT_OK_AND_ASSIGN(auto logical_type, LogicalTypeFromGeoArrowMetadata(metadata));

  ASSERT_EQ(logical_type->ToString(), "Geometry(crs=EPSG:3857)");
}

TEST(UtilJsonInternal, InvalidTrailingMetadataIsRejected) {
  auto result = LogicalTypeFromGeoArrowMetadata(
      R"({"crs":"EPSG:3857","edges":"planar","unused":[1,2,]})");

  ASSERT_RAISES(Invalid, result);
}

}  // namespace parquet
