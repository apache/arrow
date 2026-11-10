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

#include <gtest/gtest.h>

#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/simdjson_internal.h"

#include "parquet/test_util.h"

namespace parquet {

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
