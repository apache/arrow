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

#include <string>

#include "arrow/extension_type.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/result.h"
#include "arrow/util/string.h"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

namespace {
::arrow::Result<std::string> GeospatialGeoArrowCrsToParquetCrs(
    const ::arrow::rapidjson::Document& document) {
  namespace rj = ::arrow::rapidjson;

  if (!document.HasMember("crs") || document["crs"].IsNull()) {
    // Parquet GEOMETRY/GEOGRAPHY do not have a concept of a null/missing
    // CRS, but an omitted one is more likely to have meant "lon/lat" than
    // a truly unspecified one (i.e., Engineering CRS with arbitrary XY units)
    return "";
  }

  const auto& json_crs = document["crs"];
  if (json_crs.IsString() && (json_crs == "EPSG:4326" || json_crs == "OGC:CRS84")) {
    // crs can be left empty because these cases both correspond to
    // longitude/latitude in WGS84 according to the Parquet specification
    return "";
  } else if (json_crs.IsObject()) {
    // Attempt to detect common PROJJSON representations of longitude/latitude and return
    // an empty crs to maximize compatibility with readers that do not implement CRS
    // support. PROJJSON stores this in the "id" member like:
    // {..., "id": {"authority": "...", "code": "..."}}
    if (json_crs.HasMember("id")) {
      const auto& identifier = json_crs["id"];
      if (identifier.HasMember("authority") && identifier.HasMember("code")) {
        if (identifier["authority"] == "OGC" && identifier["code"] == "CRS84") {
          return "";
        } else if (identifier["authority"] == "EPSG" && identifier["code"] == "4326") {
          return "";
        } else if (identifier["authority"] == "EPSG" && identifier["code"].IsInt() &&
                   identifier["code"].GetInt() == 4326) {
          return "";
        }
      }
    }
  }

  // If we could not detect a longitude/latitude CRS, just write the string to the
  // LogicalType crs (being sure to unescape a JSON string into a regular string)
  if (json_crs.IsString()) {
    return json_crs.GetString();
  } else {
    rj::StringBuffer buffer;
    rj::Writer<rj::StringBuffer> writer(buffer);
    json_crs.Accept(writer);
    return buffer.GetString();
  }
}

// Utility for ensuring that a Parquet CRS is valid JSON when written to
// GeoArrow metadata (without escaping it if it is already valid JSON such as
// a PROJJSON string)
std::string EscapeCrsAsJsonIfRequired(std::string_view crs);

::arrow::Result<std::string> MakeGeoArrowCrsMetadata(
    std::string_view crs,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  const std::string kSridPrefix{"srid:"};
  const std::string kProjjsonPrefix{"projjson:"};

  // Two recommendations are explicitly mentioned in the Parquet format for the
  // LogicalType crs:
  //
  // - "srid:XXXX" as a way to encode an application-specific integer identifier
  // - "projjson:some_field_name" as a way to avoid repeating PROJJSON strings
  //   unnecessarily (with a suggestion to place them in the file metadata)
  //
  // While we don't currently generate those values to reduce the complexity
  // of the writer, we do interpret these values according to the suggestion in
  // the format and pass on this information to GeoArrow.
  if (crs.empty()) {
    return R"("crs": "OGC:CRS84", "crs_type": "authority_code")";
  } else if (::arrow::internal::StartsWith(crs, kSridPrefix)) {
    return R"("crs": ")" + std::string(crs.substr(kSridPrefix.size())) +
           R"(", "crs_type": "srid")";
  } else if (::arrow::internal::StartsWith(crs, kProjjsonPrefix)) {
    std::string_view metadata_field = crs.substr(kProjjsonPrefix.size());
    if (metadata && metadata->Contains(metadata_field)) {
      ARROW_ASSIGN_OR_RAISE(std::string projjson_value, metadata->Get(metadata_field));
      // This value should be valid JSON, but if it is not, we escape it as a string such
      // that it can be inspected by the consumer of GeoArrow.
      return R"("crs": )" + EscapeCrsAsJsonIfRequired(projjson_value) +
             R"(, "crs_type": "projjson")";
    }
  }

  // Pass on the string directly to GeoArrow. If the string is already valid JSON,
  // insert it directly into GeoArrow's "crs" field. Otherwise, escape it and pass it as a
  // string value.
  return R"("crs": )" + EscapeCrsAsJsonIfRequired(crs);
}

std::string EscapeCrsAsJsonIfRequired(std::string_view crs) {
  namespace rj = ::arrow::rapidjson;
  rj::Document document;
  if (document.Parse(crs.data(), crs.length()).HasParseError()) {
    rj::StringBuffer buffer;
    rj::Writer<rj::StringBuffer> writer(buffer);
    rj::Value v;
    v.SetString(crs.data(), static_cast<int32_t>(crs.size()));
    v.Accept(writer);
    return std::string(buffer.GetString());
  } else {
    return std::string(crs);
  }
}

}  // namespace

::arrow::Result<std::shared_ptr<const LogicalType>> LogicalTypeFromGeoArrowMetadata(
    std::string_view serialized_data) {
  // Parquet has no way to interpret a null or missing CRS, so we choose the most likely
  // intent here (that the user meant to use the default Parquet CRS)
  if (serialized_data.empty() || serialized_data == "{}") {
    return LogicalType::Geometry();
  }

  namespace rj = ::arrow::rapidjson;
  rj::Document document;
  if (document.Parse(serialized_data.data(), serialized_data.length()).HasParseError()) {
    return ::arrow::Status::Invalid("Invalid serialized JSON data: ", serialized_data);
  }

  ARROW_ASSIGN_OR_RAISE(std::string crs, GeospatialGeoArrowCrsToParquetCrs(document));

  if (document.HasMember("edges") && document["edges"] == "planar") {
    return LogicalType::Geometry(crs);
  } else if (document.HasMember("edges") && document["edges"] == "spherical") {
    return LogicalType::Geography(crs,
                                  LogicalType::EdgeInterpolationAlgorithm::SPHERICAL);
  } else if (document.HasMember("edges")) {
    return ::arrow::Status::Invalid("Unsupported GeoArrow edge type: ", serialized_data);
  }

  return LogicalType::Geometry(crs);
}

::arrow::Result<std::shared_ptr<::arrow::DataType>> GeoArrowTypeFromLogicalType(
    const LogicalType& logical_type,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata,
    const std::shared_ptr<::arrow::DataType>& storage_type) {
  // Check if we have a registered GeoArrow type to read into
  std::shared_ptr<::arrow::ExtensionType> maybe_geoarrow_wkb =
      ::arrow::GetExtensionType("geoarrow.wkb");
  if (!maybe_geoarrow_wkb) {
    return storage_type;
  }

  if (logical_type.is_geometry()) {
    const auto& geospatial_type =
        ::arrow::internal::checked_cast<const GeometryLogicalType&>(logical_type);
    ARROW_ASSIGN_OR_RAISE(std::string crs_metadata,
                          MakeGeoArrowCrsMetadata(geospatial_type.crs(), metadata));

    std::string serialized_data = std::string("{") + crs_metadata + "}";
    return maybe_geoarrow_wkb->Deserialize(storage_type, serialized_data);
  } else if (logical_type.is_geography()) {
    const auto& geospatial_type =
        ::arrow::internal::checked_cast<const GeographyLogicalType&>(logical_type);
    ARROW_ASSIGN_OR_RAISE(std::string crs_metadata,
                          MakeGeoArrowCrsMetadata(geospatial_type.crs(), metadata));
    std::string edges_metadata =
        R"("edges": ")" + std::string(geospatial_type.algorithm_name()) + R"(")";
    std::string serialized_data =
        std::string("{") + crs_metadata + ", " + edges_metadata + "}";
    return maybe_geoarrow_wkb->Deserialize(storage_type, serialized_data);
  } else {
    throw ParquetException("Can't export logical type ", logical_type.ToString(),
                           " as GeoArrow");
  }
}

}  // namespace parquet
