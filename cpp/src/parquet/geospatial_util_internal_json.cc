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

#include <string>

#include "arrow/config.h"

#ifdef ARROW_JSON
#  include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#  include <rapidjson/document.h>
#  include <rapidjson/writer.h>
#endif

#include "arrow/result.h"
#include "parquet/properties.h"
#include "parquet/types.h"

namespace parquet {

#ifdef ARROW_JSON
namespace {
::arrow::Result<std::string> GeospatialGeoArrowCrsToParquetCrs(
    const ::arrow::rapidjson::Document& document,
    const ArrowWriterProperties& arrow_properties) {
  namespace rj = ::arrow::rapidjson;

  std::string crs_type;
  if (document.HasMember("crs_type")) {
    crs_type = document["crs_type"].GetString();
  }

  if (!document.HasMember("crs") || document["crs"].IsNull()) {
    // Parquet GEOMETRY/GEOGRAPHY do not have a concept of a null/missing
    // CRS, but an omitted one is more likely to have meant "lon/lat" than
    // a truly unspecified one (i.e., Engineering CRS with arbitrary XY units)
    return "";
  }

  const auto& json_crs = document["crs"];
  if (json_crs.IsString() && crs_type == "srid") {
    // srid is an application-specific identifier. GeoArrow lets this be propagated via
    // "crs_type": "srid".
    return std::string("srid:") + json_crs.GetString();
  } else if (json_crs.IsString() &&
             (json_crs == "EPSG:4326" || json_crs == "OGC:CRS84")) {
    // crs can be left empty because these cases both correspond to
    // longitude/latitude in WGS84 according to the Parquet specification
    return "";
  } else if (json_crs.IsObject()) {
    if (json_crs.HasMember("id")) {
      const auto& identifier = json_crs["id"];
      if (identifier.HasMember("authority") && identifier.HasMember("code")) {
        if (identifier["authority"] == "OGC" && identifier["code"] == "CRS84") {
          // longitude/latitude
          return "";
        } else if (identifier["authority"] == "EPSG" && identifier["code"] == 4326) {
          // longitude/latitude
          return "";
        }
      }
    }

    // TODO(paleolimbot) this is not quite correct because we're supposed to put this
    // in the metadata according to the spec. I can't find a good way to get a mutable
    // reference to the global metadata here yet.
    rj::StringBuffer buffer;
    rj::Writer<rj::StringBuffer> writer(buffer);
    json_crs.Accept(writer);
    return std::string("projjson:") + buffer.GetString();
  } else {
    // e.g., authority:code, WKT2, arbitrary string. A pluggable CrsProvider
    // could handle these and return something we're allowed to write here.
    return ::arrow::Status::SerializationError("Unsupported GeoArrow CRS for Parquet");
  }
}

}  // namespace

::arrow::Result<std::shared_ptr<const LogicalType>> GeospatialLogicalTypeFromGeoArrowJSON(
    const std::string& serialized_data, const ArrowWriterProperties& arrow_properties) {
  // Parquet has no way to interpret a null or missing CRS, so we choose the most likely
  // intent here (that the user meant to use the default Parquet CRS)
  if (serialized_data.empty() || serialized_data == "{}") {
    return LogicalType::Geometry();
  }

  namespace rj = ::arrow::rapidjson;
  rj::Document document;
  if (document.Parse(serialized_data.data(), serialized_data.length()).HasParseError()) {
    return ::arrow::Status::SerializationError("Invalid serialized JSON data: ",
                                               serialized_data);
  }

  ARROW_ASSIGN_OR_RAISE(std::string crs,
                        GeospatialGeoArrowCrsToParquetCrs(document, arrow_properties));

  if (document.HasMember("edges") && document["edges"] == "planar") {
    return LogicalType::Geometry(crs);
  } else if (document.HasMember("edges") && document["edges"] == "spherical") {
    return LogicalType::Geography(crs,
                                  LogicalType::EdgeInterpolationAlgorithm::SPHERICAL);
  } else if (document.HasMember("edges")) {
    return ::arrow::Status::SerializationError("Unsupported GeoArrow edge type: ",
                                               serialized_data);
  }

  return LogicalType::Geometry(crs);
}
#else
::arrow::Result<std::shared_ptr<const LogicalType>> GeospatialLogicalTypeFromGeoArrowJSON(
    const std::string& serialized_data, const ArrowWriterProperties& arrow_properties) {
  return ::arrow::Status::NotImplemented(
      "GeospatialLogicalTypeFromGeoArrowJSON requires ARROW_JSON");
}
#endif

}  // namespace parquet
