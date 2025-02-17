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

#include "parquet/geospatial_util_internal_json.h"

#include <string>

#include "arrow/config.h"
#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/util/string.h"

#include "parquet/properties.h"
#include "parquet/types.h"
#include "parquet/xxhasher.h"

#ifdef ARROW_JSON
#  include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#  include <rapidjson/document.h>
#  include <rapidjson/writer.h>
#endif

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
    return arrow_properties.geo_crs_context()->GetParquetCrs(json_crs.GetString(),
                                                             "srid");
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

    // Use the GeoCrsContext in the ArrowWriterProperties to accumulate the PROJJSON
    // values and write them to the file metadata if needed
    rj::StringBuffer buffer;
    rj::Writer<rj::StringBuffer> writer(buffer);
    json_crs.Accept(writer);
    return arrow_properties.geo_crs_context()->GetParquetCrs(buffer.GetString(),
                                                             "projjson");
  } else {
    // e.g., authority:code, WKT2, arbitrary string that a suitably instrumented
    // GeoCrsContext may be able to handle.
    return arrow_properties.geo_crs_context()->GetParquetCrs(json_crs.GetString(),
                                                             "unknown");
  }
}

::arrow::Result<std::string> MakeGeoArrowCrsMetadata(
    const std::string& crs,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  const std::string kSridPrefix{"srid:"};
  const std::string kProjjsonPrefix{"projjson:"};

  if (crs.empty()) {
    return R"("crs": "OGC:CRS84", "crs_type": "authority_code")";
  } else if (::arrow::internal::StartsWith(crs, kSridPrefix)) {
    return R"("crs": ")" + crs.substr(kSridPrefix.size()) + R"(", "crs_type": "srid")";
  } else if (::arrow::internal::StartsWith(crs, kProjjsonPrefix)) {
    std::string metadata_field = crs.substr(kProjjsonPrefix.size());
    if (metadata && metadata->Contains(metadata_field)) {
      ARROW_ASSIGN_OR_RAISE(std::string projjson_value, metadata->Get(metadata_field));
      return R"("crs": )" + projjson_value + R"(, "crs_type": "projjson")";
    } else {
      throw ParquetException("crs field '", metadata_field, "' not found in metadata");
    }
  } else {
    return ::arrow::Status::Invalid(
        "Can't convert invalid Parquet CRS string to GeoArrow: ", crs);
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

::arrow::Result<std::shared_ptr<::arrow::DataType>> MakeGeoArrowGeometryType(
    const LogicalType& logical_type,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  // Check if we have a registered GeoArrow type to read into
  std::shared_ptr<::arrow::ExtensionType> maybe_geoarrow_wkb =
      ::arrow::GetExtensionType("geoarrow.wkb");
  if (!maybe_geoarrow_wkb) {
    return ::arrow::binary();
  }

  if (logical_type.is_geometry()) {
    const auto& geospatial_type =
        ::arrow::internal::checked_cast<const GeometryLogicalType&>(logical_type);
    ARROW_ASSIGN_OR_RAISE(std::string crs_metadata,
                          MakeGeoArrowCrsMetadata(geospatial_type.crs(), metadata));

    std::string serialized_data = std::string("{") + crs_metadata + "}";
    return maybe_geoarrow_wkb->Deserialize(::arrow::binary(), serialized_data);
  } else if (logical_type.is_geography()) {
    const auto& geospatial_type =
        ::arrow::internal::checked_cast<const GeographyLogicalType&>(logical_type);
    ARROW_ASSIGN_OR_RAISE(std::string crs_metadata,
                          MakeGeoArrowCrsMetadata(geospatial_type.crs(), metadata));
    std::string edges_metadata =
        R"("edges": ")" + std::string(geospatial_type.algorithm_name()) + R"(")";
    std::string serialized_data =
        std::string("{") + crs_metadata + ", " + edges_metadata + "}";
    return maybe_geoarrow_wkb->Deserialize(::arrow::binary(), serialized_data);
  } else {
    throw ParquetException("Can't export logical type ", logical_type.ToString(),
                           " as GeoArrow");
  }
}

std::string FileGeoCrsContext::GetParquetCrs(std::string crs_value,
                                             const std::string& crs_encoding) {
  if (crs_encoding == "srid") {
    return crs_encoding + ":" + crs_value;
  } else if (crs_encoding == "projjson") {
    // Compute a hash of the crs value to generate a crs key that is unlikely
    // to collide with an existing metadata key (unless it was generated by
    // this function whilst writing recording identical crs)
    ByteArray crs_bytearray{crs_value};
    XxHasher hasher;
    uint64_t crs_hash_int = hasher.Hash(&crs_bytearray);

    std::string crs_hash;
    crs_hash.resize(128);
    auto res = std::to_chars(crs_hash.data(), crs_hash.data() + crs_hash.size(),
                             crs_hash_int, 16);
    crs_hash.resize(res.ptr - crs_hash.data());

    std::string key = "projjson_crs_value_" + crs_hash;
    if (!projjson_crs_fields_->Contains(key)) {
      projjson_crs_fields_->Append(key, std::move(crs_value));
    }

    return "projjson:" + key;
  } else {
    throw ParquetException("Crs encoding '", crs_encoding,
                           "' is not suppored by GeoCrsContext");
  }
}

void FileGeoCrsContext::AddProjjsonCrsFieldsToFileMetadata(
    ::arrow::KeyValueMetadata* metadata) {
  for (int64_t i = 0; i < projjson_crs_fields_->size(); i++) {
    // Don't append to the file metadata if the file metadata already contains the same
    // key (the key contains a hash of the crs value, so this should minimize the
    // accumulation of schema keys when reading/writing Parquet files with store_schema())
    const std::string& key = projjson_crs_fields_->key(i);
    if (!metadata->Contains(key)) {
      metadata->Append(key, projjson_crs_fields_->value(i));
    }
  }
}

}  // namespace parquet
