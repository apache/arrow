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

#include <simdjson.h>
#include <string>

#include "arrow/extension_type.h"
#include "arrow/json/json_writer_internal.h"
#include "arrow/result.h"
#include "arrow/util/simdjson_internal.h"
#include "arrow/util/string.h"

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

namespace {
::arrow::Result<std::string> GeospatialGeoArrowCrsToParquetCrs(
    simdjson::ondemand::object object) {
  auto crs_field = object["crs"];

  if (crs_field.error() == simdjson::NO_SUCH_FIELD) {
    // Parquet GEOMETRY/GEOGRAPHY do not have a concept of a null/missing
    // CRS, but an omitted one is more likely to have meant "lon/lat" than
    // a truly unspecified one (i.e., Engineering CRS with arbitrary XY units)
    return "";
  }

  ARROW_ASSIGN_OR_RAISE(auto json_crs, ::arrow::internal::ResolveSimdjsonResult(
                                           crs_field, "Failed to get 'crs' field: "));

  ARROW_ASSIGN_OR_RAISE(bool is_null, ::arrow::internal::IsJsonNull(json_crs));
  if (is_null) {
    return "";
  }

  auto crs_string_result = ::arrow::internal::GetJsonAs<std::string_view>(json_crs);

  if (crs_string_result.ok()) {
    auto crs_string = *crs_string_result;

    if (crs_string == "EPSG:4326" || crs_string == "OGC:CRS84") {
      return "";
    }

    // If we could not detect a longitude/latitude CRS, just write the string to the
    // LogicalType crs (being sure to unescape a JSON string into a regular string)
    return std::string(crs_string);
  }

  ARROW_ASSIGN_OR_RAISE(
      auto crs_object,
      ::arrow::internal::GetJsonAs<simdjson::ondemand::object>(json_crs));

  // Attempt to detect common PROJJSON representations of longitude/latitude and return
  // an empty crs to maximize compatibility with readers that do not implement CRS
  // support. PROJJSON stores this in the "id" member like:
  // {..., "id": {"authority": "...", "code": "..."}}
  auto id_field = crs_object["id"];

  if (id_field.error() != simdjson::NO_SUCH_FIELD) {
    ARROW_ASSIGN_OR_RAISE(auto identifier, ::arrow::internal::ResolveSimdjsonResult(
                                               id_field, "Failed to get 'id' field: "));

    auto authority_field = identifier["authority"];
    auto code_field = identifier["code"];

    if (authority_field.error() != simdjson::NO_SUCH_FIELD &&
        code_field.error() != simdjson::NO_SUCH_FIELD) {
      ARROW_ASSIGN_OR_RAISE(auto authority,
                            ::arrow::internal::ResolveSimdjsonResult(
                                authority_field, "Failed to get 'authority' field: "));

      ARROW_ASSIGN_OR_RAISE(auto code, ::arrow::internal::ResolveSimdjsonResult(
                                           code_field, "Failed to get 'code' field: "));

      ARROW_ASSIGN_OR_RAISE(auto authority_string,
                            ::arrow::internal::GetJsonAs<std::string_view>(authority));

      auto code_string_result = ::arrow::internal::GetJsonAs<std::string_view>(code);

      if (code_string_result.ok()) {
        auto code_string = *code_string_result;

        if ((authority_string == "OGC" && code_string == "CRS84") ||
            (authority_string == "EPSG" && code_string == "4326")) {
          return "";
        }
      } else if (authority_string == "EPSG") {
        auto code_int_result = ::arrow::internal::GetJsonAs<int64_t>(code);

        if (code_int_result.ok() && *code_int_result == 4326) {
          return "";
        }
      }
    }
  }

  // If we could not detect a longitude/latitude CRS, just write the string to the
  // LogicalType crs (being sure to unescape a JSON string into a regular string)
  RETURN_NOT_OK(::arrow::internal::ResolveSimdjsonResult(crs_object.reset(),
                                                         "Failed to reset 'crs' object: ")
                    .status());

  ARROW_ASSIGN_OR_RAISE(auto raw_crs,
                        ::arrow::internal::ResolveSimdjsonResult(
                            crs_object.raw_json(), "Failed to get raw 'crs' JSON: "));

  std::string minified(raw_crs.size(), '\0');
  size_t minified_len = 0;

  if (auto error =
          simdjson::minify(raw_crs.data(), raw_crs.size(), minified.data(), minified_len);
      error != simdjson::SUCCESS) {
    return ::arrow::Status::Invalid("Failed to minify CRS JSON: ",
                                    simdjson::error_message(error));
  }

  minified.resize(minified_len);
  return minified;
}

// Utility for ensuring that a Parquet CRS is valid JSON when written to
// GeoArrow metadata (without escaping it if it is already valid JSON such as
// a PROJJSON string)
::arrow::Result<std::string> EscapeCrsAsJsonIfRequired(std::string_view crs);

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
  } else if (crs.starts_with(kSridPrefix)) {
    return R"("crs": ")" + std::string(crs.substr(kSridPrefix.size())) +
           R"(", "crs_type": "srid")";
  } else if (crs.starts_with(kProjjsonPrefix)) {
    std::string_view metadata_field = crs.substr(kProjjsonPrefix.size());
    if (metadata && metadata->Contains(metadata_field)) {
      ARROW_ASSIGN_OR_RAISE(std::string projjson_value, metadata->Get(metadata_field));
      // This value should be valid JSON, but if it is not, we escape it as a string such
      // that it can be inspected by the consumer of GeoArrow.
      ARROW_ASSIGN_OR_RAISE(auto escaped, EscapeCrsAsJsonIfRequired(projjson_value));
      return R"("crs": )" + escaped + R"(, "crs_type": "projjson")";
    }
  }

  // Pass on the string directly to GeoArrow. If the string is already valid JSON,
  // insert it directly into GeoArrow's "crs" field. Otherwise, escape it and pass it as a
  // string value.
  ARROW_ASSIGN_OR_RAISE(auto escaped, EscapeCrsAsJsonIfRequired(crs));

  return R"("crs": )" + escaped;
}

::arrow::Result<std::string> EscapeCrsAsJsonIfRequired(std::string_view crs) {
  simdjson::ondemand::parser parser;
  simdjson::padded_string json(crs);

  if (parser.iterate(json).error() != simdjson::SUCCESS) {
    ::arrow::json::JsonWriter writer;
    writer.String(crs);

    ARROW_ASSIGN_OR_RAISE(auto escaped, writer.GetString());
    return std::string(escaped);
  }

  return std::string(crs);
}

}  // namespace

::arrow::Result<std::shared_ptr<const LogicalType>> LogicalTypeFromGeoArrowMetadata(
    std::string_view serialized_data) {
  // Parquet has no way to interpret a null or missing CRS, so we choose the most likely
  // intent here (that the user meant to use the default Parquet CRS)
  if (serialized_data.empty() || serialized_data == "{}") {
    return LogicalType::Geometry();
  }

  simdjson::ondemand::parser parser;
  simdjson::padded_string json(serialized_data);

  simdjson::ondemand::document document;
  if (auto error = parser.iterate(json).get(document); error != simdjson::SUCCESS) {
    return ::arrow::Status::Invalid("Invalid serialized JSON data: ", serialized_data);
  }

  ARROW_ASSIGN_OR_RAISE(auto object,
                        ::arrow::internal::ResolveSimdjsonResult(
                            document.get_object(), "Failed to get JSON object: "));

  ARROW_ASSIGN_OR_RAISE(std::string crs, GeospatialGeoArrowCrsToParquetCrs(object));

  auto edges_field = object["edges"];

  if (edges_field.error() == simdjson::NO_SUCH_FIELD) {
    return LogicalType::Geometry(crs);
  }

  ARROW_ASSIGN_OR_RAISE(auto edges, ::arrow::internal::ResolveSimdjsonResult(
                                        edges_field, "Failed to get 'edges' field: "));

  ARROW_ASSIGN_OR_RAISE(auto edges_string,
                        ::arrow::internal::GetJsonAs<std::string_view>(edges));

  if (edges_string == "planar") {
    return LogicalType::Geometry(crs);
  }

  if (edges_string == "spherical") {
    return LogicalType::Geography(crs,
                                  LogicalType::EdgeInterpolationAlgorithm::SPHERICAL);
  }

  return ::arrow::Status::Invalid("Unsupported GeoArrow edge type: ", serialized_data);
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
