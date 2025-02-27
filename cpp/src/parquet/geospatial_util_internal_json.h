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

#include "arrow/util/key_value_metadata.h"

#include "parquet/properties.h"
#include "parquet/types.h"

namespace parquet {

/// \brief Compute a Parquet Logical type (Geometry(...) or Geography(...)) from
/// serialized GeoArrow metadata
///
/// Returns the appropriate LogicalType, SerializationError if the metadata was invalid,
/// or NotImplemented if Parquet was not built with ARROW_JSON.
::arrow::Result<std::shared_ptr<const LogicalType>> GeospatialLogicalTypeFromGeoArrowJSON(
    const std::string& serialized_data, const ArrowWriterProperties& arrow_properties);

// TODO(paleolimbot): This is required to write example files that correctly set
// an input type with a PROJJSON CRS as crs=projjson:some_field_name and add
// the actual definition to the global metadata. I am not sure if this is the
// best final way to do this but there is some precedent for a pointer to mutable
// state in the WriterProperties with the MemoryPool and Executor.
class GeoCrsContext {
 public:
  GeoCrsContext() : projjson_crs_fields_(::arrow::KeyValueMetadata::Make({}, {})) {}

  /// \brief Given a coordinate reference system value and encoding from GeoArrow
  /// extension metadata, return the value that should be placed in the
  /// LogicalType::Geography|Geometry(crs=) field
  ///
  /// For PROJJSON Crses (the most common way coordinate reference systems arrive
  /// in GeoArrow), the Parquet specification forces us to write them to the file
  /// metadata. This GeoCrsContext will record such values and return the required
  /// string that can be placed into the 'crs' of the Geometry or Geography logical
  /// type.
  std::string GetParquetCrs(std::string crs_value, std::string crs_encoding) {
    if (crs_encoding == "srid") {
      return crs_encoding + ":" + crs_value;
    } else if (crs_encoding == "projjson") {
      std::string key =
          "projjson_crs_value_" + std::to_string(projjson_crs_fields_->size());
      projjson_crs_fields_->Append(key, std::move(crs_value));
      return "projjson:" + key;
    } else {
      throw ParquetException("Crs encoding '", crs_encoding,
                             "' is not suppored by GeoCrsContext");
    }
  }

  /// \brief Returns true if any converted CRS values were PROJJSON whose values are
  /// stored in this object
  bool HasProjjsonCrsFields() { return projjson_crs_fields_->size() > 0; }

  /// \brief Add any stored PROJJSON values to the supplied KeyValueMetadata
  void AddProjjsonCrsFieldsToMetadata(::arrow::KeyValueMetadata* metadata) {
    if (HasProjjsonCrsFields()) {
      for (int64_t i = 0; i < projjson_crs_fields_->size(); i++) {
        metadata->Append(projjson_crs_fields_->key(i), projjson_crs_fields_->value(i));
      }
    }
  }

 private:
  std::shared_ptr<::arrow::KeyValueMetadata> projjson_crs_fields_;
};

}  // namespace parquet
