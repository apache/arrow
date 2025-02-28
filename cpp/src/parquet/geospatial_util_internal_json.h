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

#pragma once

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

/// \brief Compute a suitable DataType into which a GEOMETRY or GEOGRAPHY type should be
/// read
///
/// The result of this function depends on whether or not "geoarrow.wkb" has been
/// registered: if it has, the result will be the registered ExtensionType; if it has not,
/// the result will be binary().
::arrow::Result<std::shared_ptr<::arrow::DataType>> MakeGeoArrowGeometryType(
    const LogicalType& logical_type,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata);

/// \brief The default GeoCrsContext, which writes any PROJJSON coordinate reference
/// system values it encounters to the file metadata
class PARQUET_EXPORT FileGeoCrsContext : public GeoCrsContext {
 public:
  FileGeoCrsContext() { Clear(); }

  void Clear() override {
    projjson_crs_fields_ = ::arrow::KeyValueMetadata::Make({}, {});
  }

  std::string GetParquetCrs(std::string crs_value,
                            const std::string& crs_encoding) override;

  bool HasProjjsonCrsFields() override { return projjson_crs_fields_->size() > 0; }

  void AddProjjsonCrsFieldsToFileMetadata(::arrow::KeyValueMetadata* metadata) override;

 private:
  std::shared_ptr<::arrow::KeyValueMetadata> projjson_crs_fields_;
};

}  // namespace parquet
