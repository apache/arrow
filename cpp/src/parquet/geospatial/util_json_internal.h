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

#include <string_view>

#include "arrow/util/key_value_metadata.h"

#include "parquet/types.h"

namespace parquet {

/// \brief Compute a Parquet Logical type (Geometry(...) or Geography(...)) from
/// GeoArrow `ARROW:extension:metadata` (JSON-encoded extension type metadata)
///
/// Returns the appropriate LogicalType or Invalid if the metadata was invalid.
::arrow::Result<std::shared_ptr<const LogicalType>> LogicalTypeFromGeoArrowMetadata(
    std::string_view serialized_data);

/// \brief Compute a suitable DataType into which a GEOMETRY or GEOGRAPHY type should be
/// read
///
/// The result of this function depends on whether or not "geoarrow.wkb" has been
/// registered: if it has, the result will be the registered ExtensionType; if it has not,
/// the result will be the given storage_type.
::arrow::Result<std::shared_ptr<::arrow::DataType>> GeoArrowTypeFromLogicalType(
    const LogicalType& logical_type,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata,
    const std::shared_ptr<::arrow::DataType>& storage_type);

}  // namespace parquet
