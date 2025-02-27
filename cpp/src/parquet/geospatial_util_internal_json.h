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

}  // namespace parquet
