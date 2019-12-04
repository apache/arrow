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

#include <memory>
#include <string>

#include "arrow/c/abi.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \brief Export C++ array using the C data interface format.
///
/// The array is considered to have empty name and metadata.
/// The resulting ArrowArray struct keeps the array data and buffers alive
/// until its release callback is called by the consumer.
///
/// \param[in] array Array object to export
/// \param[out] out C struct where to export the array
ARROW_EXPORT
Status ExportArray(const Array& array, struct ArrowArray* out);

/// \brief Export C++ array using the C data interface format.
///
/// The field argument specifies the array name and metadata.
/// The resulting ArrowArray struct keeps the array data and buffers alive
/// until its release callback is called by the consumer.
///
/// \param[in] field Field object holding top-level name and metadata
/// \param[in] array Array object to export
/// \param[out] out C struct where to export the array
ARROW_EXPORT
Status ExportArray(const Field& field, const Array& array, struct ArrowArray* out);

/// \brief Export C++ record batch using the C data interface format.
///
/// The record batch is exported as if it were a struct array, but with
/// additional top-level metadata.
/// The resulting ArrowArray struct keeps the record batch data and buffers alive
/// until its release callback is called by the consumer.
///
/// \param[in] batch Record batch to export
/// \param[out] out C struct where to export the record batch
ARROW_EXPORT
Status ExportRecordBatch(const RecordBatch& batch, struct ArrowArray* out);

/// \brief Import C++ array from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting array.
///
/// \param[in,out] array C data interface struct holding the array data
/// \return Imported array object
ARROW_EXPORT
Result<std::shared_ptr<Array>> ImportArray(struct ArrowArray* array);

/// \brief Import C++ array and field from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting array.
/// In addition, a Field object is created to represent the top-level array
/// name, type and metadata.
///
/// \param[in,out] array C data interface struct holding the array data
/// \param[out] out_field Imported field object
/// \param[out] out_array Imported array object
ARROW_EXPORT
Status ImportArray(struct ArrowArray* array, std::shared_ptr<Field>* out_field,
                   std::shared_ptr<Array>* out_array);

/// \brief Import C++ record batch from the C data interface.
///
/// The array type represented by the ArrowArray struct must be a struct type
/// array.  The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting record batch.
///
/// \param[in,out] array C data interface struct holding the record batch data
/// \return Imported record batch object
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ImportRecordBatch(struct ArrowArray* array);

}  // namespace arrow
