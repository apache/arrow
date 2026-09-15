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

#include "arrow/type_fwd.h"
#include "parquet/platform.h"

namespace arrow::extension {
class VariantArray;
}  // namespace arrow::extension

namespace parquet::variant {

/// Shred an unshredded Variant array using a logical Arrow target type.
///
/// Supported targets are Variant primitives, non-empty structs, and list, large-list,
/// list-view, large-list-view, or fixed-size-list types. The logical target is
/// recursively compiled into the required Variant field groups. Values that cannot be
/// safely converted to the requested primitive type remain encoded in the residual
/// value column. Successful conversions are materialized as the target type and may
/// therefore change the encoded Variant representation after unshredding.
///
/// Input metadata and parent SQL nulls are preserved. The metadata child may be reused,
/// but the output always has a deterministic shredded schema containing value and
/// typed_value, including for empty and all-parent-null inputs. The input must contain
/// metadata and value fields and must not already contain typed_value. Invalid targets,
/// invalid visible Variant data, and failures that cannot be represented as residual
/// values raise a Parquet exception.
PARQUET_EXPORT
std::shared_ptr<::arrow::extension::VariantArray> ShredVariantArray(
    const ::arrow::extension::VariantArray& array,
    const std::shared_ptr<::arrow::DataType>& typed_value_type,
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

}  // namespace parquet::variant
