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

namespace parquet::variant {

/// Validate Variant arrays using strict writer rules or read-compatible rules.
/// The non-strict mode accepts invalid encodings retained in parquet-testing for
/// reader compatibility and must not be used to validate values for writing.
template <bool strict>
PARQUET_EXPORT void ValidateVariants(
    const ::arrow::ChunkedArray& data,
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

}  // namespace parquet::variant
