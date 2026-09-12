// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "arrow/array.h"  // IWYU pragma: export
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "parquet/platform.h"

namespace parquet::variant::internal {

PARQUET_EXPORT
std::string_view BinaryFieldView(const ::arrow::Array& array, int64_t row);

std::string_view StringFieldView(const ::arrow::Array& array, int64_t row);

PARQUET_EXPORT
std::shared_ptr<::arrow::Array> ValuesArray(const ::arrow::Array& array);

std::pair<int64_t, int64_t> ValuesRangeAt(const ::arrow::Array& array, int64_t row);

std::shared_ptr<::arrow::Buffer> FinishNullBitmap(
    ::arrow::TypedBufferBuilder<bool>& builder);

std::shared_ptr<::arrow::Buffer> NullBitmapForOutput(const ::arrow::Array& array,
                                                     ::arrow::MemoryPool* pool);

}  // namespace parquet::variant::internal
