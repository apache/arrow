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

#include "arrow/buffer.h"

#include "MatlabDataArray.hpp"

namespace arrow::matlab::bit {
::matlab::data::TypedArray<bool> unpack(
    const std::shared_ptr<arrow::Buffer>& packed_buffer, int64_t length,
    int64_t start_offset);
const uint8_t* extract_ptr(
    const ::matlab::data::TypedArray<bool>& unpacked_validity_bitmap);
}  // namespace arrow::matlab::bit
