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

#include "arrow/matlab/bit/unpack.h"

#include "arrow/util/bitmap_visit.h"

namespace arrow::matlab::bit {
    ::matlab::data::TypedArray<bool> unpack(const std::shared_ptr<arrow::Buffer>& packed_buffer, int64_t length, int64_t start_offset) {
        const auto packed_buffer_ptr = packed_buffer->data();

        ::matlab::data::ArrayFactory factory;
        
        const auto array_length = static_cast<size_t>(length);
        
        auto unpacked_buffer = factory.createBuffer<bool>(array_length);
        auto unpacked_buffer_ptr = unpacked_buffer.get();
        auto visitFcn = [&](const bool is_valid) { *unpacked_buffer_ptr++ = is_valid; };

        arrow::internal::VisitBitsUnrolled(packed_buffer_ptr, start_offset, length, visitFcn);

        ::matlab::data::TypedArray<bool> unpacked_matlab_logical_Array = factory.createArrayFromBuffer({array_length, 1}, std::move(unpacked_buffer));

        return unpacked_matlab_logical_Array;
    }

    const uint8_t* extract_ptr(const ::matlab::data::TypedArray<bool>& unpacked_validity_bitmap) {
        if (unpacked_validity_bitmap.getNumberOfElements() > 0) {
            const auto unpacked_validity_bitmap_iterator(unpacked_validity_bitmap.cbegin());
            return reinterpret_cast<const uint8_t*>(unpacked_validity_bitmap_iterator.operator->());
        } else {
            return nullptr;
        }
    }
}
