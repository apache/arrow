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

#include <cmath> // std::ceil

#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_generate.h>

#include "arrow/matlab/bit/bit_pack_matlab_logical_array.h"

namespace arrow::matlab::bit {

    // Calculate the number of bytes required in the bit-packed validity buffer.
    int64_t bitPackedLength(int64_t num_elements) {
        // Since MATLAB logical values are encoded using a full byte (8 bits),
        // we can divide the number of elements in the logical array by 8 to get
        // the bit packed length.
        return static_cast<int64_t>(std::ceil(num_elements / 8.0));
    }

    // Pack an unpacked MATLAB logical array into into a bit-packed arrow::Buffer.
    arrow::Result<std::shared_ptr<arrow::Buffer>> bitPackMatlabLogicalArray(const ::matlab::data::TypedArray<bool> matlab_logical_array) {
        // Validate that the input arrow::Buffer has sufficient size to store a full bit-packed
        // representation of the input MATLAB logical array.
        const auto unpacked_buffer_length = matlab_logical_array.getNumberOfElements();

        // Compute the bit packed length from the unpacked length.
        const auto packed_buffer_length = bitPackedLength(unpacked_buffer_length);

        ARROW_ASSIGN_OR_RAISE(auto packed_validity_bitmap_buffer,  arrow::AllocateResizableBuffer(packed_buffer_length));

        // Get pointers to the internal uint8_t arrays behind arrow::Buffer and mxArray
        // Get raw bool array pointer from MATLAB logical array.
        // Get an iterator to the raw bool data behind the MATLAB logical array.
        auto unpacked_bool_data_iterator = matlab_logical_array.cbegin();

        // Iterate over the mxLogical array and write bit-packed bools to the arrow::Buffer.
        // Call into a loop-unrolled Arrow utility for better performance when bit-packing.
        auto generator = [&]() -> bool { return *(unpacked_bool_data_iterator++); };
        const int64_t start_offset = 0;

        auto mutable_data = packed_validity_bitmap_buffer->mutable_data();

        arrow::internal::GenerateBitsUnrolled(mutable_data, start_offset, unpacked_buffer_length, generator);

        return packed_validity_bitmap_buffer;
    }

}
